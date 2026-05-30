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
    / "dedicated_vps_x_enricher_requested_event_rehydrate_readiness_probe.py"
)

FAKE_DATABASE_URL = (
    "postgresql+psycopg://github_ai_catchbot_app:"
    "unit-db-password-x-readiness@127.0.0.1:5432/github_ai_catchbot"
)
FAKE_REDIS_URL = "redis://:unit-redis-password-x-readiness@127.0.0.1:6379/0"
FAKE_RUNTIME_PATH = "/etc/github-ai-catchbot/private-runtime.env"
FAKE_STREAM_ID = "1710000000000-0"
FAKE_DEDUPE_KEY = "private-x-readiness-dedupe-key"
FAKE_SOURCE_TEXT = "Sensitive source text must not be rendered"
FAKE_X_URL = "https://x.com/openai/status/1881234567890123456"
FAKE_X_BASE_URL = "https://api.x.com/2"
FAKE_X_TOKEN = "unit-x-bearer-token-readiness"
FAKE_EXCEPTION_TEXT = "database exception contained private text"


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
        event_row: dict[str, Any] | None,
        artifact_row: dict[str, Any] | None,
        memberships: list[dict[str, Any]],
        read_only_value: str = "on",
        missing_tables: set[str] | None = None,
        raise_on_event_load: bool = False,
    ) -> None:
        self.event_row = event_row
        self.artifact_row = artifact_row
        self.memberships = memberships
        self.read_only_value = read_only_value
        self.missing_tables = missing_tables or set()
        self.raise_on_event_load = raise_on_event_load
        self.statements: list[str] = []
        self.params: list[dict[str, Any]] = []
        self.write_statements: list[str] = []
        self.membership_queries: list[str] = []
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
        if normalized == _normalize(module.SELECT_EVENT_OUTBOX_BY_ID_QUERY):
            if self.raise_on_event_load:
                raise RuntimeError(FAKE_EXCEPTION_TEXT)
            if self.event_row is None:
                return FakeResult(rows=[])
            if str(self.event_row["event_id"]) == str(params["event_id"]):
                return FakeResult(rows=[self.event_row])
            return FakeResult(rows=[])
        if normalized == _normalize(module.SELECT_ARTIFACT_BY_ID_QUERY):
            if self.artifact_row is None:
                return FakeResult(rows=[])
            if str(self.artifact_row["artifact_id"]) == str(params["artifact_id"]):
                return FakeResult(rows=[self.artifact_row])
            return FakeResult(rows=[])
        if normalized == _normalize(module.COUNT_CANDIDATE_GROUP_ARTIFACT_MEMBERSHIP_QUERY):
            self.membership_queries.append("candidate_group")
            return FakeResult(scalar=self._count_membership(params))
        if normalized == _normalize(module.COUNT_ARTIFACT_ANY_CANDIDATE_GROUP_MEMBERSHIP_QUERY):
            self.membership_queries.append("artifact_any")
            artifact_id = str(params["artifact_id"])
            return FakeResult(
                scalar=sum(1 for row in self.memberships if str(row["artifact_id"]) == artifact_id)
            )
        if normalized.startswith(("INSERT ", "UPDATE ", "DELETE ")):
            self.write_statements.append(normalized)
            raise AssertionError(f"unexpected DB write: {statement}")

        raise AssertionError(f"unexpected SQL: {statement}")

    def _count_membership(self, params: dict[str, Any]) -> int:
        candidate_group_id = str(params["candidate_group_id"])
        artifact_id = str(params["artifact_id"])
        return sum(
            1
            for row in self.memberships
            if str(row["candidate_group_id"]) == candidate_group_id
            and str(row["artifact_id"]) == artifact_id
        )

    async def rollback(self) -> None:
        self.rolled_back = True

    async def close(self) -> None:
        self.closed = True


class FakeRedis:
    def __init__(self, entries: list[tuple[str, dict[str, Any]]] | None = None) -> None:
        self.entries = entries or []
        self.ping_calls = 0
        self.xlen_calls: list[str] = []
        self.xrange_calls: list[str] = []
        self.xrevrange_calls: list[str] = []
        self.xadd_calls: list[Any] = []
        self.xack_calls: list[Any] = []
        self.xgroup_create_calls: list[Any] = []
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
    ) -> list[tuple[str, dict[str, Any]]]:
        self.xrange_calls.append(name)
        return self.entries[:count]

    async def xrevrange(
        self,
        name: str,
        max: str = "+",
        min: str = "-",
        count: int | None = None,
    ) -> list[tuple[str, dict[str, Any]]]:
        self.xrevrange_calls.append(name)
        return list(reversed(self.entries))[:count]

    async def xadd(self, *args: Any, **kwargs: Any) -> None:
        self.xadd_calls.append((args, kwargs))
        raise AssertionError("xadd must not be called by readiness probe")

    async def xack(self, *args: Any, **kwargs: Any) -> None:
        self.xack_calls.append((args, kwargs))
        raise AssertionError("xack must not be called by readiness probe")

    async def xgroup_create(self, *args: Any, **kwargs: Any) -> None:
        self.xgroup_create_calls.append((args, kwargs))
        raise AssertionError("xgroup_create must not be called by readiness probe")

    async def close(self) -> None:
        self.closed = True


def _module():
    return importlib.import_module(
        "scripts.ops.dedicated_vps_x_enricher_requested_event_rehydrate_readiness_probe"
    )


def _normalize(statement: str) -> str:
    return " ".join(statement.strip().split())


def _runtime_env(
    _path: str | Path,
    *,
    include_token: bool = True,
    x_base_url: str = FAKE_X_BASE_URL,
) -> dict[str, str]:
    values = {
        "DATABASE_URL": FAKE_DATABASE_URL,
        "REDIS_URL": FAKE_REDIS_URL,
        "X_BASE_URL": x_base_url,
    }
    if include_token:
        values["X_BEARER_TOKEN"] = FAKE_X_TOKEN
    return values


def _thin_fields(
    *,
    event_id: UUID,
    root_object_type: str,
    root_object_id: UUID,
    stage_name: str = "enrich_x",
) -> dict[str, Any]:
    return {
        "job_id": str(event_id),
        "stage_name": stage_name,
        "root_object_type": root_object_type,
        "root_object_id": str(root_object_id),
        "idempotency_key": FAKE_DEDUPE_KEY,
        "pipeline_run_id": "",
        "not_before": "",
        "trigger_event_id": str(event_id),
    }


def _event_row(
    *,
    event_id: UUID,
    candidate_group_id: UUID,
    artifact_id: UUID,
    aggregate_type: str = "candidate_group",
    event_type: str = "artifact.enrich.requested.v1",
    status: str = "published",
    provider_route: str = "x",
    artifact_type: str = "x_post",
    include_candidate_group_payload: bool = True,
    payload_candidate_group_id: UUID | None = None,
    include_refresh_depth: bool = True,
    depth_budget: int = 1,
) -> dict[str, Any]:
    aggregate_id = candidate_group_id if aggregate_type == "candidate_group" else artifact_id
    payload: dict[str, Any] = {
        "artifact_id": str(artifact_id),
        "artifact_type": artifact_type,
        "provider_route": provider_route,
        "source_text": FAKE_SOURCE_TEXT,
        "canonical_url": FAKE_X_URL,
    }
    if include_candidate_group_payload:
        payload["candidate_group_id"] = str(payload_candidate_group_id or candidate_group_id)
    if include_refresh_depth:
        payload["refresh_mode"] = "standard"
        payload["depth_budget"] = depth_budget
    return {
        "event_id": event_id,
        "event_type": event_type,
        "aggregate_type": aggregate_type,
        "aggregate_id": aggregate_id,
        "dedupe_key": FAKE_DEDUPE_KEY,
        "payload_json": payload,
        "status": status,
        "created_at": datetime.now(timezone.utc),
        "published_at": datetime.now(timezone.utc),
    }


def _artifact_row(
    *,
    artifact_id: UUID,
    artifact_type: str = "x_post",
    canonical_id: str = "x:post:1881234567890123456",
) -> dict[str, Any]:
    return {
        "artifact_id": artifact_id,
        "artifact_type": artifact_type,
        "canonical_id": canonical_id,
        "canonical_url": FAKE_X_URL,
        "normalized_host": "x.com",
        "artifact_key_json": {"post_id": "1881234567890123456"},
        "current_snapshot_id": None,
        "current_status": None,
    }


def _membership(candidate_group_id: UUID, artifact_id: UUID) -> dict[str, Any]:
    return {
        "candidate_group_id": candidate_group_id,
        "artifact_id": artifact_id,
    }


def _valid_parts(*, aggregate_type: str = "candidate_group") -> dict[str, Any]:
    event_id = uuid4()
    candidate_group_id = uuid4()
    artifact_id = uuid4()
    event = _event_row(
        event_id=event_id,
        candidate_group_id=candidate_group_id,
        artifact_id=artifact_id,
        aggregate_type=aggregate_type,
        include_candidate_group_payload=aggregate_type == "candidate_group",
    )
    root_object_id = candidate_group_id if aggregate_type == "candidate_group" else artifact_id
    redis = FakeRedis(
        [
            (
                FAKE_STREAM_ID,
                _thin_fields(
                    event_id=event_id,
                    root_object_type=aggregate_type,
                    root_object_id=root_object_id,
                ),
            )
        ]
    )
    session = FakeSession(
        event_row=event,
        artifact_row=_artifact_row(artifact_id=artifact_id),
        memberships=[_membership(candidate_group_id, artifact_id)],
    )
    return {
        "event_id": event_id,
        "candidate_group_id": candidate_group_id,
        "artifact_id": artifact_id,
        "event": event,
        "session": session,
        "redis": redis,
    }


def _run_report(
    *,
    session: FakeSession | None = None,
    redis: FakeRedis | None = None,
    include_token: bool = True,
    x_base_url: str = FAKE_X_BASE_URL,
    side_effect_flags: dict[str, bool] | None = None,
) -> tuple[Any, FakeSession, FakeRedis]:
    parts = _valid_parts()
    fake_session = session or parts["session"]
    fake_redis = redis or parts["redis"]
    module = _module()
    result = module.generate_report(
        runtime_env_path=FAKE_RUNTIME_PATH,
        runtime_env_reader=lambda path: _runtime_env(
            path,
            include_token=include_token,
            x_base_url=x_base_url,
        ),
        database_session_factory=lambda _url: fake_session,
        redis_client_factory=lambda _url: fake_redis,
        side_effect_flags=side_effect_flags,
        forbidden_raw_values=(FAKE_EXCEPTION_TEXT,),
    )
    return result, fake_session, fake_redis


def _assert_no_mutation(result: Any, session: FakeSession, redis: FakeRedis) -> None:
    assert result.report["redis_ack_attempted"] is False
    assert result.report["redis_group_mutation_attempted"] is False
    assert result.report["redis_publish_attempted"] is False
    assert result.report["database_write_attempted"] is False
    assert result.report["external_network_attempted"] is False
    assert result.report["source_tables_mutation_performed"] is False
    assert result.report["telegram_raw_updates_mutation_performed"] is False
    assert result.report["registry_mutation_performed"] is False
    assert result.report["downstream_enricher_started"] is False
    assert result.report["evidence_assembler_started"] is False
    assert result.report["judge_policy_notifier_started"] is False
    assert result.report["docker_or_systemd_changed"] is False
    assert result.report["alembic_run"] is False
    assert session.write_statements == []
    assert redis.xadd_calls == []
    assert redis.xack_calls == []
    assert redis.xgroup_create_calls == []


def test_script_exists() -> None:
    assert SCRIPT.exists()


def test_default_readiness_succeeds_with_valid_fake_db_redis_and_token() -> None:
    result, session, redis = _run_report()

    assert result.exit_code == 0
    assert result.report["contract_status"] == _module().STATUS_READY
    assert result.report["runtime_env_read"] is True
    assert result.report["database_connected"] is True
    assert result.report["redis_connected"] is True
    assert result.report["read_only_transaction"] is True
    assert result.report["x_stream_exists"] is True
    assert result.report["x_stream_length_bucket"] == "one"
    assert result.report["target_stream_entry_found_bucket"] == "one"
    assert result.report["event_outbox_rehydrate_succeeded_bucket"] == "one"
    assert result.report["provider_route_x_bucket"] == "one"
    assert result.report["artifact_type_x_post_bucket"] == "one"
    assert result.report["artifact_registry_rehydrate_succeeded_bucket"] == "one"
    assert result.report["artifact_canonical_x_post_bucket"] == "one"
    assert result.report["candidate_membership_valid_bucket"] == "one"
    assert result.report["x_bearer_token_present_bucket"] == "present"
    assert result.report["x_base_url_valid_bucket"] == "valid"
    assert redis.xlen_calls == ["q.artifact.enrich.x"]
    assert session.rolled_back is True
    _assert_no_mutation(result, session, redis)


def test_missing_x_bearer_token_blocks_after_safe_rehydration_checks() -> None:
    result, session, redis = _run_report(include_token=False)

    assert result.exit_code == 1
    assert result.report["contract_status"] == _module().STATUS_MISSING_TOKEN
    assert result.report["event_outbox_rehydrate_succeeded_bucket"] == "one"
    assert result.report["artifact_registry_rehydrate_succeeded_bucket"] == "one"
    assert result.report["candidate_membership_valid_bucket"] == "one"
    assert result.report["x_bearer_token_present_bucket"] == "missing"
    _assert_no_mutation(result, session, redis)


def test_invalid_thin_payload_blocks_before_event_rehydrate() -> None:
    parts = _valid_parts()
    fields = dict(parts["redis"].entries[0][1])
    fields["payload_json"] = {"raw": "forbidden"}
    redis = FakeRedis([(FAKE_STREAM_ID, fields)])

    result, session, redis = _run_report(session=parts["session"], redis=redis)

    assert result.exit_code == 1
    assert result.report["contract_status"] == _module().STATUS_INVALID_THIN_PAYLOAD
    assert result.report["thin_payload_shape_valid_bucket"] == "zero"
    assert result.report["event_outbox_rehydrate_succeeded_bucket"] == "zero"
    _assert_no_mutation(result, session, redis)


def test_wrong_stage_blocks_as_invalid_thin_payload() -> None:
    parts = _valid_parts()
    fields = dict(parts["redis"].entries[0][1])
    fields["stage_name"] = "enrich_github"
    redis = FakeRedis([(FAKE_STREAM_ID, fields)])

    result, session, redis = _run_report(session=parts["session"], redis=redis)

    assert result.exit_code == 1
    assert result.report["contract_status"] == _module().STATUS_INVALID_THIN_PAYLOAD
    assert result.report["thin_payload_shape_valid_bucket"] == "one"
    assert result.report["thin_payload_stage_valid_bucket"] == "zero"
    assert "redis.stage_name_mismatch" in result.report["checks_failed"]
    _assert_no_mutation(result, session, redis)


def test_missing_event_outbox_row_blocks_event_rehydrate() -> None:
    parts = _valid_parts()
    session = FakeSession(
        event_row=None,
        artifact_row=parts["session"].artifact_row,
        memberships=parts["session"].memberships,
    )

    result, session, redis = _run_report(session=session, redis=parts["redis"])

    assert result.exit_code == 1
    assert result.report["contract_status"] == _module().STATUS_EVENT_REHYDRATE_FAILED
    assert result.report["target_stream_entry_found_bucket"] == "one"
    assert "event_outbox.row_missing" in result.report["checks_failed"]
    _assert_no_mutation(result, session, redis)


def test_event_type_mismatch_blocks() -> None:
    parts = _valid_parts()
    parts["event"]["event_type"] = "source_message.created.v1"

    result, session, redis = _run_report(session=parts["session"], redis=parts["redis"])

    assert result.exit_code == 1
    assert result.report["contract_status"] == _module().STATUS_INVALID_EVENT_CONTRACT
    assert "event_outbox.event_type" in result.report["checks_failed"]
    _assert_no_mutation(result, session, redis)


def test_event_status_not_published_blocks() -> None:
    parts = _valid_parts()
    parts["event"]["status"] = "pending"

    result, session, redis = _run_report(session=parts["session"], redis=parts["redis"])

    assert result.exit_code == 1
    assert result.report["contract_status"] == _module().STATUS_INVALID_EVENT_CONTRACT
    assert "event_outbox.status" in result.report["checks_failed"]
    _assert_no_mutation(result, session, redis)


def test_provider_route_not_x_blocks() -> None:
    parts = _valid_parts()
    parts["event"]["payload_json"]["provider_route"] = "github"

    result, session, redis = _run_report(session=parts["session"], redis=parts["redis"])

    assert result.exit_code == 1
    assert result.report["contract_status"] == _module().STATUS_INVALID_EVENT_CONTRACT
    assert result.report["provider_route_x_bucket"] == "zero"
    assert "payload.provider_route" in result.report["checks_failed"]
    _assert_no_mutation(result, session, redis)


def test_artifact_type_not_x_post_blocks() -> None:
    parts = _valid_parts()
    parts["event"]["payload_json"]["artifact_type"] = "github_repo"

    result, session, redis = _run_report(session=parts["session"], redis=parts["redis"])

    assert result.exit_code == 1
    assert result.report["contract_status"] == _module().STATUS_INVALID_EVENT_CONTRACT
    assert result.report["artifact_type_x_post_bucket"] == "zero"
    assert "payload.artifact_type" in result.report["checks_failed"]
    _assert_no_mutation(result, session, redis)


def test_artifact_registry_missing_blocks() -> None:
    parts = _valid_parts()
    session = FakeSession(
        event_row=parts["event"],
        artifact_row=None,
        memberships=parts["session"].memberships,
    )

    result, session, redis = _run_report(session=session, redis=parts["redis"])

    assert result.exit_code == 1
    assert result.report["contract_status"] == _module().STATUS_INVALID_ARTIFACT
    assert result.report["artifact_registry_rehydrate_succeeded_bucket"] == "zero"
    assert "artifact_registry.row_missing" in result.report["checks_failed"]
    _assert_no_mutation(result, session, redis)


def test_artifact_canonical_id_not_x_post_blocks() -> None:
    parts = _valid_parts()
    artifact = _artifact_row(
        artifact_id=parts["artifact_id"],
        canonical_id="github:repo:openai/example",
    )
    session = FakeSession(
        event_row=parts["event"],
        artifact_row=artifact,
        memberships=parts["session"].memberships,
    )

    result, session, redis = _run_report(session=session, redis=parts["redis"])

    assert result.exit_code == 1
    assert result.report["contract_status"] == _module().STATUS_INVALID_ARTIFACT
    assert result.report["artifact_canonical_x_post_bucket"] == "zero"
    assert "artifact_registry.canonical_id" in result.report["checks_failed"]
    _assert_no_mutation(result, session, redis)


def test_candidate_group_aggregate_membership_validation_succeeds() -> None:
    result, session, redis = _run_report()

    assert result.exit_code == 0
    assert result.report["candidate_membership_valid_bucket"] == "one"
    assert session.membership_queries == ["candidate_group"]
    _assert_no_mutation(result, session, redis)


def test_artifact_aggregate_membership_validation_succeeds() -> None:
    parts = _valid_parts(aggregate_type="artifact")

    result, session, redis = _run_report(session=parts["session"], redis=parts["redis"])

    assert result.exit_code == 0
    assert result.report["contract_status"] == _module().STATUS_READY
    assert result.report["candidate_membership_valid_bucket"] == "one"
    assert session.membership_queries == ["artifact_any"]
    _assert_no_mutation(result, session, redis)


def test_payload_candidate_group_id_mismatch_blocks() -> None:
    parts = _valid_parts()
    parts["event"]["payload_json"]["candidate_group_id"] = str(uuid4())

    result, session, redis = _run_report(session=parts["session"], redis=parts["redis"])

    assert result.exit_code == 1
    assert result.report["contract_status"] == _module().STATUS_INVALID_ARTIFACT
    assert "candidate.payload_candidate_group_mismatch" in result.report["checks_failed"]
    _assert_no_mutation(result, session, redis)


def test_no_stream_entry_blocks() -> None:
    parts = _valid_parts()
    redis = FakeRedis([])

    result, session, redis = _run_report(session=parts["session"], redis=redis)

    assert result.exit_code == 1
    assert result.report["contract_status"] == _module().STATUS_NO_STREAM_ENTRY
    assert result.report["x_stream_exists"] is False
    assert result.report["target_stream_entry_found_bucket"] == "zero"
    _assert_no_mutation(result, session, redis)


def test_forbidden_side_effect_flags_block_immediately() -> None:
    parts = _valid_parts()

    result, session, redis = _run_report(
        session=parts["session"],
        redis=parts["redis"],
        side_effect_flags={"external_network_attempted": True},
    )

    assert result.exit_code == 1
    assert result.report["contract_status"] == _module().STATUS_FORBIDDEN_SIDE_EFFECT
    assert "side_effect.forbidden" in result.report["checks_failed"]
    assert session.statements == []
    assert redis.ping_calls == 0


def test_missing_refresh_mode_and_depth_budget_default_safely() -> None:
    parts = _valid_parts()
    parts["event"]["payload_json"].pop("refresh_mode")
    parts["event"]["payload_json"].pop("depth_budget")

    result, session, redis = _run_report(session=parts["session"], redis=parts["redis"])

    assert result.exit_code == 0
    assert result.report["contract_status"] == _module().STATUS_READY
    _assert_no_mutation(result, session, redis)


def test_depth_budget_other_than_one_blocks() -> None:
    parts = _valid_parts()
    parts["event"]["payload_json"]["depth_budget"] = 2

    result, session, redis = _run_report(session=parts["session"], redis=parts["redis"])

    assert result.exit_code == 1
    assert result.report["contract_status"] == _module().STATUS_INVALID_EVENT_CONTRACT
    assert "payload.depth_budget" in result.report["checks_failed"]
    _assert_no_mutation(result, session, redis)


def test_invalid_x_base_url_blocks_after_rehydration() -> None:
    result, session, redis = _run_report(x_base_url="http://api.x.com/2")

    assert result.exit_code == 1
    assert result.report["contract_status"] == _module().STATUS_INVALID_EVENT_CONTRACT
    assert result.report["x_base_url_valid_bucket"] == "invalid"
    assert result.report["candidate_membership_valid_bucket"] == "one"
    assert "runtime.x_base_url" in result.report["checks_failed"]
    _assert_no_mutation(result, session, redis)


def test_raw_values_are_not_emitted_including_ids_urls_tokens_stream_or_exception_text() -> None:
    parts = _valid_parts()

    result, session, redis = _run_report(session=parts["session"], redis=parts["redis"])
    rendered = json.dumps(result.report, sort_keys=True)

    forbidden_values = (
        str(parts["event_id"]),
        str(parts["candidate_group_id"]),
        str(parts["artifact_id"]),
        FAKE_DEDUPE_KEY,
        FAKE_SOURCE_TEXT,
        FAKE_X_URL,
        FAKE_DATABASE_URL,
        FAKE_REDIS_URL,
        "unit-db-password-x-readiness",
        "unit-redis-password-x-readiness",
        FAKE_X_TOKEN,
        FAKE_STREAM_ID,
        FAKE_RUNTIME_PATH,
        FAKE_EXCEPTION_TEXT,
    )
    for value in forbidden_values:
        assert value not in rendered
    assert result.report["raw_values_emitted"] is False
    _assert_no_mutation(result, session, redis)


def test_exception_text_is_sanitized_when_event_rehydrate_raises() -> None:
    parts = _valid_parts()
    session = FakeSession(
        event_row=parts["event"],
        artifact_row=parts["session"].artifact_row,
        memberships=parts["session"].memberships,
        raise_on_event_load=True,
    )

    result, session, redis = _run_report(session=session, redis=parts["redis"])
    rendered = json.dumps(result.report, sort_keys=True)

    assert result.exit_code == 1
    assert result.report["contract_status"] == _module().STATUS_EVENT_REHYDRATE_FAILED
    assert FAKE_EXCEPTION_TEXT not in rendered
    _assert_no_mutation(result, session, redis)
