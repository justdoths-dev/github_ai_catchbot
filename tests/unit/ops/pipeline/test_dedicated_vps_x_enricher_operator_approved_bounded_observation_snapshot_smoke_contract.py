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
    / "dedicated_vps_x_enricher_operator_approved_bounded_observation_snapshot_smoke.py"
)

FAKE_DATABASE_URL = (
    "postgresql+psycopg://github_ai_catchbot_app:"
    "unit-db-password-x-observation@127.0.0.1:5432/github_ai_catchbot"
)
FAKE_REDIS_URL = "redis://:unit-redis-password-x-observation@127.0.0.1:6379/0"
FAKE_RUNTIME_PATH = "/etc/github-ai-catchbot/private-runtime.env"
FAKE_STREAM_ID = "1710000000000-0"
FAKE_DEDUPE_KEY = "private-x-observation-dedupe-key"
FAKE_SOURCE_TEXT = "Sensitive source text must not be rendered"
FAKE_X_URL = "https://x.com/openai/status/1881234567890123456"
FAKE_X_BASE_URL = "https://api.x.com"
FAKE_X_TOKEN = "unit-x-bearer-token-observation"
FAKE_POST_ID = "1881234567890123456"
FAKE_EDIT_ID = "1881234567890999999"
FAKE_RESPONSE_BODY = "raw upstream response body must not render"
FAKE_EXCEPTION_TEXT = "private exception text must not render"


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
        order: list[str] | None = None,
        read_only_value: str = "on",
        missing_tables: set[str] | None = None,
        fail_on: str | None = None,
    ) -> None:
        self.event_row = event_row
        self.artifact_row = artifact_row
        self.memberships = memberships
        self.order = order
        self.read_only_value = read_only_value
        self.missing_tables = missing_tables or set()
        self.fail_on = fail_on
        self.statements: list[str] = []
        self.params: list[dict[str, Any]] = []
        self.writes: list[str] = []
        self.discovered_observations: list[dict[str, Any]] = []
        self.snapshot_outbox_payloads: list[dict[str, Any]] = []
        self.committed = False
        self.rolled_back_count = 0
        self.closed = False
        self.run_id = uuid4()
        self.snapshot_id = uuid4()

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
            return FakeResult(scalar=self._count_membership(params))
        if normalized == _normalize(module.SELECT_ARTIFACT_CANDIDATE_MEMBERSHIPS_QUERY):
            artifact_id = str(params["artifact_id"])
            return FakeResult(
                rows=[
                    {"candidate_group_id": row["candidate_group_id"]}
                    for row in self.memberships
                    if str(row["artifact_id"]) == artifact_id
                ][:2]
            )
        if normalized == _normalize(module.INSERT_ENRICHMENT_RUN_QUERY):
            return self._write("insert_enrichment_run", "db:insert_run", self.run_id)
        if normalized == _normalize(module.INSERT_ARTIFACT_SNAPSHOT_QUERY):
            return self._write("insert_snapshot", "db:insert_snapshot", self.snapshot_id)
        if normalized == _normalize(module.UPSERT_ARTIFACT_SNAPSHOT_X_POST_QUERY):
            return self._write("upsert_x_post", "db:upsert_x_post", None)
        if normalized == _normalize(module.INSERT_DISCOVERED_URL_OBSERVATION_QUERY):
            self.discovered_observations.append(dict(params))
            return self._write("insert_discovered_url", "db:insert_discovered_url", None)
        if normalized == _normalize(module.UPDATE_ARTIFACT_REGISTRY_CURRENT_SNAPSHOT_QUERY):
            return self._write("update_registry_current_snapshot", "db:update_registry", None)
        if normalized == _normalize(module.INSERT_SNAPSHOT_UPDATED_OUTBOX_QUERY):
            self.snapshot_outbox_payloads.append(json.loads(params["payload_json"]))
            return self._write("insert_snapshot_updated_outbox", "db:insert_outbox", None)
        if normalized == _normalize(module.FINISH_ENRICHMENT_RUN_QUERY):
            return self._write("finish_enrichment_run", "db:finish_run", None)
        if normalized.startswith(("INSERT ", "UPDATE ", "DELETE ")):
            raise AssertionError(f"unexpected DB write: {statement}")
        raise AssertionError(f"unexpected SQL: {statement}")

    def _write(self, label: str, order_label: str, scalar: Any) -> FakeResult:
        self.writes.append(label)
        if self.order is not None:
            self.order.append(order_label)
        if self.fail_on == label:
            raise RuntimeError(f"{label} failed with {FAKE_EXCEPTION_TEXT}")
        return FakeResult(scalar=scalar)

    def _count_membership(self, params: dict[str, Any]) -> int:
        artifact_id = str(params["artifact_id"])
        candidate_group_id = str(params["candidate_group_id"])
        return sum(
            1
            for row in self.memberships
            if str(row["artifact_id"]) == artifact_id
            and str(row["candidate_group_id"]) == candidate_group_id
        )

    async def commit(self) -> None:
        if self.order is not None:
            self.order.append("db:commit")
        if self.fail_on == "commit":
            raise RuntimeError(f"commit failed with {FAKE_EXCEPTION_TEXT}")
        self.committed = True

    async def rollback(self) -> None:
        self.rolled_back_count += 1
        if self.order is not None:
            self.order.append("db:rollback")

    async def close(self) -> None:
        self.closed = True


class FakeRedis:
    def __init__(
        self,
        entries: list[tuple[str, dict[str, Any]]] | None = None,
        *,
        order: list[str] | None = None,
        delivered_entries: list[tuple[str, dict[str, Any]]] | None = None,
        fail_ack: bool = False,
    ) -> None:
        self.entries = entries or []
        self.order = order
        self.delivered_entries = delivered_entries
        self.fail_ack = fail_ack
        self.ping_calls = 0
        self.xlen_calls: list[str] = []
        self.xrange_calls: list[str] = []
        self.xrevrange_calls: list[str] = []
        self.xgroup_create_calls: list[tuple[str, str, str, bool]] = []
        self.xreadgroup_calls: list[tuple[str, str, dict[str, str], dict[str, Any]]] = []
        self.xack_calls: list[tuple[str, str, tuple[str, ...]]] = []
        self.xadd_calls: list[Any] = []
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

    async def xgroup_create(
        self,
        name: str,
        groupname: str,
        id: str = "$",
        mkstream: bool = False,
    ) -> None:
        self.xgroup_create_calls.append((name, groupname, id, mkstream))
        if self.order is not None:
            self.order.append("redis:xgroup_create")

    async def xreadgroup(
        self,
        groupname: str,
        consumername: str,
        streams: dict[str, str],
        count: int | None = None,
        block: int | None = None,
    ) -> list[tuple[str, list[tuple[str, dict[str, Any]]]]]:
        self.xreadgroup_calls.append(
            (groupname, consumername, dict(streams), {"count": count, "block": block})
        )
        if self.order is not None:
            self.order.append("redis:xreadgroup")
        entries = self.delivered_entries if self.delivered_entries is not None else self.entries[:1]
        return [(_module().EXPECTED_STREAM_NAME, entries)]

    async def xack(self, name: str, groupname: str, *ids: str) -> int:
        self.xack_calls.append((name, groupname, ids))
        if self.order is not None:
            self.order.append("redis:xack")
        if self.fail_ack:
            raise RuntimeError(f"ack failed with {FAKE_EXCEPTION_TEXT}")
        return 1

    async def xadd(self, *args: Any, **kwargs: Any) -> None:
        self.xadd_calls.append((args, kwargs))
        raise AssertionError("xadd must not be called by snapshot smoke")

    async def close(self) -> None:
        self.closed = True


class FakeXClient:
    def __init__(
        self,
        response: Any | None = None,
        *,
        order: list[str] | None = None,
        raises: bool = False,
    ) -> None:
        self.response = response
        self.order = order
        self.raises = raises
        self.calls: list[Any] = []
        self.closed = False

    async def get_post(self, plan: Any) -> Any:
        self.calls.append(plan)
        if self.order is not None:
            self.order.append("x:get_post")
        if self.raises:
            raise RuntimeError(FAKE_EXCEPTION_TEXT)
        return self.response or _x_response()

    async def close(self) -> None:
        self.closed = True


def _module():
    return importlib.import_module(
        "scripts.ops.dedicated_vps_x_enricher_operator_approved_bounded_observation_snapshot_smoke"
    )


def _normalize(statement: str) -> str:
    return " ".join(statement.strip().split())


def _runtime_env(_path: str | Path, *, include_token: bool = True) -> dict[str, str]:
    values = {
        "DATABASE_URL": FAKE_DATABASE_URL,
        "REDIS_URL": FAKE_REDIS_URL,
        "X_API_BASE_URL": FAKE_X_BASE_URL,
        "X_ENRICHER_REQUEST_TIMEOUT_SEC": "3",
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
    depth_budget: int = 1,
) -> dict[str, Any]:
    aggregate_id = candidate_group_id if aggregate_type == "candidate_group" else artifact_id
    payload: dict[str, Any] = {
        "artifact_id": str(artifact_id),
        "artifact_type": artifact_type,
        "provider_route": provider_route,
        "refresh_mode": "standard",
        "depth_budget": depth_budget,
        "source_text": FAKE_SOURCE_TEXT,
        "canonical_url": FAKE_X_URL,
    }
    if include_candidate_group_payload:
        payload["candidate_group_id"] = str(candidate_group_id)
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
    canonical_id: str = f"x:post:{FAKE_POST_ID}",
) -> dict[str, Any]:
    return {
        "artifact_id": artifact_id,
        "artifact_type": artifact_type,
        "canonical_id": canonical_id,
        "canonical_url": FAKE_X_URL,
        "normalized_host": "x.com",
        "artifact_key_json": {"post_id": FAKE_POST_ID},
        "current_snapshot_id": None,
        "current_status": None,
    }


def _membership(candidate_group_id: UUID, artifact_id: UUID) -> dict[str, Any]:
    return {"candidate_group_id": candidate_group_id, "artifact_id": artifact_id}


def _valid_parts(*, aggregate_type: str = "candidate_group", order: list[str] | None = None) -> dict[str, Any]:
    event_id = uuid4()
    candidate_group_id = uuid4()
    artifact_id = uuid4()
    event = _event_row(
        event_id=event_id,
        candidate_group_id=candidate_group_id,
        artifact_id=artifact_id,
        aggregate_type=aggregate_type,
        include_candidate_group_payload=True,
    )
    root_object_id = candidate_group_id if aggregate_type == "candidate_group" else artifact_id
    fields = _thin_fields(
        event_id=event_id,
        root_object_type=aggregate_type,
        root_object_id=root_object_id,
    )
    session = FakeSession(
        event_row=event,
        artifact_row=_artifact_row(artifact_id=artifact_id),
        memberships=[_membership(candidate_group_id, artifact_id)],
        order=order,
    )
    redis = FakeRedis([(FAKE_STREAM_ID, fields)], order=order)
    x_client = FakeXClient(order=order)
    return {
        "event_id": event_id,
        "candidate_group_id": candidate_group_id,
        "artifact_id": artifact_id,
        "event": event,
        "fields": fields,
        "session": session,
        "redis": redis,
        "x_client": x_client,
    }


def _all_approvals() -> Any:
    module = _module()
    return module.ObservationApprovals(
        approved_x_api_observation_smoke=True,
        approved_external_network=True,
        approved_x_api_read=True,
        approved_db_write=True,
        approved_artifact_enrichment_run_write=True,
        approved_artifact_snapshot_write=True,
        approved_discovered_url_observation_write=True,
        approved_event_outbox_write=True,
        approved_targeted_redis_ack=True,
    )


def _approvals(**overrides: bool) -> Any:
    values = {
        "approved_x_api_observation_smoke": False,
        "approved_external_network": False,
        "approved_x_api_read": False,
        "approved_db_write": False,
        "approved_artifact_enrichment_run_write": False,
        "approved_artifact_snapshot_write": False,
        "approved_discovered_url_observation_write": False,
        "approved_event_outbox_write": False,
        "approved_targeted_redis_ack": False,
    }
    values.update(overrides)
    return _module().ObservationApprovals(**values)


def _x_payload(*, include_errors: bool = False, include_referenced_url: bool = True) -> dict[str, Any]:
    referenced = {
        "id": "1881234567890000001",
        "text": "Referenced post text",
        "author_id": "user-2",
        "entities": {
            "urls": [
                {"url": "https://t.co/ref", "expanded_url": "https://example.com/ref"}
            ]
        }
        if include_referenced_url
        else {},
    }
    payload: dict[str, Any] = {
        "data": [
            {
                "id": FAKE_POST_ID,
                "text": "Root post text with link",
                "author_id": "user-1",
                "created_at": "2026-05-30T00:00:00Z",
                "conversation_id": "1881234567890123000",
                "edit_history_tweet_ids": [FAKE_POST_ID, FAKE_EDIT_ID],
                "referenced_tweets": [{"type": "quoted", "id": referenced["id"]}],
                "entities": {
                    "urls": [
                        {"url": "https://t.co/root", "expanded_url": "https://example.com/root"}
                    ]
                },
                "public_metrics": {"retweet_count": 1, "like_count": 2},
                "attachments": {"media_keys": ["media-1"]},
            },
            referenced,
        ],
        "includes": {
            "users": [
                {
                    "id": "user-1",
                    "username": "safe_user",
                    "name": "Safe User",
                    "verified": False,
                    "created_at": "2020-01-01T00:00:00Z",
                    "public_metrics": {"followers_count": 10},
                }
            ],
            "media": [
                {
                    "media_key": "media-1",
                    "type": "photo",
                    "url": "https://example.com/media.jpg",
                    "width": 100,
                    "height": 100,
                }
            ],
        },
    }
    if include_errors:
        payload["errors"] = [{"title": "partial include missing"}]
    return payload


def _x_response(
    *,
    status_code: int = 200,
    payload: dict[str, Any] | None = None,
    malformed_json: bool = False,
    network_error: bool = False,
) -> Any:
    module = _module()
    return module.XApiResponse(
        status_code=status_code,
        payload=payload if payload is not None else _x_payload(),
        malformed_json=malformed_json,
        network_error=network_error,
    )


def _run_report(
    *,
    session: FakeSession | None = None,
    redis: FakeRedis | None = None,
    x_client: FakeXClient | None = None,
    approvals: Any | None = None,
    include_token: bool = True,
    side_effect_flags: dict[str, bool] | None = None,
) -> tuple[Any, FakeSession, FakeRedis, FakeXClient]:
    parts = _valid_parts()
    fake_session = session or parts["session"]
    fake_redis = redis or parts["redis"]
    fake_x = x_client or parts["x_client"]
    module = _module()
    result = module.generate_report(
        runtime_env_path=FAKE_RUNTIME_PATH,
        runtime_env_reader=lambda path: _runtime_env(path, include_token=include_token),
        database_session_factory=lambda _url: fake_session,
        redis_client_factory=lambda _url: fake_redis,
        x_api_client_factory=lambda _config: fake_x,
        approvals=approvals,
        side_effect_flags=side_effect_flags,
        forbidden_raw_values=(FAKE_EXCEPTION_TEXT, FAKE_RESPONSE_BODY),
    )
    return result, fake_session, fake_redis, fake_x


def _assert_no_mutation(result: Any, session: FakeSession, redis: FakeRedis, x_client: FakeXClient) -> None:
    assert result.report["x_api_call_attempted"] is False
    assert result.report["external_network_attempted"] is False
    assert result.report["database_write_attempted"] is False
    assert result.report["redis_ack_attempted"] is False
    assert result.report["evidence_assembler_started"] is False
    assert result.report["judge_policy_notifier_started"] is False
    assert result.report["source_tables_mutation_performed"] is False
    assert result.report["telegram_raw_updates_mutation_performed"] is False
    assert result.report["registry_mutation_performed"] is False
    assert result.report["candidate_mutation_performed"] is False
    assert session.writes == []
    assert redis.xgroup_create_calls == []
    assert redis.xreadgroup_calls == []
    assert redis.xack_calls == []
    assert redis.xadd_calls == []
    assert x_client.calls == []


def test_script_exists() -> None:
    assert SCRIPT.exists()


def test_default_mode_validates_rehydrates_plans_and_does_not_mutate_or_call_x_api() -> None:
    result, session, redis, x_client = _run_report()

    assert result.exit_code == 0
    assert result.report["contract_status"] == _module().STATUS_READY
    assert result.report["runtime_env_read"] is True
    assert result.report["database_connected"] is True
    assert result.report["redis_connected"] is True
    assert result.report["read_only_transaction"] is True
    assert result.report["x_stream_exists"] is True
    assert result.report["target_stream_entry_found_bucket"] == "one"
    assert result.report["thin_payload_valid_bucket"] == "one"
    assert result.report["event_outbox_rehydrate_succeeded_bucket"] == "one"
    assert result.report["artifact_registry_rehydrate_succeeded_bucket"] == "one"
    assert result.report["candidate_membership_valid_bucket"] == "one"
    assert result.report["x_bearer_token_present_bucket"] == "present"
    assert result.report["x_api_request_planned_bucket"] == "one"
    assert redis.xlen_calls == [_module().EXPECTED_STREAM_NAME]
    _assert_no_mutation(result, session, redis, x_client)


def test_partial_approval_blocks_before_runtime_network_write_group_or_ack() -> None:
    parts = _valid_parts()
    result, session, redis, x_client = _run_report(
        session=parts["session"],
        redis=parts["redis"],
        x_client=parts["x_client"],
        approvals=_approvals(approved_external_network=True),
    )

    assert result.exit_code == 1
    assert result.report["contract_status"] == _module().STATUS_MISSING_APPROVAL
    assert result.report["runtime_env_read"] is False
    assert "approval.x_api_observation_smoke" in result.report["checks_failed"]
    assert session.statements == []
    assert redis.ping_calls == 0
    assert x_client.calls == []


def test_invalid_thin_payload_blocks_before_event_rehydrate() -> None:
    parts = _valid_parts()
    fields = dict(parts["fields"])
    fields["payload_json"] = {"raw": "forbidden"}
    redis = FakeRedis([(FAKE_STREAM_ID, fields)])

    result, session, redis, x_client = _run_report(session=parts["session"], redis=redis)

    assert result.exit_code == 1
    assert result.report["contract_status"] == _module().STATUS_INVALID_STREAM
    assert result.report["thin_payload_valid_bucket"] == "zero"
    assert result.report["event_outbox_rehydrate_succeeded_bucket"] == "zero"
    _assert_no_mutation(result, session, redis, x_client)


def test_missing_event_row_blocks_rehydrate() -> None:
    parts = _valid_parts()
    session = FakeSession(
        event_row=None,
        artifact_row=parts["session"].artifact_row,
        memberships=parts["session"].memberships,
    )

    result, session, redis, x_client = _run_report(session=session, redis=parts["redis"])

    assert result.exit_code == 1
    assert result.report["contract_status"] == _module().STATUS_REHYDRATE_FAILED
    assert "event_outbox.row_missing" in result.report["checks_failed"]
    _assert_no_mutation(result, session, redis, x_client)


def test_invalid_event_contract_blocks() -> None:
    parts = _valid_parts()
    parts["event"]["payload_json"]["provider_route"] = "github"

    result, session, redis, x_client = _run_report(session=parts["session"], redis=parts["redis"])

    assert result.exit_code == 1
    assert result.report["contract_status"] == _module().STATUS_REHYDRATE_FAILED
    assert "payload.provider_route" in result.report["checks_failed"]
    _assert_no_mutation(result, session, redis, x_client)


def test_invalid_artifact_contract_blocks() -> None:
    parts = _valid_parts()
    parts["session"].artifact_row = _artifact_row(
        artifact_id=parts["artifact_id"],
        canonical_id="github:repo:openai/example",
    )

    result, session, redis, x_client = _run_report(session=parts["session"], redis=parts["redis"])

    assert result.exit_code == 1
    assert result.report["contract_status"] == _module().STATUS_REHYDRATE_FAILED
    assert "artifact_registry.canonical_id" in result.report["checks_failed"]
    _assert_no_mutation(result, session, redis, x_client)


def test_missing_bearer_token_blocks_after_safe_rehydrate_before_network() -> None:
    result, session, redis, x_client = _run_report(include_token=False)

    assert result.exit_code == 1
    assert result.report["contract_status"] == _module().STATUS_REHYDRATE_FAILED
    assert result.report["event_outbox_rehydrate_succeeded_bucket"] == "one"
    assert result.report["artifact_registry_rehydrate_succeeded_bucket"] == "one"
    assert result.report["candidate_membership_valid_bucket"] == "one"
    assert result.report["x_bearer_token_present_bucket"] == "missing"
    assert result.report["x_api_request_planned_bucket"] == "zero"
    assert "runtime.x_bearer_token_missing" in result.report["checks_failed"]
    _assert_no_mutation(result, session, redis, x_client)


def test_approved_mode_calls_x_once_writes_snapshot_outbox_then_acks_after_commit() -> None:
    order: list[str] = []
    parts = _valid_parts(order=order)

    result, session, redis, x_client = _run_report(
        session=parts["session"],
        redis=parts["redis"],
        x_client=parts["x_client"],
        approvals=_all_approvals(),
    )

    assert result.exit_code == 0
    assert result.report["contract_status"] == _module().STATUS_SNAPSHOT_WRITTEN
    assert result.report["targeted_stream_delivery_attempted"] is True
    assert result.report["targeted_stream_delivery_succeeded_bucket"] == "one"
    assert result.report["delivered_target_match_bucket"] == "one"
    assert result.report["x_api_call_attempted"] is True
    assert result.report["x_api_call_succeeded_bucket"] == "one"
    assert result.report["x_api_status_bucket"] == "2xx"
    assert result.report["x_api_result_class"] == "ready"
    assert result.report["database_write_attempted"] is True
    assert result.report["artifact_enrichment_run_written_bucket"] == "one"
    assert result.report["artifact_snapshot_written_bucket"] == "one"
    assert result.report["artifact_snapshot_x_post_written_bucket"] == "one"
    assert result.report["discovered_url_observations_written_bucket"] == "multiple"
    assert result.report["artifact_snapshot_updated_outbox_written_bucket"] == "one"
    assert result.report["artifact_registry_current_snapshot_updated_bucket"] == "one"
    assert result.report["redis_ack_attempted"] is True
    assert result.report["redis_ack_succeeded_bucket"] == "one"
    assert len(x_client.calls) == 1
    plan = x_client.calls[0]
    assert plan.post_id == FAKE_POST_ID
    assert plan.endpoint_path == "/2/tweets"
    assert "edit_history_tweet_ids" in plan.tweet_fields
    assert "referenced_tweets.id" in plan.expansions
    assert session.committed is True
    assert order.index("db:commit") < order.index("redis:xack")
    assert redis.xreadgroup_calls[0][3]["count"] == 1
    assert redis.xreadgroup_calls[0][2] == {_module().EXPECTED_STREAM_NAME: ">"}
    assert redis.xadd_calls == []


def test_approved_default_x_client_uses_bearer_auth_and_no_token_is_reported(monkeypatch) -> None:
    module = _module()
    captured: dict[str, Any] = {}

    class FakeResponse:
        status = 200

        def __enter__(self) -> "FakeResponse":
            return self

        def __exit__(self, *_args: Any) -> None:
            return None

        def read(self) -> bytes:
            return json.dumps(_x_payload()).encode("utf-8")

    def fake_urlopen(request: Any, timeout: float) -> FakeResponse:
        captured["headers"] = dict(request.header_items())
        captured["timeout"] = timeout
        return FakeResponse()

    monkeypatch.setattr(module.urllib.request, "urlopen", fake_urlopen)
    client = module._DefaultXApiClient(
        module.RuntimeConfig(
            database_url=FAKE_DATABASE_URL,
            redis_url=FAKE_REDIS_URL,
            x_bearer_token=FAKE_X_TOKEN,
            x_api_base_url=FAKE_X_BASE_URL,
            x_request_timeout_sec=2.0,
        )
    )
    parts = _valid_parts()
    response = module.generate_report(
        runtime_env_path=FAKE_RUNTIME_PATH,
        runtime_env_reader=lambda path: _runtime_env(path),
        database_session_factory=lambda _url: parts["session"],
        redis_client_factory=lambda _url: parts["redis"],
        x_api_client_factory=lambda _config: client,
        approvals=_all_approvals(),
    )
    rendered = json.dumps(response.report, sort_keys=True)

    assert response.report["contract_status"] == module.STATUS_SNAPSHOT_WRITTEN
    assert captured["headers"]["Authorization"] == f"Bearer {FAKE_X_TOKEN}"
    assert FAKE_X_TOKEN not in rendered


def test_discovered_links_are_observations_only_and_no_candidate_or_reroot_mutation_occurs() -> None:
    parts = _valid_parts()
    result, session, redis, x_client = _run_report(
        session=parts["session"],
        redis=parts["redis"],
        x_client=parts["x_client"],
        approvals=_all_approvals(),
    )

    assert result.exit_code == 0
    assert len(session.discovered_observations) == 2
    assert all(row["discovery_reason"] == "x_post_embedded_link" for row in session.discovered_observations)
    rendered_sql = "\n".join(session.statements).lower()
    assert "insert into candidate_group" not in rendered_sql
    assert "update candidate_group" not in rendered_sql
    assert "candidate_reroot_events" not in rendered_sql
    assert result.report["candidate_mutation_performed"] is False
    assert result.report["registry_mutation_performed"] is False
    assert redis.xadd_calls == []
    assert x_client.calls


def test_db_failure_rolls_back_and_does_not_ack() -> None:
    order: list[str] = []
    parts = _valid_parts(order=order)
    session = parts["session"]
    session.fail_on = "insert_snapshot"

    result, session, redis, x_client = _run_report(
        session=session,
        redis=parts["redis"],
        x_client=parts["x_client"],
        approvals=_all_approvals(),
    )

    assert result.exit_code == 1
    assert result.report["contract_status"] == _module().STATUS_DB_WRITE_FAILED
    assert result.report["database_write_attempted"] is True
    assert session.committed is False
    assert session.rolled_back_count >= 2
    assert redis.xack_calls == []
    assert "database.write" in result.report["checks_failed"]
    assert FAKE_EXCEPTION_TEXT not in json.dumps(result.report, sort_keys=True)


def _assert_x_api_classification_blocks_without_write_or_ack(
    *,
    status_code: int,
    bucket: str,
    result_class: str,
) -> None:
    parts = _valid_parts()
    x_client = FakeXClient(
        _x_response(status_code=status_code, payload={"body": FAKE_RESPONSE_BODY})
    )

    result, session, redis, _fake_x = _run_report(
        session=parts["session"],
        redis=parts["redis"],
        x_client=x_client,
        approvals=_all_approvals(),
    )
    rendered = json.dumps(result.report, sort_keys=True)

    assert result.exit_code == 1
    assert result.report["contract_status"] == _module().STATUS_X_API_FAILED
    assert result.report["x_api_call_attempted"] is True
    assert result.report["external_network_attempted"] is True
    assert result.report["targeted_stream_delivery_succeeded_bucket"] == "one"
    assert result.report["delivered_target_match_bucket"] == "one"
    assert result.report["x_api_status_bucket"] == bucket
    assert result.report["x_api_result_class"] == result_class
    assert result.report["database_write_attempted"] is False
    assert result.report["redis_ack_attempted"] is False
    assert session.writes == []
    assert session.committed is False
    assert session.rolled_back_count >= 2
    assert redis.xack_calls == []
    assert result.report["raw_values_emitted"] is False
    assert f"x_api.{bucket}" in result.report["checks_failed"]
    for value in (
        FAKE_RESPONSE_BODY,
        FAKE_X_TOKEN,
        FAKE_X_URL,
        FAKE_POST_ID,
        FAKE_DATABASE_URL,
        FAKE_REDIS_URL,
        FAKE_STREAM_ID,
        FAKE_EXCEPTION_TEXT,
    ):
        assert value not in rendered


def test_x_api_failure_classes_are_sanitized_and_do_not_write_or_ack() -> None:
    cases = [
        (401, "401_403", "access_denied"),
        (403, "401_403", "access_denied"),
        (404, "404", "failed_permanent"),
        (429, "429", "rate_limited"),
        (503, "5xx", "failed_transient"),
    ]
    for status_code, bucket, result_class in cases:
        _assert_x_api_classification_blocks_without_write_or_ack(
            status_code=status_code,
            bucket=bucket,
            result_class=result_class,
        )


def test_http_400_x_api_failure_is_request_invalid_and_sanitized() -> None:
    _assert_x_api_classification_blocks_without_write_or_ack(
        status_code=400,
        bucket="400",
        result_class="request_invalid",
    )


def test_http_405_x_api_failure_is_request_invalid_and_sanitized() -> None:
    _assert_x_api_classification_blocks_without_write_or_ack(
        status_code=405,
        bucket="405",
        result_class="request_invalid",
    )


def test_http_422_x_api_failure_is_request_invalid_and_sanitized() -> None:
    _assert_x_api_classification_blocks_without_write_or_ack(
        status_code=422,
        bucket="422",
        result_class="request_invalid",
    )


def test_http_451_x_api_failure_is_permanent_and_sanitized() -> None:
    _assert_x_api_classification_blocks_without_write_or_ack(
        status_code=451,
        bucket="451",
        result_class="failed_permanent",
    )


def test_unexpected_4xx_x_api_failures_are_permanent_and_sanitized() -> None:
    _assert_x_api_classification_blocks_without_write_or_ack(
        status_code=418,
        bucket="4xx_other",
        result_class="failed_permanent",
    )


def test_malformed_x_api_response_is_sanitized_and_not_acked() -> None:
    parts = _valid_parts()
    x_client = FakeXClient(_x_response(status_code=200, payload=None, malformed_json=True))

    result, session, redis, _fake_x = _run_report(
        session=parts["session"],
        redis=parts["redis"],
        x_client=x_client,
        approvals=_all_approvals(),
    )

    assert result.exit_code == 1
    assert result.report["contract_status"] == _module().STATUS_X_API_FAILED
    assert result.report["x_api_status_bucket"] == "malformed_json"
    assert result.report["x_api_result_class"] == "failed_transient"
    assert session.writes == []
    assert redis.xack_calls == []


def test_redis_ack_failure_after_commit_is_sanitized_and_does_not_compensate_db() -> None:
    order: list[str] = []
    parts = _valid_parts(order=order)
    redis = FakeRedis(parts["redis"].entries, order=order, fail_ack=True)
    x_client = FakeXClient(order=order)

    result, session, redis, _fake_x = _run_report(
        session=parts["session"],
        redis=redis,
        x_client=x_client,
        approvals=_all_approvals(),
    )

    assert result.exit_code == 1
    assert result.report["contract_status"] == _module().STATUS_REDIS_ACK_FAILED
    assert result.report["redis_ack_attempted"] is True
    assert result.report["redis_ack_succeeded_bucket"] == "zero"
    assert result.report["redis_ack_failure_class"] == "redis_ack_failed"
    assert session.committed is True
    assert order.index("db:commit") < order.index("redis:xack")
    assert session.writes.count("update_registry_current_snapshot") == 1
    assert FAKE_EXCEPTION_TEXT not in json.dumps(result.report, sort_keys=True)


def test_forbidden_side_effect_flags_block_immediately() -> None:
    parts = _valid_parts()
    result, session, redis, x_client = _run_report(
        session=parts["session"],
        redis=parts["redis"],
        x_client=parts["x_client"],
        side_effect_flags={"candidate_mutation_performed": True},
    )

    assert result.exit_code == 1
    assert result.report["contract_status"] == _module().STATUS_FORBIDDEN_SIDE_EFFECT
    assert "side_effect.forbidden" in result.report["checks_failed"]
    assert session.statements == []
    assert redis.ping_calls == 0
    assert x_client.calls == []


def test_no_raw_values_are_emitted_including_ids_urls_tokens_response_body_stream_id_or_exception_text() -> None:
    parts = _valid_parts()
    result, session, redis, x_client = _run_report(
        session=parts["session"],
        redis=parts["redis"],
        x_client=parts["x_client"],
        approvals=_all_approvals(),
    )
    rendered = json.dumps(result.report, sort_keys=True)

    forbidden_values = (
        str(parts["event_id"]),
        str(parts["candidate_group_id"]),
        str(parts["artifact_id"]),
        str(session.snapshot_id),
        FAKE_POST_ID,
        FAKE_EDIT_ID,
        FAKE_DEDUPE_KEY,
        FAKE_SOURCE_TEXT,
        FAKE_X_URL,
        FAKE_DATABASE_URL,
        FAKE_REDIS_URL,
        "unit-db-password-x-observation",
        "unit-redis-password-x-observation",
        FAKE_X_TOKEN,
        FAKE_STREAM_ID,
        FAKE_RUNTIME_PATH,
        FAKE_RESPONSE_BODY,
        FAKE_EXCEPTION_TEXT,
        "https://example.com/root",
        "https://example.com/ref",
    )
    for value in forbidden_values:
        assert value not in rendered
    assert result.report["raw_values_emitted"] is False
    assert result.report["source_tables_mutation_performed"] is False
    assert result.report["telegram_raw_updates_mutation_performed"] is False
    assert result.report["evidence_assembler_started"] is False
    assert result.report["judge_policy_notifier_started"] is False
    assert result.report["docker_or_systemd_changed"] is False
    assert result.report["alembic_run"] is False


def test_root_post_missing_in_2xx_blocks_without_writes_or_ack() -> None:
    parts = _valid_parts()
    x_client = FakeXClient(_x_response(status_code=200, payload={"data": []}))

    result, session, redis, _fake_x = _run_report(
        session=parts["session"],
        redis=parts["redis"],
        x_client=x_client,
        approvals=_all_approvals(),
    )

    assert result.exit_code == 1
    assert result.report["contract_status"] == _module().STATUS_X_API_FAILED
    assert result.report["x_api_status_bucket"] == "2xx"
    assert result.report["x_api_result_class"] == "root_post_missing"
    assert session.writes == []
    assert redis.xack_calls == []


def test_root_post_missing_edit_history_in_2xx_blocks_without_writes_or_ack() -> None:
    parts = _valid_parts()
    payload = _x_payload()
    del payload["data"][0]["edit_history_tweet_ids"]
    x_client = FakeXClient(_x_response(status_code=200, payload=payload))

    result, session, redis, _fake_x = _run_report(
        session=parts["session"],
        redis=parts["redis"],
        x_client=x_client,
        approvals=_all_approvals(),
    )

    assert result.exit_code == 1
    assert result.report["contract_status"] == _module().STATUS_X_API_FAILED
    assert result.report["x_api_status_bucket"] == "2xx"
    assert result.report["x_api_result_class"] == "edit_history_missing"
    assert "x_api.edit_history_missing" in result.report["checks_failed"]
    assert result.report["database_write_attempted"] is False
    assert result.report["redis_ack_attempted"] is False
    assert session.writes == []
    assert session.committed is False
    assert session.rolled_back_count >= 2
    assert redis.xack_calls == []
    rendered = json.dumps(result.report, sort_keys=True)
    assert FAKE_POST_ID not in rendered
    assert FAKE_X_URL not in rendered
    assert FAKE_RESPONSE_BODY not in rendered


def test_root_post_empty_edit_history_in_2xx_blocks_without_writes_or_ack() -> None:
    parts = _valid_parts()
    payload = _x_payload()
    payload["data"][0]["edit_history_tweet_ids"] = []
    x_client = FakeXClient(_x_response(status_code=200, payload=payload))

    result, session, redis, _fake_x = _run_report(
        session=parts["session"],
        redis=parts["redis"],
        x_client=x_client,
        approvals=_all_approvals(),
    )

    assert result.exit_code == 1
    assert result.report["contract_status"] == _module().STATUS_X_API_FAILED
    assert result.report["x_api_status_bucket"] == "2xx"
    assert result.report["x_api_result_class"] == "edit_history_missing"
    assert "x_api.edit_history_missing" in result.report["checks_failed"]
    assert result.report["database_write_attempted"] is False
    assert result.report["redis_ack_attempted"] is False
    assert session.writes == []
    assert session.committed is False
    assert session.rolled_back_count >= 2
    assert redis.xack_calls == []
    rendered = json.dumps(result.report, sort_keys=True)
    assert FAKE_POST_ID not in rendered
    assert FAKE_X_URL not in rendered
    assert FAKE_RESPONSE_BODY not in rendered


def test_partial_ready_when_x_response_has_nonfatal_partial_errors() -> None:
    parts = _valid_parts()
    x_client = FakeXClient(_x_response(payload=_x_payload(include_errors=True)))

    result, session, redis, _fake_x = _run_report(
        session=parts["session"],
        redis=parts["redis"],
        x_client=x_client,
        approvals=_all_approvals(),
    )

    assert result.exit_code == 0
    assert result.report["contract_status"] == _module().STATUS_SNAPSHOT_WRITTEN
    assert result.report["x_api_result_class"] == "partial_ready"
    assert session.snapshot_outbox_payloads[0]["status"] == "partial_ready"
    assert redis.xack_calls


def test_network_exception_is_sanitized_and_does_not_write_or_ack() -> None:
    parts = _valid_parts()
    x_client = FakeXClient(raises=True)

    result, session, redis, _fake_x = _run_report(
        session=parts["session"],
        redis=parts["redis"],
        x_client=x_client,
        approvals=_all_approvals(),
    )

    assert result.exit_code == 1
    assert result.report["contract_status"] == _module().STATUS_X_API_FAILED
    assert result.report["x_api_status_bucket"] == "network_error"
    assert result.report["x_api_result_class"] == "failed_transient"
    assert session.writes == []
    assert redis.xack_calls == []
    assert FAKE_EXCEPTION_TEXT not in json.dumps(result.report, sort_keys=True)
