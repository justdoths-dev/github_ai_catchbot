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
    / "dedicated_vps_artifact_enrich_requested_x_route_bounded_publish_smoke.py"
)

FAKE_DATABASE_URL = (
    "postgresql+psycopg://github_ai_catchbot_app:"
    "unit-db-password-x-route-publish@127.0.0.1:5432/github_ai_catchbot"
)
FAKE_REDIS_URL = "redis://:unit-redis-password-x-route-publish@127.0.0.1:6379/0"
FAKE_RUNTIME_PATH = "/etc/github-ai-catchbot/private-runtime.env"
FAKE_STREAM_ID = "1710000000000-0"
FAKE_DEDUPE_KEY = "private-x-route-dedupe-key"
FAKE_SOURCE_TEXT = "Sensitive source text must not be rendered"
FAKE_X_URL = "https://x.com/openai/status/1234567890123456789"
FAKE_SECRET = "fake-secret-value"


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
        rows: list[dict[str, Any]] | None = None,
        *,
        memberships: list[dict[str, Any]] | None = None,
        published_x_count: int = 0,
        read_only_value: str = "on",
        missing_tables: set[str] | None = None,
        raise_on_update: bool = False,
        raise_on_job_attempt: bool = False,
        order: list[str] | None = None,
    ) -> None:
        self.rows = rows or []
        self.memberships = memberships or []
        self.published_x_count = published_x_count
        self.read_only_value = read_only_value
        self.missing_tables = missing_tables or set()
        self.raise_on_update = raise_on_update
        self.raise_on_job_attempt = raise_on_job_attempt
        self.order = order
        self.statements: list[str] = []
        self.params: list[dict[str, Any]] = []
        self.published_event_ids: list[UUID] = []
        self.job_attempts: list[dict[str, Any]] = []
        self.validation_queries: list[str] = []
        self.committed = False
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
        if normalized == _normalize(module.SELECT_PENDING_ENRICH_EVENTS_QUERY):
            limit = int(params.get("limit", len(self.rows)))
            return FakeResult(rows=self.rows[:limit])
        if normalized == _normalize(module.COUNT_PUBLISHED_X_ENRICH_EVENTS_QUERY):
            return FakeResult(scalar=self.published_x_count)
        if normalized == _normalize(module.VALIDATE_CANDIDATE_GROUP_AGGREGATE_QUERY):
            self.validation_queries.append("candidate_group")
            return FakeResult(scalar=self._count_membership(params))
        if normalized == _normalize(module.VALIDATE_ARTIFACT_AGGREGATE_WITH_GROUP_QUERY):
            self.validation_queries.append("artifact_with_group")
            return FakeResult(scalar=self._count_membership(params))
        if normalized == _normalize(module.VALIDATE_ARTIFACT_AGGREGATE_QUERY):
            self.validation_queries.append("artifact")
            artifact_id = str(params["artifact_id"])
            return FakeResult(
                scalar=sum(1 for row in self.memberships if str(row["artifact_id"]) == artifact_id)
            )
        if normalized.startswith("UPDATE event_outbox"):
            if self.raise_on_update:
                raise RuntimeError(f"db update failed with {FAKE_SECRET}")
            if self.order is not None:
                self.order.append("db:update_published")
            self.published_event_ids.append(UUID(str(params["event_id"])))
            return FakeResult()
        if normalized.startswith("INSERT INTO job_attempts"):
            if self.raise_on_job_attempt:
                raise RuntimeError(f"job attempt failed with {FAKE_SECRET}")
            if self.order is not None:
                self.order.append("db:insert_job_attempt")
            self.job_attempts.append(dict(params))
            return FakeResult()

        raise AssertionError(f"unexpected SQL: {statement}")

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
        self.committed = True
        if self.order is not None:
            self.order.append("db:commit")

    async def rollback(self) -> None:
        self.rolled_back = True
        if self.order is not None:
            self.order.append("db:rollback")

    async def close(self) -> None:
        self.closed = True


class FakeRedis:
    def __init__(
        self,
        *,
        fail_xadd: bool = False,
        order: list[str] | None = None,
    ) -> None:
        self.fail_xadd = fail_xadd
        self.order = order
        self.ping_calls = 0
        self.xlen_calls: list[str] = []
        self.xadd_calls: list[tuple[str, dict[str, str], dict[str, Any]]] = []
        self.closed = False

    async def ping(self) -> bool:
        self.ping_calls += 1
        return True

    async def xlen(self, name: str) -> int:
        self.xlen_calls.append(name)
        return 0

    async def xadd(
        self,
        name: str,
        fields: dict[str, str],
        maxlen: int | None = None,
        approximate: bool | None = None,
    ) -> str:
        self.xadd_calls.append((name, dict(fields), {"maxlen": maxlen, "approximate": approximate}))
        if self.order is not None:
            self.order.append("redis:xadd")
        if self.fail_xadd:
            raise RuntimeError(f"redis publish failed with {FAKE_SECRET}")
        return FAKE_STREAM_ID

    async def close(self) -> None:
        self.closed = True


class MismatchedRouteResolver:
    def resolve(self, _row: Any) -> Any:
        module = _module()
        return module.QueueRoute(queue_name="q.artifact.enrich.github", stage_name="enrich_github")


def _module():
    return importlib.import_module(
        "scripts.ops.dedicated_vps_artifact_enrich_requested_x_route_bounded_publish_smoke"
    )


def _normalize(statement: str) -> str:
    return " ".join(statement.strip().split())


def _runtime_env(_path: str | Path) -> dict[str, str]:
    return {
        "DATABASE_URL": FAKE_DATABASE_URL,
        "REDIS_URL": FAKE_REDIS_URL,
        "OUTBOX_RELAY_XADD_MAXLEN": "10000",
    }


def _all_approvals() -> Any:
    module = _module()
    return module.PublishApprovals(
        artifact_enrich_x_route_publish_smoke=True,
        redis_publish=True,
        event_outbox_published_update=True,
        job_attempt_write=True,
    )


def _approvals(**overrides: bool) -> Any:
    values = {
        "artifact_enrich_x_route_publish_smoke": False,
        "redis_publish": False,
        "event_outbox_published_update": False,
        "job_attempt_write": False,
    }
    values.update(overrides)
    return _module().PublishApprovals(**values)


def _fake_row(
    *,
    event_id: UUID | None = None,
    candidate_group_id: UUID | None = None,
    artifact_id: UUID | None = None,
    aggregate_type: str = "candidate_group",
    provider_route: str = "x",
    include_candidate_group_payload: bool = True,
    dedupe_key: str = FAKE_DEDUPE_KEY,
) -> dict[str, Any]:
    event_id = event_id or uuid4()
    candidate_group_id = candidate_group_id or uuid4()
    artifact_id = artifact_id or uuid4()
    aggregate_id = candidate_group_id if aggregate_type == "candidate_group" else artifact_id
    payload: dict[str, Any] = {
        "artifact_id": str(artifact_id),
        "artifact_type": "x_post",
        "provider_route": provider_route,
        "refresh_mode": "standard",
        "depth_budget": 1,
        "requested_at": datetime.now(timezone.utc).isoformat(),
        "source_text": FAKE_SOURCE_TEXT,
        "canonical_url": FAKE_X_URL,
    }
    if include_candidate_group_payload:
        payload["candidate_group_id"] = str(candidate_group_id)
    return {
        "event_id": event_id,
        "event_type": "artifact.enrich.requested.v1",
        "aggregate_type": aggregate_type,
        "aggregate_id": aggregate_id,
        "dedupe_key": dedupe_key,
        "payload_json": payload,
        "status": "pending",
        "fail_count": 0,
        "created_at": datetime.now(timezone.utc),
    }


def _membership(row: dict[str, Any], *, candidate_group_id: UUID | None = None) -> dict[str, Any]:
    payload = row["payload_json"]
    group_id = candidate_group_id or UUID(str(payload.get("candidate_group_id") or uuid4()))
    return {
        "candidate_group_id": group_id,
        "artifact_id": UUID(str(payload["artifact_id"])),
    }


def _run_report(
    *,
    rows: list[dict[str, Any]] | None = None,
    memberships: list[dict[str, Any]] | None = None,
    approvals: Any | None = None,
    session: FakeSession | None = None,
    redis: FakeRedis | None = None,
    route_resolver: Any | None = None,
    published_x_count: int = 0,
    side_effect_flags: dict[str, bool] | None = None,
) -> tuple[Any, FakeSession, FakeRedis]:
    module = _module()
    fake_session = session or FakeSession(
        rows,
        memberships=memberships,
        published_x_count=published_x_count,
    )
    fake_redis = redis or FakeRedis()
    result = module.generate_report(
        runtime_env_path=FAKE_RUNTIME_PATH,
        approvals=approvals,
        runtime_env_reader=_runtime_env,
        database_session_factory=lambda _url: fake_session,
        redis_client_factory=lambda _url: fake_redis,
        route_resolver=route_resolver,
        side_effect_flags=side_effect_flags,
        forbidden_raw_values=(FAKE_RUNTIME_PATH, FAKE_SECRET),
    )
    return result, fake_session, fake_redis


def test_script_exists() -> None:
    assert SCRIPT.exists()


def test_default_mode_is_read_only_and_does_not_publish_update_or_write() -> None:
    row = _fake_row()
    result, session, redis = _run_report(rows=[row], memberships=[_membership(row)])

    assert result.exit_code == 0
    assert result.report["contract_status"] == _module().STATUS_READY
    assert result.report["runtime_env_read"] is True
    assert result.report["database_connected"] is True
    assert result.report["redis_connected"] is True
    assert result.report["read_only_transaction"] is True
    assert result.report["pending_x_enrich_event_selected_bucket"] == "one"
    assert result.report["redis_publish_attempted"] is False
    assert result.report["event_outbox_update_attempted"] is False
    assert result.report["job_attempt_write_attempted"] is False
    assert redis.xadd_calls == []
    assert redis.xlen_calls == ["q.artifact.enrich.x"]
    assert session.published_event_ids == []
    assert session.job_attempts == []
    assert session.rolled_back is True


def test_target_selection_supports_candidate_group_aggregate_shape() -> None:
    row = _fake_row(aggregate_type="candidate_group")
    result, session, _redis = _run_report(rows=[row], memberships=[_membership(row)])

    assert result.exit_code == 0
    assert result.report["contract_status"] == _module().STATUS_READY
    assert result.report["provider_route_x_bucket"] == "one"
    assert result.report["route_queue_valid_bucket"] == "one"
    assert result.report["route_stage_valid_bucket"] == "one"
    assert "candidate_group" in session.validation_queries


def test_target_selection_supports_artifact_aggregate_shape_through_members() -> None:
    candidate_group_id = uuid4()
    artifact_id = uuid4()
    row = _fake_row(
        candidate_group_id=candidate_group_id,
        artifact_id=artifact_id,
        aggregate_type="artifact",
        include_candidate_group_payload=False,
    )
    result, session, _redis = _run_report(
        rows=[row],
        memberships=[{"candidate_group_id": candidate_group_id, "artifact_id": artifact_id}],
    )

    assert result.exit_code == 0
    assert result.report["contract_status"] == _module().STATUS_READY
    assert result.report["pending_x_enrich_event_selected_bucket"] == "one"
    assert "artifact" in session.validation_queries


def test_non_x_provider_route_blocks_without_publish_or_write() -> None:
    row = _fake_row(provider_route="github")
    result, session, redis = _run_report(rows=[row], memberships=[_membership(row)])

    assert result.exit_code == 1
    assert result.report["contract_status"] == _module().STATUS_INVALID_ROUTE
    assert result.report["provider_route_x_bucket"] == "zero"
    assert "route.provider_route_not_x" in result.report["checks_failed"]
    assert redis.xadd_calls == []
    assert session.published_event_ids == []
    assert session.job_attempts == []


def test_invalid_route_blocks_before_publish_or_write() -> None:
    row = _fake_row()
    result, session, redis = _run_report(
        rows=[row],
        memberships=[_membership(row)],
        approvals=_all_approvals(),
        route_resolver=MismatchedRouteResolver(),
    )

    assert result.exit_code == 1
    assert result.report["contract_status"] == _module().STATUS_INVALID_ROUTE
    assert result.report["route_queue_valid_bucket"] == "zero"
    assert result.report["route_stage_valid_bucket"] == "zero"
    assert redis.xadd_calls == []
    assert session.published_event_ids == []
    assert session.job_attempts == []


def test_partial_approvals_block_before_publish_write_and_connections() -> None:
    row = _fake_row()
    session = FakeSession([row], memberships=[_membership(row)])
    redis = FakeRedis()
    result, session, redis = _run_report(
        rows=[row],
        session=session,
        redis=redis,
        approvals=_approvals(
            artifact_enrich_x_route_publish_smoke=True,
            redis_publish=True,
            event_outbox_published_update=True,
        ),
    )

    assert result.exit_code == 1
    assert result.report["contract_status"] == _module().STATUS_MISSING_APPROVAL
    assert "approval.job_attempt_write" in result.report["checks_failed"]
    assert result.report["database_connected"] is False
    assert result.report["redis_connected"] is False
    assert session.statements == []
    assert redis.xadd_calls == []
    assert session.published_event_ids == []
    assert session.job_attempts == []


def test_approved_mode_publishes_exactly_one_redis_message_to_x_queue() -> None:
    first = _fake_row()
    second = _fake_row()
    redis = FakeRedis()
    result, session, redis = _run_report(
        rows=[first, second],
        memberships=[_membership(first), _membership(second)],
        approvals=_all_approvals(),
        redis=redis,
    )

    assert result.exit_code == 0
    assert result.report["contract_status"] == _module().STATUS_PUBLISHED
    assert len(redis.xadd_calls) == 1
    stream_name, fields, _options = redis.xadd_calls[0]
    assert stream_name == "q.artifact.enrich.x"
    assert fields["job_id"] == str(first["event_id"])
    assert fields["stage_name"] == "enrich_x"
    assert fields["root_object_type"] == "candidate_group"
    assert fields["root_object_id"] == str(first["aggregate_id"])
    assert fields["idempotency_key"] == first["dedupe_key"]
    assert fields["pipeline_run_id"] == ""
    assert fields["not_before"] == ""
    assert fields["trigger_event_id"] == str(first["event_id"])
    assert session.published_event_ids == [first["event_id"]]
    assert len(session.job_attempts) == 1


def test_redis_stream_message_is_thin_without_payload_source_text_url_or_raw_fields() -> None:
    row = _fake_row()
    redis = FakeRedis()
    result, _session, redis = _run_report(
        rows=[row],
        memberships=[_membership(row)],
        approvals=_all_approvals(),
        redis=redis,
    )

    assert result.exit_code == 0
    fields = redis.xadd_calls[0][1]
    assert set(fields) == _module().ALLOWED_REDIS_THIN_FIELDS
    rendered_fields = json.dumps(fields, sort_keys=True)
    forbidden_tokens = ("payload_json", "payload", "source_text", "canonical_url", FAKE_X_URL, FAKE_SOURCE_TEXT)
    for token in forbidden_tokens:
        assert token not in rendered_fields


def test_event_outbox_is_marked_published_only_after_redis_publish_succeeds() -> None:
    order: list[str] = []
    row = _fake_row()
    session = FakeSession([row], memberships=[_membership(row)], order=order)
    redis = FakeRedis(order=order)
    result, session, _redis = _run_report(
        rows=[row],
        session=session,
        redis=redis,
        approvals=_all_approvals(),
    )

    assert result.exit_code == 0
    assert order[:4] == [
        "redis:xadd",
        "db:update_published",
        "db:insert_job_attempt",
        "db:commit",
    ]


def test_job_attempt_succeeded_row_is_written() -> None:
    row = _fake_row()
    result, session, _redis = _run_report(
        rows=[row],
        memberships=[_membership(row)],
        approvals=_all_approvals(),
    )

    assert result.report["job_attempt_write_attempted"] is True
    assert result.report["job_attempt_succeeded_bucket"] == "one"
    assert len(session.job_attempts) == 1
    attempt = session.job_attempts[0]
    assert attempt["stage_name"] == "enrich_x"
    assert attempt["queue_name"] == "q.artifact.enrich.x"
    assert attempt["root_object_type"] == "candidate_group"
    assert attempt["root_object_id"] == str(row["aggregate_id"])
    assert attempt["attempt_status"] == "succeeded"
    assert attempt["error_code"] is None


def test_redis_publish_failure_rolls_back_db_and_does_not_mark_published() -> None:
    row = _fake_row()
    redis = FakeRedis(fail_xadd=True)
    result, session, redis = _run_report(
        rows=[row],
        memberships=[_membership(row)],
        approvals=_all_approvals(),
        redis=redis,
    )

    assert result.exit_code == 1
    assert result.report["contract_status"] == _module().STATUS_REDIS_PUBLISH_FAILED
    assert result.report["redis_publish_attempted"] is True
    assert result.report["redis_publish_succeeded_bucket"] == "zero"
    assert result.report["event_outbox_update_attempted"] is False
    assert result.report["job_attempt_write_attempted"] is False
    assert len(redis.xadd_calls) == 1
    assert session.published_event_ids == []
    assert session.job_attempts == []
    assert session.rolled_back is True


def test_db_job_attempt_failure_after_redis_publish_is_sanitized() -> None:
    row = _fake_row()
    session = FakeSession([row], memberships=[_membership(row)], raise_on_job_attempt=True)
    redis = FakeRedis()
    result, session, redis = _run_report(
        rows=[row],
        session=session,
        redis=redis,
        approvals=_all_approvals(),
    )
    rendered = json.dumps(result.report, sort_keys=True)

    assert result.exit_code == 1
    assert result.report["contract_status"] == _module().STATUS_DB_WRITE_FAILED_AFTER_REDIS
    assert result.report["redis_publish_succeeded_bucket"] == "one"
    assert len(redis.xadd_calls) == 1
    assert session.rolled_back is True
    assert FAKE_SECRET not in rendered
    assert "job attempt failed" not in rendered


def test_no_source_telegram_or_registry_mutation_is_reported() -> None:
    row = _fake_row()
    result, _session, _redis = _run_report(rows=[row], memberships=[_membership(row)])

    assert result.report["source_tables_mutation_performed"] is False
    assert result.report["telegram_raw_updates_mutation_performed"] is False
    assert result.report["registry_mutation_performed"] is False


def test_no_downstream_enricher_judge_policy_or_notifier_start() -> None:
    row = _fake_row()
    result, _session, _redis = _run_report(rows=[row], memberships=[_membership(row)])
    text = SCRIPT.read_text(encoding="utf-8")

    assert result.report["downstream_enricher_started"] is False
    assert "src.services.x_enricher" not in text
    assert "src.services.gh_enricher" not in text
    assert "src.services.web_enricher" not in text
    assert "src.services.evidence_assembler" not in text
    assert "src.services.judge_openai" not in text
    assert "src.services.policy_engine" not in text
    assert "src.services.notifier_telegram" not in text


def test_forbidden_side_effect_flags_block() -> None:
    result, _session, _redis = _run_report(
        rows=[_fake_row()],
        memberships=[],
        side_effect_flags={"external_network_attempted": True},
    )

    assert result.exit_code == 1
    assert result.report["contract_status"] == _module().STATUS_FORBIDDEN_SIDE_EFFECT
    assert "side_effect.forbidden" in result.report["checks_failed"]


def test_no_raw_values_emitted_including_fake_ids_urls_secrets_or_stream_id() -> None:
    event_id = uuid4()
    candidate_group_id = uuid4()
    artifact_id = uuid4()
    row = _fake_row(
        event_id=event_id,
        candidate_group_id=candidate_group_id,
        artifact_id=artifact_id,
        dedupe_key=FAKE_DEDUPE_KEY,
    )
    result, _session, _redis = _run_report(
        rows=[row],
        memberships=[_membership(row)],
        approvals=_all_approvals(),
    )
    rendered = json.dumps(result.report, sort_keys=True)

    forbidden_values = (
        str(event_id),
        str(candidate_group_id),
        str(artifact_id),
        FAKE_DEDUPE_KEY,
        FAKE_SOURCE_TEXT,
        FAKE_X_URL,
        FAKE_DATABASE_URL,
        FAKE_REDIS_URL,
        "unit-db-password-x-route-publish",
        "unit-redis-password-x-route-publish",
        FAKE_RUNTIME_PATH,
        FAKE_STREAM_ID,
        FAKE_SECRET,
    )
    for value in forbidden_values:
        assert value not in rendered
    assert result.report["raw_values_emitted"] is False


def test_already_published_state_is_acceptable_when_no_pending_x_event_remains() -> None:
    result, _session, redis = _run_report(rows=[], memberships=[], published_x_count=1)

    assert result.exit_code == 0
    assert result.report["contract_status"] == _module().STATUS_ALREADY_PUBLISHED
    assert result.report["pending_x_enrich_event_selected_bucket"] == "zero"
    assert redis.xadd_calls == []
