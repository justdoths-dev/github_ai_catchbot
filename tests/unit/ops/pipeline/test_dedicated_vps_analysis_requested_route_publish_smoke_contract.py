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
    / "dedicated_vps_analysis_requested_route_publish_smoke.py"
)

FAKE_DATABASE_CREDENTIAL = "unit" + "-db" + "-credential" + "-analysis-route"
FAKE_REDIS_CREDENTIAL = "unit" + "-redis" + "-credential" + "-analysis-route"
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
FAKE_DEDUPE_KEY = "private-analysis-requested-dedupe-key"
FAKE_SOURCE_TEXT = " ".join(["private", "material", "fixture"])
FAKE_URL = "https" + ":" + "/" + "/" + "example.invalid" + "/private/example"
FAKE_SENSITIVE_VALUE = "fake" + "-sensitive-value"


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
        rows: list[dict[str, Any]] | None = None,
        *,
        candidate_groups: dict[str, UUID | None] | None = None,
        bundles: dict[str, dict[str, Any]] | None = None,
        member_counts: dict[str, int] | None = None,
        read_only_value: str = "on",
        missing_tables: set[str] | None = None,
        raise_on_update: bool = False,
        raise_on_job_attempt: bool = False,
        order: list[str] | None = None,
    ) -> None:
        self.rows = rows or []
        self.candidate_groups = candidate_groups or {}
        self.bundles = bundles or {}
        self.member_counts = member_counts or {}
        self.read_only_value = read_only_value
        self.missing_tables = missing_tables or set()
        self.raise_on_update = raise_on_update
        self.raise_on_job_attempt = raise_on_job_attempt
        self.order = order
        self.statements: list[str] = []
        self.params: list[dict[str, Any]] = []
        self.published_event_ids: list[UUID] = []
        self.job_attempts: list[dict[str, Any]] = []
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
        if normalized == _normalize(module.SELECT_PENDING_ANALYSIS_REQUESTED_EVENTS_QUERY):
            limit = int(params.get("limit", len(self.rows)))
            return FakeResult(rows=self.rows[:limit])
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
        if normalized == _normalize(module.SELECT_BUNDLE_STATE_QUERY):
            bundle_id = str(params["bundle_id"])
            bundle = self.bundles.get(bundle_id)
            if bundle is None:
                return FakeResult(rows=[])
            return FakeResult(rows=[bundle])
        if normalized == _normalize(module.COUNT_CANDIDATE_EVIDENCE_MEMBERS_QUERY):
            return FakeResult(scalar=self.member_counts.get(str(params["bundle_id"]), 0))
        if normalized.startswith("UPDATE event_outbox"):
            if self.raise_on_update:
                raise RuntimeError(f"db update failed with {FAKE_SENSITIVE_VALUE}")
            if self.order is not None:
                self.order.append("db:update_published")
            self.published_event_ids.append(UUID(str(params["event_id"])))
            return FakeResult()
        if normalized.startswith("INSERT INTO job_attempts"):
            if self.raise_on_job_attempt:
                raise RuntimeError(f"job attempt failed with {FAKE_SENSITIVE_VALUE}")
            if self.order is not None:
                self.order.append("db:insert_job_attempt")
            self.job_attempts.append(dict(params))
            return FakeResult()

        raise AssertionError(f"unexpected SQL: {statement}")

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
            raise RuntimeError(f"redis publish failed with {FAKE_SENSITIVE_VALUE}")
        return FAKE_STREAM_ID

    async def close(self) -> None:
        self.closed = True


class MismatchedRouteResolver:
    def resolve(self, _row: Any) -> Any:
        module = _module()
        return module.QueueRoute(queue_name="q.analysis.judge", stage_name="judge")


def _module():
    return importlib.import_module(
        "scripts.ops.dedicated_vps_analysis_requested_route_publish_smoke"
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
        analysis_requested_route_publish=True,
        redis_publish=True,
        event_outbox_update=True,
        job_attempt_write=True,
    )


def _approvals(**overrides: bool) -> Any:
    values = {
        "analysis_requested_route_publish": False,
        "redis_publish": False,
        "event_outbox_update": False,
        "job_attempt_write": False,
    }
    values.update(overrides)
    return _module().PublishApprovals(**values)


def _fake_row(
    *,
    event_id: UUID | None = None,
    candidate_group_id: UUID | None = None,
    bundle_id: UUID | None = None,
    aggregate_type: str = "candidate_group",
    aggregate_id: UUID | None = None,
    status: str = "pending",
    event_type: str = "analysis.requested.v1",
    judge_profile: str | None = "github_primary",
    include_candidate_group_id: bool = True,
    include_bundle_id: bool = True,
    include_judge_profile: bool = True,
    dedupe_key: str = FAKE_DEDUPE_KEY,
) -> dict[str, Any]:
    event_id = event_id or uuid4()
    candidate_group_id = candidate_group_id or uuid4()
    bundle_id = bundle_id or uuid4()
    payload: dict[str, Any] = {
        "escalation_allowed": True,
        "source_text": FAKE_SOURCE_TEXT,
        "canonical_url": FAKE_URL,
    }
    if include_candidate_group_id:
        payload["candidate_group_id"] = str(candidate_group_id)
    if include_bundle_id:
        payload["bundle_id"] = str(bundle_id)
    if include_judge_profile and judge_profile is not None:
        payload["judge_profile"] = judge_profile
    return {
        "event_id": event_id,
        "event_type": event_type,
        "aggregate_type": aggregate_type,
        "aggregate_id": aggregate_id or candidate_group_id,
        "dedupe_key": dedupe_key,
        "payload_json": payload,
        "status": status,
        "fail_count": 0,
        "created_at": datetime.now(timezone.utc),
    }


def _session_for_row(
    row: dict[str, Any],
    *,
    current_bundle_id: UUID | None | object = None,
    bundle_candidate_group_id: UUID | None = None,
    ready_for_analysis: bool = True,
    member_count: int = 1,
    **session_kwargs: Any,
) -> FakeSession:
    candidate_group_id = UUID(str(row["payload_json"]["candidate_group_id"]))
    bundle_id = UUID(str(row["payload_json"]["bundle_id"]))
    if current_bundle_id is None:
        current_bundle_id = bundle_id
    bundle_candidate_group_id = bundle_candidate_group_id or candidate_group_id
    return FakeSession(
        [row],
        candidate_groups={str(candidate_group_id): current_bundle_id},  # type: ignore[dict-item]
        bundles={
            str(bundle_id): {
                "bundle_id": bundle_id,
                "candidate_group_id": bundle_candidate_group_id,
                "ready_for_analysis": ready_for_analysis,
            }
        },
        member_counts={str(bundle_id): member_count},
        **session_kwargs,
    )


def _run_report(
    *,
    row: dict[str, Any] | None = None,
    rows: list[dict[str, Any]] | None = None,
    approvals: Any | None = None,
    session: FakeSession | None = None,
    redis: FakeRedis | None = None,
    route_resolver: Any | None = None,
    side_effect_flags: dict[str, bool] | None = None,
) -> tuple[Any, FakeSession, FakeRedis]:
    module = _module()
    if rows is None and row is not None:
        rows = [row]
    if session is None:
        if row is not None:
            session = _session_for_row(row)
        else:
            session = FakeSession(rows or [])
    fake_redis = redis or FakeRedis()
    result = module.generate_report(
        runtime_env_path=FAKE_RUNTIME_PATH,
        approvals=approvals,
        runtime_env_reader=_runtime_env,
        database_session_factory=lambda _url: session,
        redis_client_factory=lambda _url: fake_redis,
        route_resolver=route_resolver,
        side_effect_flags=side_effect_flags,
        forbidden_raw_values=(FAKE_RUNTIME_PATH, FAKE_SENSITIVE_VALUE),
    )
    return result, session, fake_redis


def test_script_exists() -> None:
    assert SCRIPT.exists()


def test_default_mode_is_read_only_and_does_not_publish_update_or_write() -> None:
    row = _fake_row()
    result, session, redis = _run_report(row=row)

    assert result.exit_code == 0
    assert result.report["contract_status"] == _module().STATUS_READY
    assert result.report["runtime_env_read"] is True
    assert result.report["database_connected"] is True
    assert result.report["redis_connected"] is True
    assert result.report["read_only_transaction"] is True
    assert result.report["analysis_requested_pending_event_found_bucket"] == "one"
    assert result.report["redis_publish_planned_bucket"] == "one"
    assert result.report["redis_publish_attempted"] is False
    assert result.report["event_outbox_update_attempted"] is False
    assert result.report["job_attempt_write_attempted"] is False
    assert redis.xadd_calls == []
    assert redis.xlen_calls == ["q.analysis.route"]
    assert session.published_event_ids == []
    assert session.job_attempts == []
    assert session.rolled_back is True


def test_partial_approvals_block_before_db_or_redis_connections() -> None:
    row = _fake_row()
    session = _session_for_row(row)
    redis = FakeRedis()
    result, session, redis = _run_report(
        row=row,
        session=session,
        redis=redis,
        approvals=_approvals(
            analysis_requested_route_publish=True,
            redis_publish=True,
            event_outbox_update=True,
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


def test_uses_outbox_route_resolver_for_analysis_requested_route() -> None:
    row = _fake_row()
    result, _session, _redis = _run_report(row=row)
    text = SCRIPT.read_text(encoding="utf-8")

    assert result.exit_code == 0
    assert "OutboxRouteResolver()" in text
    assert result.report["redis_stream_name_bucket"] == "one"
    assert result.report["redis_thin_payload_valid_bucket"] == "one"


def test_route_mismatch_blocks_before_publish_or_write() -> None:
    row = _fake_row()
    result, session, redis = _run_report(
        row=row,
        approvals=_all_approvals(),
        route_resolver=MismatchedRouteResolver(),
    )

    assert result.exit_code == 1
    assert result.report["contract_status"] == _module().STATUS_ROUTE_MISMATCH
    assert "route.queue" in result.report["checks_failed"]
    assert redis.xadd_calls == []
    assert session.published_event_ids == []
    assert session.job_attempts == []


def test_redis_message_is_thin_and_has_exactly_allowed_fields() -> None:
    row = _fake_row()
    redis = FakeRedis()
    result, _session, redis = _run_report(
        row=row,
        approvals=_all_approvals(),
        redis=redis,
    )

    assert result.exit_code == 0
    stream_name, fields, _options = redis.xadd_calls[0]
    assert stream_name == "q.analysis.route"
    assert set(fields) == _module().ALLOWED_REDIS_THIN_FIELDS
    assert fields["job_id"] == str(row["event_id"])
    assert fields["stage_name"] == "analysis_route"
    assert fields["root_object_type"] == "candidate_group"
    assert fields["root_object_id"] == str(row["aggregate_id"])
    assert fields["idempotency_key"] == row["dedupe_key"]
    assert fields["pipeline_run_id"] == ""
    assert fields["not_before"] == ""
    assert fields["trigger_event_id"] == str(row["event_id"])


def test_missing_payload_fields_block_before_publish_or_write() -> None:
    row = _fake_row(include_bundle_id=False)
    session = FakeSession([row])
    redis = FakeRedis()
    result, session, redis = _run_report(row=row, session=session, redis=redis)

    assert result.exit_code == 1
    assert result.report["contract_status"] == _module().STATUS_INVALID_EVENT
    assert "payload.bundle_id" in result.report["checks_failed"]
    assert redis.xadd_calls == []
    assert session.published_event_ids == []
    assert session.job_attempts == []


def test_candidate_group_current_bundle_mismatch_blocks_before_publish_or_write() -> None:
    row = _fake_row()
    session = _session_for_row(row, current_bundle_id=uuid4())
    result, session, redis = _run_report(row=row, session=session)

    assert result.exit_code == 1
    assert result.report["contract_status"] == _module().STATUS_INVALID_BUNDLE
    assert result.report["candidate_group_current_bundle_match_bucket"] == "zero"
    assert "candidate_group.current_bundle_id" in result.report["checks_failed"]
    assert redis.xadd_calls == []
    assert session.published_event_ids == []
    assert session.job_attempts == []


def test_bundle_not_ready_blocks_before_publish_or_write() -> None:
    row = _fake_row()
    session = _session_for_row(row, ready_for_analysis=False)
    result, session, redis = _run_report(row=row, session=session)

    assert result.exit_code == 1
    assert result.report["contract_status"] == _module().STATUS_INVALID_BUNDLE
    assert result.report["bundle_ready_for_analysis_bucket"] == "zero"
    assert "bundle.ready_for_analysis" in result.report["checks_failed"]
    assert redis.xadd_calls == []
    assert session.published_event_ids == []
    assert session.job_attempts == []


def test_missing_candidate_evidence_members_block_before_publish_or_write() -> None:
    row = _fake_row()
    session = _session_for_row(row, member_count=0)
    result, session, redis = _run_report(row=row, session=session)

    assert result.exit_code == 1
    assert result.report["contract_status"] == _module().STATUS_INVALID_BUNDLE
    assert result.report["candidate_evidence_member_found_bucket"] == "zero"
    assert "bundle.candidate_evidence_members" in result.report["checks_failed"]
    assert redis.xadd_calls == []
    assert session.published_event_ids == []
    assert session.job_attempts == []


def test_invalid_judge_profile_blocks_before_publish_or_write() -> None:
    row = _fake_row(judge_profile="experimental_private_profile")
    session = _session_for_row(row)
    result, session, redis = _run_report(row=row, session=session)

    assert result.exit_code == 1
    assert result.report["contract_status"] == _module().STATUS_INVALID_JUDGE_PROFILE
    assert result.report["judge_profile_allowed_bucket"] == "zero"
    assert "judge_profile.allowed" in result.report["checks_failed"]
    assert redis.xadd_calls == []
    assert session.published_event_ids == []
    assert session.job_attempts == []


def test_approved_mode_publishes_then_marks_event_published_and_writes_job_attempt() -> None:
    order: list[str] = []
    row = _fake_row()
    session = _session_for_row(row, order=order)
    redis = FakeRedis(order=order)
    result, session, redis = _run_report(
        row=row,
        session=session,
        redis=redis,
        approvals=_all_approvals(),
    )

    assert result.exit_code == 0
    assert result.report["contract_status"] == _module().STATUS_PUBLISHED
    assert len(redis.xadd_calls) == 1
    assert session.published_event_ids == [row["event_id"]]
    assert len(session.job_attempts) == 1
    attempt = session.job_attempts[0]
    assert attempt["stage_name"] == "analysis_route"
    assert attempt["queue_name"] == "q.analysis.route"
    assert attempt["root_object_type"] == "candidate_group"
    assert attempt["root_object_id"] == str(row["aggregate_id"])
    assert attempt["attempt_status"] == "succeeded"
    assert order[:4] == [
        "redis:xadd",
        "db:update_published",
        "db:insert_job_attempt",
        "db:commit",
    ]


def test_redis_publish_failure_rolls_back_db_and_does_not_mark_event_published() -> None:
    row = _fake_row()
    session = _session_for_row(row)
    redis = FakeRedis(fail_xadd=True)
    result, session, redis = _run_report(
        row=row,
        session=session,
        redis=redis,
        approvals=_all_approvals(),
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


def test_db_write_failure_after_redis_publish_is_sanitized_and_starts_no_downstream() -> None:
    row = _fake_row()
    session = _session_for_row(row, raise_on_job_attempt=True)
    redis = FakeRedis()
    result, session, redis = _run_report(
        row=row,
        session=session,
        redis=redis,
        approvals=_all_approvals(),
    )
    rendered = json.dumps(result.report, sort_keys=True)

    assert result.exit_code == 1
    assert result.report["contract_status"] == _module().STATUS_DB_UPDATE_FAILED
    assert result.report["redis_publish_succeeded_bucket"] == "one"
    assert len(redis.xadd_calls) == 1
    assert session.rolled_back is True
    assert result.report["analysis_router_started"] is False
    assert result.report["judge_openai_started"] is False
    assert result.report["policy_engine_started"] is False
    assert result.report["notifier_started"] is False
    assert FAKE_SENSITIVE_VALUE not in rendered
    assert "job attempt failed" not in rendered


def test_no_analysis_router_judge_policy_or_notifier_starts() -> None:
    row = _fake_row()
    result, _session, _redis = _run_report(row=row)
    text = SCRIPT.read_text(encoding="utf-8")

    assert result.report["analysis_router_started"] is False
    assert result.report["judge_openai_started"] is False
    assert result.report["policy_engine_started"] is False
    assert result.report["notifier_started"] is False
    assert "src.services.analysis_router" not in text
    assert "src.services.judge_openai" not in text
    assert "src.services.policy_engine" not in text
    assert "src.services.notifier_telegram" not in text


def test_no_judge_runs_or_judge_call_requested_outbox_rows_are_written() -> None:
    row = _fake_row()
    result, session, _redis = _run_report(row=row, approvals=_all_approvals())
    all_sql = "\n".join(session.statements).lower()

    assert result.exit_code == 0
    assert result.report["judge_run_written_bucket"] == "zero"
    assert result.report["judge_call_requested_outbox_written_bucket"] == "zero"
    assert "insert into judge_runs" not in all_sql
    assert "judge.call.requested.v1" not in all_sql


def test_forbidden_side_effect_flags_block() -> None:
    result, _session, _redis = _run_report(
        row=_fake_row(),
        side_effect_flags={"external_network_attempted": True},
    )

    assert result.exit_code == 1
    assert result.report["contract_status"] == _module().STATUS_FORBIDDEN_SIDE_EFFECT
    assert "side_effect.forbidden" in result.report["checks_failed"]


def test_no_raw_values_emitted_including_ids_urls_credentials_stream_id_or_exception_body() -> None:
    event_id = uuid4()
    candidate_group_id = uuid4()
    bundle_id = uuid4()
    row = _fake_row(
        event_id=event_id,
        candidate_group_id=candidate_group_id,
        bundle_id=bundle_id,
        dedupe_key=FAKE_DEDUPE_KEY,
    )
    result, _session, _redis = _run_report(row=row, approvals=_all_approvals())
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
        FAKE_SENSITIVE_VALUE,
    )
    for value in forbidden_values:
        assert value not in rendered
    assert result.report["raw_values_emitted"] is False
