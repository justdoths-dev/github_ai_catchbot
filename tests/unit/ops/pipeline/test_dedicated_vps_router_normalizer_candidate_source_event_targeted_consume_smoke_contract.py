from __future__ import annotations

import importlib
import json
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from uuid import UUID, uuid4


ROOT = Path(__file__).resolve().parents[4]
SCRIPT = (
    ROOT
    / "scripts"
    / "ops"
    / "dedicated_vps_router_normalizer_candidate_source_event_targeted_consume_smoke.py"
)

FAKE_DATABASE_URL = (
    "postgresql+psycopg://github_ai_catchbot_app:"
    "unit-db-password-targeted-consume@127.0.0.1:5432/github_ai_catchbot"
)
FAKE_REDIS_URL = "redis://:unit-redis-password-targeted-consume@127.0.0.1:6379/0"
FAKE_RUNTIME_PATH = "/etc/github-ai-catchbot/private-runtime.env"
X_TEXT = "Candidate route https://x.com/openai/status/1234567890123456789"
X_URL = "https://x.com/openai/status/1234567890123456789"
WEAK_TEXT = "ai"
FAKE_DEDUPE_KEY = "private-source-event-dedupe-key"
FAKE_IDEMPOTENCY_KEY = "private-target-idempotency-key"


class FakeMappings:
    def __init__(self, rows: list[dict[str, Any]]) -> None:
        self._rows = rows

    def first(self) -> dict[str, Any] | None:
        return self._rows[0] if self._rows else None

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
        source_rows: list[dict[str, Any]],
        event_row: dict[str, Any] | None,
        read_only_value: str = "on",
        missing_tables: set[str] | None = None,
        existing_normalization_runs: int = 0,
        existing_candidate_groups: int = 0,
        normalization_run_rows: list[dict[str, Any]] | None = None,
        candidate_group_rows: list[dict[str, Any]] | None = None,
        candidate_member_rows: list[dict[str, Any]] | None = None,
        enrich_outbox_rows: list[dict[str, Any]] | None = None,
        artifact_observation_rows: list[dict[str, Any]] | None = None,
        suppression_trace_rows: list[dict[str, Any]] | None = None,
        proof_error_on: str | None = None,
        order: list[str] | None = None,
    ) -> None:
        self.source_rows = source_rows
        self.event_row = event_row
        self.read_only_value = read_only_value
        self.missing_tables = missing_tables or set()
        self.existing_normalization_runs = existing_normalization_runs
        self.existing_candidate_groups = existing_candidate_groups
        self.normalization_run_rows = normalization_run_rows or []
        self.candidate_group_rows = candidate_group_rows or []
        self.candidate_member_rows = candidate_member_rows or []
        self.enrich_outbox_rows = enrich_outbox_rows or []
        self.artifact_observation_rows = artifact_observation_rows or []
        self.suppression_trace_rows = suppression_trace_rows or []
        self.proof_error_on = proof_error_on
        self.order = order
        self.statements: list[str] = []
        self.params: list[dict[str, Any]] = []
        self.source_query_limits: list[int] = []
        self.written_tables: list[str] = []
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
        if "FROM source_messages sm LEFT JOIN LATERAL" in normalized:
            limit = int(params["limit"])
            self.source_query_limits.append(limit)
            return FakeResult(rows=self.source_rows[:limit])
        if "FROM event_outbox WHERE event_type = ANY" in normalized:
            return FakeResult(rows=[] if self.event_row is None else [self.event_row])
        if "FROM normalization_runs WHERE source_message_id" in normalized:
            self._record_proof("normalization_runs")
            self._maybe_raise_proof_error("normalization_runs")
            return FakeResult(scalar=self._count_normalization_runs(params))
        if "FROM candidate_group_proposals WHERE source_message_id" in normalized:
            self._record_proof("candidate_groups")
            self._maybe_raise_proof_error("candidate_groups")
            return FakeResult(scalar=self._count_candidate_groups(params))
        if "FROM event_outbox eo WHERE eo.event_type = 'artifact.enrich.requested.v1'" in normalized:
            self._record_proof("enrich_outbox")
            self._maybe_raise_proof_error("enrich_outbox")
            assert "payload_json->>'source_message_id'" not in normalized
            assert "payload_json->>'source_version_no'" not in normalized
            assert "eo.aggregate_type::text = 'candidate_group'" in normalized
            assert "payload_json->>'candidate_group_id'" in normalized
            assert "eo.aggregate_type::text = 'artifact'" in normalized
            assert "payload_json->>'artifact_id'" in normalized
            assert "candidate_group_members cgm" in normalized
            return FakeResult(scalar=self._count_enrich_outbox_for_candidate_groups(params))
        if "SELECT COUNT(DISTINCT artifact_id) FROM artifact_observations" in normalized:
            self._record_proof("artifacts")
            self._maybe_raise_proof_error("artifacts")
            return FakeResult(scalar=self._count_artifacts(params))
        if "FROM artifact_observations WHERE source_message_id" in normalized:
            self._record_proof("artifact_observations")
            self._maybe_raise_proof_error("artifact_observations")
            return FakeResult(scalar=self._count_artifact_observations(params))
        if "FROM candidate_group_members cgm JOIN candidate_group_proposals cgp" in normalized:
            self._record_proof("candidate_members")
            self._maybe_raise_proof_error("candidate_members")
            return FakeResult(scalar=self._count_candidate_members(params))
        if "FROM normalization_suppression_traces nst JOIN normalization_runs nr" in normalized:
            self._record_proof("suppression_traces")
            self._maybe_raise_proof_error("suppression_traces")
            return FakeResult(scalar=self._count_suppression_traces(params))

        raise AssertionError(f"unexpected SQL: {statement}")

    def _record_proof(self, name: str) -> None:
        if self.order is not None:
            self.order.append(f"proof:{name}")

    def _maybe_raise_proof_error(self, name: str) -> None:
        if self.proof_error_on == name:
            raise RuntimeError("proof query failed with private DB detail")

    def _source_version_for(self, source_message_id: UUID) -> int:
        source_message_text = str(source_message_id)
        for row in self.source_rows:
            if str(row["source_message_id"]) == source_message_text:
                return int(row.get("version_no") or row["current_version_no"])
        return 1

    def _count_normalization_runs(self, params: dict[str, Any]) -> int:
        source_message_id = str(params["source_message_id"])
        source_version_no = int(params["source_version_no"])
        normalizer_version = str(params["normalizer_version"])
        row_count = sum(
            1
            for row in self.normalization_run_rows
            if str(row["source_message_id"]) == source_message_id
            and int(row["source_version_no"]) == source_version_no
            and str(row["normalizer_version"]) == normalizer_version
        )
        return self.existing_normalization_runs + row_count

    def _matching_candidate_group_ids(self, params: dict[str, Any]) -> set[str]:
        source_message_id = str(params["source_message_id"])
        source_version_no = int(params["source_version_no"])
        return {
            str(row["candidate_group_id"])
            for row in self.candidate_group_rows
            if str(row["source_message_id"]) == source_message_id
            and int(row["source_version_no"]) == source_version_no
        }

    def _count_candidate_groups(self, params: dict[str, Any]) -> int:
        return self.existing_candidate_groups + len(self._matching_candidate_group_ids(params))

    def _matching_member_artifact_ids(self, params: dict[str, Any]) -> set[str]:
        candidate_group_ids = self._matching_candidate_group_ids(params)
        return {
            str(row["artifact_id"])
            for row in self.candidate_member_rows
            if str(row["candidate_group_id"]) in candidate_group_ids
        }

    def _count_enrich_outbox_for_candidate_groups(self, params: dict[str, Any]) -> int:
        candidate_group_ids = self._matching_candidate_group_ids(params)
        member_artifact_ids = self._matching_member_artifact_ids(params)
        count = 0
        for row in self.enrich_outbox_rows:
            payload = row.get("payload_json") or {}
            if str(row.get("event_type")) != "artifact.enrich.requested.v1":
                continue
            aggregate_match = (
                str(row.get("aggregate_type")) == "candidate_group"
                and str(row.get("aggregate_id")) in candidate_group_ids
            )
            payload_match = str(payload.get("candidate_group_id")) in candidate_group_ids
            artifact_aggregate_match = (
                str(row.get("aggregate_type")) == "artifact"
                and str(row.get("aggregate_id")) in member_artifact_ids
            )
            artifact_payload_match = str(payload.get("artifact_id")) in member_artifact_ids
            if aggregate_match or payload_match or artifact_aggregate_match or artifact_payload_match:
                count += 1
        return count

    def _matching_artifact_observations(self, params: dict[str, Any]) -> list[dict[str, Any]]:
        source_message_id = str(params["source_message_id"])
        source_version_no = int(params["source_version_no"])
        return [
            row
            for row in self.artifact_observation_rows
            if str(row["source_message_id"]) == source_message_id
            and int(row["source_version_no"]) == source_version_no
        ]

    def _count_artifacts(self, params: dict[str, Any]) -> int:
        artifact_ids = {
            str(row["artifact_id"])
            for row in self._matching_artifact_observations(params)
        }
        return len(artifact_ids)

    def _count_artifact_observations(self, params: dict[str, Any]) -> int:
        return len(self._matching_artifact_observations(params))

    def _count_candidate_members(self, params: dict[str, Any]) -> int:
        candidate_group_ids = self._matching_candidate_group_ids(params)
        return sum(
            1
            for row in self.candidate_member_rows
            if str(row["candidate_group_id"]) in candidate_group_ids
        )

    def _count_suppression_traces(self, params: dict[str, Any]) -> int:
        source_message_id = str(params["source_message_id"])
        source_version_no = int(params["source_version_no"])
        normalizer_version = str(params["normalizer_version"])
        matching_run_ids = {
            str(row["normalization_run_id"])
            for row in self.normalization_run_rows
            if str(row["source_message_id"]) == source_message_id
            and int(row["source_version_no"]) == source_version_no
            and str(row["normalizer_version"]) == normalizer_version
        }
        return sum(
            1
            for row in self.suppression_trace_rows
            if str(row["normalization_run_id"]) in matching_run_ids
        )

    async def commit(self) -> None:
        self.committed = True
        if self.order is not None:
            self.order.append("commit")

    async def rollback(self) -> None:
        self.rolled_back = True
        if self.order is not None:
            self.order.append("rollback")

    async def close(self) -> None:
        self.closed = True

    def record_write(self, table: str) -> None:
        self.written_tables.append(table)
        if self.order is not None:
            self.order.append(f"write:{table}")


class FakeRedis:
    def __init__(
        self,
        *,
        entries: list[tuple[str, dict[str, Any]]],
        delivered_override: list[tuple[str, dict[str, Any]]] | None = None,
        ack_error: Exception | None = None,
        order: list[str] | None = None,
    ) -> None:
        self.entries = entries
        self.delivered_override = delivered_override
        self.ack_error = ack_error
        self.order = order
        self.group_created = False
        self.group_start_id: str | None = None
        self.group_destroyed = False
        self.acked: list[str] = []
        self.closed = False

    async def ping(self) -> bool:
        return True

    async def xlen(self, name: str) -> int:
        assert name == "q.source.normalize"
        return len(self.entries)

    async def xrange(
        self,
        name: str,
        min: str = "-",
        max: str = "+",
        count: int | None = None,
    ) -> list[tuple[str, dict[str, Any]]]:
        assert name == "q.source.normalize"
        return list(self.entries[:count])

    async def xrevrange(
        self,
        name: str,
        max: str = "+",
        min: str = "-",
        count: int | None = None,
    ) -> list[tuple[str, dict[str, Any]]]:
        assert name == "q.source.normalize"
        return list(reversed(self.entries))[:count]

    async def xgroup_create(
        self,
        name: str,
        groupname: str,
        id: str = "$",
        mkstream: bool = False,
    ) -> None:
        assert name == "q.source.normalize"
        assert mkstream is False
        self.group_created = True
        self.group_start_id = id

    async def xreadgroup(
        self,
        groupname: str,
        consumername: str,
        streams: dict[str, str],
        count: int | None = None,
        block: int | None = None,
    ) -> list[tuple[str, list[tuple[str, dict[str, Any]]]]]:
        assert self.group_created is True
        assert streams == {"q.source.normalize": ">"}
        if self.delivered_override is not None:
            return [("q.source.normalize", self.delivered_override)]
        assert self.group_start_id is not None
        for stream_id, fields in self.entries:
            if _stream_gt(stream_id, self.group_start_id):
                return [("q.source.normalize", [(stream_id, dict(fields))])]
        return []

    async def xack(self, name: str, groupname: str, *ids: str) -> int:
        assert name == "q.source.normalize"
        if self.order is not None:
            self.order.append("ack")
        if self.ack_error is not None:
            raise self.ack_error
        self.acked.extend(ids)
        return len(ids)

    async def xgroup_destroy(self, name: str, groupname: str) -> int:
        assert name == "q.source.normalize"
        self.group_destroyed = True
        return 1

    async def aclose(self) -> None:
        self.closed = True


def _module():
    return importlib.import_module(
        "scripts.ops.dedicated_vps_router_normalizer_candidate_source_event_targeted_consume_smoke"
    )


def _normalize(statement: str) -> str:
    return " ".join(statement.strip().split())


def _stream_tuple(stream_id: str) -> tuple[int, int]:
    left, right = stream_id.split("-", 1)
    return int(left), int(right)


def _stream_gt(left: str, right: str) -> bool:
    return _stream_tuple(left) > _stream_tuple(right)


def _runtime_env(_path: str | Path) -> dict[str, str]:
    return {
        "DATABASE_URL": FAKE_DATABASE_URL,
        "REDIS_URL": FAKE_REDIS_URL,
    }


def _all_approvals() -> Any:
    module = _module()
    return module.ConsumeApprovals(
        targeted_router_normalizer_consume_smoke=True,
        redis_targeted_consumer_group=True,
        normalization_write=True,
        artifact_candidate_write=True,
        event_outbox_write=True,
        targeted_redis_ack=True,
    )


def _approvals(**overrides: bool) -> Any:
    values = {
        "targeted_router_normalizer_consume_smoke": False,
        "redis_targeted_consumer_group": False,
        "normalization_write": False,
        "artifact_candidate_write": False,
        "event_outbox_write": False,
        "targeted_redis_ack": False,
    }
    values.update(overrides)
    return _module().ConsumeApprovals(**values)


def _source_row(
    *,
    source_message_id: UUID,
    text: str = X_TEXT,
    include_version: bool = True,
) -> dict[str, Any]:
    return {
        "source_message_id": source_message_id,
        "current_version_no": 1,
        "text_body": text,
        "caption_text": None,
        "text_surface": text,
        "entities_json": [],
        "url_surface_json": [],
        "raw_message_json": {"private_text": text},
        "deleted_at": None,
        "version_no": 1 if include_version else None,
        "version_text_surface": text if include_version else None,
        "version_entities_json": [] if include_version else None,
        "version_raw_message_json": {"private_version_text": text} if include_version else None,
    }


def _event_row(*, event_id: UUID, source_message_id: UUID) -> dict[str, Any]:
    now = datetime.now(timezone.utc)
    return {
        "event_id": event_id,
        "event_type": "source_message.created.v1",
        "aggregate_type": "source_message",
        "aggregate_id": source_message_id,
        "dedupe_key": FAKE_DEDUPE_KEY,
        "payload_json": {
            "event_id": str(event_id),
            "source_message_id": str(source_message_id),
            "current_version_no": 1,
            "message_text": X_TEXT,
        },
        "status": "published",
        "created_at": now,
        "published_at": now,
    }


def _candidate_group_row(
    *,
    candidate_group_id: UUID,
    source_message_id: UUID,
    source_version_no: int = 1,
) -> dict[str, Any]:
    return {
        "candidate_group_id": candidate_group_id,
        "source_message_id": source_message_id,
        "source_version_no": source_version_no,
    }


def _normalization_run_row(
    *,
    normalization_run_id: UUID,
    source_message_id: UUID,
    source_version_no: int = 1,
    normalizer_version: str | None = None,
) -> dict[str, Any]:
    return {
        "normalization_run_id": normalization_run_id,
        "source_message_id": source_message_id,
        "source_version_no": source_version_no,
        "normalizer_version": normalizer_version or _module().DEFAULT_NORMALIZER_VERSION,
    }


def _candidate_member_row(*, candidate_group_id: UUID, artifact_id: UUID) -> dict[str, Any]:
    return {
        "candidate_group_id": candidate_group_id,
        "artifact_id": artifact_id,
    }


def _enrich_outbox_row(
    *,
    candidate_group_id: UUID,
    artifact_id: UUID | None = None,
    aggregate_shape: str = "candidate_group",
    payload_shape: str = "candidate_group",
) -> dict[str, Any]:
    artifact_id = artifact_id or uuid4()
    if aggregate_shape == "artifact":
        aggregate_type = "artifact"
        aggregate_id = artifact_id
    else:
        aggregate_type = "candidate_group"
        aggregate_id = candidate_group_id
    payload: dict[str, Any] = {
        "artifact_type": "x_post",
        "provider_route": "x",
    }
    if payload_shape == "artifact":
        payload["artifact_id"] = str(artifact_id)
    elif payload_shape == "both":
        payload["candidate_group_id"] = str(candidate_group_id)
        payload["artifact_id"] = str(artifact_id)
    else:
        payload["candidate_group_id"] = str(candidate_group_id)
    return {
        "event_type": "artifact.enrich.requested.v1",
        "aggregate_type": aggregate_type,
        "aggregate_id": aggregate_id,
        "payload_json": payload,
    }


def _artifact_observation_row(
    *,
    source_message_id: UUID,
    source_version_no: int = 1,
    artifact_id: UUID | None = None,
) -> dict[str, Any]:
    return {
        "artifact_id": artifact_id or uuid4(),
        "source_message_id": source_message_id,
        "source_version_no": source_version_no,
    }


def _thin_fields(*, event_id: UUID, source_message_id: UUID, **overrides: Any) -> dict[str, Any]:
    fields: dict[str, Any] = {
        "job_id": str(event_id),
        "stage_name": "normalize",
        "root_object_type": "source_message",
        "root_object_id": str(source_message_id),
        "idempotency_key": FAKE_IDEMPOTENCY_KEY,
        "pipeline_run_id": "",
        "not_before": "",
        "trigger_event_id": str(event_id),
    }
    fields.update(overrides)
    return fields


def _candidate_result() -> SimpleNamespace:
    return _candidate_doc_result()


def _candidate_doc_result() -> SimpleNamespace:
    return SimpleNamespace(
        candidate_eligible=True,
        proposals_created=99,
        observations_created=99,
        enrich_events_created=99,
        suppression_reason_codes=[],
    )


def _candidate_local_result() -> SimpleNamespace:
    return SimpleNamespace(
        candidate_eligible=True,
        artifact_count=99,
        candidate_group_count=99,
        suppression_reason_codes=[],
    )


def _write_fake_candidate_flow(
    session: FakeSession,
    *,
    source_message_id: UUID,
    source_version_no: int | None = None,
    aggregate_shape: str = "artifact",
    payload_shape: str = "artifact",
) -> None:
    version_no = source_version_no or session._source_version_for(source_message_id)
    normalization_run_id = uuid4()
    candidate_group_id = uuid4()
    artifact_id = uuid4()
    session.normalization_run_rows.append(
        _normalization_run_row(
            normalization_run_id=normalization_run_id,
            source_message_id=source_message_id,
            source_version_no=version_no,
        )
    )
    session.artifact_observation_rows.append(
        _artifact_observation_row(
            source_message_id=source_message_id,
            source_version_no=version_no,
            artifact_id=artifact_id,
        )
    )
    session.candidate_group_rows.append(
        _candidate_group_row(
            candidate_group_id=candidate_group_id,
            source_message_id=source_message_id,
            source_version_no=version_no,
        )
    )
    session.candidate_member_rows.append(
        _candidate_member_row(candidate_group_id=candidate_group_id, artifact_id=artifact_id)
    )
    session.enrich_outbox_rows.append(
        _enrich_outbox_row(
            candidate_group_id=candidate_group_id,
            artifact_id=artifact_id,
            aggregate_shape=aggregate_shape,
            payload_shape=payload_shape,
        )
    )


def _run_report(
    *,
    source_rows: list[dict[str, Any]] | None = None,
    event_row: dict[str, Any] | None | object = ...,
    redis_entries: list[tuple[str, dict[str, Any]]] | None = None,
    approvals: Any | None = None,
    session: FakeSession | None = None,
    redis: FakeRedis | None = None,
    normalizer_runner: Any | None = None,
    side_effect_flags: dict[str, bool] | None = None,
    forbidden_raw_values: tuple[str, ...] = (),
) -> tuple[Any, FakeSession, FakeRedis, UUID, UUID]:
    event_id = uuid4()
    source_message_id = uuid4()
    rows = source_rows if source_rows is not None else [_source_row(source_message_id=source_message_id)]
    effective_event = (
        _event_row(event_id=event_id, source_message_id=source_message_id)
        if event_row is ...
        else event_row
    )
    entries = redis_entries
    if entries is None:
        entries = [
            (
                "1710000000000-0",
                _thin_fields(event_id=event_id, source_message_id=source_message_id),
            )
        ]
    fake_session = session or FakeSession(
        source_rows=rows,
        event_row=effective_event,
    )
    fake_redis = redis or FakeRedis(entries=entries)
    result = _module().generate_report(
        runtime_env_path=FAKE_RUNTIME_PATH,
        runtime_env_reader=_runtime_env,
        database_session_factory=lambda _url: fake_session,
        redis_client_factory=lambda _url: fake_redis,
        approvals=approvals,
        normalizer_runner=normalizer_runner or _default_runner(_candidate_result()),
        side_effect_flags=side_effect_flags,
        forbidden_raw_values=forbidden_raw_values,
    )
    return result, fake_session, fake_redis, event_id, source_message_id


def _default_runner(result: Any):
    async def runner(_config: Any, _message: Any, session: FakeSession) -> Any:
        session.record_write("normalization_runs")
        _write_fake_candidate_flow(
            session,
            source_message_id=UUID(_message.root_object_id),
            aggregate_shape="artifact",
            payload_shape="artifact",
        )
        return result

    return runner


def test_script_exists() -> None:
    assert SCRIPT.exists()


def test_default_mode_correlates_candidate_event_and_target_stream_read_only() -> None:
    result, session, redis, _event_id, _source_message_id = _run_report()

    assert result.exit_code == 0
    assert result.report["contract_status"] == (
        "router_normalizer_candidate_source_event_targeted_consume_smoke_ready"
    )
    assert result.report["runtime_env_read"] is True
    assert result.report["database_connected"] is True
    assert result.report["redis_connected"] is True
    assert result.report["read_only_transaction"] is True
    assert result.report["candidate_source_selected_bucket"] == "one"
    assert result.report["published_source_event_selected_bucket"] == "one"
    assert result.report["target_stream_entry_found_bucket"] == "one"
    assert result.report["redis_group_mutation_attempted"] is False
    assert result.report["normalization_write_attempted"] is False
    assert result.report["redis_ack_attempted"] is False
    assert redis.group_created is False
    assert redis.acked == []
    assert session.written_tables == []
    assert session.rolled_back is True


def test_partial_approvals_fail_before_group_write_or_ack() -> None:
    result, session, redis, _event_id, _source_message_id = _run_report(
        approvals=_approvals(
            targeted_router_normalizer_consume_smoke=True,
            normalization_write=True,
            artifact_candidate_write=True,
            event_outbox_write=True,
        )
    )

    assert result.exit_code == 1
    assert "approval.redis_targeted_consumer_group" in result.report["checks_failed"]
    assert "approval.targeted_redis_ack" in result.report["checks_failed"]
    assert result.report["redis_group_mutation_attempted"] is False
    assert result.report["normalization_write_attempted"] is False
    assert result.report["redis_ack_attempted"] is False
    assert redis.group_created is False
    assert redis.acked == []
    assert session.written_tables == []


def test_no_candidate_source_blocks() -> None:
    result, _session, redis, _event_id, _source_message_id = _run_report(
        source_rows=[_source_row(source_message_id=uuid4(), text=WEAK_TEXT)]
    )

    assert result.exit_code == 1
    assert result.report["contract_status"] == (
        "blocked_router_normalizer_candidate_source_event_targeted_consume_smoke_no_candidate_source"
    )
    assert result.report["candidate_source_selected_bucket"] == "zero"
    assert redis.group_created is False


def test_candidate_source_without_published_source_event_blocks() -> None:
    result, _session, redis, _event_id, _source_message_id = _run_report(event_row=None)

    assert result.exit_code == 1
    assert result.report["contract_status"] == (
        "blocked_router_normalizer_candidate_source_event_targeted_consume_smoke_no_published_source_event"
    )
    assert result.report["published_source_event_selected_bucket"] == "zero"
    assert redis.group_created is False


def test_published_event_without_matching_redis_trigger_blocks() -> None:
    event_id = uuid4()
    source_message_id = uuid4()
    result, _session, redis, _event_id, _source_message_id = _run_report(
        source_rows=[_source_row(source_message_id=source_message_id)],
        event_row=_event_row(event_id=event_id, source_message_id=source_message_id),
        redis_entries=[
            (
                "1710000000000-0",
                _thin_fields(event_id=uuid4(), source_message_id=source_message_id),
            )
        ]
    )

    assert result.exit_code == 1
    assert result.report["contract_status"] == (
        "blocked_router_normalizer_candidate_source_event_targeted_consume_smoke_no_target_stream_entry"
    )
    assert result.report["target_stream_entry_found_bucket"] == "zero"
    assert redis.group_created is False


def test_invalid_thin_stream_shape_blocks() -> None:
    event_id = uuid4()
    source_message_id = uuid4()
    fields = _thin_fields(event_id=event_id, source_message_id=source_message_id)
    fields.pop("not_before")
    fields["payload_json"] = "{}"
    result, _session, redis, _event_id, _source_message_id = _run_report(
        source_rows=[_source_row(source_message_id=source_message_id)],
        event_row=_event_row(event_id=event_id, source_message_id=source_message_id),
        redis_entries=[("1710000000000-0", fields)],
    )

    assert result.exit_code == 1
    assert "redis.thin_payload_shape" in result.report["checks_failed"]
    assert result.report["target_stream_shape_valid_bucket"] == "zero"
    assert redis.group_created is False


def test_stage_and_root_mismatch_block() -> None:
    event_id = uuid4()
    source_message_id = uuid4()
    fields = _thin_fields(
        event_id=event_id,
        source_message_id=uuid4(),
        stage_name="enrich",
        root_object_type="artifact",
    )
    result, _session, redis, _event_id, _source_message_id = _run_report(
        source_rows=[_source_row(source_message_id=source_message_id)],
        event_row=_event_row(event_id=event_id, source_message_id=source_message_id),
        redis_entries=[("1710000000000-0", fields)],
    )

    assert result.exit_code == 1
    assert "redis.stage_name_mismatch" in result.report["checks_failed"]
    assert "redis.root_object_type_mismatch" in result.report["checks_failed"]
    assert result.report["target_stream_stage_valid_bucket"] == "zero"
    assert result.report["target_stream_root_valid_bucket"] == "zero"
    assert redis.group_created is False


def test_targeted_delivery_mismatch_blocks_without_db_write_or_ack() -> None:
    event_id = uuid4()
    source_message_id = uuid4()
    redis = FakeRedis(
        entries=[
            (
                "1710000000000-0",
                _thin_fields(event_id=event_id, source_message_id=source_message_id),
            )
        ],
        delivered_override=[
            (
                "1710000000001-0",
                _thin_fields(event_id=uuid4(), source_message_id=source_message_id),
            )
        ],
    )
    result, session, redis, _event_id, _source_message_id = _run_report(
        source_rows=[_source_row(source_message_id=source_message_id)],
        event_row=_event_row(event_id=event_id, source_message_id=source_message_id),
        redis=redis,
        approvals=_all_approvals(),
    )

    assert result.exit_code == 1
    assert result.report["contract_status"] == (
        "blocked_router_normalizer_candidate_source_event_targeted_consume_smoke_target_delivery_mismatch"
    )
    assert result.report["normalization_write_attempted"] is False
    assert result.report["redis_ack_attempted"] is False
    assert session.written_tables == []
    assert redis.acked == []
    assert redis.group_destroyed is True


def test_approved_mode_consumes_target_entry_not_older_unrelated_entry() -> None:
    event_id = uuid4()
    source_message_id = uuid4()
    older_event_id = uuid4()
    entries = [
        ("100-0", _thin_fields(event_id=older_event_id, source_message_id=uuid4())),
        ("101-0", _thin_fields(event_id=event_id, source_message_id=source_message_id)),
    ]
    result, _session, redis, _event_id, _source_message_id = _run_report(
        source_rows=[_source_row(source_message_id=source_message_id)],
        event_row=_event_row(event_id=event_id, source_message_id=source_message_id),
        redis_entries=entries,
        approvals=_all_approvals(),
    )

    assert result.exit_code == 0
    assert result.report["contract_status"] == (
        "router_normalizer_candidate_source_event_targeted_consume_smoke_consumed"
    )
    assert redis.acked == ["101-0"]
    assert "100-0" not in redis.acked


def test_approved_mode_uses_db_proof_for_doc_style_result_fields() -> None:
    async def runner(_config: Any, message: Any, session: FakeSession) -> Any:
        session.record_write("normalization_runs")
        _write_fake_candidate_flow(
            session,
            source_message_id=UUID(message.root_object_id),
            aggregate_shape="candidate_group",
            payload_shape="candidate_group",
        )
        return _candidate_doc_result()

    result, _session, redis, _event_id, _source_message_id = _run_report(
        approvals=_all_approvals(),
        normalizer_runner=runner,
    )

    assert result.exit_code == 0
    assert result.report["candidate_plan_x_route_bucket"] == "one"
    assert result.report["normalization_runs_written_bucket"] == "one"
    assert result.report["artifacts_written_bucket"] == "one"
    assert result.report["artifact_observations_written_bucket"] == "one"
    assert result.report["candidate_groups_written_bucket"] == "one"
    assert result.report["candidate_members_written_bucket"] == "one"
    assert result.report["enrich_outbox_events_written_bucket"] == "one"
    assert redis.acked == ["1710000000000-0"]


def test_approved_mode_uses_db_proof_for_current_local_result_fields() -> None:
    result, _session, redis, _event_id, _source_message_id = _run_report(
        approvals=_all_approvals(),
        normalizer_runner=_default_runner(_candidate_local_result()),
    )

    assert result.exit_code == 0
    assert result.report["contract_status"] == (
        "router_normalizer_candidate_source_event_targeted_consume_smoke_consumed"
    )
    assert result.report["normalization_runs_written_bucket"] == "one"
    assert result.report["artifacts_written_bucket"] == "one"
    assert result.report["artifact_observations_written_bucket"] == "one"
    assert result.report["candidate_groups_written_bucket"] == "one"
    assert result.report["candidate_members_written_bucket"] == "one"
    assert result.report["enrich_outbox_events_written_bucket"] == "one"
    assert redis.acked == ["1710000000000-0"]


def test_db_proof_happens_before_commit_and_redis_ack_happens_after_commit() -> None:
    order: list[str] = []
    event_id = uuid4()
    source_message_id = uuid4()
    session = FakeSession(
        source_rows=[_source_row(source_message_id=source_message_id)],
        event_row=_event_row(event_id=event_id, source_message_id=source_message_id),
        order=order,
    )
    redis = FakeRedis(
        entries=[
            (
                "1710000000000-0",
                _thin_fields(event_id=event_id, source_message_id=source_message_id),
            )
        ],
        order=order,
    )
    result, session, redis, _event_id, _source_message_id = _run_report(
        session=session,
        redis=redis,
        approvals=_all_approvals(),
    )

    assert result.exit_code == 0
    assert "proof:normalization_runs" in order
    assert "proof:enrich_outbox" in order
    assert order.index("write:normalization_runs") < order.index("proof:normalization_runs")
    assert all(
        order.index(marker) < order.index("commit")
        for marker in order
        if marker.startswith("proof:")
    )
    assert order.index("commit") < order.index("ack")
    assert order.count("commit") == 1
    assert session.committed is True
    assert redis.acked == ["1710000000000-0"]


def test_db_proof_failure_rolls_back_before_commit_and_does_not_ack() -> None:
    event_id = uuid4()
    source_message_id = uuid4()
    session = FakeSession(
        source_rows=[_source_row(source_message_id=source_message_id)],
        event_row=_event_row(event_id=event_id, source_message_id=source_message_id),
        proof_error_on="enrich_outbox",
    )
    redis = FakeRedis(
        entries=[
            (
                "1710000000000-0",
                _thin_fields(event_id=event_id, source_message_id=source_message_id),
            )
        ]
    )
    result, session, redis, _event_id, _source_message_id = _run_report(
        session=session,
        redis=redis,
        approvals=_all_approvals(),
        forbidden_raw_values=("private DB detail",),
    )
    rendered = json.dumps(result.report, sort_keys=True)

    assert result.exit_code == 1
    assert "database.write_proof_or_commit" in result.report["checks_failed"]
    assert session.rolled_back is True
    assert session.committed is False
    assert result.report["redis_ack_attempted"] is False
    assert redis.acked == []
    assert "private DB detail" not in rendered


def test_db_write_failure_rolls_back_and_does_not_ack() -> None:
    async def failing_runner(_config: Any, _message: Any, _session: Any) -> Any:
        raise RuntimeError("db write failed with private raw text")

    result, session, redis, _event_id, _source_message_id = _run_report(
        approvals=_all_approvals(),
        normalizer_runner=failing_runner,
        forbidden_raw_values=("private raw text",),
    )
    rendered = json.dumps(result.report, sort_keys=True)

    assert result.exit_code == 1
    assert "database.normalizer_write" in result.report["checks_failed"]
    assert session.rolled_back is True
    assert session.committed is False
    assert result.report["redis_ack_attempted"] is False
    assert redis.acked == []
    assert "private raw text" not in rendered


def test_redis_ack_failure_after_commit_is_sanitized() -> None:
    redis = FakeRedis(
        entries=[],
        ack_error=RuntimeError("ack failed with private ack text"),
    )
    result, session, redis, event_id, source_message_id = _run_report(
        redis=redis,
        approvals=_all_approvals(),
        forbidden_raw_values=("private ack text",),
    )
    redis.entries = [
        (
            "1710000000000-0",
            _thin_fields(event_id=event_id, source_message_id=source_message_id),
        )
    ]
    result, session, redis, _event_id, _source_message_id = _run_report(
        source_rows=[_source_row(source_message_id=source_message_id)],
        event_row=_event_row(event_id=event_id, source_message_id=source_message_id),
        redis=redis,
        approvals=_all_approvals(),
        forbidden_raw_values=("private ack text",),
    )
    rendered = json.dumps(result.report, sort_keys=True)

    assert result.exit_code == 1
    assert session.committed is True
    assert result.report["redis_ack_attempted"] is True
    assert result.report["redis_ack_succeeded_bucket"] == "zero"
    assert result.report["redis_ack_failure_class"] == "RuntimeError"
    assert "private ack text" not in rendered
    assert "ack failed with" not in rendered


def test_already_consumed_path_does_not_duplicate_rows() -> None:
    event_id = uuid4()
    source_message_id = uuid4()
    candidate_group_id = uuid4()
    session = FakeSession(
        source_rows=[_source_row(source_message_id=source_message_id)],
        event_row=_event_row(event_id=event_id, source_message_id=source_message_id),
        existing_normalization_runs=1,
        candidate_group_rows=[
            _candidate_group_row(
                candidate_group_id=candidate_group_id,
                source_message_id=source_message_id,
            )
        ],
        enrich_outbox_rows=[_enrich_outbox_row(candidate_group_id=candidate_group_id)],
    )
    redis = FakeRedis(entries=[])
    result, session, redis, _event_id, _source_message_id = _run_report(
        session=session,
        redis=redis,
        approvals=_all_approvals(),
    )

    assert result.exit_code == 0
    assert result.report["contract_status"] == (
        "router_normalizer_candidate_source_event_targeted_consume_smoke_already_consumed"
    )
    assert result.report["normalization_write_attempted"] is False
    assert result.report["redis_ack_attempted"] is False
    assert session.written_tables == []
    assert redis.group_created is False
    enrich_query = next(
        statement
        for statement in session.statements
        if "artifact.enrich.requested.v1" in statement
    )
    assert "candidate_group_proposals cgp" in enrich_query
    assert "eo.aggregate_type::text = 'candidate_group'" in enrich_query
    assert "eo.aggregate_id IN" in enrich_query
    assert "SELECT cgp.candidate_group_id" in enrich_query
    assert "payload_json->>'source_message_id'" not in enrich_query
    assert "payload_json->>'source_version_no'" not in enrich_query


def test_already_consumed_path_detects_artifact_aggregate_enrich_outbox() -> None:
    event_id = uuid4()
    source_message_id = uuid4()
    candidate_group_id = uuid4()
    artifact_id = uuid4()
    session = FakeSession(
        source_rows=[_source_row(source_message_id=source_message_id)],
        event_row=_event_row(event_id=event_id, source_message_id=source_message_id),
        existing_normalization_runs=1,
        candidate_group_rows=[
            _candidate_group_row(
                candidate_group_id=candidate_group_id,
                source_message_id=source_message_id,
            )
        ],
        candidate_member_rows=[
            _candidate_member_row(candidate_group_id=candidate_group_id, artifact_id=artifact_id)
        ],
        enrich_outbox_rows=[
            _enrich_outbox_row(
                candidate_group_id=candidate_group_id,
                artifact_id=artifact_id,
                aggregate_shape="artifact",
                payload_shape="artifact",
            )
        ],
    )
    redis = FakeRedis(entries=[])
    result, session, redis, _event_id, _source_message_id = _run_report(
        session=session,
        redis=redis,
        approvals=_all_approvals(),
    )

    assert result.exit_code == 0
    assert result.report["contract_status"] == (
        "router_normalizer_candidate_source_event_targeted_consume_smoke_already_consumed"
    )
    assert result.report["normalization_write_attempted"] is False
    assert result.report["redis_ack_attempted"] is False
    assert session.written_tables == []
    assert redis.group_created is False
    enrich_query = next(
        statement
        for statement in session.statements
        if "artifact.enrich.requested.v1" in statement
    )
    assert "eo.aggregate_type::text = 'artifact'" in enrich_query
    assert "payload_json->>'artifact_id'" in enrich_query
    assert "payload_json->>'source_message_id'" not in enrich_query
    assert "payload_json->>'source_version_no'" not in enrich_query


def test_source_telegram_and_registry_mutation_flags_are_forbidden() -> None:
    result, session, redis, _event_id, _source_message_id = _run_report(
        side_effect_flags={
            "source_tables_mutation_performed": True,
            "telegram_raw_updates_mutation_performed": True,
            "registry_mutation_performed": True,
        }
    )

    assert result.exit_code == 1
    assert result.report["contract_status"] == "blocked_forbidden_side_effect_detected"
    assert "side_effect.forbidden" in result.report["checks_failed"]
    assert result.report["runtime_env_read"] is False
    assert session.statements == []
    assert redis.group_created is False


def test_downstream_service_and_platform_side_effect_flags_stay_false() -> None:
    result, _session, _redis, _event_id, _source_message_id = _run_report()

    assert result.exit_code == 0
    assert result.report["downstream_service_started"] is False
    assert result.report["external_network_attempted"] is False
    assert result.report["docker_or_systemd_changed"] is False
    assert result.report["alembic_run"] is False


def test_report_does_not_emit_raw_values() -> None:
    result, _session, _redis, event_id, source_message_id = _run_report(
        approvals=_all_approvals(),
        forbidden_raw_values=(FAKE_RUNTIME_PATH, FAKE_DATABASE_URL, FAKE_REDIS_URL),
    )
    rendered = json.dumps(result.report, sort_keys=True)
    forbidden = (
        str(event_id),
        str(source_message_id),
        FAKE_DATABASE_URL,
        FAKE_REDIS_URL,
        "unit-db-password-targeted-consume",
        "unit-redis-password-targeted-consume",
        FAKE_RUNTIME_PATH,
        X_TEXT,
        X_URL,
        FAKE_DEDUPE_KEY,
        FAKE_IDEMPOTENCY_KEY,
        "1710000000000-0",
    )
    for value in forbidden:
        assert value not in rendered
    assert result.report["raw_values_emitted"] is False


def test_previous_redis_stream_id_handles_zero_and_nonzero_sequences() -> None:
    module = _module()

    assert module.previous_redis_stream_id("1710000000000-7") == "1710000000000-6"
    assert module.previous_redis_stream_id("1710000000000-0") == (
        "1709999999999-18446744073709551615"
    )
