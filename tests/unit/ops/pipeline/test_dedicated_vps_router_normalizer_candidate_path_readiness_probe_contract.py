from __future__ import annotations

import importlib
import inspect
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
    / "dedicated_vps_router_normalizer_candidate_path_readiness_probe.py"
)

FAKE_DATABASE_URL = (
    "postgresql+psycopg://github_ai_catchbot_app:"
    "unit-db-password-candidate-probe@127.0.0.1:5432/github_ai_catchbot"
)
FAKE_RUNTIME_PATH = "/etc/github-ai-catchbot/private-runtime.env"
GITHUB_TEXT = "New agent repo https://github.com/octocat/Hello-World"
GITHUB_URL = "https://github.com/octocat/Hello-World"
WEAK_AI_TEXT = "ai"
TEXT_IDEA = "AI developer agent SDK workflow with reproducible Python package"


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
        read_only_value: str = "on",
        missing_tables: set[str] | None = None,
        read_only_error: Exception | None = None,
        source_query_error: Exception | None = None,
        existing_normalization_runs: int = 0,
        existing_candidate_groups: int = 0,
        existing_enrich_outbox: int = 0,
    ) -> None:
        self.source_rows = source_rows
        self.read_only_value = read_only_value
        self.missing_tables = missing_tables or set()
        self.read_only_error = read_only_error
        self.source_query_error = source_query_error
        self.existing_normalization_runs = existing_normalization_runs
        self.existing_candidate_groups = existing_candidate_groups
        self.existing_enrich_outbox = existing_enrich_outbox
        self.statements: list[str] = []
        self.params: list[dict[str, Any]] = []
        self.source_query_limits: list[int] = []
        self.rolled_back = False
        self.closed = False

    async def execute(self, statement: Any, params: dict[str, Any] | None = None) -> FakeResult:
        params = params or {}
        normalized = _normalize(str(statement))
        self.statements.append(normalized)
        self.params.append(dict(params))
        module = _module()

        if normalized == _normalize(module.SET_TRANSACTION_READ_ONLY_QUERY):
            if self.read_only_error is not None:
                raise self.read_only_error
            return FakeResult()
        if normalized == _normalize(module.SHOW_TRANSACTION_READ_ONLY_QUERY):
            return FakeResult(scalar=self.read_only_value)
        if normalized == _normalize(module.SELECT_ONE_QUERY):
            return FakeResult(scalar=1)
        if normalized == _normalize(module.TABLE_AVAILABLE_QUERY):
            table_name = str(params["qualified_table_name"]).removeprefix("public.")
            return FakeResult(scalar=table_name not in self.missing_tables)
        if "FROM source_messages sm LEFT JOIN LATERAL" in normalized:
            if self.source_query_error is not None:
                raise self.source_query_error
            limit = int(params["limit"])
            self.source_query_limits.append(limit)
            return FakeResult(rows=self.source_rows[:limit])
        if "FROM normalization_runs WHERE source_message_id" in normalized:
            return FakeResult(scalar=self.existing_normalization_runs)
        if "FROM candidate_group_proposals WHERE source_message_id" in normalized:
            return FakeResult(scalar=self.existing_candidate_groups)
        if "FROM event_outbox WHERE event_type = 'artifact.enrich.requested.v1'" in normalized:
            return FakeResult(scalar=self.existing_enrich_outbox)

        raise AssertionError(f"unexpected SQL: {statement}")

    async def rollback(self) -> None:
        self.rolled_back = True

    async def close(self) -> None:
        self.closed = True


class SpyNoNetworkResolver:
    def __init__(self) -> None:
        self.calls: list[str] = []

    async def resolve(self, url: Any) -> Any:
        self.calls.append(url.observed_url)
        module = _module()
        return module.ResolvedUrl(
            observed_url=url.observed_url,
            normalized_url=url.observed_url,
            resolved_url=None,
            source_kind=url.source_kind,
            context_path=url.context_path,
            resolution_status="network_disabled",
        )


def _module():
    return importlib.import_module(
        "scripts.ops.dedicated_vps_router_normalizer_candidate_path_readiness_probe"
    )


def _normalize(statement: str) -> str:
    return " ".join(statement.strip().split())


def _runtime_env(_path: str | Path) -> dict[str, str]:
    return {"DATABASE_URL": FAKE_DATABASE_URL}


def _source_row(
    *,
    text: str,
    source_message_id: UUID | None = None,
    deleted: bool = False,
    include_version: bool = True,
) -> dict[str, Any]:
    row_id = source_message_id or uuid4()
    return {
        "source_message_id": row_id,
        "current_version_no": 1,
        "text_body": text,
        "caption_text": None,
        "text_surface": text,
        "entities_json": [],
        "url_surface_json": [],
        "raw_message_json": {"private_text": text},
        "deleted_at": datetime.now(timezone.utc) if deleted else None,
        "version_no": 1 if include_version else None,
        "version_text_surface": text if include_version else None,
        "version_entities_json": [] if include_version else None,
        "version_raw_message_json": {"private_version_text": text} if include_version else None,
    }


def _run_report(
    *,
    rows: list[dict[str, Any]],
    session: FakeSession | None = None,
    max_source_rows: int = 50,
    short_url_resolver_factory: Any | None = None,
    side_effect_flags: dict[str, bool] | None = None,
    forbidden_raw_values: tuple[str, ...] = (),
) -> tuple[Any, FakeSession]:
    fake_session = session or FakeSession(source_rows=rows)
    result = _module().generate_report(
        runtime_env_path=FAKE_RUNTIME_PATH,
        max_source_rows=max_source_rows,
        runtime_env_reader=_runtime_env,
        database_session_factory=lambda _url: fake_session,
        short_url_resolver_factory=short_url_resolver_factory,
        side_effect_flags=side_effect_flags,
        forbidden_raw_values=forbidden_raw_values,
    )
    return result, fake_session


def test_script_exists() -> None:
    assert SCRIPT.exists()


def test_default_probe_enforces_read_only_db_transaction() -> None:
    result, session = _run_report(rows=[_source_row(text=GITHUB_TEXT)])

    assert result.exit_code == 0
    assert result.report["read_only_transaction"] is True
    assert result.report["database_connected"] is True
    assert _normalize(_module().SET_TRANSACTION_READ_ONLY_QUERY) in session.statements
    assert _normalize(_module().SHOW_TRANSACTION_READ_ONLY_QUERY) in session.statements
    assert session.statements.index(_normalize(_module().SET_TRANSACTION_READ_ONLY_QUERY)) < (
        session.statements.index(_normalize(_module().SELECT_ONE_QUERY))
    )
    assert session.rolled_back is True
    assert session.closed is True


def test_probe_scans_bounded_number_of_rows_only() -> None:
    rows = [_source_row(text=WEAK_AI_TEXT) for _ in range(250)]
    result, session = _run_report(rows=rows, max_source_rows=7)

    assert result.exit_code == 0
    assert session.source_query_limits == [7]
    assert result.report["source_rows_scanned_bucket"] == "multiple"


def test_candidate_eligible_github_url_reports_sanitized_candidate_found() -> None:
    row = _source_row(text=GITHUB_TEXT)
    result, _session = _run_report(rows=[row])
    rendered = json.dumps(result.report, sort_keys=True)

    assert result.exit_code == 0
    assert result.report["contract_status"] == (
        "router_normalizer_candidate_path_readiness_probe_candidate_found"
    )
    assert result.report["candidate_eligible_rows_bucket"] == "one"
    assert result.report["planned_github_route_bucket"] == "one"
    assert result.report["planned_candidate_groups_bucket"] == "one"
    assert str(row["source_message_id"]) not in rendered
    assert GITHUB_TEXT not in rendered
    assert GITHUB_URL not in rendered
    assert FAKE_DATABASE_URL not in rendered
    assert FAKE_RUNTIME_PATH not in rendered
    assert result.report["raw_values_emitted"] is False


def test_weak_ai_only_row_reports_no_candidate_with_suppression_bucket() -> None:
    result, _session = _run_report(rows=[_source_row(text=WEAK_AI_TEXT)])

    assert result.exit_code == 0
    assert result.report["contract_status"] == (
        "router_normalizer_candidate_path_readiness_probe_no_candidate_found"
    )
    assert result.report["signal_detected_rows_bucket"] == "one"
    assert result.report["candidate_eligible_rows_bucket"] == "zero"
    assert result.report["suppression_only_rows_bucket"] == "one"


def test_text_idea_eligible_row_reports_text_idea_without_db_writes() -> None:
    result, _session = _run_report(rows=[_source_row(text=TEXT_IDEA)])

    assert result.exit_code == 0
    assert result.report["contract_status"] == (
        "router_normalizer_candidate_path_readiness_probe_candidate_found"
    )
    assert result.report["planned_text_idea_bucket"] == "one"
    assert result.report["planned_artifacts_bucket"] == "one"
    assert result.report["normalizer_tables_mutation_performed"] is False
    assert result.report["event_outbox_mutation_performed"] is False


def test_deleted_source_row_is_not_reported_as_candidate() -> None:
    result, _session = _run_report(rows=[_source_row(text=GITHUB_TEXT, deleted=True)])

    assert result.exit_code == 0
    assert result.report["contract_status"] == (
        "router_normalizer_candidate_path_readiness_probe_no_candidate_found"
    )
    assert result.report["candidate_eligible_rows_bucket"] == "zero"
    assert result.report["planned_github_route_bucket"] == "zero"
    assert result.report["suppression_only_rows_bucket"] == "one"


def test_missing_required_table_blocks_before_source_scan() -> None:
    session = FakeSession(
        source_rows=[_source_row(text=GITHUB_TEXT)],
        missing_tables={"candidate_group_members"},
    )
    result, session = _run_report(rows=[], session=session)

    assert result.exit_code == 1
    assert result.report["contract_status"] == (
        "blocked_router_normalizer_candidate_path_readiness_probe_not_ready"
    )
    assert "database.required_tables" in result.report["checks_failed"]
    assert session.source_query_limits == []


def test_db_read_only_enforcement_failure_blocks() -> None:
    session = FakeSession(source_rows=[_source_row(text=GITHUB_TEXT)], read_only_value="off")
    result, _session = _run_report(rows=[], session=session)

    assert result.exit_code == 1
    assert "database.read_only_transaction" in result.report["checks_failed"]
    assert result.report["read_only_transaction"] is False


def test_raw_values_and_exception_text_are_not_emitted() -> None:
    session = FakeSession(
        source_rows=[],
        source_query_error=RuntimeError("private exception text should stay hidden"),
    )
    result, _session = _run_report(
        rows=[],
        session=session,
        forbidden_raw_values=(
            "private exception text should stay hidden",
            FAKE_DATABASE_URL,
            FAKE_RUNTIME_PATH,
        ),
    )
    rendered = json.dumps(result.report, sort_keys=True)

    assert result.exit_code == 1
    assert "database.connection_or_schema" in result.report["checks_failed"]
    assert "private exception text" not in rendered
    assert FAKE_DATABASE_URL not in rendered
    assert FAKE_RUNTIME_PATH not in rendered
    assert result.report["raw_values_emitted"] is False


def test_forbidden_side_effect_flags_fail_closed_before_db_work() -> None:
    called = False

    def fail_if_called(_url: str) -> Any:
        nonlocal called
        called = True
        raise AssertionError("database factory must not be called")

    result = _module().generate_report(
        runtime_env_path=FAKE_RUNTIME_PATH,
        runtime_env_reader=_runtime_env,
        database_session_factory=fail_if_called,
        side_effect_flags={"external_network_attempted": True},
    )

    assert result.exit_code == 1
    assert result.report["contract_status"] == "blocked_forbidden_side_effect_detected"
    assert "side_effect.forbidden" in result.report["checks_failed"]
    assert result.report["runtime_env_read"] is False
    assert called is False


def test_no_redis_factory_or_client_is_required_or_called() -> None:
    signature = inspect.signature(_module().generate_report)
    result, _session = _run_report(rows=[_source_row(text=WEAK_AI_TEXT)])

    assert "redis_client_factory" not in signature.parameters
    assert result.exit_code == 0
    assert result.report["redis_mutation_performed"] is False


def test_deterministic_planning_uses_no_network_resolver() -> None:
    resolver = SpyNoNetworkResolver()
    result, _session = _run_report(
        rows=[_source_row(text=GITHUB_TEXT)],
        short_url_resolver_factory=lambda: resolver,
    )

    assert result.exit_code == 0
    assert resolver.calls == [GITHUB_URL]
    assert result.report["external_network_attempted"] is False
    assert result.report["planned_github_route_bucket"] == "one"
