from __future__ import annotations

import importlib
import inspect
import json
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[4]
SCRIPT = (
    ROOT
    / "scripts"
    / "ops"
    / "dedicated_vps_joined_channel_candidate_bearing_history_availability_probe.py"
)

FAKE_DATABASE_URL = (
    "postgresql+psycopg://github_ai_catchbot_app:"
    "unit-db-password-history-candidate@127.0.0.1:5432/github_ai_catchbot"
)
FAKE_RUNTIME_PATH = "/etc/github-ai-catchbot/private-runtime.env"
RAW_CHAT_ID = 9876543210123
RAW_MESSAGE_ID = 444555666
RAW_REGISTRY_ID = "6ac4211c-7a8f-42bf-a8d5-raw-registry"
GITHUB_TEXT = "New agent repo https://github.com/octocat/Hello-World"
GITHUB_URL = "https://github.com/octocat/Hello-World"
WEAK_AI_TEXT = "just ai"
TEXT_IDEA = "AI developer agent SDK workflow with reproducible Python package"
RAW_EXCEPTION_TEXT = "private tdlib exception text should stay hidden"


class FakeMappings:
    def __init__(self, rows: list[dict[str, Any]]) -> None:
        self._rows = rows

    def first(self) -> dict[str, Any] | None:
        return self._rows[0] if self._rows else None

    def all(self) -> list[dict[str, Any]]:
        return self._rows


class FakeResult:
    def __init__(
        self,
        *,
        scalar: Any = None,
        rows: list[dict[str, Any]] | None = None,
    ) -> None:
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
        joined_rows: list[dict[str, Any]],
        read_only_value: str = "on",
        registry_table_available: bool = True,
    ) -> None:
        self.joined_rows = joined_rows
        self.read_only_value = read_only_value
        self.registry_table_available = registry_table_available
        self.statements: list[str] = []
        self.params: list[dict[str, Any]] = []
        self.joined_query_limits: list[int] = []
        self.rolled_back = False
        self.closed = False

    async def execute(self, statement: Any, params: dict[str, Any] | None = None) -> FakeResult:
        params = params or {}
        normalized = _normalize(str(statement))
        self.statements.append(normalized)
        self.params.append(dict(params))
        module = _module()

        padded = f" {normalized.upper()} "
        assert " INSERT " not in padded
        assert " UPDATE " not in padded
        assert " DELETE " not in padded

        if normalized == _normalize(module.SET_TRANSACTION_READ_ONLY_QUERY):
            return FakeResult()
        if normalized == _normalize(module.SHOW_TRANSACTION_READ_ONLY_QUERY):
            return FakeResult(scalar=self.read_only_value)
        if normalized == _normalize(module.SELECT_ONE_QUERY):
            return FakeResult(scalar=1)
        if normalized == _normalize(module.TABLE_AVAILABLE_QUERY):
            return FakeResult(scalar=self.registry_table_available)
        if normalized == _normalize(module.SELECT_JOINED_CHANNEL_ROWS_LIMIT_QUERY):
            limit = int(params["limit"])
            self.joined_query_limits.append(limit)
            return FakeResult(rows=self.joined_rows[:limit])

        raise AssertionError(f"unexpected SQL: {statement}")

    async def rollback(self) -> None:
        self.rolled_back = True

    async def close(self) -> None:
        self.closed = True


class FakeHistoryProbe:
    def __init__(
        self,
        *,
        results: list[Any] | None = None,
        status: str = "ready",
        final_authorization_state: str | None = "authorizationStateReady",
        readiness_request_types_sent: list[str] | None = None,
    ) -> None:
        self.results = list(results or [])
        self.status = status
        self.final_authorization_state = final_authorization_state
        self.readiness_request_types_sent = readiness_request_types_sent or [
            "getAuthorizationState",
            "setTdlibParameters",
        ]
        self.initialized = False
        self.closed = False
        self.fetch_calls: list[dict[str, int]] = []
        self.tdlib_send_called = False
        self.tdlib_receive_called = False

    @property
    def tdlib_ready_probe_summary(self) -> dict[str, Any]:
        return {
            "tdlib_ready_probe_status": self.status,
            "tdlib_ready_probe_final_authorization_state": self.final_authorization_state,
            "tdlib_ready_probe_request_types_sent": list(
                self.readiness_request_types_sent
            ),
        }

    async def initialize(self) -> None:
        self.initialized = True
        self.tdlib_send_called = True
        self.tdlib_receive_called = True

    async def fetch_chat_history(self, *, chat_id: int, limit: int) -> Any:
        self.fetch_calls.append({"chat_id": chat_id, "limit": limit})
        self.tdlib_send_called = True
        self.tdlib_receive_called = True
        if self.results:
            result = self.results.pop(0)
        else:
            result = _module().HistoryFetchResult(status="empty")
        if isinstance(result, Exception):
            raise result
        return result

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
        "scripts.ops.dedicated_vps_joined_channel_candidate_bearing_history_availability_probe"
    )


def _normalize(statement: str) -> str:
    return " ".join(statement.strip().split())


def _runtime_env(_path: str | Path) -> dict[str, str]:
    return {"DATABASE_URL": FAKE_DATABASE_URL}


def _joined_row(index: int, *, chat_id: int | None = None) -> dict[str, Any]:
    return {
        "registry_id": f"{RAW_REGISTRY_ID}-{index}",
        "chat_id": RAW_CHAT_ID + index if chat_id is None else chat_id,
    }


def _message_text(text: str, *, message_id: int = RAW_MESSAGE_ID) -> dict[str, Any]:
    return {
        "@type": "message",
        "chat_id": RAW_CHAT_ID,
        "id": message_id,
        "date": 1_700_000_000,
        "is_channel_post": True,
        "content": {
            "@type": "messageText",
            "text": {"@type": "formattedText", "text": text, "entities": []},
        },
    }


def _message_photo_only() -> dict[str, Any]:
    return {
        "@type": "message",
        "chat_id": RAW_CHAT_ID,
        "id": RAW_MESSAGE_ID + 1,
        "date": 1_700_000_001,
        "is_channel_post": True,
        "content": {"@type": "messagePhoto", "photo": {"id": "raw-photo-id"}},
    }


def _history(*messages: dict[str, Any]) -> Any:
    return _module().HistoryFetchResult(status="history", messages=tuple(messages))


def _failed_history() -> Any:
    return _module().HistoryFetchResult(status="failed", failure_class="transient_error")


def _run_report(
    *,
    session: FakeSession | None = None,
    probe: FakeHistoryProbe | None = None,
    approvals: bool = False,
    max_chats: int = 3,
    history_limit_per_chat: int = 20,
    private_candidate_locator_output: str | Path | None = None,
    short_url_resolver_factory: Any | None = None,
    side_effect_flags: dict[str, bool] | None = None,
    forbidden_raw_values: tuple[str, ...] = (),
) -> tuple[Any, FakeSession, FakeHistoryProbe | None]:
    fake_session = session or FakeSession(joined_rows=[_joined_row(0)])
    result = _module().generate_report(
        runtime_env_path=FAKE_RUNTIME_PATH,
        max_chats=max_chats,
        history_limit_per_chat=history_limit_per_chat,
        approved_candidate_bearing_history_probe=approvals,
        approved_tdlib_existing_session_read=approvals,
        approved_get_chat_history=approvals,
        private_candidate_locator_output=private_candidate_locator_output,
        runtime_env_reader=_runtime_env,
        database_session_factory=lambda _url: fake_session,
        history_probe_factory=(
            (lambda _values, _max, _timeout, _overall: probe)
            if probe is not None
            else None
        ),
        short_url_resolver_factory=short_url_resolver_factory,
        side_effect_flags=side_effect_flags,
        forbidden_raw_values=forbidden_raw_values,
    )
    return result, fake_session, probe


def test_script_exists() -> None:
    assert SCRIPT.exists()


def test_default_no_approval_mode_reads_db_only_without_tdlib_history() -> None:
    called = False

    def fail_if_called(*_args: Any) -> Any:
        nonlocal called
        called = True
        raise AssertionError("TDLib factory must not be called")

    result = _module().generate_report(
        runtime_env_path=FAKE_RUNTIME_PATH,
        runtime_env_reader=_runtime_env,
        database_session_factory=lambda _url: FakeSession(joined_rows=[_joined_row(0)]),
        history_probe_factory=fail_if_called,
    )

    assert result.exit_code == 0
    assert result.report["contract_status"] == (
        "joined_channel_candidate_bearing_history_availability_probe_ready"
    )
    assert result.report["runtime_env_read"] is True
    assert result.report["database_connected"] is True
    assert result.report["read_only_transaction"] is True
    assert result.report["tdlib_connection_attempted"] is False
    assert result.report["history_requests_attempted_bucket"] == "zero"
    assert called is False


def test_partial_approvals_fail_before_tdlib_connection_or_db_work() -> None:
    called = False

    def fail_if_called(_url: str) -> Any:
        nonlocal called
        called = True
        raise AssertionError("database factory must not be called")

    result = _module().generate_report(
        runtime_env_path=FAKE_RUNTIME_PATH,
        approved_candidate_bearing_history_probe=True,
        approved_tdlib_existing_session_read=False,
        approved_get_chat_history=True,
        runtime_env_reader=_runtime_env,
        database_session_factory=fail_if_called,
    )

    assert result.exit_code == 1
    assert result.report["contract_status"] == (
        "blocked_joined_channel_candidate_bearing_history_availability_probe_not_ready"
    )
    assert "approval.partial" in result.report["checks_failed"]
    assert result.report["tdlib_connection_attempted"] is False
    assert result.report["runtime_env_read"] is False
    assert called is False


def test_joined_channel_query_is_bounded_by_max_chats_hard_cap() -> None:
    rows = [_joined_row(index) for index in range(20)]
    session = FakeSession(joined_rows=rows)
    probe = FakeHistoryProbe(results=[_module().HistoryFetchResult(status="empty") for _ in range(10)])

    result, session, probe = _run_report(
        session=session,
        probe=probe,
        approvals=True,
        max_chats=999,
    )

    assert result.exit_code == 0
    assert session.joined_query_limits == [10]
    assert probe is not None
    assert len(probe.fetch_calls) == 10


def test_history_get_chat_history_calls_are_bounded_by_per_chat_hard_cap() -> None:
    probe = FakeHistoryProbe(results=[_module().HistoryFetchResult(status="empty")])

    result, _session, probe = _run_report(
        probe=probe,
        approvals=True,
        history_limit_per_chat=999,
    )

    assert result.exit_code == 0
    assert probe is not None
    assert probe.fetch_calls == [{"chat_id": RAW_CHAT_ID, "limit": 100}]


def test_fake_history_github_url_message_reports_candidate_found_without_raw_values() -> None:
    probe = FakeHistoryProbe(results=[_history(_message_text(GITHUB_TEXT))])

    result, _session, _probe = _run_report(probe=probe, approvals=True)
    rendered = json.dumps(result.report, sort_keys=True)

    assert result.exit_code == 0
    assert result.report["contract_status"] == (
        "joined_channel_candidate_bearing_history_availability_probe_candidate_found"
    )
    assert result.report["candidate_eligible_history_messages_bucket"] == "one"
    assert result.report["planned_github_route_bucket"] == "one"
    assert result.report["planned_candidate_groups_bucket"] == "one"
    assert GITHUB_TEXT not in rendered
    assert GITHUB_URL not in rendered
    assert str(RAW_CHAT_ID) not in rendered
    assert str(RAW_MESSAGE_ID) not in rendered
    assert RAW_REGISTRY_ID not in rendered
    assert result.report["raw_values_emitted"] is False


def test_fake_weak_ai_only_history_message_reports_suppression_only() -> None:
    probe = FakeHistoryProbe(results=[_history(_message_text(WEAK_AI_TEXT))])

    result, _session, _probe = _run_report(probe=probe, approvals=True)

    assert result.exit_code == 0
    assert result.report["contract_status"] == (
        "joined_channel_candidate_bearing_history_availability_probe_no_candidate_found"
    )
    assert result.report["signal_detected_history_messages_bucket"] == "one"
    assert result.report["candidate_eligible_history_messages_bucket"] == "zero"
    assert result.report["suppression_only_history_messages_bucket"] == "one"


def test_fake_text_idea_history_message_reports_text_idea_bucket() -> None:
    probe = FakeHistoryProbe(results=[_history(_message_text(TEXT_IDEA))])

    result, _session, _probe = _run_report(probe=probe, approvals=True)

    assert result.exit_code == 0
    assert result.report["contract_status"] == (
        "joined_channel_candidate_bearing_history_availability_probe_candidate_found"
    )
    assert result.report["planned_text_idea_bucket"] == "one"
    assert result.report["planned_artifacts_bucket"] == "one"


def test_deleted_or_unsupported_media_only_message_is_not_candidate() -> None:
    probe = FakeHistoryProbe(results=[_history(_message_photo_only())])

    result, _session, _probe = _run_report(probe=probe, approvals=True)

    assert result.exit_code == 0
    assert result.report["contract_status"] == (
        "joined_channel_candidate_bearing_history_availability_probe_no_candidate_found"
    )
    assert result.report["history_messages_projected_bucket"] == "one"
    assert result.report["candidate_eligible_history_messages_bucket"] == "zero"


def test_tdlib_not_ready_blocks_without_auth_attempt_or_history_fetch() -> None:
    probe = FakeHistoryProbe(
        results=[_history(_message_text(GITHUB_TEXT))],
        status="not_ready",
        final_authorization_state="authorizationStateWaitCode",
        readiness_request_types_sent=["getAuthorizationState"],
    )

    result, _session, probe = _run_report(probe=probe, approvals=True)

    assert result.exit_code == 1
    assert result.report["contract_status"] == (
        "blocked_joined_channel_candidate_bearing_history_availability_probe_not_ready"
    )
    assert "tdlib.not_ready" in result.report["checks_failed"]
    assert result.report["tdlib_ready"] is False
    assert result.report["tdlib_auth_attempted"] is False
    assert probe is not None
    assert probe.fetch_calls == []


def test_per_channel_history_failure_is_bucketed_and_sanitized() -> None:
    session = FakeSession(joined_rows=[_joined_row(0), _joined_row(1)])
    probe = FakeHistoryProbe(
        results=[_failed_history(), _history(_message_text(GITHUB_TEXT))]
    )

    result, _session, _probe = _run_report(
        session=session,
        probe=probe,
        approvals=True,
        forbidden_raw_values=(RAW_EXCEPTION_TEXT,),
    )
    rendered = json.dumps(result.report, sort_keys=True)

    assert result.exit_code == 0
    assert result.report["history_requests_failed_bucket"] == "one"
    assert result.report["history_requests_succeeded_bucket"] == "one"
    assert result.report["contract_status"] == (
        "joined_channel_candidate_bearing_history_availability_probe_candidate_found"
    )
    assert RAW_EXCEPTION_TEXT not in rendered


def test_all_history_reads_failing_blocks() -> None:
    session = FakeSession(joined_rows=[_joined_row(0), _joined_row(1)])
    probe = FakeHistoryProbe(results=[_failed_history(), RuntimeError(RAW_EXCEPTION_TEXT)])

    result, _session, _probe = _run_report(
        session=session,
        probe=probe,
        approvals=True,
        forbidden_raw_values=(RAW_EXCEPTION_TEXT,),
    )
    rendered = json.dumps(result.report, sort_keys=True)

    assert result.exit_code == 1
    assert result.report["contract_status"] == (
        "blocked_joined_channel_candidate_bearing_history_availability_probe_not_ready"
    )
    assert "history.all_reads_failed" in result.report["checks_failed"]
    assert result.report["history_requests_failed_bucket"] == "multiple"
    assert RAW_EXCEPTION_TEXT not in rendered


def test_private_locator_is_not_written_by_default(tmp_path: Path) -> None:
    probe = FakeHistoryProbe(results=[_history(_message_text(GITHUB_TEXT))])

    result, _session, _probe = _run_report(probe=probe, approvals=True)

    assert result.exit_code == 0
    assert result.report["private_locator_path_configured"] is False
    assert result.report["private_locator_written"] is False
    assert list(tmp_path.iterdir()) == []


def test_private_locator_writes_only_with_explicit_path_and_stdout_stays_sanitized(
    tmp_path: Path,
) -> None:
    locator_path = tmp_path / "github_ai_candidate_bearing_history_locator_DO_NOT_PASTE.json"
    probe = FakeHistoryProbe(results=[_history(_message_text(GITHUB_TEXT))])

    result, _session, _probe = _run_report(
        probe=probe,
        approvals=True,
        private_candidate_locator_output=locator_path,
    )
    rendered = json.dumps(result.report, sort_keys=True)
    locator = json.loads(locator_path.read_text(encoding="utf-8"))

    assert result.exit_code == 0
    assert result.report["private_locator_path_configured"] is True
    assert result.report["private_locator_written"] is True
    assert locator["chat_id"] == RAW_CHAT_ID
    assert locator["message_id"] == RAW_MESSAGE_ID
    assert str(locator_path) not in rendered
    assert str(RAW_CHAT_ID) not in rendered
    assert str(RAW_MESSAGE_ID) not in rendered
    assert RAW_REGISTRY_ID not in rendered


def test_forbidden_side_effect_flags_fail_closed_before_db_or_tdlib_work() -> None:
    called = False

    def fail_if_called(_url: str) -> Any:
        nonlocal called
        called = True
        raise AssertionError("database factory must not be called")

    result = _module().generate_report(
        runtime_env_path=FAKE_RUNTIME_PATH,
        runtime_env_reader=_runtime_env,
        database_session_factory=fail_if_called,
        side_effect_flags={"redis_mutation_performed": True},
    )

    assert result.exit_code == 1
    assert result.report["contract_status"] == "blocked_forbidden_side_effect_detected"
    assert "side_effect.forbidden" in result.report["checks_failed"]
    assert result.report["runtime_env_read"] is False
    assert called is False


def test_no_redis_factory_client_or_import_is_required_or_called() -> None:
    signature = inspect.signature(_module().generate_report)
    result, _session, _probe = _run_report(
        probe=FakeHistoryProbe(results=[_module().HistoryFetchResult(status="empty")]),
        approvals=True,
    )

    assert "redis_client_factory" not in signature.parameters
    assert "import redis" not in SCRIPT.read_text(encoding="utf-8")
    assert result.report["redis_mutation_performed"] is False


def test_no_external_network_short_url_resolver_is_used() -> None:
    resolver = SpyNoNetworkResolver()
    probe = FakeHistoryProbe(results=[_history(_message_text(GITHUB_TEXT))])

    result, _session, _probe = _run_report(
        probe=probe,
        approvals=True,
        short_url_resolver_factory=lambda: resolver,
    )

    assert result.exit_code == 0
    assert resolver.calls == [GITHUB_URL]
    assert result.report["external_network_attempted"] is False


def test_report_does_not_emit_private_values_or_raw_tdlib_json() -> None:
    message = _message_text(GITHUB_TEXT)
    raw_tdlib_json = json.dumps(message, sort_keys=True)
    probe = FakeHistoryProbe(results=[_history(message)])

    result, _session, _probe = _run_report(
        probe=probe,
        approvals=True,
        forbidden_raw_values=(
            FAKE_DATABASE_URL,
            FAKE_RUNTIME_PATH,
            str(RAW_CHAT_ID),
            str(RAW_MESSAGE_ID),
            RAW_REGISTRY_ID,
            GITHUB_TEXT,
            GITHUB_URL,
            raw_tdlib_json,
        ),
    )
    rendered = json.dumps(result.report, sort_keys=True)

    assert result.exit_code == 0
    for raw_value in (
        FAKE_DATABASE_URL,
        FAKE_RUNTIME_PATH,
        str(RAW_CHAT_ID),
        str(RAW_MESSAGE_ID),
        RAW_REGISTRY_ID,
        GITHUB_TEXT,
        GITHUB_URL,
        raw_tdlib_json,
    ):
        assert raw_value not in rendered
    assert result.report["raw_values_emitted"] is False


def test_no_source_registry_outbox_or_normalizer_mutation_sql_is_executed() -> None:
    session = FakeSession(joined_rows=[_joined_row(0)])
    probe = FakeHistoryProbe(results=[_history(_message_text(GITHUB_TEXT))])

    result, session, _probe = _run_report(
        session=session,
        probe=probe,
        approvals=True,
    )

    assert result.exit_code == 0
    for statement in session.statements:
        padded = f" {statement.upper()} "
        assert " INSERT " not in padded
        assert " UPDATE " not in padded
        assert " DELETE " not in padded
    assert result.report["source_tables_mutation_performed"] is False
    assert result.report["registry_mutation_performed"] is False
    assert result.report["event_outbox_mutation_performed"] is False
    assert result.report["normalizer_tables_mutation_performed"] is False
