from __future__ import annotations

import copy
import importlib
import inspect
import json
from pathlib import Path
from typing import Any
from uuid import NAMESPACE_URL, uuid5

import pytest


ROOT = Path(__file__).resolve().parents[4]
SCRIPT = (
    ROOT
    / "scripts"
    / "ops"
    / "dedicated_vps_candidate_bearing_history_locator_bounded_ingest_smoke.py"
)

FAKE_DATABASE_URL = (
    "postgresql+psycopg://github_ai_catchbot_app:"
    "unit-db-password-locator-ingest@127.0.0.1:5432/github_ai_catchbot"
)
FAKE_RUNTIME_PATH = "/etc/github-ai-catchbot/private-runtime.env"
FAKE_LOCATOR_PATH = "/tmp/unit-private-candidate-locator.json"
RAW_CHAT_ID = 9876543210123
RAW_MESSAGE_ID = 444555666
RAW_OTHER_MESSAGE_ID = 444555667
RAW_MESSAGE_DATE = 1_700_000_000
RAW_REGISTRY_ID = "6ac4211c-7a8f-42bf-a8d5-raw-registry"
GITHUB_TEXT = "New agent repo https://github.com/octocat/Hello-World"
GITHUB_URL = "https://github.com/octocat/Hello-World"
X_TEXT = "Agent workflow thread https://x.com/octocat/status/1234567890123456789"
TEXT_IDEA = "AI developer agent SDK workflow with reproducible Python package"
WEAK_AI_TEXT = "just ai"
RAW_TDLIB_JSON_VALUE = "raw tdlib json value must stay hidden"
RAW_EXCEPTION_TEXT = "private database exception text must stay hidden"


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

    def scalar_one(self) -> Any:
        return self._scalar

    def mappings(self) -> FakeMappings:
        return FakeMappings(self._rows)

    def fetchall(self) -> list[dict[str, Any]]:
        return self._rows


class FakeSession:
    def __init__(
        self,
        *,
        read_only_value: str = "on",
        missing_tables: set[str] | None = None,
    ) -> None:
        self.read_only_value = read_only_value
        self.missing_tables = missing_tables or set()
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
            table_name = str(params["qualified_table_name"]).removeprefix("public.")
            return FakeResult(scalar=table_name not in self.missing_tables)

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

    async def fetch_chat_history(
        self,
        *,
        chat_id: int,
        from_message_id: int,
        limit: int,
    ) -> Any:
        self.fetch_calls.append(
            {
                "chat_id": chat_id,
                "from_message_id": from_message_id,
                "limit": limit,
            }
        )
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


class FakeRepositoryTransaction:
    def __init__(self, repository: "FakeRepository") -> None:
        self.repository = repository
        self.committed = False
        self.rolled_back = False
        self._snapshot: Any = None

    async def __aenter__(self) -> "FakeRepository":
        self.repository.order.append("transaction_enter")
        self.repository.transactions.append(self)
        self._snapshot = self.repository.snapshot()
        return self.repository

    async def __aexit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        if exc_type is not None:
            self.repository.restore(self._snapshot)
            self.repository.order.append("rollback")
            self.rolled_back = True
            return None
        self.repository.order.append("commit")
        self.committed = True
        return None


class FakeRepository:
    def __init__(
        self,
        *,
        fail_outbox_exception: Exception | None = None,
    ) -> None:
        self.fail_outbox_exception = fail_outbox_exception
        self.messages: dict[tuple[int, int], dict[str, Any]] = {}
        self.versions: dict[str, list[dict[str, Any]]] = {}
        self.outbox: list[dict[str, Any]] = []
        self.pending_events_by_source: dict[str, int] = {}
        self.transactions: list[FakeRepositoryTransaction] = []
        self.order: list[str] = []

    def snapshot(self) -> Any:
        return (
            copy.deepcopy(self.messages),
            copy.deepcopy(self.versions),
            copy.deepcopy(self.outbox),
            copy.deepcopy(self.pending_events_by_source),
            list(self.order),
        )

    def restore(self, snapshot: Any) -> None:
        (
            self.messages,
            self.versions,
            self.outbox,
            self.pending_events_by_source,
            self.order,
        ) = snapshot

    def transaction(self) -> FakeRepositoryTransaction:
        return FakeRepositoryTransaction(self)

    async def get_source_message(
        self,
        *,
        platform: str,
        chat_id: int,
        message_id: int,
    ) -> dict[str, Any] | None:
        self.order.append("get_existing")
        assert platform == "telegram"
        return self.messages.get((chat_id, message_id))

    async def count_pending_source_events(self, *, source_message_id: str) -> int:
        self.order.append("count_pending")
        return self.pending_events_by_source.get(source_message_id, 0)

    async def upsert_source_message(
        self,
        projection: Any,
        *,
        platform: str = "telegram",
    ) -> dict[str, Any]:
        self.order.append("upsert")
        assert platform == "telegram"
        key = (projection.chat_id, projection.message_id)
        row = self.messages.get(key)
        if row is None:
            row = {
                "source_message_id": str(
                    uuid5(
                        NAMESPACE_URL,
                        f"telegram:{projection.chat_id}:{projection.message_id}",
                    )
                ),
                "chat_id": projection.chat_id,
                "message_id": projection.message_id,
                "current_version_no": 0,
                "content_hash": None,
            }
            self.messages[key] = row
            self.versions[row["source_message_id"]] = []
        row["logical_post_key"] = projection.logical_post_key
        return row

    async def append_source_message_version_if_changed(
        self,
        *,
        source_message_id: str,
        projection: Any,
        version_reason: str,
        observed_at: Any = None,
        telegram_edit_date: Any = None,
    ) -> tuple[bool, dict[str, Any] | None]:
        self.order.append("version")
        versions = self.versions.setdefault(source_message_id, [])
        previous_hash = versions[-1]["content_hash"] if versions else None
        if previous_hash == projection.content_hash:
            return False, None
        row = {
            "source_message_id": source_message_id,
            "version_no": len(versions) + 1,
            "version_reason": version_reason,
            "content_hash": projection.content_hash,
        }
        versions.append(row)
        for current in self.messages.values():
            if current["source_message_id"] == source_message_id:
                current["current_version_no"] = row["version_no"]
                current["content_hash"] = projection.content_hash
                break
        return True, row

    async def insert_outbox_event(self, event: Any) -> None:
        self.order.append("outbox")
        if self.fail_outbox_exception is not None:
            raise self.fail_outbox_exception
        self.outbox.append({"event": event, "status": "pending"})
        self.pending_events_by_source[str(event.aggregate_id)] = (
            self.pending_events_by_source.get(str(event.aggregate_id), 0) + 1
        )

    def add_existing_message(self, *, chat_id: int, message_id: int, pending_events: int) -> str:
        source_message_id = str(uuid5(NAMESPACE_URL, f"telegram:{chat_id}:{message_id}"))
        self.messages[(chat_id, message_id)] = {
            "source_message_id": source_message_id,
            "chat_id": chat_id,
            "message_id": message_id,
            "current_version_no": 1,
            "content_hash": "existing",
        }
        self.versions[source_message_id] = [
            {"source_message_id": source_message_id, "version_no": 1}
        ]
        self.pending_events_by_source[source_message_id] = pending_events
        if pending_events:
            self.outbox.append({"event": "existing", "status": "pending"})
        return source_message_id


class FakeRepositoryContext:
    def __init__(self, repository: FakeRepository) -> None:
        self.repository = repository
        self.entered = False
        self.exited = False

    async def __aenter__(self) -> FakeRepository:
        self.entered = True
        return self.repository

    async def __aexit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        self.exited = True


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
        "scripts.ops.dedicated_vps_candidate_bearing_history_locator_bounded_ingest_smoke"
    )


def _normalize(statement: str) -> str:
    return " ".join(statement.strip().split())


def _runtime_env(_path: str | Path) -> dict[str, str]:
    return {"DATABASE_URL": FAKE_DATABASE_URL}


def _valid_locator_json() -> str:
    return json.dumps(
        {
            "chat_id": RAW_CHAT_ID,
            "message_id": RAW_MESSAGE_ID,
            "message_date": RAW_MESSAGE_DATE,
            "registry_id": RAW_REGISTRY_ID,
        }
    )


def _message_text(text: str, *, message_id: int = RAW_MESSAGE_ID) -> dict[str, Any]:
    return {
        "@type": "message",
        "chat_id": RAW_CHAT_ID,
        "id": message_id,
        "date": RAW_MESSAGE_DATE,
        "is_channel_post": True,
        "content": {
            "@type": "messageText",
            "text": {"@type": "formattedText", "text": text, "entities": []},
            "private_json_marker": RAW_TDLIB_JSON_VALUE,
        },
    }


def _history(*messages: dict[str, Any]) -> Any:
    return _module().HistoryFetchResult(status="history", messages=tuple(messages))


def _approvals() -> dict[str, bool]:
    return {
        "approved_candidate_locator_ingest_smoke": True,
        "approved_private_locator_read": True,
        "approved_tdlib_existing_session_read": True,
        "approved_get_chat_history": True,
        "approved_source_table_write": True,
        "approved_event_outbox_write": True,
    }


def _run_report(
    *,
    session: FakeSession | None = None,
    locator_exists: bool = True,
    locator_text: str | None = None,
    locator_reader: Any | None = None,
    probe: FakeHistoryProbe | None = None,
    repository: FakeRepository | None = None,
    approvals: bool = False,
    side_effect_flags: dict[str, bool] | None = None,
    history_limit: int = 5,
) -> tuple[Any, FakeSession, FakeHistoryProbe | None, FakeRepository, dict[str, int]]:
    fake_session = session or FakeSession()
    fake_repository = repository or FakeRepository()
    calls = {"exists": 0, "locator_read": 0, "history_factory": 0, "repo_factory": 0}

    def exists_checker(_path: str | Path) -> bool:
        calls["exists"] += 1
        return locator_exists

    def read_locator(_path: str | Path) -> str:
        calls["locator_read"] += 1
        if locator_reader is not None:
            return locator_reader(_path)
        return locator_text if locator_text is not None else _valid_locator_json()

    def history_factory(_values: Any, _max: int, _timeout: float, _overall: float) -> Any:
        calls["history_factory"] += 1
        return probe

    def repo_factory(_values: Any) -> FakeRepositoryContext:
        calls["repo_factory"] += 1
        return FakeRepositoryContext(fake_repository)

    kwargs = _approvals() if approvals else {}
    result = _module().generate_report(
        runtime_env_path=FAKE_RUNTIME_PATH,
        private_candidate_locator_input=FAKE_LOCATOR_PATH,
        history_limit=history_limit,
        runtime_env_reader=_runtime_env,
        database_session_factory=lambda _url: fake_session,
        locator_exists_checker=exists_checker,
        locator_reader=read_locator,
        history_probe_factory=history_factory if probe is not None else None,
        repository_context_factory=repo_factory,
        side_effect_flags=side_effect_flags,
        **kwargs,
    )
    return result, fake_session, probe, fake_repository, calls


def test_script_exists() -> None:
    assert SCRIPT.exists()


def test_default_no_approval_mode_checks_readiness_without_locator_read_tdlib_or_write() -> None:
    def fail_if_read(_path: str | Path) -> str:
        raise AssertionError("locator contents must not be read in default mode")

    result, session, _probe, repository, calls = _run_report(locator_reader=fail_if_read)

    assert result.exit_code == 0
    assert result.report["contract_status"] == (
        "candidate_bearing_history_locator_bounded_ingest_smoke_ready"
    )
    assert result.report["runtime_env_read"] is True
    assert result.report["database_connected"] is True
    assert result.report["read_only_transaction"] is True
    assert result.report["private_locator_path_configured"] is True
    assert result.report["private_locator_exists"] is True
    assert result.report["private_locator_read_attempted"] is False
    assert result.report["tdlib_connection_attempted"] is False
    assert result.report["source_write_attempted"] is False
    assert repository.order == []
    assert calls["exists"] == 1
    assert calls["locator_read"] == 0
    assert session.rolled_back is True
    assert session.closed is True


def test_partial_approvals_fail_before_locator_read_tdlib_or_db_write() -> None:
    called = {"database": 0, "exists": 0}

    def database_factory(_url: str) -> Any:
        called["database"] += 1
        raise AssertionError("database must not be opened for partial approvals")

    def exists_checker(_path: str | Path) -> bool:
        called["exists"] += 1
        raise AssertionError("locator metadata must not be checked for partial approvals")

    result = _module().generate_report(
        runtime_env_path=FAKE_RUNTIME_PATH,
        private_candidate_locator_input=FAKE_LOCATOR_PATH,
        approved_candidate_locator_ingest_smoke=True,
        approved_private_locator_read=True,
        approved_tdlib_existing_session_read=False,
        approved_get_chat_history=True,
        approved_source_table_write=True,
        approved_event_outbox_write=True,
        runtime_env_reader=_runtime_env,
        database_session_factory=database_factory,
        locator_exists_checker=exists_checker,
        locator_reader=lambda _path: _valid_locator_json(),
    )

    assert result.exit_code == 1
    assert result.report["contract_status"] == (
        "blocked_candidate_bearing_history_locator_bounded_ingest_smoke_not_ready"
    )
    assert "approval.partial" in result.report["checks_failed"]
    assert result.report["runtime_env_read"] is False
    assert result.report["private_locator_read_attempted"] is False
    assert result.report["tdlib_connection_attempted"] is False
    assert result.report["source_write_attempted"] is False
    assert called == {"database": 0, "exists": 0}


def test_missing_private_locator_path_blocks_without_reading_runtime_or_locator() -> None:
    result, _session, _probe, _repository, calls = _run_report(locator_exists=False)

    assert result.exit_code == 1
    assert "private_locator.missing" in result.report["checks_failed"]
    assert result.report["runtime_env_read"] is False
    assert result.report["private_locator_exists"] is False
    assert result.report["private_locator_read_attempted"] is False
    assert calls["locator_read"] == 0


def test_malformed_private_locator_blocks_without_raw_output_or_tdlib() -> None:
    raw_locator_text = f'{{"chat_id": "{RAW_CHAT_ID}", "message_id": "bad", "secret": "{RAW_TDLIB_JSON_VALUE}"}}'
    result, _session, _probe, repository, calls = _run_report(
        locator_text=raw_locator_text,
        approvals=True,
        probe=FakeHistoryProbe(),
    )
    rendered = json.dumps(result.report, sort_keys=True)

    assert result.exit_code == 1
    assert "private_locator.malformed" in result.report["checks_failed"]
    assert result.report["private_locator_read_attempted"] is True
    assert result.report["private_locator_shape_valid_bucket"] == "malformed"
    assert result.report["tdlib_connection_attempted"] is False
    assert repository.order == []
    assert calls["locator_read"] == 1
    assert RAW_TDLIB_JSON_VALUE not in rendered
    assert FAKE_LOCATOR_PATH not in rendered


def test_tdlib_not_ready_blocks_without_auth_attempt_or_write() -> None:
    probe = FakeHistoryProbe(
        status="not_ready",
        final_authorization_state="authorizationStateWaitCode",
        readiness_request_types_sent=["getAuthorizationState"],
    )
    result, _session, probe, repository, _calls = _run_report(
        approvals=True,
        probe=probe,
    )

    assert result.exit_code == 1
    assert "tdlib.not_ready" in result.report["checks_failed"]
    assert result.report["tdlib_connection_attempted"] is True
    assert result.report["tdlib_ready"] is False
    assert result.report["tdlib_auth_attempted"] is False
    assert probe is not None
    assert probe.fetch_calls == []
    assert repository.order == []


def test_exact_located_message_missing_blocks_without_db_write() -> None:
    probe = FakeHistoryProbe(results=[_history(_message_text(GITHUB_TEXT, message_id=RAW_OTHER_MESSAGE_ID))])
    result, _session, probe, repository, _calls = _run_report(
        approvals=True,
        probe=probe,
    )

    assert result.exit_code == 1
    assert result.report["contract_status"] == (
        "blocked_candidate_bearing_history_locator_bounded_ingest_smoke_exact_message_missing"
    )
    assert result.report["history_request_succeeded_bucket"] == "one"
    assert result.report["exact_message_found_bucket"] == "zero"
    assert result.report["source_write_attempted"] is False
    assert repository.order == []
    assert probe is not None
    assert probe.fetch_calls == [
        {"chat_id": RAW_CHAT_ID, "from_message_id": RAW_MESSAGE_ID, "limit": 5}
    ]


def test_located_message_that_is_no_longer_candidate_blocks_without_db_write() -> None:
    probe = FakeHistoryProbe(results=[_history(_message_text(WEAK_AI_TEXT))])
    result, _session, _probe, repository, _calls = _run_report(
        approvals=True,
        probe=probe,
    )

    assert result.exit_code == 1
    assert result.report["contract_status"] == (
        "blocked_candidate_bearing_history_locator_bounded_ingest_smoke_not_candidate"
    )
    assert result.report["exact_message_found_bucket"] == "one"
    assert result.report["message_projected_bucket"] == "one"
    assert result.report["candidate_eligible_bucket"] == "zero"
    assert result.report["signal_detected_bucket"] == "one"
    assert result.report["source_write_attempted"] is False
    assert repository.order == []


@pytest.mark.parametrize(
    ("text", "expected_route"),
    [
        (GITHUB_TEXT, "planned_github_route_bucket"),
        (X_TEXT, "planned_x_route_bucket"),
        (TEXT_IDEA, "planned_text_idea_bucket"),
    ],
)
def test_candidate_messages_write_source_version_and_pending_source_outbox(
    text: str,
    expected_route: str,
) -> None:
    repository = FakeRepository()
    probe = FakeHistoryProbe(results=[_history(_message_text(text))])
    result, _session, _probe, repository, _calls = _run_report(
        approvals=True,
        probe=probe,
        repository=repository,
    )

    assert result.exit_code == 0
    assert result.report["contract_status"] == (
        "candidate_bearing_history_locator_bounded_ingest_smoke_ingested"
    )
    assert result.report["candidate_eligible_bucket"] == "one"
    assert result.report[expected_route] == "one"
    assert result.report["source_messages_written_bucket"] == "one"
    assert result.report["source_message_versions_written_bucket"] == "one"
    assert result.report["event_outbox_source_events_written_bucket"] == "one"
    assert result.report["event_outbox_pending_bucket"] == "one"
    assert result.report["telegram_raw_updates_written_bucket"] == "zero"
    assert result.report["redis_mutation_performed"] is False
    assert len(repository.messages) == 1
    assert sum(len(versions) for versions in repository.versions.values()) == 1
    assert len(repository.outbox) == 1
    assert repository.outbox[0]["status"] == "pending"
    assert repository.outbox[0]["event"].event_type == "source_message.created.v1"


def test_db_commit_happens_after_source_version_and_outbox_writes() -> None:
    repository = FakeRepository()
    probe = FakeHistoryProbe(results=[_history(_message_text(GITHUB_TEXT))])
    result, _session, _probe, repository, _calls = _run_report(
        approvals=True,
        probe=probe,
        repository=repository,
    )

    assert result.exit_code == 0
    assert repository.order.index("upsert") < repository.order.index("version")
    assert repository.order.index("version") < repository.order.index("outbox")
    assert repository.order.index("outbox") < repository.order.index("commit")
    assert repository.transactions[-1].committed is True
    assert repository.transactions[-1].rolled_back is False


def test_db_write_failure_rolls_back_and_emits_sanitized_failure_class_only() -> None:
    repository = FakeRepository(
        fail_outbox_exception=RuntimeError(RAW_EXCEPTION_TEXT),
    )
    probe = FakeHistoryProbe(results=[_history(_message_text(GITHUB_TEXT))])
    result, _session, _probe, repository, _calls = _run_report(
        approvals=True,
        probe=probe,
        repository=repository,
    )
    rendered = json.dumps(result.report, sort_keys=True)

    assert result.exit_code == 1
    assert "database.write_failed" in result.report["checks_failed"]
    assert result.report["db_write_failure_class"] == "RuntimeError"
    assert RAW_EXCEPTION_TEXT not in rendered
    assert repository.messages == {}
    assert repository.versions == {}
    assert repository.outbox == []
    assert repository.transactions[-1].rolled_back is True
    assert repository.transactions[-1].committed is False


def test_already_ingested_path_does_not_duplicate_source_rows_or_outbox() -> None:
    repository = FakeRepository()
    repository.add_existing_message(
        chat_id=RAW_CHAT_ID,
        message_id=RAW_MESSAGE_ID,
        pending_events=1,
    )
    probe = FakeHistoryProbe(results=[_history(_message_text(GITHUB_TEXT))])
    result, _session, _probe, repository, _calls = _run_report(
        approvals=True,
        probe=probe,
        repository=repository,
    )

    assert result.exit_code == 0
    assert result.report["contract_status"] == (
        "candidate_bearing_history_locator_bounded_ingest_smoke_already_ingested"
    )
    assert result.report["existing_source_message_bucket"] == "one"
    assert result.report["existing_event_outbox_bucket"] == "one"
    assert result.report["source_messages_written_bucket"] == "zero"
    assert result.report["source_message_versions_written_bucket"] == "zero"
    assert result.report["event_outbox_source_events_written_bucket"] == "zero"
    assert len(repository.messages) == 1
    assert len(repository.outbox) == 1
    assert "upsert" not in repository.order
    assert "outbox" not in repository.order


def test_existing_source_without_pending_outbox_blocks_without_mutation() -> None:
    repository = FakeRepository()
    repository.add_existing_message(
        chat_id=RAW_CHAT_ID,
        message_id=RAW_MESSAGE_ID,
        pending_events=0,
    )
    probe = FakeHistoryProbe(results=[_history(_message_text(GITHUB_TEXT))])
    result, _session, _probe, repository, _calls = _run_report(
        approvals=True,
        probe=probe,
        repository=repository,
    )

    assert result.exit_code == 1
    assert result.report["blocked_existing_without_pending_outbox"] is True
    assert "blocked_existing_without_pending_outbox" in result.report["checks_failed"]
    assert len(repository.messages) == 1
    assert len(repository.outbox) == 0
    assert "upsert" not in repository.order
    assert "outbox" not in repository.order


def test_no_redis_factory_client_or_import_is_required_or_called() -> None:
    source = SCRIPT.read_text(encoding="utf-8")
    result, _session, _probe, _repository, _calls = _run_report()

    assert "import redis" not in source
    assert "from redis" not in source
    assert result.report["redis_mutation_performed"] is False


def test_no_downstream_service_or_relay_is_started() -> None:
    probe = FakeHistoryProbe(results=[_history(_message_text(GITHUB_TEXT))])
    result, _session, _probe, _repository, _calls = _run_report(
        approvals=True,
        probe=probe,
    )

    assert result.exit_code == 0
    assert result.report["downstream_service_started"] is False
    assert result.report["external_network_attempted"] is False
    assert result.report["docker_or_systemd_changed"] is False
    assert result.report["alembic_run"] is False


def test_report_does_not_emit_locator_message_text_url_db_runtime_or_tdlib_raw_json() -> None:
    probe = FakeHistoryProbe(results=[_history(_message_text(GITHUB_TEXT))])
    result, _session, _probe, _repository, _calls = _run_report(
        approvals=True,
        probe=probe,
    )
    rendered = json.dumps(result.report, sort_keys=True)

    assert result.exit_code == 0
    for raw in (
        FAKE_LOCATOR_PATH,
        str(RAW_CHAT_ID),
        str(RAW_MESSAGE_ID),
        str(RAW_MESSAGE_DATE),
        RAW_REGISTRY_ID,
        GITHUB_TEXT,
        GITHUB_URL,
        FAKE_DATABASE_URL,
        FAKE_RUNTIME_PATH,
        RAW_TDLIB_JSON_VALUE,
    ):
        assert raw not in rendered
    assert result.report["raw_values_emitted"] is False


def test_forbidden_side_effect_flags_fail_before_locator_db_or_tdlib_work() -> None:
    called = {"runtime": 0, "database": 0, "exists": 0, "reader": 0}

    def runtime_reader(_path: str | Path) -> dict[str, str]:
        called["runtime"] += 1
        return {"DATABASE_URL": FAKE_DATABASE_URL}

    def database_factory(_url: str) -> Any:
        called["database"] += 1
        return FakeSession()

    def exists_checker(_path: str | Path) -> bool:
        called["exists"] += 1
        return True

    def locator_reader(_path: str | Path) -> str:
        called["reader"] += 1
        return _valid_locator_json()

    result = _module().generate_report(
        runtime_env_path=FAKE_RUNTIME_PATH,
        private_candidate_locator_input=FAKE_LOCATOR_PATH,
        runtime_env_reader=runtime_reader,
        database_session_factory=database_factory,
        locator_exists_checker=exists_checker,
        locator_reader=locator_reader,
        side_effect_flags={"redis_mutation_performed": True},
    )

    assert result.exit_code == 1
    assert result.report["contract_status"] == "blocked_forbidden_side_effect_detected"
    assert "side_effect.forbidden" in result.report["checks_failed"]
    assert result.report["runtime_env_read"] is False
    assert result.report["private_locator_read_attempted"] is False
    assert result.report["tdlib_connection_attempted"] is False
    assert called == {"runtime": 0, "database": 0, "exists": 0, "reader": 0}


def test_script_has_no_git_staging_or_private_locator_generation_commands() -> None:
    source = SCRIPT.read_text(encoding="utf-8")
    forbidden_snippets = (
        "git add .",
        "git add -A",
        "git commit",
        "git push",
        "write_text(",
        "open(",
    )

    for snippet in forbidden_snippets:
        assert snippet not in source


def test_public_api_signature_keeps_injected_test_seams_for_private_boundaries() -> None:
    signature = inspect.signature(_module().generate_report)

    assert "locator_exists_checker" in signature.parameters
    assert "locator_reader" in signature.parameters
    assert "history_probe_factory" in signature.parameters
    assert "repository_context_factory" in signature.parameters
