from __future__ import annotations

import importlib
import json
import os
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping
from uuid import UUID, uuid4

import pytest


ROOT = Path(__file__).resolve().parents[4]
SCRIPT = (
    ROOT
    / "scripts"
    / "ops"
    / "dedicated_vps_judge_openai_real_bundle_context_live_diagnostic.py"
)

FAKE_DATABASE_CREDENTIAL = "unit" + "-db" + "-credential" + "-real-context"
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
FAKE_RUNTIME_PATH = "/etc/github-ai-catchbot/private-real-context-runtime.env"
FAKE_KEY = "private" + "-openai" + "-key" + "-real-context"
FAKE_PROJECT = "private" + "-openai" + "-project" + "-real-context"
FAKE_KEY_PATH = "/private/openai/key-file-real-context"
FAKE_SOURCE_TEXT = "private source text real context must not leak"
FAKE_URL = "https" + ":" + "/" + "/" + "example.invalid/private/real-context"
FAKE_ERROR_TEXT = "raw openai error message real context"
FAKE_REQUEST_ID = "req_private_real_context"
FAKE_MODEL = "private-model-value-real-context"
DEFAULT_PROMPT = "diagnostic judge-output schema probe"
DEFAULT_CONTEXT = "diagnostic candidate evidence placeholder"


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
        candidate_rows: list[dict[str, Any]] | None = None,
        judge_run_row: dict[str, Any] | None | object = ...,
        bundle_row: dict[str, Any] | None | object = ...,
        output_count: int = 0,
        ready_count: int = 0,
        judge_call_count: int = 1,
        missing_tables: set[str] | None = None,
        read_only_value: str = "on",
    ) -> None:
        default_candidate = _candidate_row()
        self.candidate_rows = candidate_rows if candidate_rows is not None else [default_candidate]
        first_candidate = self.candidate_rows[0] if self.candidate_rows else default_candidate
        self.judge_run_row = (
            _judge_run_row(
                judge_run_id=first_candidate["judge_run_id"],
                bundle_id=first_candidate["bundle_id"],
            )
            if judge_run_row is ...
            else judge_run_row
        )
        self.bundle_row = (
            _bundle_row(bundle_id=first_candidate["bundle_id"])
            if bundle_row is ...
            else bundle_row
        )
        self.output_count = output_count
        self.ready_count = ready_count
        self.judge_call_count = judge_call_count
        self.missing_tables = missing_tables or set()
        self.read_only_value = read_only_value
        self.statements: list[str] = []
        self.params: list[dict[str, Any]] = []
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
        if normalized == _normalize(module.SELECT_REPLAY_LIVE_SMOKE_CANDIDATES_QUERY):
            return FakeResult(rows=self.candidate_rows[: int(params["limit"])])
        if normalized == _normalize(module.COUNT_JUDGE_OUTPUTS_FOR_RUN_QUERY):
            return FakeResult(scalar=self.output_count)
        if normalized == _normalize(module.COUNT_JUDGE_OUTPUT_READY_OUTBOX_FOR_RUN_QUERY):
            return FakeResult(scalar=self.ready_count)
        if normalized == _normalize(module.COUNT_JUDGE_CALL_REQUESTED_OUTBOX_FOR_RUN_QUERY):
            return FakeResult(scalar=self.judge_call_count)
        if "FROM judge_runs" in normalized and "WHERE judge_run_id" in normalized:
            return FakeResult(rows=[] if self.judge_run_row is None else [self.judge_run_row])
        if "FROM candidate_evidence_bundles" in normalized and "WHERE bundle_id" in normalized:
            return FakeResult(rows=[] if self.bundle_row is None else [self.bundle_row])

        raise AssertionError(f"unexpected SQL: {statement}")

    async def rollback(self) -> None:
        self.rollback_count += 1

    async def close(self) -> None:
        self.closed = True


def _module():
    return importlib.import_module(
        "scripts.ops.dedicated_vps_judge_openai_real_bundle_context_live_diagnostic"
    )


def _normalize(statement: str) -> str:
    return " ".join(statement.strip().split())


def _runtime_env(_path: str | Path) -> dict[str, str]:
    return {
        "DATABASE_URL": FAKE_DATABASE_URL,
        "OPENAI_API_KEY": FAKE_KEY,
        "OPENAI_API_KEY_FILE": FAKE_KEY_PATH,
        "OPENAI_PROJECT": FAKE_PROJECT,
    }


def _runtime_env_direct_key(_path: str | Path) -> dict[str, str]:
    return {
        "DATABASE_URL": FAKE_DATABASE_URL,
        "OPENAI_API_KEY": FAKE_KEY,
        "OPENAI_PROJECT": FAKE_PROJECT,
    }


def _candidate_row(
    *,
    judge_run_id: UUID | None = None,
    bundle_id: UUID | None = None,
    event_id: UUID | None = None,
    status: str = "failed_terminal",
    finish_reason: str | None = "openai_permanent_error",
    recency_at: datetime | None = None,
    created_at: datetime | None = None,
) -> dict[str, Any]:
    now = datetime(2026, 6, 1, 1, 2, 3, tzinfo=timezone.utc)
    return {
        "judge_run_id": judge_run_id or uuid4(),
        "bundle_id": bundle_id or uuid4(),
        "judge_run_status": status,
        "finish_reason": finish_reason,
        "judge_call_requested_event_id": event_id or uuid4(),
        "recency_at": recency_at or now,
        "judge_call_requested_created_at": created_at or now,
    }


def _judge_run_row(
    *,
    judge_run_id: UUID,
    bundle_id: UUID,
    judge_profile: str = "github_primary",
    model: str = "gpt-5.4-mini",
    reasoning_effort: str = "low",
    prompt_version: str = "judge_prompt_v1__replay_live_smoke_v1",
    prompt_cache_key: str | None = "judge:github_primary:judge_prompt_v1:judge_output_v1:policy_v1",
    status: str = "failed_terminal",
) -> dict[str, Any]:
    return {
        "judge_run_id": judge_run_id,
        "bundle_id": bundle_id,
        "judge_profile": judge_profile,
        "model": model,
        "reasoning_effort": reasoning_effort,
        "prompt_version": prompt_version,
        "schema_version": "judge_output_v1",
        "policy_version": "verdict_policy_v1",
        "prompt_cache_key": prompt_cache_key,
        "status": status,
        "schema_retry_count": 0,
    }


def _bundle_row(
    *,
    bundle_id: UUID,
    primary_summary: dict[str, Any] | None = None,
    supporting_summaries_json: list[dict[str, Any]] | None = None,
    discovered_links_summary_json: list[dict[str, Any]] | None = None,
    evidence_limitations: list[str] | None = None,
    token_budget_profile: str | None = "medium",
) -> dict[str, Any]:
    return {
        "bundle_id": bundle_id,
        "candidate_group_id": uuid4(),
        "current_primary_artifact_id": uuid4(),
        "primary_summary": primary_summary
        if primary_summary is not None
        else {"headline": "private headline", "summary": FAKE_SOURCE_TEXT},
        "supporting_summaries_json": supporting_summaries_json
        if supporting_summaries_json is not None
        else [{"kind": "repo", "summary": "private supporting summary"}],
        "discovered_links_summary_json": discovered_links_summary_json
        if discovered_links_summary_json is not None
        else [{"classification": "repo", "canonical_url": FAKE_URL}],
        "evidence_limitations": evidence_limitations
        if evidence_limitations is not None
        else ["readme_excerpt_missing"],
        "token_budget_profile": token_budget_profile,
        "reroot_count": 0,
        "created_at": datetime(2026, 6, 1, 1, 2, 3, tzinfo=timezone.utc),
    }


def _run_db_preflight(
    *,
    session: FakeSession | None = None,
    forbidden_raw_values: tuple[str, ...] = (),
) -> tuple[Any, FakeSession]:
    session = session or FakeSession()
    result = _module().generate_report(
        approve_db_read=True,
        runtime_env_path=FAKE_RUNTIME_PATH,
        runtime_env_reader=_runtime_env,
        database_session_factory=lambda _url: session,
        forbidden_raw_values=(
            FAKE_RUNTIME_PATH,
            FAKE_DATABASE_URL,
            FAKE_KEY,
            FAKE_KEY_PATH,
            FAKE_PROJECT,
            FAKE_SOURCE_TEXT,
            FAKE_URL,
            *forbidden_raw_values,
        ),
    )
    return result, session


def _run_live(
    *,
    session: FakeSession | None = None,
    runtime_env_reader: Any = _runtime_env_direct_key,
    sdk_loader: Any,
    forbidden_raw_values: tuple[str, ...] = (),
) -> tuple[Any, FakeSession]:
    session = session or FakeSession()
    result = _module().generate_report(
        approve_db_read=True,
        approve_key_read=True,
        approve_live_openai=True,
        max_live_calls=1,
        runtime_env_path=FAKE_RUNTIME_PATH,
        runtime_env_reader=runtime_env_reader,
        database_session_factory=lambda _url: session,
        sdk_loader=sdk_loader,
        forbidden_raw_values=(
            FAKE_RUNTIME_PATH,
            FAKE_DATABASE_URL,
            FAKE_KEY,
            FAKE_KEY_PATH,
            FAKE_PROJECT,
            FAKE_SOURCE_TEXT,
            FAKE_URL,
            DEFAULT_PROMPT,
            DEFAULT_CONTEXT,
            *forbidden_raw_values,
        ),
    )
    return result, session


def _assert_no_db_writes(session: FakeSession) -> None:
    assert session.rollback_count == 1
    assert session.closed is True
    for statement in session.statements:
        assert statement.split()[0].upper() in {"SET", "SHOW", "SELECT", "WITH"}
        assert " UPDATE " not in f" {statement.upper()} "
        assert " INSERT " not in f" {statement.upper()} "
        assert " DELETE " not in f" {statement.upper()} "


def _assert_no_live_or_downstream(report: dict[str, Any]) -> None:
    assert report["openai_call_attempted"] is False
    assert report["live_openai_call_attempted"] is False
    assert report["live_openai_call_attempted_bucket"] == "zero"
    assert report["live_openai_call_completed_bucket"] == "zero"
    _assert_no_downstream(report)


def _assert_no_downstream(report: dict[str, Any]) -> None:
    assert report["database_write_attempted"] is False
    assert report["redis_write_attempted"] is False
    assert report["redis_ack_attempted"] is False
    assert report["redis_delete_or_trim_attempted"] is False
    assert report["analysis_validator_started"] is False
    assert report["policy_engine_started"] is False
    assert report["notifier_started"] is False
    assert report["telegram_send_attempted"] is False
    assert report["raw_values_emitted"] is False


def _assert_no_raw_values(report: dict[str, Any], *values: str) -> None:
    rendered = _module().render_json(report)
    for value in values:
        assert value not in rendered


def test_script_exists() -> None:
    assert SCRIPT.exists()


def test_default_mode_does_not_read_runtime_db_key_sdk_or_openai() -> None:
    result = _module().generate_report(
        runtime_env_reader=_raising_runtime_reader,
        database_session_factory=_raising_database_factory,
        sdk_loader=_raising_sdk_loader,
        forbidden_raw_values=(FAKE_KEY, FAKE_PROJECT, FAKE_RUNTIME_PATH),
    )

    report = result.report
    assert result.exit_code == 0
    assert report["contract_status"] == _module().STATUS_PREFLIGHT_PASSED
    assert report["runtime_env_read"] is False
    assert report["database_connected"] is False
    assert report["read_only_transaction"] is False
    assert report["openai_key_read_bucket"] == "zero"
    assert report["openai_key_file_read_bucket"] == "zero"
    assert report["sdk_import_bucket"] == "zero"
    assert report["request_shape_valid_bucket"] == "one"
    assert report["top_level_request_key_presence_buckets"]["max_output_tokens"] == "one"
    assert report["top_level_request_key_presence_buckets"]["prompt_cache_key"] == "one"
    assert report["optional_null_field_count_bucket"] == "zero"
    assert report["optional_null_field_name_buckets"] == []
    assert report["max_output_tokens_presence_bucket"] == "one"
    assert report["max_output_tokens_null_bucket"] == "zero"
    assert report["prompt_cache_key_presence_bucket"] == "one"
    assert report["text_format_type_bucket"] == "json_schema"
    assert report["json_schema_strict_bucket"] == "one"
    assert report["tools_count_bucket"] == "zero"
    _assert_no_live_or_downstream(report)


@pytest.mark.parametrize(
    "kwargs, expected_check",
    [
        ({"runtime_env_path": FAKE_RUNTIME_PATH}, "approval.required_all"),
        (
            {
                "approve_db_read": True,
                "approve_key_read": True,
                "runtime_env_path": FAKE_RUNTIME_PATH,
            },
            "approval.required_all",
        ),
        (
            {
                "approve_db_read": True,
                "approve_live_openai": True,
                "max_live_calls": 1,
                "runtime_env_path": FAKE_RUNTIME_PATH,
            },
            "approval.required_all",
        ),
        (
            {
                "approve_db_read": True,
                "approve_key_read": True,
                "approve_live_openai": True,
                "max_live_calls": 2,
                "runtime_env_path": FAKE_RUNTIME_PATH,
            },
            "approval.required_all",
        ),
        ({"approve_db_read": True}, "runtime_env_path.required"),
    ],
)
def test_partial_approval_blocks_before_db_key_or_live(
    kwargs: dict[str, Any],
    expected_check: str,
) -> None:
    result = _module().generate_report(
        runtime_env_reader=_raising_runtime_reader,
        database_session_factory=_raising_database_factory,
        sdk_loader=_raising_sdk_loader,
        forbidden_raw_values=(FAKE_KEY, FAKE_PROJECT, FAKE_RUNTIME_PATH),
        **kwargs,
    )

    report = result.report
    assert result.exit_code == 1
    assert report["contract_status"] == _module().STATUS_NOT_APPROVED
    assert report["runtime_env_read"] is False
    assert report["database_connected"] is False
    assert report["openai_key_read_bucket"] == "zero"
    assert report["sdk_import_bucket"] == "zero"
    assert report["checks_failed"] == [expected_check]
    _assert_no_live_or_downstream(report)


def test_approved_db_read_preflight_uses_read_only_transaction_and_builds_request() -> None:
    result, session = _run_db_preflight()

    report = result.report
    assert result.exit_code == 0
    assert report["contract_status"] == _module().STATUS_DB_PREFLIGHT_PASSED
    assert report["runtime_env_read"] is True
    assert report["database_configured"] is True
    assert report["database_connected"] is True
    assert report["read_only_transaction"] is True
    assert report["target_replay_candidate_found_bucket"] == "one"
    assert report["target_judge_run_status_bucket"] == "failed_terminal"
    assert report["target_finish_reason_bucket"] == "openai_permanent_error"
    assert report["target_judge_call_requested_outbox_bucket"] == "one"
    assert report["existing_judge_outputs_for_run_bucket"] == "zero"
    assert report["existing_judge_output_ready_outbox_for_run_bucket"] == "zero"
    assert report["bundle_context_loaded_bucket"] == "one"
    assert report["bundle_structurally_usable_bucket"] == "one"
    assert report["prompt_rendered_bucket"] == "one"
    assert report["context_builder_bucket"] == "one"
    assert report["developer_prompt_size_bucket"] in {"small", "medium"}
    assert report["user_context_size_bucket"] in {"small", "medium"}
    assert report["evidence_limitations_count_bucket"] == "one"
    assert report["supporting_summary_count_bucket"] == "one"
    assert report["discovered_link_count_bucket"] == "one"
    assert report["token_budget_profile_bucket"] == "medium"
    assert report["request_shape_valid_bucket"] == "one"
    assert report["request_shape_issue_count_bucket"] == "zero"
    assert report["top_level_request_key_presence_buckets"]["max_output_tokens"] == "zero"
    assert report["top_level_request_key_presence_buckets"]["prompt_cache_key"] == "one"
    assert report["optional_null_field_count_bucket"] == "zero"
    assert report["optional_null_field_name_buckets"] == []
    assert report["max_output_tokens_presence_bucket"] == "zero"
    assert report["max_output_tokens_null_bucket"] == "zero"
    assert report["prompt_cache_key_presence_bucket"] == "one"
    assert report["text_format_type_bucket"] == "json_schema"
    assert report["json_schema_strict_bucket"] == "one"
    assert report["tools_count_bucket"] == "zero"
    assert report["openai_key_read_bucket"] == "zero"
    assert report["openai_key_file_read_bucket"] == "zero"
    assert report["sdk_import_bucket"] == "zero"
    _assert_no_live_or_downstream(report)
    _assert_no_db_writes(session)


def test_no_candidate_blocks_with_sanitized_bucket() -> None:
    result, session = _run_db_preflight(session=FakeSession(candidate_rows=[]))

    assert result.exit_code == 1
    assert result.report["contract_status"] == _module().STATUS_DB_READ_FAILED
    assert result.report["target_replay_candidate_found_bucket"] == "zero"
    assert result.report["checks_failed"] == ["candidate.none"]
    _assert_no_live_or_downstream(result.report)
    _assert_no_db_writes(session)


def test_ambiguous_same_recency_candidates_block() -> None:
    timestamp = datetime(2026, 6, 1, 1, 2, 3, tzinfo=timezone.utc)
    rows = [
        _candidate_row(recency_at=timestamp, created_at=timestamp),
        _candidate_row(recency_at=timestamp, created_at=timestamp),
    ]
    result, session = _run_db_preflight(session=FakeSession(candidate_rows=rows))

    assert result.exit_code == 1
    assert result.report["contract_status"] == _module().STATUS_DB_READ_FAILED
    assert result.report["target_replay_candidate_found_bucket"] == "multiple"
    assert result.report["checks_failed"] == ["candidate.ambiguous_recency"]
    _assert_no_live_or_downstream(result.report)
    _assert_no_db_writes(session)


@pytest.mark.parametrize(
    "session, expected_check",
    [
        (FakeSession(bundle_row=None), "bundle.missing"),
        (
            FakeSession(
                bundle_row=_bundle_row(
                    bundle_id=uuid4(),
                    primary_summary={},
                    token_budget_profile=None,
                )
            ),
            "judge_run.bundle_mismatch",
        ),
    ],
)
def test_bundle_missing_or_structurally_unusable_blocks_before_live(
    session: FakeSession,
    expected_check: str,
) -> None:
    if expected_check == "judge_run.bundle_mismatch":
        candidate = session.candidate_rows[0]
        session.bundle_row = _bundle_row(
            bundle_id=candidate["bundle_id"],
            primary_summary={},
            token_budget_profile=None,
        )

    result, session = _run_db_preflight(session=session)

    assert result.exit_code == 1
    assert result.report["contract_status"] == _module().STATUS_CONTEXT_FAILED
    assert result.report["checks_failed"] == [
        "bundle.structurally_unusable"
        if expected_check == "judge_run.bundle_mismatch"
        else expected_check
    ]
    assert result.report["openai_call_attempted"] is False
    _assert_no_downstream(result.report)
    _assert_no_db_writes(session)


def test_unsupported_judge_profile_blocks_before_live() -> None:
    candidate = _candidate_row()
    session = FakeSession(
        candidate_rows=[candidate],
        judge_run_row=_judge_run_row(
            judge_run_id=candidate["judge_run_id"],
            bundle_id=candidate["bundle_id"],
            judge_profile="unsupported_private_profile",
        ),
        bundle_row=_bundle_row(bundle_id=candidate["bundle_id"]),
    )

    result, session = _run_db_preflight(session=session)

    assert result.exit_code == 1
    assert result.report["contract_status"] == _module().STATUS_CONTEXT_FAILED
    assert result.report["checks_failed"] == ["prompt.render"]
    assert result.report["openai_call_attempted"] is False
    _assert_no_downstream(result.report)
    _assert_no_db_writes(session)


def test_invalid_real_context_request_shape_blocks_before_live() -> None:
    candidate = _candidate_row()
    session = FakeSession(
        candidate_rows=[candidate],
        judge_run_row=_judge_run_row(
            judge_run_id=candidate["judge_run_id"],
            bundle_id=candidate["bundle_id"],
            model=FAKE_MODEL,
        ),
        bundle_row=_bundle_row(bundle_id=candidate["bundle_id"]),
    )

    result, session = _run_db_preflight(session=session, forbidden_raw_values=(FAKE_MODEL,))

    assert result.exit_code == 1
    assert result.report["contract_status"] == _module().STATUS_CONTEXT_FAILED
    assert result.report["request_shape_valid_bucket"] == "zero"
    assert result.report["request_shape_issue_buckets"] == ["model.outside_locked_set"]
    assert result.report["checks_failed"] == ["request_shape.invalid"]
    _assert_no_raw_values(result.report, FAKE_MODEL)
    _assert_no_live_or_downstream(result.report)
    _assert_no_db_writes(session)


def test_approved_db_preflight_does_not_read_openai_key_even_when_env_contains_it() -> None:
    result, session = _run_db_preflight()

    report = result.report
    assert result.exit_code == 0
    assert report["openai_key_source_bucket"] == "zero"
    assert report["openai_key_read_bucket"] == "zero"
    assert report["openai_key_file_read_bucket"] == "zero"
    assert report["openai_project_present_bucket"] == "zero"
    _assert_no_raw_values(report, FAKE_KEY, FAKE_KEY_PATH, FAKE_PROJECT)
    _assert_no_db_writes(session)


def test_approved_live_mode_fake_sdk_performs_exactly_one_call_after_context_build() -> None:
    recorder: dict[str, Any] = {}
    result, session = _run_live(
        sdk_loader=_fake_sdk_loader(
            recorder,
            response={"status": "completed", "output_text": "{}", "usage": {"input_tokens": 1}},
        )
    )

    report = result.report
    assert result.exit_code == 0
    assert report["contract_status"] == _module().STATUS_LIVE_SUCCEEDED
    assert report["database_connected"] is True
    assert report["bundle_context_loaded_bucket"] == "one"
    assert report["request_shape_valid_bucket"] == "one"
    assert report["openai_key_source_bucket"] == "env"
    assert report["openai_key_read_bucket"] == "one"
    assert report["sdk_import_bucket"] == "one"
    assert report["async_openai_constructor_bucket"] == "one"
    assert report["responses_create_callable_bucket"] == "one"
    assert report["openai_call_attempted"] is True
    assert report["live_openai_call_attempted"] is True
    assert report["live_openai_call_attempted_bucket"] == "one"
    assert report["live_openai_call_completed_bucket"] == "one"
    assert report["live_result_class_bucket"] == "success"
    assert report["http_status_bucket"] == "2xx"
    assert report["response_parse_bucket"] == "one"
    assert report["structured_output_observed_bucket"] == "one"
    assert report["usage_present_bucket"] == "one"
    assert report["latency_ms_present_bucket"] == "one"
    assert recorder["constructor"]["api_key"] == FAKE_KEY
    assert recorder["constructor"]["project"] == FAKE_PROJECT
    assert recorder["constructor"]["max_retries"] == 0
    assert recorder["constructor"]["timeout"] == 10.0
    assert len(recorder["requests"]) == 1
    request = recorder["requests"][0]
    assert request["text"]["format"]["type"] == "json_schema"
    assert request["text"]["format"]["strict"] is True
    assert request["tools"] == []
    assert "max_output_tokens" not in request
    assert request["prompt_cache_key"] == (
        "judge:github_primary:judge_prompt_v1:judge_output_v1:policy_v1"
    )
    assert report["top_level_request_key_presence_buckets"]["max_output_tokens"] == "zero"
    assert report["top_level_request_key_presence_buckets"]["prompt_cache_key"] == "one"
    assert report["optional_null_field_count_bucket"] == "zero"
    assert report["optional_null_field_name_buckets"] == []
    assert report["max_output_tokens_presence_bucket"] == "zero"
    assert report["max_output_tokens_null_bucket"] == "zero"
    assert report["prompt_cache_key_presence_bucket"] == "one"
    _assert_no_downstream(report)
    _assert_no_db_writes(session)


@pytest.mark.parametrize(
    (
        "name",
        "status_code",
        "error_type",
        "code",
        "result_bucket",
        "http_bucket",
        "openai_error_bucket",
    ),
    [
        ("AuthenticationError", 401, "authentication_error", None, "authentication", "401", "authentication_error"),
        ("PermissionDeniedError", 403, "permission_error", None, "permission", "403", "permission_error"),
        ("BadRequestError", 400, "invalid_request_error", "json_schema_invalid", "schema_rejected", "400", "invalid_request_error"),
        ("UnprocessableEntityError", 422, "invalid_request_error", "response_format_schema_invalid", "schema_rejected", "422", "invalid_request_error"),
        ("RateLimitError", 429, "rate_limit_error", None, "rate_limit", "429", "rate_limit_error"),
        ("APITimeoutError", None, None, None, "timeout", "zero", "api_timeout_error"),
        ("APIConnectionError", None, None, None, "api_connection_error", "zero", "api_connection_error"),
        ("InternalServerError", 500, "server_error", None, "api_status_error", "5xx", "server_error"),
    ],
)
def test_approved_live_mode_classifies_fake_sdk_exceptions(
    name: str,
    status_code: int | None,
    error_type: str | None,
    code: str | None,
    result_bucket: str,
    http_bucket: str,
    openai_error_bucket: str,
) -> None:
    recorder: dict[str, Any] = {}
    exc = _fake_exception(
        name=name,
        status_code=status_code,
        error_type=error_type,
        code=code,
    )

    result, session = _run_live(
        sdk_loader=_fake_sdk_loader(recorder, side_effect=exc),
        forbidden_raw_values=(FAKE_ERROR_TEXT, FAKE_REQUEST_ID),
    )

    report = result.report
    assert result.exit_code == 1
    assert report["contract_status"] == _module().STATUS_LIVE_FAILED
    assert len(recorder["requests"]) == 1
    assert report["openai_call_attempted"] is True
    assert report["live_openai_call_attempted"] is True
    assert report["live_openai_call_attempted_bucket"] == "one"
    assert report["live_openai_call_completed_bucket"] == "zero"
    assert report["live_result_class_bucket"] == result_bucket
    assert report["http_status_bucket"] == http_bucket
    assert report["openai_error_type_bucket"] == openai_error_bucket
    assert report["openai_error_code_bucket"] in {
        "zero",
        "json_schema",
        "response_format",
    }
    assert report["openai_error_param_bucket"] == "zero"
    assert report["openai_error_message_hint_count_bucket"] in {"zero", "one", "multiple"}
    assert report["response_parse_bucket"] == "zero"
    assert report["structured_output_observed_bucket"] == "zero"
    assert report["usage_present_bucket"] == "zero"
    assert report["checks_failed"] == [f"openai.live_call.{result_bucket}"]
    _assert_no_raw_values(report, FAKE_ERROR_TEXT, FAKE_REQUEST_ID)
    _assert_no_downstream(report)
    _assert_no_db_writes(session)


def test_approved_live_mode_reports_sanitized_error_code_param_and_message_hints() -> None:
    recorder: dict[str, Any] = {}
    exc = _fake_exception(
        name="BadRequestError",
        status_code=400,
        error_type="invalid_request_error",
        code="unsupported_parameter",
        param="max_output_tokens",
    )
    exc.body = {
        "message": (
            "Unsupported parameter max_output_tokens cannot be null. "
            + FAKE_ERROR_TEXT
        ),
        "code": "unsupported_parameter",
        "param": "max_output_tokens",
    }

    result, session = _run_live(
        sdk_loader=_fake_sdk_loader(recorder, side_effect=exc),
        forbidden_raw_values=(FAKE_ERROR_TEXT, FAKE_REQUEST_ID),
    )

    report = result.report
    assert result.exit_code == 1
    assert report["contract_status"] == _module().STATUS_LIVE_FAILED
    assert report["live_result_class_bucket"] == "invalid_request"
    assert report["http_status_bucket"] == "400"
    assert report["openai_error_type_bucket"] == "invalid_request_error"
    assert report["openai_error_code_bucket"] == "unsupported_parameter"
    assert report["openai_error_param_bucket"] == "max_output_tokens"
    assert report["openai_error_message_hint_count_bucket"] == "multiple"
    assert set(report["openai_error_message_hint_buckets"]) >= {
        "optional_null",
        "unsupported_parameter",
        "max_output_tokens",
    }
    _assert_no_raw_values(report, FAKE_ERROR_TEXT, FAKE_REQUEST_ID)
    _assert_no_downstream(report)
    _assert_no_db_writes(session)


def test_key_source_conflict_fails_before_sdk_construction_or_live_call() -> None:
    recorder: dict[str, Any] = {}
    result, session = _run_live(
        runtime_env_reader=_runtime_env,
        sdk_loader=_fake_sdk_loader(recorder),
    )

    report = result.report
    assert result.exit_code == 1
    assert report["contract_status"] == _module().STATUS_LIVE_FAILED
    assert report["openai_key_source_bucket"] == "both_conflict"
    assert report["openai_key_read_bucket"] == "zero"
    assert report["openai_key_file_read_bucket"] == "zero"
    assert "constructor" not in recorder
    _assert_no_live_or_downstream(report)
    assert report["checks_failed"] == ["openai_key.source_conflict"]
    _assert_no_raw_values(report, FAKE_KEY, FAKE_KEY_PATH, FAKE_PROJECT)
    _assert_no_db_writes(session)


def test_report_never_emits_raw_runtime_key_project_db_prompt_context_or_url() -> None:
    recorder: dict[str, Any] = {}
    result, session = _run_live(
        sdk_loader=_fake_sdk_loader(recorder),
        forbidden_raw_values=(FAKE_ERROR_TEXT, FAKE_REQUEST_ID),
    )

    assert result.report["raw_values_emitted"] is False
    _assert_no_raw_values(
        result.report,
        FAKE_RUNTIME_PATH,
        FAKE_DATABASE_URL,
        FAKE_KEY,
        FAKE_KEY_PATH,
        FAKE_PROJECT,
        FAKE_SOURCE_TEXT,
        FAKE_URL,
        DEFAULT_PROMPT,
        DEFAULT_CONTEXT,
        FAKE_ERROR_TEXT,
        FAKE_REQUEST_ID,
    )
    _assert_no_db_writes(session)


def test_db_redis_validator_policy_notifier_and_telegram_flags_remain_false() -> None:
    result, session = _run_db_preflight()

    _assert_no_live_or_downstream(result.report)
    _assert_no_db_writes(session)


def test_cli_outputs_json_in_default_mode_without_runtime_reads() -> None:
    private_env_key = "private" + "-cli" + "-openai" + "-key"
    private_database_url = "postgresql://" + "private-cli-db"
    env = os.environ.copy()
    env["OPENAI_API_KEY"] = private_env_key
    env["DATABASE_URL"] = private_database_url

    completed = subprocess.run(
        [sys.executable, str(SCRIPT), "--format", "json"],
        cwd=ROOT,
        env=env,
        text=True,
        capture_output=True,
        timeout=15,
        check=True,
    )

    report = json.loads(completed.stdout)
    assert report["contract_status"] == _module().STATUS_PREFLIGHT_PASSED
    assert report["runtime_env_read"] is False
    assert report["database_connected"] is False
    assert report["openai_key_read_bucket"] == "zero"
    assert report["openai_call_attempted"] is False
    assert private_env_key not in completed.stdout
    assert private_database_url not in completed.stdout
    assert completed.stderr == ""


def _fake_sdk_loader(
    recorder: dict[str, Any],
    *,
    response: Any | None = None,
    side_effect: Exception | None = None,
) -> Any:
    def loader() -> Any:
        module = _module()

        class FakeAsyncOpenAI:
            def __init__(
                self,
                *,
                api_key: str,
                project: str | None,
                timeout: float,
                max_retries: int,
            ) -> None:
                recorder["constructor"] = {
                    "api_key": api_key,
                    "project": project,
                    "timeout": timeout,
                    "max_retries": max_retries,
                }
                recorder.setdefault("requests", [])
                self.responses = FakeResponses(
                    recorder,
                    response=response,
                    side_effect=side_effect,
                )

            async def aclose(self) -> None:
                recorder["closed"] = True

        return module.SdkImports(async_openai=FakeAsyncOpenAI)

    return loader


class FakeResponses:
    def __init__(
        self,
        recorder: dict[str, Any],
        *,
        response: Any | None,
        side_effect: Exception | None,
    ) -> None:
        self._recorder = recorder
        self._response = response
        self._side_effect = side_effect

    async def create(self, **request: Any) -> Any:
        self._recorder.setdefault("requests", []).append(request)
        if self._side_effect is not None:
            raise self._side_effect
        return self._response or {"status": "completed", "output_text": "{}", "usage": {"input_tokens": 1}}


def _fake_exception(
    *,
    name: str,
    status_code: int | None,
    error_type: str | None,
    code: str | None,
    param: str | None = None,
) -> Exception:
    cls = type(name, (Exception,), {})
    exc = cls(FAKE_ERROR_TEXT)
    exc.status_code = status_code
    exc.type = error_type
    exc.code = code
    exc.param = param
    exc.body = {"message": FAKE_ERROR_TEXT, "code": code, "param": param}
    exc.request_id = FAKE_REQUEST_ID
    return exc


def _raising_runtime_reader(_path: str | Path) -> Mapping[str, str]:
    raise AssertionError("runtime env should not be read")


def _raising_database_factory(_database_url: str) -> Any:
    raise AssertionError("database should not be opened")


def _raising_sdk_loader() -> Any:
    raise AssertionError("sdk should not be loaded")
