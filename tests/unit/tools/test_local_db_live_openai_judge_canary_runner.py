from __future__ import annotations

import ast
import json
from pathlib import Path
from uuid import UUID

import pytest

from tools import local_db_live_openai_judge_canary_runner as runner


ROOT = Path(__file__).resolve().parents[3]
SOURCE_FIXTURE = "tests/fixtures/upstream/source_message_github_repo_signal.json"
GITHUB_FIXTURE = "tests/fixtures/upstream/github_repo_snapshot_example_tool.json"
PG_SCHEME = "postgresql+psycopg"
SAFE_DATABASE_NAME = "github_ai_catchbot_test"
SOCKET_HOST = "/var/run/postgresql"
SAFE_SOCKET_URL = f"{PG_SCHEME}:///{SAFE_DATABASE_NAME}?host={SOCKET_HOST}"
SECRET_VALUE = "local" + "_" + "secret"
PASSWORD_URL = f"{PG_SCHEME}://local_user:{SECRET_VALUE}@127.0.0.1:5432/{SAFE_DATABASE_NAME}"
FAKE_API_KEY = "unit_fake_openai_key"
RAW_EXCEPTION_MESSAGE = "raw exception message with unit_fake_openai_key should stay hidden"
RAW_RESPONSE_BODY = "raw OpenAI response body should stay hidden"
RAW_PROMPT_TEXT = "unit raw prompt text should stay hidden"
RAW_EVIDENCE_TEXT = "unit raw evidence text should stay hidden"
RAW_SCHEMA_DESCRIPTION = "unit raw schema description should stay hidden"
GROUP_ID = UUID("11111111-1111-4111-8111-111111111111")
BUNDLE_ID = UUID("22222222-2222-4222-8222-222222222222")
JUDGE_RUN_ID = UUID("33333333-3333-4333-8333-333333333333")
ARTIFACT_ID = UUID("44444444-4444-4444-8444-444444444444")


class RecordingResponses:
    def __init__(self, response=None) -> None:
        self.calls = []
        self.response = response or {"status": "completed", "output_text": "{}"}

    def create(self, **request):
        self.calls.append(request)
        return self.response


class RecordingLiveClient:
    def __init__(self, response=None) -> None:
        self.responses = RecordingResponses(response=response)


class FactoryRecorder:
    def __init__(self, response=None) -> None:
        self.calls = []
        self.clients = []
        self.response = response

    def __call__(self, *, api_key: str):
        self.calls.append(api_key)
        client = RecordingLiveClient(response=self.response)
        self.clients.append(client)
        return client


class RaisingResponses:
    def __init__(self, exc: Exception) -> None:
        self.calls = []
        self.exc = exc

    def create(self, **request):
        self.calls.append(request)
        raise self.exc


class RaisingLiveClient:
    def __init__(self, exc: Exception) -> None:
        self.responses = RaisingResponses(exc)


class RaisingFactory:
    def __init__(self, exc: Exception) -> None:
        self.calls = []
        self.clients = []
        self.exc = exc

    def __call__(self, *, api_key: str):
        self.calls.append(api_key)
        client = RaisingLiveClient(self.exc)
        self.clients.append(client)
        return client


class FakeOpenAIException(Exception):
    def __init__(
        self,
        message: str = RAW_EXCEPTION_MESSAGE,
        *,
        status_code: int | str | None = None,
        code: str | None = None,
        error_type: str | None = None,
        body: dict[str, object] | None = None,
    ) -> None:
        super().__init__(message)
        self.status_code = status_code
        self.code = code
        self.type = error_type
        self.body = body


class AuthenticationError(FakeOpenAIException):
    pass


class PermissionDeniedError(FakeOpenAIException):
    pass


class RateLimitError(FakeOpenAIException):
    pass


class APIConnectionError(FakeOpenAIException):
    pass


class APITimeoutError(FakeOpenAIException):
    pass


def _parse_args(*extra: str):
    return runner.build_parser().parse_args(
        [
            "--database-url",
            SAFE_SOCKET_URL,
            "--source-fixture",
            SOURCE_FIXTURE,
            "--github-snapshot-fixture",
            GITHUB_FIXTURE,
            "--replay-namespace",
            "unit-live-openai-judge-canary",
            *extra,
        ]
    )


def _authorized_args(*extra: str):
    return _parse_args(
        "--confirm-local-test-db",
        "--allow-live-openai",
        "--confirm-live-openai-call",
        "--openai-api-key-env",
        "OPENAI_API_KEY",
        *extra,
    )


def _run(args=None, *, env=None, live_client_factory=None):
    return runner.run(
        args or _authorized_args(),
        env=env or {"APP_ENV": "test", "OPENAI_API_KEY": FAKE_API_KEY},
        live_client_factory=live_client_factory,
        repo_root=ROOT,
    )


def test_pass_delegates_to_restricted_runner_and_returns_stable_authority_flags(monkeypatch) -> None:
    restricted_calls = _patch_restricted_run(monkeypatch, call_openai=True)
    factory = FactoryRecorder()

    result = _run(live_client_factory=factory)

    assert result.exit_code == 0
    assert result.report == _expected_pass_report()
    assert len(restricted_calls) == 1
    assert restricted_calls[0]["args"].confirm_local_test_db is True
    assert factory.calls == [FAKE_API_KEY]
    assert factory.clients[0].responses.calls[0]["max_output_tokens"] == runner.DEFAULT_MAX_OUTPUT_TOKENS


def test_refuses_missing_confirm_local_test_db_before_delegate_or_client_creation(monkeypatch) -> None:
    restricted_calls = _patch_restricted_run(monkeypatch, call_openai=True)
    factory = FactoryRecorder()

    result = _run(_parse_args("--allow-live-openai", "--confirm-live-openai-call"), live_client_factory=factory)

    assert result.exit_code == 1
    assert result.report["checks_failed"] == ["confirm_local_test_db_required"]
    assert restricted_calls == []
    assert factory.calls == []


def test_refuses_missing_allow_live_openai(monkeypatch) -> None:
    restricted_calls = _patch_restricted_run(monkeypatch, call_openai=True)
    factory = FactoryRecorder()

    result = _run(
        _parse_args("--confirm-local-test-db", "--confirm-live-openai-call"),
        live_client_factory=factory,
    )

    assert result.exit_code == 1
    assert result.report["checks_failed"] == ["allow_live_openai_required"]
    assert restricted_calls == []
    assert factory.calls == []


def test_refuses_missing_confirm_live_openai_call(monkeypatch) -> None:
    restricted_calls = _patch_restricted_run(monkeypatch, call_openai=True)
    factory = FactoryRecorder()

    result = _run(_parse_args("--confirm-local-test-db", "--allow-live-openai"), live_client_factory=factory)

    assert result.exit_code == 1
    assert result.report["checks_failed"] == ["confirm_live_openai_call_required"]
    assert restricted_calls == []
    assert factory.calls == []


def test_refuses_app_env_prod_before_delegate_or_client_creation(monkeypatch) -> None:
    restricted_calls = _patch_restricted_run(monkeypatch, call_openai=True)
    factory = FactoryRecorder()

    result = _run(env={"APP_ENV": "prod", "OPENAI_API_KEY": FAKE_API_KEY}, live_client_factory=factory)

    assert result.exit_code == 1
    assert result.report["checks_failed"] == ["app_env_test_required"]
    assert restricted_calls == []
    assert factory.calls == []


def test_refuses_unsafe_database_url_before_delegate_or_client_creation(monkeypatch) -> None:
    restricted_calls = _patch_restricted_run(monkeypatch, call_openai=True)
    factory = FactoryRecorder()
    args = runner.build_parser().parse_args(
        [
            "--database-url",
            "postgresql+psycopg://db.example.com/github_ai_catchbot_prod",
            "--source-fixture",
            SOURCE_FIXTURE,
            "--github-snapshot-fixture",
            GITHUB_FIXTURE,
            "--replay-namespace",
            "unit-live-openai-unsafe-db",
            "--confirm-local-test-db",
            "--allow-live-openai",
            "--confirm-live-openai-call",
        ]
    )

    result = _run(args, live_client_factory=factory)

    assert result.exit_code == 1
    assert result.report["database_url_guard_passed"] is False
    assert "database_url_remote_host_rejected" in result.report["checks_failed"]
    assert restricted_calls == []
    assert factory.calls == []


def test_refuses_disallowed_openai_api_key_env_before_reading_key_or_delegating(monkeypatch) -> None:
    restricted_calls = _patch_restricted_run(monkeypatch, call_openai=True)
    factory = FactoryRecorder()

    result = _run(
        _authorized_args("--openai-api-key-env", "OTHER_OPENAI_KEY"),
        env={"APP_ENV": "test", "OTHER_OPENAI_KEY": "other"},
        live_client_factory=factory,
    )

    assert result.exit_code == 1
    assert result.report["openai_api_key_env_allowed"] is False
    assert result.report["openai_api_key_present"] is False
    assert result.report["checks_failed"] == ["openai_api_key_env_disallowed"]
    assert restricted_calls == []
    assert factory.calls == []


def test_refuses_missing_api_key_env_after_non_secret_gates_and_before_delegate(monkeypatch) -> None:
    restricted_calls = _patch_restricted_run(monkeypatch, call_openai=True)
    factory = FactoryRecorder()

    result = _run(env={"APP_ENV": "test"}, live_client_factory=factory)

    assert result.exit_code == 1
    assert result.report["database_url_guard_passed"] is True
    assert result.report["openai_api_key_env_allowed"] is True
    assert result.report["openai_api_key_present"] is False
    assert result.report["checks_failed"] == ["openai_api_key_missing"]
    assert restricted_calls == []
    assert factory.calls == []


def test_reuse_pass_does_not_create_client_or_call_live_openai(monkeypatch) -> None:
    _patch_restricted_run(monkeypatch, call_openai=False)
    factory = FactoryRecorder()

    result = _run(live_client_factory=factory)

    assert result.exit_code == 0
    assert result.report["existing_judge_output_reused"] is True
    assert result.report["openai_client_created"] is False
    assert result.report["live_openai_called"] is False
    assert result.report["openai_request_max_output_tokens_capped"] is False
    assert factory.calls == []


def test_sdk_missing_is_reported_without_requiring_openai_package(monkeypatch) -> None:
    _patch_restricted_run(monkeypatch, call_openai=True, catch_openai_error=True)

    real_import = __import__

    def guarded_import(name, *args, **kwargs):  # noqa: ANN001
        if name == "openai":
            raise ImportError("missing")
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr("builtins.__import__", guarded_import)
    result = _run(live_client_factory=None)

    assert result.exit_code == 1
    assert result.report["checks_failed"] == ["openai_sdk_missing"]
    assert result.report["openai_client_created"] is False
    assert result.report["live_openai_called"] is False


@pytest.mark.parametrize(
    ("exc", "expected"),
    [
        (AuthenticationError(), "openai_authentication_failed"),
        (PermissionDeniedError(), "openai_permission_denied"),
        (RateLimitError(), "openai_rate_limited"),
        (FakeOpenAIException(status_code=404, code="model_not_found"), "openai_model_not_found_or_unavailable"),
        (FakeOpenAIException(status_code=400, error_type="invalid_request_error"), "openai_bad_request"),
        (
            FakeOpenAIException(
                status_code=429,
                body={
                    "error": {
                        "code": "insufficient_quota",
                        "type": "rate_limit_error",
                        "message": RAW_RESPONSE_BODY,
                    }
                },
            ),
            "openai_quota_or_billing_failed",
        ),
        (APIConnectionError(), "openai_connection_failed"),
        (APITimeoutError(), "openai_timeout"),
        (FakeOpenAIException(status_code=503), "openai_server_error"),
        (FakeOpenAIException(code="unknown_error_code"), "openai_responses_create_failed"),
    ],
)
def test_classifies_openai_responses_create_exceptions(exc: Exception, expected: str) -> None:
    assert runner.classify_openai_responses_create_exception(exc) == expected


def test_classified_failure_output_omits_raw_exception_response_prompt_key_and_db(monkeypatch) -> None:
    _patch_restricted_run(monkeypatch, call_openai=True, catch_openai_error=True)
    exc = FakeOpenAIException(
        status_code=400,
        body={
            "error": {
                "code": "invalid_request_error",
                "type": "invalid_request_error",
                "message": RAW_RESPONSE_BODY,
            }
        },
    )
    factory = RaisingFactory(exc)
    args = _authorized_args()
    args.database_url = PASSWORD_URL

    result = _run(args, live_client_factory=factory)
    text = runner.render_json(result.report)

    assert result.exit_code == 1
    assert result.report["checks_failed"] == ["openai_bad_request"]
    assert result.report["openai_client_created"] is True
    assert result.report["live_openai_called"] is True
    assert factory.calls == [FAKE_API_KEY]
    for forbidden in (
        PASSWORD_URL,
        SECRET_VALUE,
        FAKE_API_KEY,
        RAW_EXCEPTION_MESSAGE,
        RAW_RESPONSE_BODY,
        "Synthetic local fixture for a developer workflow helper.",
        "example/example-tool",
        "FakeOpenAIException",
    ):
        assert forbidden not in text


def test_sanitized_openai_request_diagnostic_includes_structure_without_text_or_secret_values() -> None:
    request = _openai_request()
    request["input"][0]["content"][0]["text"] = RAW_PROMPT_TEXT
    request["input"][1]["content"][0]["text"] = json.dumps(
        {
            "evidence": RAW_EVIDENCE_TEXT,
            "database_url": PASSWORD_URL,
            "openai_key": FAKE_API_KEY,
        }
    )
    prepared, audit = runner.prepare_openai_responses_request(
        request,
        max_output_tokens=runner.DEFAULT_MAX_OUTPUT_TOKENS,
        sensitive_values=(PASSWORD_URL, FAKE_API_KEY),
    )

    diagnostic = runner.build_sanitized_openai_request_diagnostic(
        prepared,
        max_output_tokens=runner.DEFAULT_MAX_OUTPUT_TOKENS,
        preparation_error_code=audit.error_code,
    )
    text = runner.render_json({"diagnostic": diagnostic})

    assert diagnostic["schema_version"] == runner.DIAGNOSTIC_SCHEMA_VERSION
    assert diagnostic["request_preparation_error_code"] == "openai_request_sensitive_content_rejected"
    assert diagnostic["top_level_keys"] == [
        "input",
        "max_output_tokens",
        "model",
        "prompt_cache_key",
        "reasoning",
        "text",
        "tools",
    ]
    assert diagnostic["model_allowed"] is True
    assert diagnostic["model"] == "gpt-5.4-mini"
    assert diagnostic["input_item_count"] == 2
    assert diagnostic["input_items"][0]["role"] == "developer"
    assert diagnostic["input_items"][1]["role"] == "user"
    assert diagnostic["input_items"][0]["content_items"][0]["type"] == "input_text"
    assert diagnostic["input_items"][0]["content_items"][0]["keys"] == ["text", "type"]
    assert diagnostic["input_items"][0]["content_items"][0]["has_text"] is True
    assert diagnostic["input_text_content_present"] is True
    assert diagnostic["tools_count"] == 0
    assert diagnostic["tools_empty"] is True
    assert diagnostic["max_output_tokens"] == runner.DEFAULT_MAX_OUTPUT_TOKENS
    assert diagnostic["max_output_tokens_cap_status"] == "within_cap"
    assert diagnostic["reasoning_keys"] == ["effort"]
    assert diagnostic["reasoning_effort"] == "low"
    for forbidden in (
        RAW_PROMPT_TEXT,
        RAW_EVIDENCE_TEXT,
        PASSWORD_URL,
        SECRET_VALUE,
        FAKE_API_KEY,
        RAW_EXCEPTION_MESSAGE,
        RAW_RESPONSE_BODY,
    ):
        assert forbidden not in text


def test_sanitized_openai_request_diagnostic_reports_schema_structure_without_descriptions() -> None:
    request = _openai_request()
    schema = request["text"]["format"]["schema"]
    schema["description"] = RAW_SCHEMA_DESCRIPTION
    schema["properties"]["headline"]["description"] = RAW_SCHEMA_DESCRIPTION

    diagnostic = runner.build_sanitized_openai_request_diagnostic(
        request,
        max_output_tokens=runner.DEFAULT_MAX_OUTPUT_TOKENS,
    )
    text = runner.render_json({"diagnostic": diagnostic})

    assert diagnostic["text_format_keys"] == ["name", "schema", "strict", "type"]
    assert diagnostic["text_format_type"] == "json_schema"
    assert diagnostic["text_format_name"] == "judge_output_v1"
    assert diagnostic["text_format_strict"] is True
    assert "description" in diagnostic["json_schema_top_level_keys"]
    assert diagnostic["json_schema_required_count"] == len(runner.restricted_runner.REQUIRED_OUTPUT_KEYS)
    assert diagnostic["json_schema_property_keys"] == sorted(runner.restricted_runner.REQUIRED_OUTPUT_KEYS)
    assert diagnostic["json_schema_subset_issue_count"] == 0
    assert diagnostic["json_schema_subset_issue_codes"] == []
    assert diagnostic["json_schema_subset_issues"] == []
    assert RAW_SCHEMA_DESCRIPTION not in text


def test_sanitized_openai_request_diagnostic_detects_unsupported_top_level_keys_without_values() -> None:
    request = _openai_request()
    request["unsupported_unit_key"] = {"body": RAW_EVIDENCE_TEXT}

    diagnostic = runner.build_sanitized_openai_request_diagnostic(
        request,
        max_output_tokens=runner.DEFAULT_MAX_OUTPUT_TOKENS,
    )
    text = runner.render_json({"diagnostic": diagnostic})

    assert diagnostic["unsupported_top_level_keys"] == ["unsupported_unit_key"]
    assert diagnostic["unsupported_top_level_key_count"] == 1
    assert "unsupported_unit_key" in diagnostic["top_level_keys"]
    assert RAW_EVIDENCE_TEXT not in text


def test_structured_output_subset_validator_accepts_valid_minimal_schema() -> None:
    assert runner.validate_openai_structured_output_schema_subset(_minimal_openai_schema()) == []


def test_structured_output_subset_validator_flags_nested_object_missing_additional_properties_false() -> None:
    schema = _minimal_openai_schema(
        {
            "child": {
                "type": "object",
                "required": ["name"],
                "properties": {"name": {"type": "string"}},
            }
        }
    )

    issues = runner.validate_openai_structured_output_schema_subset(schema)

    assert {
        "issue_code": "object_missing_additional_properties_false",
        "schema_path": "/properties/child",
    } in issues


def test_structured_output_subset_validator_flags_nested_required_property_mismatch() -> None:
    schema = _minimal_openai_schema(
        {
            "child": {
                "type": "object",
                "additionalProperties": False,
                "required": ["name"],
                "properties": {
                    "name": {"type": "string"},
                    "age": {"type": "integer"},
                },
            }
        }
    )

    issues = runner.validate_openai_structured_output_schema_subset(schema)

    assert {
        "issue_code": "required_property_mismatch",
        "schema_path": "/properties/child/required",
        "key": "age",
    } in issues


def test_structured_output_subset_validator_flags_property_missing_type() -> None:
    schema = _minimal_openai_schema({"name": {"description": RAW_SCHEMA_DESCRIPTION}})

    issues = runner.validate_openai_structured_output_schema_subset(schema)

    assert {
        "issue_code": "property_missing_type",
        "schema_path": "/properties/name",
        "key": "name",
    } in issues


def test_structured_output_subset_validator_flags_array_missing_items() -> None:
    schema = _minimal_openai_schema({"tags": {"type": "array"}})

    issues = runner.validate_openai_structured_output_schema_subset(schema)

    assert {
        "issue_code": "array_missing_items",
        "schema_path": "/properties/tags",
        "key": "tags",
    } in issues


def test_structured_output_subset_validator_flags_root_anyof() -> None:
    schema = _minimal_openai_schema()
    schema["anyOf"] = [_minimal_openai_schema()]

    issues = runner.validate_openai_structured_output_schema_subset(schema)

    assert {
        "issue_code": "root_anyof_disallowed",
        "schema_path": "",
    } in issues


def test_structured_output_subset_validator_flags_unsupported_keywords_without_leaking_descriptions() -> None:
    schema = _minimal_openai_schema(
        {
            "name": {
                "type": "string",
                "description": RAW_SCHEMA_DESCRIPTION,
                "default": RAW_SCHEMA_DESCRIPTION,
            }
        }
    )

    issues = runner.validate_openai_structured_output_schema_subset(schema)
    text = runner.render_json({"issues": issues})

    assert {
        "issue_code": "unsupported_schema_keyword",
        "schema_path": "/properties/name/default",
        "key": "default",
    } in issues
    assert RAW_SCHEMA_DESCRIPTION not in text


def test_real_openai_request_schema_matches_local_structured_output_subset() -> None:
    schema = _openai_request()["text"]["format"]["schema"]

    issues = runner.validate_openai_structured_output_schema_subset(schema)

    assert issues == []


def test_diagnostic_flag_captures_request_without_live_authority_api_key_or_client(monkeypatch) -> None:
    calls = []

    def fake_restricted_run(args, *, env, openai_client, repo_root):
        calls.append(
            {
                "args": args,
                "env": dict(env),
                "openai_client_type": type(openai_client).__name__,
                "repo_root": repo_root,
            }
        )
        try:
            openai_client.responses.create(**_openai_request())
        except RuntimeError as exc:
            assert str(exc) == runner.OPENAI_REQUEST_DIAGNOSTIC_CAPTURED
        else:  # pragma: no cover - explicit assertion path.
            raise AssertionError("diagnostic adapter did not stop before an OpenAI response")
        report = _restricted_pass_report()
        report.update(
            {
                "status": "fail",
                "openai_structured_output_received": False,
                "judge_output_created": False,
                "judge_run_updated": False,
                "judge_output_ready_event_created": False,
                "checks_failed": ["RuntimeError"],
            }
        )
        return runner.restricted_runner.RunnerResult(exit_code=1, report=report)

    monkeypatch.setattr(runner.restricted_runner, "run", fake_restricted_run)
    factory = FactoryRecorder()
    args = _parse_args("--confirm-local-test-db", "--print-sanitized-openai-request-diagnostic")

    result = runner.run(
        args,
        env={"APP_ENV": "test"},
        live_client_factory=factory,
        repo_root=ROOT,
    )

    assert result.exit_code == 0
    assert result.report["status"] == "pass"
    assert result.report["checks_failed"] == []
    assert result.report["sanitized_openai_request_diagnostic_requested"] is True
    assert result.report["live_openai_authority_confirmed"] is False
    assert result.report["openai_api_key_present"] is False
    assert result.report["openai_client_created"] is False
    assert result.report["live_openai_called"] is False
    assert result.report["openai_request_diagnostic"]["model"] == "gpt-5.4-mini"
    assert calls[0]["openai_client_type"] == "SanitizedOpenAIRequestDiagnosticClientAdapter"
    assert factory.calls == []
    assert "allow_live_openai_required" not in result.report["checks_failed"]
    assert "confirm_live_openai_call_required" not in result.report["checks_failed"]
    assert "openai_api_key_missing" not in result.report["checks_failed"]


def test_adapter_rejects_disallowed_model() -> None:
    request = _openai_request()
    request["model"] = "gpt-5.4"
    adapter = _adapter()

    with pytest.raises(RuntimeError, match="openai_request_model_disallowed"):
        adapter.responses.create(**request)

    assert adapter.audit.model_allowed is False


def test_adapter_rejects_non_empty_tools() -> None:
    request = _openai_request()
    request["tools"] = [{"type": "web_search"}]
    adapter = _adapter()

    with pytest.raises(RuntimeError, match="openai_request_tools_not_disabled"):
        adapter.responses.create(**request)

    assert adapter.audit.tools_disabled is False


def test_adapter_rejects_non_strict_schema() -> None:
    request = _openai_request()
    request["text"]["format"]["strict"] = False
    adapter = _adapter()

    with pytest.raises(RuntimeError, match="openai_request_schema_not_strict"):
        adapter.responses.create(**request)

    assert adapter.audit.strict_schema_valid is False


@pytest.mark.parametrize(
    "unsafe_value",
    [
        SAFE_SOCKET_URL,
        FAKE_API_KEY,
        "sk-" + "testsecretvalue123456",
        "web_search",
        "file_search",
    ],
)
def test_adapter_rejects_sensitive_or_external_fetch_request_content(unsafe_value: str) -> None:
    request = _openai_request()
    request["input"][1]["content"][0]["text"] = json.dumps({"unsafe": unsafe_value})
    adapter = _adapter()

    with pytest.raises(RuntimeError, match="openai_request_sensitive_content_rejected"):
        adapter.responses.create(**request)

    assert adapter.audit.sensitive_content_absent is False


def test_adapter_applies_max_output_token_cap_before_forwarding() -> None:
    request = _openai_request()
    request.pop("max_output_tokens", None)
    factory = FactoryRecorder()
    adapter = _adapter(factory=factory, max_output_tokens=777)

    response = adapter.responses.create(**request)

    assert response["status"] == "completed"
    assert adapter.audit.max_output_tokens_capped is True
    assert factory.clients[0].responses.calls[0]["max_output_tokens"] == 777


def test_sanitized_output_does_not_contain_db_url_or_api_key(monkeypatch) -> None:
    _patch_restricted_run(monkeypatch, call_openai=True)
    factory = FactoryRecorder()
    args = _authorized_args()
    args.database_url = PASSWORD_URL

    result = _run(args, live_client_factory=factory)
    text = runner.render_json(result.report)

    assert result.exit_code == 0
    assert PASSWORD_URL not in text
    assert SECRET_VALUE not in text
    assert FAKE_API_KEY not in text


def test_runner_source_has_no_forbidden_runtime_imports_or_downstream_clients() -> None:
    source = (ROOT / "tools/local_db_live_openai_judge_canary_runner.py").read_text(encoding="utf-8")
    tree = ast.parse(source)
    top_level_imports = {
        alias.name.split(".", 1)[0]
        for node in tree.body
        if isinstance(node, ast.Import)
        for alias in node.names
    }
    top_level_imports.update(
        node.module.split(".", 1)[0]
        for node in tree.body
        if isinstance(node, ast.ImportFrom) and node.module
    )

    assert {"redis", "telegram", "docker", "systemd", "requests", "httpx", "aiohttp", "urllib"}.isdisjoint(
        top_level_imports
    )
    assert "policy_engine" not in source
    assert "notifier_telegram" not in source
    assert "openai_base_url" not in source
    assert "OPENAI_BASE_URL" not in source
    assert "os.getenv" not in source
    assert 'os.environ["OPENAI_API_KEY"]' not in source


def _patch_restricted_run(monkeypatch, *, call_openai: bool, catch_openai_error: bool = False):
    calls = []

    def fake_run(args, *, env, openai_client, repo_root):
        calls.append({"args": args, "env": dict(env), "repo_root": repo_root})
        if call_openai:
            try:
                openai_client.responses.create(**_openai_request())
            except Exception:
                if not catch_openai_error:
                    raise
                report = _restricted_pass_report()
                report.update(
                    {
                        "status": "fail",
                        "openai_structured_output_received": False,
                        "judge_output_created": False,
                        "judge_run_updated": False,
                        "judge_output_ready_event_created": False,
                        "checks_failed": ["RuntimeError"],
                    }
                )
                return runner.restricted_runner.RunnerResult(exit_code=1, report=report)
        return runner.restricted_runner.RunnerResult(exit_code=0, report=_restricted_pass_report())

    monkeypatch.setattr(runner.restricted_runner, "run", fake_run)
    return calls


def _adapter(*, factory: FactoryRecorder | None = None, max_output_tokens: int = runner.DEFAULT_MAX_OUTPUT_TOKENS):
    return runner.LiveOpenAIResponsesClientAdapter(
        api_key=FAKE_API_KEY,
        live_client_factory=factory or FactoryRecorder(),
        max_output_tokens=max_output_tokens,
        sensitive_values=(FAKE_API_KEY, SAFE_SOCKET_URL),
    )


def _openai_request() -> dict[str, object]:
    return runner.restricted_runner.build_openai_responses_request(
        judge_run=_judge_run(),
        bundle=_bundle_context(),
    )


def _minimal_openai_schema(properties: dict[str, object] | None = None) -> dict[str, object]:
    schema_properties = properties if properties is not None else {"name": {"type": "string"}}
    return {
        "type": "object",
        "additionalProperties": False,
        "required": list(schema_properties),
        "properties": schema_properties,
    }


def _judge_run() -> runner.restricted_runner.JudgeRunRecord:
    return runner.restricted_runner.JudgeRunRecord(
        judge_run_id=JUDGE_RUN_ID,
        bundle_id=BUNDLE_ID,
        judge_profile="github_primary",
        model="gpt-5.4-mini",
        reasoning_effort="low",
        prompt_version="judge_github_primary_v1",
        schema_version="judge_output_v1",
        policy_version="verdict_policy_v1",
        prompt_cache_key="judge:github_primary:judge_github_primary_v1:judge_output_v1:verdict_policy_v1",
        status="pending",
    )


def _bundle_context() -> runner.restricted_runner.BundleContext:
    return runner.restricted_runner.BundleContext(
        bundle_id=BUNDLE_ID,
        candidate_group_id=GROUP_ID,
        current_primary_artifact_id=ARTIFACT_ID,
        current_bundle_id=BUNDLE_ID,
        primary_summary={
            "repo_full_name": "example/example-tool",
            "headline": "Synthetic local fixture for a developer workflow helper.",
            "test_paths": ["tests/test_example_tool.py"],
            "ci_paths": [".github/workflows/test.yml"],
            "docs_paths": ["docs/usage.md"],
        },
        supporting_summaries_json=[{"artifact_id": str(ARTIFACT_ID), "kind": "github_repo"}],
        discovered_links_summary_json=[],
        evidence_limitations=["synthetic local fixture; no live OpenAI call"],
        token_budget_profile="small",
        reroot_count=0,
        ready_for_analysis=True,
    )


def _restricted_pass_report() -> dict[str, object]:
    return {
        "schema_version": "local_db_restricted_openai_judge_canary_v1",
        "status": "pass",
        "database_url_guard_passed": True,
        "openai_live_call_authorized": False,
        "openai_client_injected": True,
        "analysis_router_replay_confirmed": True,
        "judge_call_requested_event_found": True,
        "judge_run_loaded": True,
        "evidence_bundle_loaded": True,
        "judge_request_built": True,
        "judge_request_uses_bundle_only": True,
        "openai_responses_request_shape_valid": True,
        "openai_structured_output_received": True,
        "judge_output_created": True,
        "judge_run_updated": True,
        "judge_output_ready_event_created": True,
        "live_openai_called": False,
        "telegram_called": False,
        "live_github_called": False,
        "workers_started": False,
        "redis_mutation": False,
        "production_db_write": False,
        "alembic_or_ddl_ran": False,
        "analysis_created": False,
        "notification_created": False,
        "checks_failed": [],
    }


def _expected_pass_report() -> dict[str, object]:
    return {
        "schema_version": "local_db_live_openai_judge_canary_v1",
        "status": "pass",
        "database_url_guard_passed": True,
        "live_openai_authority_confirmed": True,
        "openai_api_key_env_allowed": True,
        "openai_api_key_present": True,
        "openai_client_created": True,
        "restricted_judge_canary_delegated": True,
        "analysis_router_replay_confirmed": True,
        "judge_call_requested_event_found": True,
        "judge_run_loaded": True,
        "evidence_bundle_loaded": True,
        "judge_request_built": True,
        "judge_request_uses_bundle_only": True,
        "openai_responses_request_shape_valid": True,
        "openai_request_model_allowed": True,
        "openai_request_tools_disabled": True,
        "openai_request_max_output_tokens_capped": True,
        "openai_structured_output_received": True,
        "judge_output_created": True,
        "judge_run_updated": True,
        "judge_output_ready_event_created": True,
        "live_openai_called": True,
        "existing_judge_output_reused": False,
        "telegram_called": False,
        "live_github_called": False,
        "workers_started": False,
        "redis_mutation": False,
        "production_db_write": False,
        "alembic_or_ddl_ran": False,
        "analysis_created": False,
        "notification_created": False,
        "checks_failed": [],
    }
