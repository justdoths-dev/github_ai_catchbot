from __future__ import annotations

import importlib
import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Any

import pytest


ROOT = Path(__file__).resolve().parents[4]
SCRIPT = (
    ROOT
    / "scripts"
    / "ops"
    / "dedicated_vps_judge_openai_live_error_classification_diagnostic.py"
)

FAKE_KEY = "private" + "-openai" + "-key" + "-must-not-leak"
FAKE_PROJECT = "private" + "-openai" + "-project" + "-must-not-leak"
FAKE_KEY_PATH = "/private/openai/key-file-must-not-leak"
FAKE_RUNTIME_PATH = "/etc/github-ai-catchbot/private-runtime.env"
FAKE_ERROR_TEXT = "raw openai error message must not leak"
FAKE_REQUEST_ID = "req_private_openai_request_id"
FAKE_PROMPT = "diagnostic judge-output schema probe"
FAKE_CONTEXT = "diagnostic candidate evidence placeholder"


def _module():
    return importlib.import_module(
        "scripts.ops.dedicated_vps_judge_openai_live_error_classification_diagnostic"
    )


def test_script_exists() -> None:
    assert SCRIPT.exists()


def test_default_mode_is_local_preflight_without_key_or_live_call() -> None:
    result = _module().generate_report(
        runtime_env_reader=_raising_runtime_reader,
        sdk_loader=_raising_sdk_loader,
        forbidden_raw_values=(FAKE_KEY, FAKE_PROJECT, FAKE_RUNTIME_PATH),
    )

    report = result.report
    assert result.exit_code == 0
    assert report["contract_status"] == (
        "judge_openai_live_error_classification_preflight_passed"
    )
    assert report["approval_live_openai_bucket"] == "zero"
    assert report["approval_key_read_bucket"] == "zero"
    assert report["max_live_calls_bucket"] == "zero"
    assert report["runtime_env_read"] is False
    assert report["openai_key_source_bucket"] == "zero"
    assert report["openai_key_read_bucket"] == "zero"
    assert report["openai_key_file_read_bucket"] == "zero"
    assert report["request_shape_valid_bucket"] == "one"
    assert report["request_shape_issue_count_bucket"] == "zero"
    assert report["request_shape_issue_buckets"] == []
    _assert_no_live_or_downstream_boundary(report)
    assert report["raw_values_emitted"] is False
    assert report["checks_failed"] == []


@pytest.mark.parametrize(
    "approval_kwargs",
    [
        {"approve_live_openai": True, "approve_key_read": False, "max_live_calls": 1},
        {"approve_live_openai": False, "approve_key_read": True, "max_live_calls": 1},
        {"approve_live_openai": True, "approve_key_read": True, "max_live_calls": 0},
        {"approve_live_openai": True, "approve_key_read": True, "max_live_calls": 2},
    ],
)
def test_missing_approval_flags_block_key_read_and_live_call(
    approval_kwargs: dict[str, Any],
) -> None:
    result = _module().generate_report(
        runtime_env_reader=_raising_runtime_reader,
        sdk_loader=_raising_sdk_loader,
        runtime_env_path=FAKE_RUNTIME_PATH,
        forbidden_raw_values=(FAKE_KEY, FAKE_PROJECT, FAKE_RUNTIME_PATH),
        **approval_kwargs,
    )

    report = result.report
    assert result.exit_code == 1
    assert report["contract_status"] == (
        "blocked_judge_openai_live_error_classification_not_approved"
    )
    assert report["runtime_env_read"] is False
    assert report["openai_key_source_bucket"] == "zero"
    assert report["openai_key_read_bucket"] == "zero"
    assert report["openai_key_file_read_bucket"] == "zero"
    _assert_no_live_or_downstream_boundary(report)
    assert report["checks_failed"] == ["approval.required_all"]


def test_approved_mode_with_fake_sdk_performs_one_sanitized_success_call() -> None:
    recorder: dict[str, Any] = {}
    result = _approved_result(
        recorder=recorder,
        sdk_loader=_fake_sdk_loader(
            recorder,
            response={"status": "completed", "output_text": "{}", "usage": {"input_tokens": 1}},
        ),
    )

    report = result.report
    assert result.exit_code == 0
    assert report["contract_status"] == (
        "judge_openai_live_error_classification_approved_live_succeeded"
    )
    assert report["runtime_env_read"] is True
    assert report["openai_key_source_bucket"] == "env"
    assert report["openai_key_read_bucket"] == "one"
    assert report["openai_key_file_read_bucket"] == "zero"
    assert report["openai_project_present_bucket"] == "one"
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
    _assert_no_downstream_side_effects(report)
    assert report["checks_failed"] == []

    assert recorder["constructor"]["api_key"] == FAKE_KEY
    assert recorder["constructor"]["project"] == FAKE_PROJECT
    assert recorder["constructor"]["max_retries"] == 0
    assert recorder["constructor"]["timeout"] == 10.0
    assert len(recorder["requests"]) == 1
    request = recorder["requests"][0]
    assert request["model"] == "gpt-5.4-mini"
    assert request["reasoning"] == {"effort": "low"}
    assert request["text"]["format"]["type"] == "json_schema"
    assert request["text"]["format"]["strict"] is True
    assert request["tools"] == []
    _assert_no_raw_values(report, FAKE_KEY, FAKE_PROJECT, FAKE_RUNTIME_PATH, FAKE_PROMPT, FAKE_CONTEXT)


def test_approved_mode_can_read_key_file_without_emitting_path_or_key(tmp_path: Path) -> None:
    key_file = tmp_path / "openai-key.txt"
    key_file.write_text(FAKE_KEY, encoding="utf-8")
    recorder: dict[str, Any] = {}

    result = _module().generate_report(
        approve_live_openai=True,
        approve_key_read=True,
        max_live_calls=1,
        runtime_env_path=FAKE_RUNTIME_PATH,
        runtime_env_reader=lambda _path: {"OPENAI_API_KEY_FILE": str(key_file)},
        sdk_loader=_fake_sdk_loader(recorder),
        forbidden_raw_values=(FAKE_KEY, str(key_file), FAKE_RUNTIME_PATH),
    )

    report = result.report
    assert result.exit_code == 0
    assert report["openai_key_source_bucket"] == "file"
    assert report["openai_key_read_bucket"] == "one"
    assert report["openai_key_file_read_bucket"] == "one"
    assert recorder["constructor"]["api_key"] == FAKE_KEY
    _assert_no_raw_values(report, FAKE_KEY, str(key_file), FAKE_RUNTIME_PATH)


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
        ("PermissionDeniedError", 403, "permission_error", "model_not_available", "model_access", "403", "permission_error"),
        ("BadRequestError", 400, "invalid_request_error", "json_schema_invalid", "schema_rejected", "400", "invalid_request_error"),
        ("UnprocessableEntityError", 422, "invalid_request_error", "response_format_schema_invalid", "schema_rejected", "422", "invalid_request_error"),
        ("RateLimitError", 429, "rate_limit_error", None, "rate_limit", "429", "rate_limit_error"),
        ("APITimeoutError", None, None, None, "timeout", "zero", "api_timeout_error"),
        ("APIConnectionError", None, None, None, "api_connection_error", "zero", "api_connection_error"),
        ("InternalServerError", 500, "server_error", None, "api_status_error", "5xx", "server_error"),
    ],
)
def test_approved_mode_classifies_sdk_exceptions_without_raw_error_text(
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

    result = _approved_result(
        recorder=recorder,
        sdk_loader=_fake_sdk_loader(recorder, side_effect=exc),
        forbidden_raw_values=(FAKE_ERROR_TEXT, FAKE_REQUEST_ID),
    )

    report = result.report
    assert result.exit_code == 1
    assert report["contract_status"] == (
        "blocked_judge_openai_live_error_classification_live_failed"
    )
    assert len(recorder["requests"]) == 1
    assert report["openai_call_attempted"] is True
    assert report["live_openai_call_attempted"] is True
    assert report["live_openai_call_attempted_bucket"] == "one"
    assert report["live_openai_call_completed_bucket"] == "zero"
    assert report["live_result_class_bucket"] == result_bucket
    assert report["http_status_bucket"] == http_bucket
    assert report["openai_error_type_bucket"] == openai_error_bucket
    assert report["response_parse_bucket"] == "zero"
    assert report["structured_output_observed_bucket"] == "zero"
    assert report["usage_present_bucket"] == "zero"
    assert report["latency_ms_present_bucket"] == "one"
    _assert_no_downstream_side_effects(report)
    assert report["checks_failed"] == [f"openai.live_call.{result_bucket}"]
    _assert_no_raw_values(
        report,
        FAKE_KEY,
        FAKE_PROJECT,
        FAKE_RUNTIME_PATH,
        FAKE_ERROR_TEXT,
        FAKE_REQUEST_ID,
        FAKE_PROMPT,
        FAKE_CONTEXT,
    )


def test_key_source_conflict_fails_closed_before_live_call() -> None:
    recorder: dict[str, Any] = {}
    result = _module().generate_report(
        approve_live_openai=True,
        approve_key_read=True,
        max_live_calls=1,
        runtime_env_path=FAKE_RUNTIME_PATH,
        runtime_env_reader=lambda _path: {
            "OPENAI_API_KEY": FAKE_KEY,
            "OPENAI_API_KEY_FILE": FAKE_KEY_PATH,
            "OPENAI_PROJECT": FAKE_PROJECT,
        },
        sdk_loader=_fake_sdk_loader(recorder),
        forbidden_raw_values=(FAKE_KEY, FAKE_KEY_PATH, FAKE_PROJECT, FAKE_RUNTIME_PATH),
    )

    report = result.report
    assert result.exit_code == 1
    assert report["contract_status"] == (
        "blocked_judge_openai_live_error_classification_live_failed"
    )
    assert report["openai_key_source_bucket"] == "both_conflict"
    assert report["openai_key_read_bucket"] == "zero"
    assert report["openai_key_file_read_bucket"] == "zero"
    assert "constructor" not in recorder
    _assert_no_live_or_downstream_boundary(report)
    assert report["checks_failed"] == ["openai_key.source_conflict"]
    _assert_no_raw_values(report, FAKE_KEY, FAKE_KEY_PATH, FAKE_PROJECT, FAKE_RUNTIME_PATH)


def test_missing_key_fails_closed_before_sdk_constructor() -> None:
    recorder: dict[str, Any] = {}
    result = _module().generate_report(
        approve_live_openai=True,
        approve_key_read=True,
        max_live_calls=1,
        runtime_env_path=FAKE_RUNTIME_PATH,
        runtime_env_reader=lambda _path: {"OPENAI_PROJECT": FAKE_PROJECT},
        sdk_loader=_fake_sdk_loader(recorder),
        forbidden_raw_values=(FAKE_PROJECT, FAKE_RUNTIME_PATH),
    )

    report = result.report
    assert result.exit_code == 1
    assert report["openai_key_source_bucket"] == "missing"
    assert report["openai_key_read_bucket"] == "zero"
    assert "constructor" not in recorder
    _assert_no_live_or_downstream_boundary(report)
    assert report["checks_failed"] == ["openai_key.missing"]


def test_runtime_env_parser_reads_only_openai_fields() -> None:
    parsed = _module().parse_runtime_env_text(
        "\n".join(
            [
                "OPENAI_API_KEY='private-openai-key'",
                "OPENAI_PROJECT=private-openai-project",
                "DATABASE_URL=postgresql://private-db",
                "REDIS_URL=redis://private-redis",
            ]
        )
    )

    assert parsed == {
        "OPENAI_API_KEY": "private-openai-key",
        "OPENAI_PROJECT": "private-openai-project",
    }


def test_cli_outputs_json_without_reading_env_key_values() -> None:
    private_env_key = "private" + "-cli" + "-openai" + "-key"
    private_env_project = "private" + "-cli" + "-openai" + "-project"
    env = os.environ.copy()
    env["OPENAI_API_KEY"] = private_env_key
    env["OPENAI_PROJECT"] = private_env_project

    result = subprocess.run(
        [sys.executable, str(SCRIPT), "--format", "json"],
        check=True,
        capture_output=True,
        text=True,
        env=env,
    )

    report = json.loads(result.stdout)
    assert report["contract_status"] == (
        "judge_openai_live_error_classification_preflight_passed"
    )
    assert report["openai_key_read_bucket"] == "zero"
    assert report["openai_call_attempted"] is False
    assert private_env_key not in result.stdout
    assert private_env_project not in result.stdout
    assert result.stderr == ""


def _approved_result(
    *,
    recorder: dict[str, Any],
    sdk_loader: Any,
    forbidden_raw_values: tuple[str, ...] = (),
) -> Any:
    return _module().generate_report(
        approve_live_openai=True,
        approve_key_read=True,
        max_live_calls=1,
        runtime_env_path=FAKE_RUNTIME_PATH,
        runtime_env_reader=lambda _path: {
            "OPENAI_API_KEY": FAKE_KEY,
            "OPENAI_PROJECT": FAKE_PROJECT,
        },
        sdk_loader=sdk_loader,
        forbidden_raw_values=(
            FAKE_KEY,
            FAKE_PROJECT,
            FAKE_RUNTIME_PATH,
            *forbidden_raw_values,
        ),
    )


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
                self.responses = FakeResponses(recorder, response=response, side_effect=side_effect)

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
) -> Exception:
    cls = type(name, (Exception,), {})
    exc = cls(FAKE_ERROR_TEXT)
    exc.status_code = status_code
    exc.type = error_type
    exc.code = code
    exc.body = {"message": FAKE_ERROR_TEXT}
    exc.request_id = FAKE_REQUEST_ID
    return exc


def _raising_runtime_reader(_path: str | Path) -> Mapping[str, str]:
    raise AssertionError("runtime env should not be read")


def _raising_sdk_loader() -> Any:
    raise AssertionError("sdk should not be loaded")


def _assert_no_live_or_downstream_boundary(report: dict[str, Any]) -> None:
    assert report["openai_call_attempted"] is False
    assert report["live_openai_call_attempted"] is False
    assert report["live_openai_call_attempted_bucket"] == "zero"
    assert report["live_openai_call_completed_bucket"] == "zero"
    _assert_no_downstream_side_effects(report)


def _assert_no_downstream_side_effects(report: dict[str, Any]) -> None:
    assert report["database_write_attempted"] is False
    assert report["redis_write_attempted"] is False
    assert report["redis_ack_attempted"] is False
    assert report["redis_delete_or_trim_attempted"] is False
    assert report["analysis_validator_started"] is False
    assert report["policy_engine_started"] is False
    assert report["notifier_started"] is False
    assert report["telegram_send_attempted"] is False
    assert report["raw_values_emitted"] is False


def _assert_no_raw_values(report: dict[str, Any], *raw_values: str) -> None:
    rendered = json.dumps(report, sort_keys=True)
    for raw_value in raw_values:
        assert raw_value not in rendered
