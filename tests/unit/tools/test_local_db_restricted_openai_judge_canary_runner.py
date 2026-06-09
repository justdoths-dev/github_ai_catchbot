from __future__ import annotations

import ast
import json
import time
from pathlib import Path
from uuid import UUID

from tools import local_db_restricted_openai_judge_canary_runner as runner


ROOT = Path(__file__).resolve().parents[3]
SOURCE_FIXTURE = "tests/fixtures/upstream/source_message_github_repo_signal.json"
GITHUB_FIXTURE = "tests/fixtures/upstream/github_repo_snapshot_example_tool.json"
PG_SCHEME = "postgresql+psycopg"
SAFE_DATABASE_NAME = "github_ai_catchbot_test"
SOCKET_HOST = "/var/run/postgresql"
SAFE_SOCKET_URL = f"{PG_SCHEME}:///{SAFE_DATABASE_NAME}?host={SOCKET_HOST}"
SECRET_VALUE = "local" + "_" + "secret"
PASSWORD_URL = f"{PG_SCHEME}://local_user:{SECRET_VALUE}@127.0.0.1:5432/{SAFE_DATABASE_NAME}"
GROUP_ID = UUID("11111111-1111-4111-8111-111111111111")
BUNDLE_ID = UUID("22222222-2222-4222-8222-222222222222")
JUDGE_RUN_ID = UUID("33333333-3333-4333-8333-333333333333")
ARTIFACT_ID = UUID("44444444-4444-4444-8444-444444444444")


class FakeAnalysisRouterRunner:
    def __init__(self, *, report_overrides=None, exit_code: int = 0) -> None:
        self.calls = []
        self.report_overrides = report_overrides or {}
        self.exit_code = exit_code

    def run(
        self,
        *,
        database_url: str,
        source_fixture_path: Path,
        github_snapshot_fixture_path: Path,
        replay_namespace: str,
        env,
        repo_root: Path,
    ):
        self.calls.append(
            {
                "database_url": database_url,
                "source_fixture_path": source_fixture_path,
                "github_snapshot_fixture_path": github_snapshot_fixture_path,
                "replay_namespace": replay_namespace,
                "env": dict(env),
                "repo_root": repo_root,
            }
        )
        report = {
            "schema_version": "local_db_analysis_router_fixture_replay_v1",
            "status": "pass",
            "source_candidate_replay_confirmed": True,
            "artifact_snapshot_replay_confirmed": True,
            "evidence_bundle_replay_confirmed": True,
            "analysis_requested_event_found": True,
            "candidate_current_bundle_confirmed": True,
            "evidence_bundle_ready_confirmed": True,
            "judge_profile_allowed": True,
            "routing_policy_applied": True,
            "judge_run_created_or_reused": True,
            "judge_call_requested_event_created": True,
            "default_model_selected": True,
            "prompt_cache_key_created": True,
            "checks_failed": [],
        }
        report.update(self.report_overrides)
        return runner.analysis_router_runner.RunnerResult(exit_code=self.exit_code, report=report)


class FakeExecutor:
    def __init__(self, *, result: runner.ReplayExecutionResult | None = None, raises: Exception | None = None) -> None:
        self.calls = []
        self.result = result or _passing_execution_result()
        self.raises = raises

    def execute(
        self,
        *,
        database_url: str,
        replay_namespace: str,
        openai_client,
    ) -> runner.ReplayExecutionResult:
        self.calls.append((database_url, replay_namespace, openai_client))
        if self.raises is not None:
            raise self.raises
        return self.result


class RecordingResponses:
    def __init__(self, response=None) -> None:
        self.calls = []
        self.response = response

    def create(self, **request):
        self.calls.append(request)
        return self.response or {"status": "completed", "output_text": "{}"}


class RecordingOpenAIClient:
    def __init__(self, response=None) -> None:
        self.responses = RecordingResponses(response=response)


def _parse_args(*args: str):
    return runner.build_parser().parse_args(args)


def _run(*args: str, env=None, openai_client=None, executor=None, predecessor_runner=None) -> runner.RunnerResult:
    return runner.run(
        _parse_args(*args),
        env=env or {"APP_ENV": "test"},
        openai_client=openai_client,
        executor=executor,
        predecessor_runner=predecessor_runner,
        repo_root=ROOT,
    )


def test_fake_predecessor_executor_and_injected_client_return_expected_pass_output() -> None:
    predecessor = FakeAnalysisRouterRunner()
    executor = FakeExecutor()
    openai_client = RecordingOpenAIClient()

    result = _run(
        "--database-url",
        SAFE_SOCKET_URL,
        "--source-fixture",
        SOURCE_FIXTURE,
        "--github-snapshot-fixture",
        GITHUB_FIXTURE,
        "--replay-namespace",
        "unit-restricted-openai-judge-pass",
        "--confirm-local-test-db",
        predecessor_runner=predecessor,
        executor=executor,
        openai_client=openai_client,
    )

    assert result.exit_code == 0
    assert result.report == _expected_pass_report()
    assert len(predecessor.calls) == 1
    assert len(executor.calls) == 1


def test_required_output_shape_is_stable_json_without_raw_url_or_password() -> None:
    result = _run(
        "--database-url",
        PASSWORD_URL,
        "--source-fixture",
        SOURCE_FIXTURE,
        "--github-snapshot-fixture",
        GITHUB_FIXTURE,
        "--replay-namespace",
        "unit-output-shape",
        "--confirm-local-test-db",
        predecessor_runner=FakeAnalysisRouterRunner(),
        executor=FakeExecutor(),
        openai_client=RecordingOpenAIClient(),
    )

    text = runner.render_json(result.report)
    parsed = json.loads(text)
    assert list(parsed) == list(_expected_pass_report())
    assert PASSWORD_URL not in text
    assert SECRET_VALUE not in text


def test_rejects_missing_confirmation_before_predecessor_runs() -> None:
    predecessor = FakeAnalysisRouterRunner()
    executor = FakeExecutor()

    result = _run(
        "--database-url",
        SAFE_SOCKET_URL,
        "--source-fixture",
        SOURCE_FIXTURE,
        "--github-snapshot-fixture",
        GITHUB_FIXTURE,
        "--replay-namespace",
        "unit-missing-confirm",
        predecessor_runner=predecessor,
        executor=executor,
        openai_client=RecordingOpenAIClient(),
    )

    assert result.exit_code == 1
    assert result.report["database_url_guard_passed"] is True
    assert result.report["checks_failed"] == ["confirm_local_test_db_required"]
    assert predecessor.calls == []
    assert executor.calls == []


def test_rejects_app_env_prod_before_predecessor_runs() -> None:
    predecessor = FakeAnalysisRouterRunner()
    executor = FakeExecutor()

    result = _run(
        "--database-url",
        SAFE_SOCKET_URL,
        "--source-fixture",
        SOURCE_FIXTURE,
        "--github-snapshot-fixture",
        GITHUB_FIXTURE,
        "--replay-namespace",
        "unit-prod-env",
        "--confirm-local-test-db",
        env={"APP_ENV": "prod"},
        predecessor_runner=predecessor,
        executor=executor,
        openai_client=RecordingOpenAIClient(),
    )

    assert result.exit_code == 1
    assert result.report["checks_failed"] == ["app_env_production_rejected"]
    assert predecessor.calls == []
    assert executor.calls == []


def test_refuses_missing_injected_openai_client_without_live_fallback() -> None:
    predecessor = FakeAnalysisRouterRunner()
    executor = FakeExecutor()

    result = _run(
        "--database-url",
        SAFE_SOCKET_URL,
        "--source-fixture",
        SOURCE_FIXTURE,
        "--github-snapshot-fixture",
        GITHUB_FIXTURE,
        "--replay-namespace",
        "unit-missing-openai-client",
        "--confirm-local-test-db",
        predecessor_runner=predecessor,
        executor=executor,
        openai_client=None,
    )

    assert result.exit_code == 1
    assert result.report["openai_client_injected"] is False
    assert result.report["live_openai_called"] is False
    assert result.report["checks_failed"] == ["openai_client_injected_required"]
    assert predecessor.calls == []
    assert executor.calls == []


def test_database_url_guard_rejects_remote_prod_and_default_targets() -> None:
    cases = [
        ("mysql" + "://localhost/" + SAFE_DATABASE_NAME, "database_url_unsupported_scheme"),
        (_network_url("db.example.com", SAFE_DATABASE_NAME), "database_url_remote_host_rejected"),
        (_socket_url(SAFE_DATABASE_NAME, host="db.example.com"), "database_url_remote_query_host_rejected"),
        (_socket_url("postgres"), "database_url_forbidden_database_name"),
        (_socket_url("github_ai_catchbot"), "database_url_forbidden_database_name"),
        (_socket_url("github_ai_catchbot_prod_test"), "database_url_production_name_rejected"),
        (_socket_url("github_ai_catchbot_stage"), "database_url_missing_local_test_marker"),
    ]
    for database_url, expected_failure in cases:
        ok, failures, parsed = runner.validate_database_url(database_url)
        assert ok is False
        assert expected_failure in failures
        assert parsed is not None


def test_predecessor_failure_stops_before_executor_or_openai_client_call() -> None:
    predecessor = FakeAnalysisRouterRunner(report_overrides={"status": "fail", "checks_failed": ["broken"]})
    executor = FakeExecutor()
    openai_client = RecordingOpenAIClient()

    result = _run(
        "--database-url",
        SAFE_SOCKET_URL,
        "--source-fixture",
        SOURCE_FIXTURE,
        "--github-snapshot-fixture",
        GITHUB_FIXTURE,
        "--replay-namespace",
        "unit-predecessor-fails",
        "--confirm-local-test-db",
        predecessor_runner=predecessor,
        executor=executor,
        openai_client=openai_client,
    )

    assert result.exit_code == 1
    assert result.report["checks_failed"] == ["analysis_router_replay_failed"]
    assert len(predecessor.calls) == 1
    assert executor.calls == []
    assert openai_client.responses.calls == []


def test_stale_or_missing_judge_call_request_fails_safely_without_openai_call() -> None:
    openai_client = RecordingOpenAIClient()
    executor = FakeExecutor(
        result=runner.ReplayExecutionResult(
            judge_call_requested_event_found=False,
            judge_run_loaded=False,
            evidence_bundle_loaded=False,
            judge_request_built=False,
            judge_request_uses_bundle_only=False,
            openai_responses_request_shape_valid=False,
            openai_structured_output_received=False,
            judge_output_created=False,
            judge_run_updated=False,
            judge_output_ready_event_created=False,
            checks_failed=("judge_call_requested_event_missing_or_invalid",),
        )
    )

    result = _run(
        "--database-url",
        SAFE_SOCKET_URL,
        "--source-fixture",
        SOURCE_FIXTURE,
        "--github-snapshot-fixture",
        GITHUB_FIXTURE,
        "--replay-namespace",
        "unit-missing-judge-call",
        "--confirm-local-test-db",
        predecessor_runner=FakeAnalysisRouterRunner(),
        executor=executor,
        openai_client=openai_client,
    )

    assert result.exit_code == 1
    assert result.report["judge_call_requested_event_found"] is False
    assert result.report["judge_output_ready_event_created"] is False
    assert result.report["checks_failed"] == [
        "judge_call_requested_event_missing_or_invalid",
        "judge_call_requested_event_found:missing",
        "judge_run_loaded:missing",
        "evidence_bundle_loaded:missing",
        "judge_request_built:missing",
        "judge_request_uses_bundle_only:missing",
        "openai_responses_request_shape_valid:missing",
        "openai_structured_output_received:missing",
        "judge_output_created:missing",
        "judge_run_updated:missing",
        "judge_output_ready_event_created:missing",
    ]
    assert openai_client.responses.calls == []


def test_judge_request_is_built_from_bundle_only_with_route_metadata_outside_user_context() -> None:
    request = runner.build_openai_responses_request(judge_run=_judge_run(), bundle=_bundle_context())

    user_context = json.loads(_user_context(request))
    developer_text = request["input"][0]["content"][0]["text"]
    assert set(user_context) == set(runner.REQUEST_CONTEXT_KEYS)
    assert user_context["bundle_id"] == str(BUNDLE_ID)
    assert user_context["candidate_group_id"] == str(GROUP_ID)
    assert "source_messages" not in json.dumps(request)
    assert "artifact_snapshots" not in json.dumps(request)
    assert "notification_" not in json.dumps(request)
    assert "prompt_version=judge_github_primary_v1" in developer_text
    assert "schema_version=judge_output_v1" in developer_text
    assert "policy_version=verdict_policy_v1" in developer_text
    assert request["prompt_cache_key"] == "judge:github_primary:judge_github_primary_v1:judge_output_v1:verdict_policy_v1"
    assert runner.judge_request_uses_bundle_only(request)


def test_request_uses_strict_judge_output_schema_and_no_tools_or_external_fetch() -> None:
    request = runner.build_openai_responses_request(judge_run=_judge_run(), bundle=_bundle_context())

    text_format = request["text"]["format"]
    assert request["tools"] == []
    assert text_format["type"] == "json_schema"
    assert text_format["name"] == "judge_output_v1"
    assert text_format["strict"] is True
    assert text_format["schema"]["required"] == list(runner.REQUIRED_OUTPUT_KEYS)
    assert "web_search" not in json.dumps(request)
    assert "file_search" not in json.dumps(request)
    assert runner.openai_responses_request_shape_valid(request)


def test_fake_response_maps_to_valid_judge_outputs_insert_payload() -> None:
    bundle = _bundle_context()
    payload = runner.build_fake_judge_output_payload(bundle)
    response = {
        "status": "completed",
        "output_text": json.dumps(payload),
        "usage": {
            "input_tokens": 100,
            "input_tokens_details": {"cached_tokens": 20},
            "output_tokens": 80,
            "output_tokens_details": {"reasoning_tokens": 10},
        },
    }

    parsed = runner.parse_openai_response(response, started_monotonic=time.monotonic())

    assert parsed.finish_reason == "stop"
    assert parsed.refusal_detected is False
    assert parsed.usage.input_tokens == 100
    assert parsed.usage.cached_input_tokens == 20
    assert runner.structured_output_payload_valid(parsed.payload_json or {}, candidate_group_id=GROUP_ID)
    assert parsed.payload_json["model_proposed_verdict"] == "later"
    assert parsed.payload_json["model_confidence_band"] == "medium"


def test_refusal_response_is_explicitly_unsupported_without_ready_event() -> None:
    parsed = runner.parse_openai_response(
        {
            "status": "completed",
            "output": [
                {
                    "type": "message",
                    "content": [{"type": "refusal", "refusal": "refused"}],
                }
            ],
            "usage": {},
        },
        started_monotonic=time.monotonic(),
    )
    result = _run(
        "--database-url",
        SAFE_SOCKET_URL,
        "--source-fixture",
        SOURCE_FIXTURE,
        "--github-snapshot-fixture",
        GITHUB_FIXTURE,
        "--replay-namespace",
        "unit-refusal",
        "--confirm-local-test-db",
        predecessor_runner=FakeAnalysisRouterRunner(),
        executor=FakeExecutor(
            result=runner.ReplayExecutionResult(
                judge_call_requested_event_found=True,
                judge_run_loaded=True,
                evidence_bundle_loaded=True,
                judge_request_built=True,
                judge_request_uses_bundle_only=True,
                openai_responses_request_shape_valid=True,
                openai_structured_output_received=False,
                judge_output_created=False,
                judge_run_updated=False,
                judge_output_ready_event_created=False,
                checks_failed=("openai_refusal_unsupported",),
            )
        ),
        openai_client=RecordingOpenAIClient(),
    )

    assert parsed.payload_json is None
    assert parsed.refusal_detected is True
    assert result.exit_code == 1
    assert "openai_refusal_unsupported" in result.report["checks_failed"]
    assert result.report["judge_output_ready_event_created"] is False


def test_sanitized_failure_output_does_not_contain_db_url_or_token_like_values() -> None:
    executor = FakeExecutor(raises=RuntimeError(PASSWORD_URL + " " + SECRET_VALUE))

    result = _run(
        "--database-url",
        PASSWORD_URL,
        "--source-fixture",
        SOURCE_FIXTURE,
        "--github-snapshot-fixture",
        GITHUB_FIXTURE,
        "--replay-namespace",
        "unit-sanitized-failure",
        "--confirm-local-test-db",
        predecessor_runner=FakeAnalysisRouterRunner(),
        executor=executor,
        openai_client=RecordingOpenAIClient(),
    )

    text = runner.render_json(result.report)
    assert result.exit_code == 1
    assert result.report["checks_failed"] == ["RuntimeError"]
    assert PASSWORD_URL not in text
    assert SECRET_VALUE not in text


def test_stable_authority_flags_remain_closed() -> None:
    result = _run(
        "--database-url",
        SAFE_SOCKET_URL,
        "--source-fixture",
        SOURCE_FIXTURE,
        "--github-snapshot-fixture",
        GITHUB_FIXTURE,
        "--replay-namespace",
        "unit-authority-flags",
        "--confirm-local-test-db",
        predecessor_runner=FakeAnalysisRouterRunner(),
        executor=FakeExecutor(),
        openai_client=RecordingOpenAIClient(),
    )

    assert result.exit_code == 0
    for key in runner.FALSE_RESULT_KEYS:
        assert result.report[key] is False
    assert result.report["openai_client_injected"] is True


def test_runner_source_has_no_forbidden_runtime_imports_or_openai_env_reads() -> None:
    source = (ROOT / "tools/local_db_restricted_openai_judge_canary_runner.py").read_text(encoding="utf-8")
    tree = ast.parse(source)
    imported_roots = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imported_roots.update(alias.name.split(".", 1)[0] for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            imported_roots.add(node.module.split(".", 1)[0])

    assert {"redis", "openai", "telegram", "docker", "systemd", "requests", "httpx", "aiohttp", "urllib"}.isdisjoint(
        imported_roots
    )
    assert "OPENAI_API_KEY" not in source
    assert "OPENAI_API_KEY_FILE" not in source
    assert "os.getenv" not in source
    assert "policy_engine" not in source
    assert "notifier_telegram" not in source


def _socket_url(database_name: str, *, host: str = SOCKET_HOST) -> str:
    return f"{PG_SCHEME}:///{database_name}?host={host}"


def _network_url(host: str, database_name: str, *, password: str = "secret") -> str:
    return f"{PG_SCHEME}://user:{password}@{host}/{database_name}"


def _passing_execution_result() -> runner.ReplayExecutionResult:
    return runner.ReplayExecutionResult(
        judge_call_requested_event_found=True,
        judge_run_loaded=True,
        evidence_bundle_loaded=True,
        judge_request_built=True,
        judge_request_uses_bundle_only=True,
        openai_responses_request_shape_valid=True,
        openai_structured_output_received=True,
        judge_output_created=True,
        judge_run_updated=True,
        judge_output_ready_event_created=True,
        checks_failed=(),
    )


def _judge_run() -> runner.JudgeRunRecord:
    return runner.JudgeRunRecord(
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


def _bundle_context() -> runner.BundleContext:
    return runner.BundleContext(
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


def _user_context(request: dict[str, object]) -> str:
    return request["input"][1]["content"][0]["text"]


def _expected_pass_report() -> dict[str, object]:
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
