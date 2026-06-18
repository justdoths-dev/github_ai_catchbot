from __future__ import annotations

import ast
import json
from pathlib import Path
from uuid import UUID

from tools import local_db_fake_judge_output_fixture_replay_runner as runner


ROOT = Path(__file__).resolve().parents[3]
SOURCE_FIXTURE = "tests/fixtures/upstream/source_message_github_repo_signal.json"
GITHUB_FIXTURE = "tests/fixtures/upstream/github_repo_snapshot_example_tool.json"
PG_SCHEME = "postgresql" + "+psycopg"
SAFE_DATABASE_NAME = "github_ai_catchbot_test"
SOCKET_HOST = "/var/run/postgresql"
SAFE_SOCKET_URL = f"{PG_SCHEME}:///{SAFE_DATABASE_NAME}?host={SOCKET_HOST}"
PASSWORD_VALUE = "local" + "_" + "password"
PASSWORD_URL = f"{PG_SCHEME}://local_user:{PASSWORD_VALUE}@127.0.0.1:5432/{SAFE_DATABASE_NAME}"
GROUP_ID = UUID("11111111-1111-4111-8111-111111111111")
BUNDLE_ID = UUID("22222222-2222-4222-8222-222222222222")


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


class FakeFakeJudgeExecutor:
    def __init__(
        self,
        *,
        checks_failed: tuple[str, ...] = (),
        ready_event_insert_attempted: bool = True,
    ) -> None:
        self.calls = []
        self.ready_event_insert_attempted = ready_event_insert_attempted
        self.checks_failed = checks_failed

    def execute(
        self,
        *,
        database_url: str,
        replay_namespace: str,
    ) -> runner.ReplayExecutionResult:
        self.calls.append((database_url, replay_namespace))
        return runner.ReplayExecutionResult(
            judge_call_requested_event_found=True,
            judge_run_pending_confirmed=True,
            bundle_context_loaded=True,
            fake_judge_output_created_or_reused=True,
            judge_run_succeeded=True,
            judge_output_ready_event_created=True,
            structured_output_schema_valid=True,
            usage_telemetry_recorded=True,
            checks_failed=self.checks_failed,
        )


def _socket_url(database_name: str, *, host: str = SOCKET_HOST) -> str:
    return f"{PG_SCHEME}:///{database_name}?host={host}"


def _network_url(host: str, database_name: str, *, password: str = "secret") -> str:
    return f"{PG_SCHEME}://user:{password}@{host}/{database_name}"


def _parse_args(*args: str):
    return runner.build_parser().parse_args(args)


def _run(*args: str, env=None, executor=None, predecessor_runner=None) -> runner.RunnerResult:
    return runner.run(
        _parse_args(*args),
        env=env or {"APP_ENV": "test"},
        executor=executor,
        predecessor_runner=predecessor_runner,
        repo_root=ROOT,
    )


def test_fake_predecessor_and_executor_return_expected_pass_output() -> None:
    predecessor = FakeAnalysisRouterRunner()
    executor = FakeFakeJudgeExecutor()

    result = _run(
        "--database-url",
        SAFE_SOCKET_URL,
        "--source-fixture",
        SOURCE_FIXTURE,
        "--github-snapshot-fixture",
        GITHUB_FIXTURE,
        "--replay-namespace",
        "operator-local-db-source-candidate",
        "--confirm-local-test-db",
        predecessor_runner=predecessor,
        executor=executor,
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
        executor=FakeFakeJudgeExecutor(),
    )

    text = runner.render_json(result.report)
    parsed = json.loads(text)
    assert list(parsed) == list(_expected_pass_report())
    assert PASSWORD_URL not in text
    assert PASSWORD_VALUE not in text


def test_rejects_missing_confirmation_before_predecessor_runs() -> None:
    predecessor = FakeAnalysisRouterRunner()
    executor = FakeFakeJudgeExecutor()

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
    )

    assert result.exit_code == 1
    assert result.report["database_url_guard_passed"] is True
    assert result.report["checks_failed"] == ["confirm_local_test_db_required"]
    assert predecessor.calls == []
    assert executor.calls == []


def test_rejects_app_env_prod_before_predecessor_runs() -> None:
    predecessor = FakeAnalysisRouterRunner()
    executor = FakeFakeJudgeExecutor()

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
    )

    assert result.exit_code == 1
    assert result.report["checks_failed"] == ["app_env_production_rejected"]
    assert predecessor.calls == []
    assert executor.calls == []


def test_database_url_guard_delegates_to_predecessor_guard(monkeypatch) -> None:
    calls = []
    unsafe_url = PG_SCHEME + ":///unsafe"

    def fake_validate(database_url):
        calls.append(database_url)
        return False, ["delegated_failure"], None

    monkeypatch.setattr(runner.analysis_router_runner, "validate_database_url", fake_validate)

    ok, failures, parsed = runner.validate_database_url(unsafe_url)

    assert ok is False
    assert failures == ["delegated_failure"]
    assert parsed is None
    assert calls == [unsafe_url]


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


def test_runner_source_has_no_forbidden_runtime_or_network_imports() -> None:
    source = (ROOT / "tools/local_db_fake_judge_output_fixture_replay_runner.py").read_text(encoding="utf-8")
    tree = ast.parse(source)
    imported_roots = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imported_roots.update(alias.name.split(".", 1)[0] for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            imported_roots.add(node.module.split(".", 1)[0])

    assert {"redis", "openai", "telegram", "docker", "systemd", "requests", "httpx", "aiohttp"}.isdisjoint(
        imported_roots
    )


def test_fake_judge_output_payload_has_required_judge_output_v1_fields() -> None:
    payload = runner.build_fake_judge_output_payload(_bundle_context())

    assert list(payload) == list(runner.REQUIRED_OUTPUT_KEYS)
    assert payload["judge_schema_version"] == "judge_output_v1"
    assert payload["candidate_group_id"] == str(GROUP_ID)
    assert payload["headline"] == "example/example-tool"
    assert payload["model_proposed_verdict"] == "later"
    assert payload["model_confidence_band"] == "medium"
    assert set(payload["scores"]) == set(runner.REQUIRED_SCORE_KEYS)
    assert payload["scores"]["practical_usefulness"] == 58
    assert payload["scores"]["evidence_strength"] == 45
    assert payload["scores"]["confidence"] == 45
    assert payload["scores"]["code_quality"] == 58
    assert payload["scores"]["specificity"] is None
    assert payload["comparables"] == []
    assert "comparison_gap" in payload["reason_codes"]
    assert "insufficient_comparables" in payload["reason_codes"]
    assert any("comparison_gap" in item for item in payload["evidence_limitations_ko"])
    assert "implementation_signal" not in payload["scores"]
    assert "urgency" not in payload["scores"]
    assert runner.structured_output_schema_valid(payload)


def test_fake_judge_output_is_deterministic_across_equivalent_bundle_summaries() -> None:
    left = _bundle_context(primary_summary={"repo_full_name": "example/example-tool", "headline": "ignored"})
    right = _bundle_context(primary_summary={"headline": "ignored", "repo_full_name": "example/example-tool"})

    assert runner.build_fake_judge_output_payload(left) == runner.build_fake_judge_output_payload(right)


def test_existing_judge_output_reuse_does_not_require_duplicate_ready_event_in_executor_fake_path() -> None:
    executor = FakeFakeJudgeExecutor(ready_event_insert_attempted=False)

    result = _run(
        "--database-url",
        SAFE_SOCKET_URL,
        "--source-fixture",
        SOURCE_FIXTURE,
        "--github-snapshot-fixture",
        GITHUB_FIXTURE,
        "--replay-namespace",
        "unit-existing-output",
        "--confirm-local-test-db",
        predecessor_runner=FakeAnalysisRouterRunner(),
        executor=executor,
    )

    assert result.exit_code == 0
    assert result.report == _expected_pass_report()
    assert executor.ready_event_insert_attempted is False


def test_existing_output_payload_mismatch_fails_with_sanitized_failure_code() -> None:
    result = _run(
        "--database-url",
        PASSWORD_URL,
        "--source-fixture",
        SOURCE_FIXTURE,
        "--github-snapshot-fixture",
        GITHUB_FIXTURE,
        "--replay-namespace",
        "unit-output-mismatch",
        "--confirm-local-test-db",
        predecessor_runner=FakeAnalysisRouterRunner(),
        executor=FakeFakeJudgeExecutor(checks_failed=("judge_output_payload_mismatch",)),
    )

    text = runner.render_json(result.report)
    assert result.exit_code == 1
    assert result.report["checks_failed"] == ["judge_output_payload_mismatch"]
    assert PASSWORD_URL not in text
    assert PASSWORD_VALUE not in text


def _bundle_context(*, primary_summary=None) -> runner.BundleContext:
    return runner.BundleContext(
        bundle_id=BUNDLE_ID,
        candidate_group_id=GROUP_ID,
        current_bundle_id=BUNDLE_ID,
        primary_summary=primary_summary or {
            "repo_full_name": "example/example-tool",
            "headline": "Synthetic local fixture for a developer workflow helper.",
            "test_paths": ["tests/test_example_tool.py"],
            "ci_paths": [".github/workflows/test.yml"],
            "docs_paths": ["docs/usage.md"],
        },
        evidence_limitations=["synthetic local fixture; no GitHub API call"],
        ready_for_analysis=True,
    )


def _expected_pass_report() -> dict[str, object]:
    return {
        "schema_version": "local_db_fake_judge_output_fixture_replay_v1",
        "status": "pass",
        "database_url_guard_passed": True,
        "source_candidate_replay_confirmed": True,
        "artifact_snapshot_replay_confirmed": True,
        "evidence_bundle_replay_confirmed": True,
        "analysis_router_replay_confirmed": True,
        "judge_call_requested_event_found": True,
        "judge_run_pending_confirmed": True,
        "bundle_context_loaded": True,
        "fake_judge_output_created_or_reused": True,
        "judge_run_succeeded": True,
        "judge_output_ready_event_created": True,
        "structured_output_schema_valid": True,
        "usage_telemetry_recorded": True,
        "production_db_write": False,
        "live_github_called": False,
        "live_telegram_called": False,
        "openai_called": False,
        "workers_started": False,
        "redis_mutation": False,
        "analysis_created": False,
        "notification_created": False,
        "checks_failed": [],
    }
