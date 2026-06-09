from __future__ import annotations

import ast
import json
from pathlib import Path

import pytest

from tools import local_db_analysis_router_fixture_replay_runner as runner


ROOT = Path(__file__).resolve().parents[3]
SOURCE_FIXTURE = "tests/fixtures/upstream/source_message_github_repo_signal.json"
GITHUB_FIXTURE = "tests/fixtures/upstream/github_repo_snapshot_example_tool.json"
PG_SCHEME = "postgresql+psycopg"
SAFE_DATABASE_NAME = "github_ai_catchbot_test"
SOCKET_HOST = "/var/run/postgresql"
SAFE_SOCKET_URL = f"{PG_SCHEME}:///{SAFE_DATABASE_NAME}?host={SOCKET_HOST}"
SECRET_VALUE = "local" + "_" + "secret"
PASSWORD_URL = f"{PG_SCHEME}://local_user:{SECRET_VALUE}@127.0.0.1:5432/{SAFE_DATABASE_NAME}"


class FakeEvidenceBundleRunner:
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
            "schema_version": "local_db_evidence_bundle_fixture_replay_v1",
            "status": "pass",
            "source_candidate_replay_confirmed": True,
            "artifact_snapshot_replay_confirmed": True,
            "evidence_bundle_created_or_reused": True,
            "candidate_evidence_members_created_or_reused": True,
            "candidate_current_bundle_updated": True,
            "analysis_requested_event_created": True,
            "ready_for_analysis": True,
        }
        report.update(self.report_overrides)
        return runner.evidence_bundle_runner.RunnerResult(exit_code=self.exit_code, report=report)


class FakeAnalysisRouterExecutor:
    def __init__(self, *, event_insert_attempted: bool = True) -> None:
        self.calls = []
        self.event_insert_attempted = event_insert_attempted

    def execute(
        self,
        *,
        database_url: str,
        replay_namespace: str,
    ) -> runner.ReplayExecutionResult:
        self.calls.append((database_url, replay_namespace))
        return runner.ReplayExecutionResult(
            analysis_requested_event_found=True,
            candidate_current_bundle_confirmed=True,
            evidence_bundle_ready_confirmed=True,
            judge_profile_allowed=True,
            routing_policy_applied=True,
            judge_run_created_or_reused=True,
            judge_call_requested_event_created=True,
            default_model_selected=True,
            prompt_cache_key_created=True,
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
    predecessor = FakeEvidenceBundleRunner()
    executor = FakeAnalysisRouterExecutor()

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
        predecessor_runner=FakeEvidenceBundleRunner(),
        executor=FakeAnalysisRouterExecutor(),
    )

    text = runner.render_json(result.report)
    parsed = json.loads(text)
    assert list(parsed) == list(_expected_pass_report())
    assert PASSWORD_URL not in text
    assert SECRET_VALUE not in text


def test_rejects_missing_confirmation_before_predecessor_runs() -> None:
    predecessor = FakeEvidenceBundleRunner()
    executor = FakeAnalysisRouterExecutor()

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
    predecessor = FakeEvidenceBundleRunner()
    executor = FakeAnalysisRouterExecutor()

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

    def fake_validate(database_url):
        calls.append(database_url)
        return False, ["delegated_failure"], None

    monkeypatch.setattr(runner.evidence_bundle_runner, "validate_database_url", fake_validate)

    ok, failures, parsed = runner.validate_database_url("postgresql+psycopg:///unsafe")

    assert ok is False
    assert failures == ["delegated_failure"]
    assert parsed is None
    assert calls == ["postgresql+psycopg:///unsafe"]


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
    source = (ROOT / "tools/local_db_analysis_router_fixture_replay_runner.py").read_text(encoding="utf-8")
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


def test_judge_profile_allowlist_accepts_only_locked_profiles() -> None:
    accepted = {
        profile
        for profile in (
            "github_primary",
            "x_primary",
            "text_idea_primary",
            "idea_primary",
            "web_primary",
            "unknown",
            "",
            None,
        )
        if runner.is_judge_profile_allowed(profile)
    }

    assert accepted == {"github_primary", "x_primary", "text_idea_primary"}


def test_idea_primary_and_web_primary_are_rejected() -> None:
    for profile in ("idea_primary", "web_primary"):
        assert not runner.is_judge_profile_allowed(profile)
        with pytest.raises(ValueError, match="judge_profile_not_allowed"):
            runner.prompt_version_for_profile(profile)


def test_prompt_cache_key_builder_returns_locked_github_key() -> None:
    assert (
        runner.build_prompt_cache_key(
            judge_profile="github_primary",
            prompt_version="judge_github_primary_v1",
            schema_version="judge_output_v1",
            policy_version="verdict_policy_v1",
        )
        == "judge:github_primary:judge_github_primary_v1:judge_output_v1:verdict_policy_v1"
    )


def test_existing_judge_run_reuse_does_not_require_duplicate_event_in_fake_path() -> None:
    executor = FakeAnalysisRouterExecutor(event_insert_attempted=False)

    result = _run(
        "--database-url",
        SAFE_SOCKET_URL,
        "--source-fixture",
        SOURCE_FIXTURE,
        "--github-snapshot-fixture",
        GITHUB_FIXTURE,
        "--replay-namespace",
        "unit-existing-judge-run",
        "--confirm-local-test-db",
        predecessor_runner=FakeEvidenceBundleRunner(),
        executor=executor,
    )

    assert result.exit_code == 0
    assert result.report == _expected_pass_report()
    assert executor.event_insert_attempted is False


def _expected_pass_report() -> dict[str, object]:
    return {
        "schema_version": "local_db_analysis_router_fixture_replay_v1",
        "status": "pass",
        "database_url_guard_passed": True,
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
        "production_db_write": False,
        "live_github_called": False,
        "live_telegram_called": False,
        "openai_called": False,
        "workers_started": False,
        "redis_mutation": False,
        "judge_output_created": False,
        "analysis_created": False,
        "notification_created": False,
        "checks_failed": [],
    }
