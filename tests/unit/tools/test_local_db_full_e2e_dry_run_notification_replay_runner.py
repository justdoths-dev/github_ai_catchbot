from __future__ import annotations

import ast
import json
from pathlib import Path

from tools import local_db_full_e2e_dry_run_notification_replay_runner as runner


ROOT = Path(__file__).resolve().parents[3]
SOURCE_FIXTURE = "tests/fixtures/upstream/source_message_github_repo_signal.json"
GITHUB_FIXTURE = "tests/fixtures/upstream/github_repo_snapshot_example_tool.json"
PG_SCHEME = "postgresql+psycopg"
SAFE_DATABASE_NAME = "github_ai_catchbot_test"
SOCKET_HOST = "/var/run/postgresql"
SAFE_SOCKET_URL = f"{PG_SCHEME}:///{SAFE_DATABASE_NAME}?host={SOCKET_HOST}"
SECRET_VALUE = "local" + "_" + "secret"
PASSWORD_URL = f"{PG_SCHEME}://local_user:{SECRET_VALUE}@127.0.0.1:5432/{SAFE_DATABASE_NAME}"


class FakeNotifierRunner:
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
            "schema_version": "local_db_notifier_fixture_replay_v1",
            "status": "pass",
            "telegram_called": False,
            "send_message_called": False,
            "edit_message_called": False,
            "openai_called": False,
            "live_github_called": False,
            "live_telegram_called": False,
            "workers_started": False,
            "redis_mutation": False,
            "production_db_write": False,
            "checks_failed": [],
        }
        report.update(self.report_overrides)
        return runner.notifier_runner.RunnerResult(exit_code=self.exit_code, report=report)


class FakeFullChainVerifier:
    def __init__(self, *, result_overrides=None) -> None:
        self.calls = []
        self.result_overrides = result_overrides or {}

    def verify(
        self,
        *,
        database_url: str,
        source_fixture_path: Path,
        github_snapshot_fixture_path: Path,
        replay_namespace: str,
        repo_root: Path,
    ) -> runner.FullChainVerificationResult:
        self.calls.append(
            {
                "database_url": database_url,
                "source_fixture_path": source_fixture_path,
                "github_snapshot_fixture_path": github_snapshot_fixture_path,
                "replay_namespace": replay_namespace,
                "repo_root": repo_root,
            }
        )
        values = {
            "source_message_created": True,
            "artifact_created": True,
            "candidate_group_created": True,
            "artifact_snapshot_created": True,
            "evidence_bundle_created": True,
            "analysis_requested_event_created": True,
            "judge_run_created": True,
            "judge_call_requested_event_created": True,
            "judge_output_created": True,
            "judge_output_ready_event_created": True,
            "analysis_validated_state_transition_created": True,
            "analysis_policy_apply_event_created": True,
            "analysis_created": True,
            "notification_plan_intent_event_created": True,
            "notification_plan_created": True,
            "notification_render_created": True,
            "notification_delivery_record_created": True,
            "notification_delivery_state_transition_created": True,
            "notification_delivery_result_event_created": True,
            "checks_failed": (),
        }
        values.update(self.result_overrides)
        return runner.FullChainVerificationResult(**values)


def _parse_args(*args: str):
    return runner.build_parser().parse_args(args)


def _run(*args: str, env=None, verifier=None, predecessor_runner=None) -> runner.RunnerResult:
    return runner.run(
        _parse_args(*args),
        env=env or {"APP_ENV": "test"},
        verifier=verifier,
        predecessor_runner=predecessor_runner,
        repo_root=ROOT,
    )


def test_fake_notifier_delegation_and_verifier_return_expected_pass_output() -> None:
    predecessor = FakeNotifierRunner()
    verifier = FakeFullChainVerifier()

    result = _run(
        "--database-url",
        SAFE_SOCKET_URL,
        "--source-fixture",
        SOURCE_FIXTURE,
        "--github-snapshot-fixture",
        GITHUB_FIXTURE,
        "--replay-namespace",
        "unit-full-e2e",
        "--confirm-local-test-db",
        predecessor_runner=predecessor,
        verifier=verifier,
    )

    assert result.exit_code == 0
    assert result.report == _expected_pass_report()
    assert len(predecessor.calls) == 1
    assert len(verifier.calls) == 1
    assert predecessor.calls[0]["source_fixture_path"] == Path(SOURCE_FIXTURE)
    assert verifier.calls[0]["github_snapshot_fixture_path"] == Path(GITHUB_FIXTURE)


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
        predecessor_runner=FakeNotifierRunner(),
        verifier=FakeFullChainVerifier(),
    )

    text = runner.render_json(result.report)
    parsed = json.loads(text)
    assert list(parsed) == list(_expected_pass_report())
    assert PASSWORD_URL not in text
    assert SECRET_VALUE not in text


def test_rejects_missing_confirmation_before_predecessor_runs() -> None:
    predecessor = FakeNotifierRunner()
    verifier = FakeFullChainVerifier()

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
        verifier=verifier,
    )

    assert result.exit_code == 1
    assert result.report["database_url_guard_passed"] is True
    assert result.report["checks_failed"] == ["confirm_local_test_db_required"]
    assert predecessor.calls == []
    assert verifier.calls == []


def test_rejects_app_env_prod_before_predecessor_runs() -> None:
    predecessor = FakeNotifierRunner()
    verifier = FakeFullChainVerifier()

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
        verifier=verifier,
    )

    assert result.exit_code == 1
    assert result.report["checks_failed"] == ["app_env_production_rejected"]
    assert predecessor.calls == []
    assert verifier.calls == []


def test_database_url_guard_delegates_to_notifier_runner_guard(monkeypatch) -> None:
    calls = []
    unsafe_url = f"{PG_SCHEME}:///unsafe"

    def fake_validate(database_url):
        calls.append(database_url)
        return False, ["delegated_failure"], None

    monkeypatch.setattr(runner.notifier_runner, "validate_database_url", fake_validate)

    ok, failures, parsed = runner.validate_database_url(unsafe_url)

    assert ok is False
    assert failures == ["delegated_failure"]
    assert parsed is None
    assert calls == [unsafe_url]


def test_guard_failure_stops_before_notifier_delegation(monkeypatch) -> None:
    predecessor = FakeNotifierRunner()
    verifier = FakeFullChainVerifier()

    def fake_validate(_database_url):
        return False, ["delegated_failure"], None

    monkeypatch.setattr(runner.notifier_runner, "validate_database_url", fake_validate)

    result = _run(
        "--database-url",
        SAFE_SOCKET_URL,
        "--source-fixture",
        SOURCE_FIXTURE,
        "--github-snapshot-fixture",
        GITHUB_FIXTURE,
        "--replay-namespace",
        "unit-guard-failure",
        "--confirm-local-test-db",
        predecessor_runner=predecessor,
        verifier=verifier,
    )

    assert result.exit_code == 1
    assert result.report["checks_failed"] == ["delegated_failure"]
    assert predecessor.calls == []
    assert verifier.calls == []


def test_report_fails_when_required_stage_flag_is_missing() -> None:
    result = _run(
        "--database-url",
        SAFE_SOCKET_URL,
        "--source-fixture",
        SOURCE_FIXTURE,
        "--github-snapshot-fixture",
        GITHUB_FIXTURE,
        "--replay-namespace",
        "unit-missing-stage",
        "--confirm-local-test-db",
        predecessor_runner=FakeNotifierRunner(),
        verifier=FakeFullChainVerifier(result_overrides={"artifact_created": False}),
    )

    assert result.exit_code == 1
    assert result.report["artifact_created"] is False
    assert result.report["checks_failed"] == ["artifact_created:missing"]


def test_notifier_side_effect_flag_failure_is_rejected() -> None:
    result = _run(
        "--database-url",
        SAFE_SOCKET_URL,
        "--source-fixture",
        SOURCE_FIXTURE,
        "--github-snapshot-fixture",
        GITHUB_FIXTURE,
        "--replay-namespace",
        "unit-side-effect",
        "--confirm-local-test-db",
        predecessor_runner=FakeNotifierRunner(report_overrides={"openai_called": True}),
        verifier=FakeFullChainVerifier(),
    )

    assert result.exit_code == 1
    assert result.report["checks_failed"] == ["openai_called:unexpected"]


def test_runner_source_has_no_forbidden_runtime_or_network_imports() -> None:
    source = (ROOT / "tools/local_db_full_e2e_dry_run_notification_replay_runner.py").read_text(encoding="utf-8")
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


def _expected_pass_report() -> dict[str, object]:
    return {
        "schema_version": "local_db_full_e2e_dry_run_notification_replay_v1",
        "status": "pass",
        "database_url_guard_passed": True,
        "source_message_created": True,
        "artifact_created": True,
        "candidate_group_created": True,
        "artifact_snapshot_created": True,
        "evidence_bundle_created": True,
        "analysis_requested_event_created": True,
        "judge_run_created": True,
        "judge_call_requested_event_created": True,
        "judge_output_created": True,
        "judge_output_ready_event_created": True,
        "analysis_validated_state_transition_created": True,
        "analysis_policy_apply_event_created": True,
        "analysis_created": True,
        "notification_plan_intent_event_created": True,
        "notification_plan_created": True,
        "notification_render_created": True,
        "notification_delivery_record_created": True,
        "notification_delivery_state_transition_created": True,
        "notification_delivery_result_event_created": True,
        "telegram_called": False,
        "send_message_called": False,
        "edit_message_called": False,
        "openai_called": False,
        "live_github_called": False,
        "live_telegram_called": False,
        "workers_started": False,
        "redis_mutation": False,
        "production_db_write": False,
        "alembic_or_ddl_ran": False,
        "checks_failed": [],
    }
