from __future__ import annotations

import ast
import json
from pathlib import Path

from tools import local_db_github_snapshot_fixture_replay_runner as runner


ROOT = Path(__file__).resolve().parents[3]
SOURCE_FIXTURE = "tests/fixtures/upstream/source_message_github_repo_signal.json"
GITHUB_FIXTURE = "tests/fixtures/upstream/github_repo_snapshot_example_tool.json"
PG_SCHEME = "postgresql+psycopg"
SAFE_DATABASE_NAME = "github_ai_catchbot_test"
SOCKET_HOST = "/var/run/postgresql"
SAFE_SOCKET_URL = f"{PG_SCHEME}:///{SAFE_DATABASE_NAME}?host={SOCKET_HOST}"
SECRET_VALUE = "local" + "_" + "secret"
PASSWORD_URL = f"{PG_SCHEME}://local_user:{SECRET_VALUE}@127.0.0.1:5432/{SAFE_DATABASE_NAME}"


class FakeSourceCandidateRunner:
    def __init__(self) -> None:
        self.calls: list[tuple[str, Path, str]] = []

    def run(
        self,
        *,
        database_url: str,
        source_fixture_path: Path,
        replay_namespace: str,
        env,
        repo_root: Path,
    ):
        self.calls.append((database_url, source_fixture_path, replay_namespace))
        return runner.source_candidate_runner.RunnerResult(
            exit_code=0,
            report={"schema_version": "local_db_source_candidate_replay_v1", "status": "pass"},
        )


class FakeSnapshotExecutor:
    def __init__(self) -> None:
        self.calls = []

    def execute(
        self,
        *,
        database_url: str,
        source_fixture,
        github_fixture: runner.GitHubSnapshotFixture,
        replay_namespace: str,
    ) -> runner.ReplayExecutionResult:
        self.calls.append((database_url, source_fixture, github_fixture, replay_namespace))
        return runner.ReplayExecutionResult(
            source_candidate_replay_confirmed=True,
            enrich_requested_event_found=True,
            artifact_snapshot_created_or_reused=True,
            github_repo_snapshot_created_or_reused=True,
            github_file_samples_created_or_reused=True,
            artifact_current_snapshot_updated=True,
            snapshot_updated_outbox_event_created=True,
            evidence_bundle_created=False,
            analysis_requested_event_created=False,
            notification_created=False,
        )


def _socket_url(database_name: str, *, host: str = SOCKET_HOST) -> str:
    return f"{PG_SCHEME}:///{database_name}?host={host}"


def _network_url(host: str, database_name: str, *, password: str = "secret") -> str:
    return f"{PG_SCHEME}://user:{password}@{host}/{database_name}"


def _parse_args(*args: str):
    return runner.build_parser().parse_args(args)


def _run(*args: str, env=None, executor=None, source_replay_runner=None) -> runner.RunnerResult:
    return runner.run(
        _parse_args(*args),
        env=env or {"APP_ENV": "test"},
        executor=executor,
        source_replay_runner=source_replay_runner,
        repo_root=ROOT,
    )


def test_runner_passes_with_guarded_socket_url_fixture_and_fake_executors() -> None:
    source = FakeSourceCandidateRunner()
    executor = FakeSnapshotExecutor()

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
        executor=executor,
        source_replay_runner=source,
    )

    assert result.exit_code == 0
    assert result.report == {
        "schema_version": "local_db_github_snapshot_fixture_replay_v1",
        "status": "pass",
        "database_url_guard_passed": True,
        "source_candidate_replay_confirmed": True,
        "enrich_requested_event_found": True,
        "github_snapshot_fixture_loaded": True,
        "artifact_snapshot_created_or_reused": True,
        "github_repo_snapshot_created_or_reused": True,
        "github_file_samples_created_or_reused": True,
        "artifact_current_snapshot_updated": True,
        "snapshot_updated_outbox_event_created": True,
        "production_db_write": False,
        "live_github_called": False,
        "live_telegram_called": False,
        "openai_called": False,
        "workers_started": False,
        "redis_mutation": False,
        "evidence_bundle_created": False,
        "analysis_requested_event_created": False,
        "notification_created": False,
        "checks_failed": [],
    }
    assert len(source.calls) == 1
    assert len(executor.calls) == 1
    assert executor.calls[0][2].repo_full_name == "example/example-tool"


def test_load_github_snapshot_fixture_validates_synthetic_repo_shape() -> None:
    fixture = runner.load_github_snapshot_fixture(Path(GITHUB_FIXTURE), repo_root=ROOT)

    assert fixture.artifact_canonical_id == "github:repo:example/example-tool"
    assert fixture.artifact_type == "github_repo"
    assert fixture.provider == "github"
    assert fixture.status == "ready"
    assert fixture.content_anchor == "commit:1111111111111111111111111111111111111111"
    assert fixture.repo_full_name == "example/example-tool"
    assert [sample.path for sample in fixture.file_samples] == [
        "README.md",
        "pyproject.toml",
        "tests/test_example_tool.py",
    ]


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
        executor=FakeSnapshotExecutor(),
        source_replay_runner=FakeSourceCandidateRunner(),
    )

    text = runner.render_json(result.report)
    parsed = json.loads(text)
    assert list(parsed) == [
        "schema_version",
        "status",
        "database_url_guard_passed",
        "source_candidate_replay_confirmed",
        "enrich_requested_event_found",
        "github_snapshot_fixture_loaded",
        "artifact_snapshot_created_or_reused",
        "github_repo_snapshot_created_or_reused",
        "github_file_samples_created_or_reused",
        "artifact_current_snapshot_updated",
        "snapshot_updated_outbox_event_created",
        "production_db_write",
        "live_github_called",
        "live_telegram_called",
        "openai_called",
        "workers_started",
        "redis_mutation",
        "evidence_bundle_created",
        "analysis_requested_event_created",
        "notification_created",
        "checks_failed",
    ]
    assert PASSWORD_URL not in text
    assert SECRET_VALUE not in text


def test_rejects_missing_confirmation_before_replay_runs() -> None:
    source = FakeSourceCandidateRunner()
    executor = FakeSnapshotExecutor()

    result = _run(
        "--database-url",
        SAFE_SOCKET_URL,
        "--source-fixture",
        SOURCE_FIXTURE,
        "--github-snapshot-fixture",
        GITHUB_FIXTURE,
        "--replay-namespace",
        "unit-missing-confirm",
        executor=executor,
        source_replay_runner=source,
    )

    assert result.exit_code == 1
    assert result.report["database_url_guard_passed"] is True
    assert result.report["github_snapshot_fixture_loaded"] is True
    assert result.report["checks_failed"] == ["confirm_local_test_db_required"]
    assert source.calls == []
    assert executor.calls == []


def test_rejects_app_env_prod_before_replay_runs() -> None:
    source = FakeSourceCandidateRunner()

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
        source_replay_runner=source,
        executor=FakeSnapshotExecutor(),
    )

    assert result.exit_code == 1
    assert result.report["checks_failed"] == ["app_env_production_rejected"]
    assert source.calls == []


def test_database_url_guard_allows_local_unix_socket_fixture_url() -> None:
    ok, failures, parsed = runner.validate_database_url(SAFE_SOCKET_URL)

    assert ok is True
    assert failures == []
    assert parsed is not None
    assert parsed.database_name == "github_ai_catchbot_test"
    assert parsed.hostname == ""


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
    source = (ROOT / "tools/local_db_github_snapshot_fixture_replay_runner.py").read_text(encoding="utf-8")
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
    assert "github_client" not in source
    assert "GitHubClient" not in source
    assert "GhEnricherService" not in source
