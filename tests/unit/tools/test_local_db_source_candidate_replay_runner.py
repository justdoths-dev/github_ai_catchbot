from __future__ import annotations

import ast
import json
import subprocess
import sys
from pathlib import Path

import pytest

from tools import local_db_source_candidate_replay_runner as runner


ROOT = Path(__file__).resolve().parents[3]
FIXTURE = "tests/fixtures/upstream/source_message_github_repo_signal.json"
PG_SCHEME = "postgresql+psycopg"
SAFE_DATABASE_NAME = "github_ai_catchbot_test"
SOCKET_HOST = "/var/run/postgresql"
SAFE_SOCKET_URL = f"{PG_SCHEME}:///{SAFE_DATABASE_NAME}?host={SOCKET_HOST}"
SECRET_VALUE = "local" + "_" + "secret"
PASSWORD_URL = f"{PG_SCHEME}://local_user:{SECRET_VALUE}@127.0.0.1:5432/{SAFE_DATABASE_NAME}"


class FakeExecutor:
    def __init__(self) -> None:
        self.calls: list[tuple[str, runner.SourceFixture, str]] = []

    def execute(
        self,
        *,
        database_url: str,
        fixture: runner.SourceFixture,
        replay_namespace: str,
    ) -> runner.ReplayExecutionResult:
        self.calls.append((database_url, fixture, replay_namespace))
        return runner.ReplayExecutionResult(
            source_message_upserted=True,
            source_version_upserted=True,
            source_outbox_event_created=True,
            normalization_run_created=True,
            artifact_created=True,
            artifact_observation_created=True,
            candidate_group_created=True,
            candidate_member_created=True,
            enrich_requested_event_created=True,
        )


def _socket_url(database_name: str, *, host: str = SOCKET_HOST) -> str:
    return f"{PG_SCHEME}:///{database_name}?host={host}"


def _network_url(host: str, database_name: str, *, password: str = "secret") -> str:
    return f"{PG_SCHEME}://user:{password}@{host}/{database_name}"


def _parse_args(*args: str):
    return runner.build_parser().parse_args(args)


def _run(*args: str, env=None, executor=None) -> runner.RunnerResult:
    return runner.run(_parse_args(*args), env=env or {"APP_ENV": "test"}, executor=executor, repo_root=ROOT)


def _run_cli(*args: str, app_env: str = "test") -> subprocess.CompletedProcess[str]:
    env = {"APP_ENV": app_env, "PATH": ""}
    return subprocess.run(
        [sys.executable, "-m", "tools.local_db_source_candidate_replay_runner", *args],
        check=False,
        capture_output=True,
        cwd=ROOT,
        env=env,
        text=True,
        timeout=30,
    )


def test_runner_passes_with_guarded_socket_url_fixture_and_fake_executor() -> None:
    executor = FakeExecutor()

    result = _run(
        "--database-url",
        SAFE_SOCKET_URL,
        "--fixture",
        FIXTURE,
        "--replay-namespace",
        "operator-local-db-source-candidate",
        "--confirm-local-test-db",
        executor=executor,
    )

    assert result.exit_code == 0
    assert result.report == {
        "schema_version": "local_db_source_candidate_replay_v1",
        "status": "pass",
        "fixture_loaded": True,
        "database_url_guard_passed": True,
        "production_db_write": False,
        "source_message_upserted": True,
        "source_version_upserted": True,
        "source_outbox_event_created": True,
        "normalization_run_created": True,
        "artifact_created": True,
        "artifact_observation_created": True,
        "candidate_group_created": True,
        "candidate_member_created": True,
        "enrich_requested_event_created": True,
        "live_telegram_called": False,
        "openai_called": False,
        "workers_started": False,
        "redis_mutation": False,
        "checks_failed": [],
    }
    assert len(executor.calls) == 1
    assert executor.calls[0][0] == SAFE_SOCKET_URL
    assert executor.calls[0][1].source_message_id.hex == "11111111111141118111111111111111"
    assert executor.calls[0][2] == "operator-local-db-source-candidate"


def test_load_source_fixture_coerces_canonical_fixture_fields() -> None:
    fixture = runner.load_source_fixture(Path(FIXTURE), repo_root=ROOT)

    assert fixture.platform == "telegram"
    assert fixture.chat_id == -1001000000001
    assert fixture.message_id == 7001
    assert fixture.source_version_no == 1
    assert fixture.url_surface_json[0]["observed_url"] == "https://github.com/example/example-tool"


def test_required_output_shape_is_stable_json_without_raw_url() -> None:
    executor = FakeExecutor()
    result = _run(
        "--database-url",
        PASSWORD_URL,
        "--fixture",
        FIXTURE,
        "--replay-namespace",
        "unit-output-shape",
        "--confirm-local-test-db",
        executor=executor,
    )

    text = runner.render_json(result.report)
    parsed = json.loads(text)
    assert list(parsed) == [
        "schema_version",
        "status",
        "fixture_loaded",
        "database_url_guard_passed",
        "production_db_write",
        "source_message_upserted",
        "source_version_upserted",
        "source_outbox_event_created",
        "normalization_run_created",
        "artifact_created",
        "artifact_observation_created",
        "candidate_group_created",
        "candidate_member_created",
        "enrich_requested_event_created",
        "live_telegram_called",
        "openai_called",
        "workers_started",
        "redis_mutation",
        "checks_failed",
    ]
    assert PASSWORD_URL not in text
    assert SECRET_VALUE not in text


def test_rejects_missing_confirmation_before_executor_runs() -> None:
    executor = FakeExecutor()

    result = _run(
        "--database-url",
        SAFE_SOCKET_URL,
        "--fixture",
        FIXTURE,
        "--replay-namespace",
        "unit-missing-confirm",
        executor=executor,
    )

    assert result.exit_code == 1
    assert result.report["status"] == "fail"
    assert result.report["database_url_guard_passed"] is True
    assert result.report["fixture_loaded"] is True
    assert result.report["checks_failed"] == ["confirm_local_test_db_required"]
    assert executor.calls == []


def test_rejects_app_env_prod_before_executor_runs() -> None:
    executor = FakeExecutor()

    result = _run(
        "--database-url",
        SAFE_SOCKET_URL,
        "--fixture",
        FIXTURE,
        "--replay-namespace",
        "unit-prod-env",
        "--confirm-local-test-db",
        env={"APP_ENV": "prod"},
        executor=executor,
    )

    assert result.exit_code == 1
    assert result.report["checks_failed"] == ["app_env_production_rejected"]
    assert executor.calls == []


@pytest.mark.parametrize(
    ("database_url", "expected_failure"),
    [
        ("mysql" + "://localhost/" + SAFE_DATABASE_NAME, "database_url_unsupported_scheme"),
        (_network_url("db.example.com", SAFE_DATABASE_NAME), "database_url_remote_host_rejected"),
        (
            _socket_url(SAFE_DATABASE_NAME, host="db.example.com"),
            "database_url_remote_query_host_rejected",
        ),
        (_socket_url("postgres"), "database_url_forbidden_database_name"),
        (_socket_url("github_ai_catchbot"), "database_url_forbidden_database_name"),
        (
            _socket_url("github_ai_catchbot_prod_test"),
            "database_url_production_name_rejected",
        ),
        (
            _socket_url("github_ai_catchbot_stage"),
            "database_url_missing_local_test_marker",
        ),
    ],
)
def test_database_url_guard_rejects_unsafe_targets(database_url: str, expected_failure: str) -> None:
    ok, failures, parsed = runner.validate_database_url(database_url)

    assert ok is False
    assert expected_failure in failures
    assert parsed is not None


def test_database_url_guard_allows_local_unix_socket_fixture_url() -> None:
    ok, failures, parsed = runner.validate_database_url(SAFE_SOCKET_URL)

    assert ok is True
    assert failures == []
    assert parsed is not None
    assert parsed.database_name == "github_ai_catchbot_test"
    assert parsed.hostname == ""


def test_replay_namespace_guard_rejects_unsafe_values() -> None:
    assert runner.validate_replay_namespace("operator-local-db-source-candidate") == (True, [])
    assert runner.validate_replay_namespace("../bad") == (False, ["replay_namespace_unsafe"])
    assert runner.validate_replay_namespace("") == (False, ["replay_namespace_required"])


def test_redaction_never_exposes_password() -> None:
    redacted = runner.redact_database_url(PASSWORD_URL)

    assert SECRET_VALUE not in redacted
    assert PASSWORD_URL not in redacted
    assert redacted == f"{PG_SCHEME}://local_user:<redacted>@127.0.0.1:5432/{SAFE_DATABASE_NAME}"


def test_cli_guard_failure_prints_json_only_without_password_or_raw_url() -> None:
    unsafe_url = _network_url("db.example.com", SAFE_DATABASE_NAME, password=SECRET_VALUE)

    result = _run_cli(
        "--database-url",
        unsafe_url,
        "--fixture",
        FIXTURE,
        "--replay-namespace",
        "unit-cli-redaction",
        "--confirm-local-test-db",
    )

    assert result.returncode == 1
    report = json.loads(result.stdout)
    assert report["status"] == "fail"
    assert "database_url_remote_host_rejected" in report["checks_failed"]
    assert unsafe_url not in result.stdout
    assert unsafe_url not in result.stderr
    assert SECRET_VALUE not in result.stdout
    assert SECRET_VALUE not in result.stderr
    assert result.stderr == ""


def test_no_forbidden_runtime_modules_are_imported_directly() -> None:
    source = (ROOT / "tools" / "local_db_source_candidate_replay_runner.py").read_text(encoding="utf-8")
    tree = ast.parse(source)

    imported_roots: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imported_roots.update(alias.name.split(".", 1)[0] for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            imported_roots.add(node.module.split(".", 1)[0])

    assert imported_roots.isdisjoint({"redis", "openai", "telegram", "docker", "systemd"})
    assert "TDLib" not in source
