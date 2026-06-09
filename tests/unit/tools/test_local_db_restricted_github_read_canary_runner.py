from __future__ import annotations

import ast
import base64
import json
from pathlib import Path
from uuid import UUID

import pytest

from tools import local_db_restricted_github_read_canary_runner as runner


ROOT = Path(__file__).resolve().parents[3]
SOURCE_FIXTURE = "tests/fixtures/upstream/source_message_github_public_repo_octocat_hello_world.json"
PG_SCHEME = "postgresql+psycopg"
SAFE_DATABASE_NAME = "github_ai_catchbot_test"
SOCKET_HOST = "/var/run/postgresql"
SAFE_SOCKET_URL = f"{PG_SCHEME}:///{SAFE_DATABASE_NAME}?host={SOCKET_HOST}"
SECRET_VALUE = "local" + "_" + "secret"
PASSWORD_URL = f"{PG_SCHEME}://local_user:{SECRET_VALUE}@127.0.0.1:5432/{SAFE_DATABASE_NAME}"
TOKEN_VALUE = "ghp" + "_" + "secret"
ARTIFACT_ID = UUID("33333333-3333-4333-8333-333333333333")
CANDIDATE_GROUP_ID = UUID("44444444-4444-4444-8444-444444444444")
COMMIT_SHA = "abcd1234abcd1234abcd1234abcd1234abcd1234"


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


class FakeCanaryExecutor:
    def __init__(
        self,
        preflight: runner.ArtifactPreflightResult | None = None,
        write_result: runner.SnapshotWriteResult | None = None,
    ) -> None:
        self.preflight = preflight
        self.write_result = write_result
        self.preflight_calls = []
        self.write_calls = []

    def load_predecessor_state(
        self,
        *,
        database_url: str,
        source_fixture,
        replay_namespace: str,
        expected_artifact_canonical_id: str,
    ) -> runner.ArtifactPreflightResult:
        self.preflight_calls.append((database_url, source_fixture, replay_namespace, expected_artifact_canonical_id))
        return self.preflight or _preflight(expected_artifact_canonical_id)

    def write_snapshot(
        self,
        *,
        database_url: str,
        artifact_id: UUID,
        candidate_group_id: UUID,
        snapshot_plan,
        replay_namespace: str,
    ) -> runner.SnapshotWriteResult:
        self.write_calls.append((database_url, artifact_id, candidate_group_id, snapshot_plan, replay_namespace))
        return self.write_result or runner.SnapshotWriteResult(
            artifact_enrichment_run_created=True,
            artifact_snapshot_created=True,
            github_repo_child_snapshot_created=True,
            github_readme_file_sample_created=True,
            artifact_current_snapshot_updated=True,
            artifact_snapshot_updated_event_created=True,
        )


class RecordingHttpGet:
    def __init__(self, responses: list[runner.GitHubHttpResponse]) -> None:
        self.responses = list(responses)
        self.calls: list[tuple[str, float]] = []

    def __call__(self, url: str, *, timeout_seconds: float) -> runner.GitHubHttpResponse:
        self.calls.append((url, timeout_seconds))
        return self.responses.pop(0)


def _preflight(canonical_id: str) -> runner.ArtifactPreflightResult:
    return runner.ArtifactPreflightResult(
        source_candidate_replay_confirmed=True,
        candidate_group_loaded=True,
        github_artifact_loaded=True,
        artifact_matches_requested_repo=True,
        enrich_requested_event_found=True,
        artifact_id=ARTIFACT_ID,
        candidate_group_id=CANDIDATE_GROUP_ID,
        artifact_canonical_id=canonical_id,
        artifact_type="github_repo",
    )


def _socket_url(database_name: str, *, host: str = SOCKET_HOST) -> str:
    return f"{PG_SCHEME}:///{database_name}?host={host}"


def _network_url(host: str, database_name: str, *, password: str = "secret") -> str:
    return f"{PG_SCHEME}://user:{password}@{host}/{database_name}"


def _parse_args(*args: str):
    return runner.build_parser().parse_args(args)


def _base_args(*extra: str) -> tuple[str, ...]:
    return (
        "--database-url",
        SAFE_SOCKET_URL,
        "--source-fixture",
        SOURCE_FIXTURE,
        "--replay-namespace",
        "unit-restricted-github-read",
        "--repo-full-name",
        "octocat/Hello-World",
        "--confirm-local-test-db",
        "--allow-network-read",
        *extra,
    )


def _run(*args: str, env=None, http_get=None, executor=None, source_replay_runner=None) -> runner.RunnerResult:
    return runner.run(
        _parse_args(*args),
        env=env or {"APP_ENV": "test"},
        http_get=http_get or _success_http_get(),
        executor=executor or FakeCanaryExecutor(),
        source_replay_runner=source_replay_runner or FakeSourceCandidateRunner(),
        repo_root=ROOT,
    )


def _success_http_get() -> RecordingHttpGet:
    return RecordingHttpGet([_repo_response(), _commit_response(), _readme_response()])


def _repo_response(**overrides) -> runner.GitHubHttpResponse:
    payload = {
        "full_name": "octocat/Hello-World",
        "default_branch": "master",
        "description": "A public canary repository.",
        "homepage": None,
        "pushed_at": "2026-06-01T00:00:00Z",
        "stargazers_count": 2500,
        "forks_count": 600,
        "open_issues_count": 12,
        "watchers_count": 2500,
        "archived": False,
        "fork": False,
        "is_template": False,
        "license": {"spdx_id": "NOASSERTION"},
        "topics": ["canary", "public"],
        "language": "C",
    }
    payload.update(overrides)
    return runner.GitHubHttpResponse(status_code=200, json_payload=payload, headers={"Authorization": TOKEN_VALUE})


def _commit_response(sha: str = COMMIT_SHA) -> runner.GitHubHttpResponse:
    return runner.GitHubHttpResponse(status_code=200, json_payload={"sha": sha})


def _readme_response(text: str = "Hello World\nThis is a README excerpt.") -> runner.GitHubHttpResponse:
    raw = text.encode("utf-8")
    return runner.GitHubHttpResponse(
        status_code=200,
        json_payload={
            "path": "README.md",
            "size": len(raw),
            "encoding": "base64",
            "content": base64.b64encode(raw).decode("ascii"),
            "download_url": "https://raw.githubusercontent.com/octocat/Hello-World/master/README.md",
            "sha": "readmesha",
        },
    )


def test_refuses_network_read_unless_allow_network_read() -> None:
    source = FakeSourceCandidateRunner()
    executor = FakeCanaryExecutor()
    http = _success_http_get()

    result = _run(
        "--database-url",
        SAFE_SOCKET_URL,
        "--source-fixture",
        SOURCE_FIXTURE,
        "--replay-namespace",
        "unit-no-network",
        "--repo-full-name",
        "octocat/Hello-World",
        "--confirm-local-test-db",
        source_replay_runner=source,
        executor=executor,
        http_get=http,
    )

    assert result.exit_code == 1
    assert "allow_network_read_required" in result.report["checks_failed"]
    assert result.report["network_read_authorized"] is False
    assert source.calls == []
    assert executor.preflight_calls == []
    assert http.calls == []


def test_refuses_missing_confirmation_non_test_env_and_unsafe_database_before_replay() -> None:
    cases = [
        (_base_args(), {"APP_ENV": "prod"}, "app_env_test_required"),
        (
            (
                "--database-url",
                SAFE_SOCKET_URL,
                "--source-fixture",
                SOURCE_FIXTURE,
                "--replay-namespace",
                "unit-missing-confirm",
                "--repo-full-name",
                "octocat/Hello-World",
                "--allow-network-read",
            ),
            {"APP_ENV": "test"},
            "confirm_local_test_db_required",
        ),
        (
            (
                "--database-url",
                _socket_url("github_ai_catchbot_prod_test"),
                "--source-fixture",
                SOURCE_FIXTURE,
                "--replay-namespace",
                "unit-prod-db",
                "--repo-full-name",
                "octocat/Hello-World",
                "--confirm-local-test-db",
                "--allow-network-read",
            ),
            {"APP_ENV": "test"},
            "database_url_production_name_rejected",
        ),
    ]
    for args, env, expected in cases:
        source = FakeSourceCandidateRunner()
        executor = FakeCanaryExecutor()
        http = _success_http_get()

        result = _run(
            *args,
            env=env,
            source_replay_runner=source,
            executor=executor,
            http_get=http,
        )

        assert result.exit_code == 1
        assert expected in result.report["checks_failed"]
        assert source.calls == []
        assert executor.preflight_calls == []
        assert http.calls == []


def test_refuses_non_github_api_base_url() -> None:
    source = FakeSourceCandidateRunner()
    http = _success_http_get()

    result = _run(
        *_base_args("--github-api-base-url", "https://example.com"),
        source_replay_runner=source,
        http_get=http,
    )

    assert result.exit_code == 1
    assert result.report["github_api_base_url_allowed"] is False
    assert result.report["checks_failed"] == ["github_api_base_url_not_allowed"]
    assert source.calls == []
    assert http.calls == []


@pytest.mark.parametrize("repo_full_name", ["octocat", "../Hello-World", "octocat/", "octocat/Hello World"])
def test_refuses_malformed_repo_full_name(repo_full_name: str) -> None:
    source = FakeSourceCandidateRunner()
    http = _success_http_get()

    result = _run(
        "--database-url",
        SAFE_SOCKET_URL,
        "--source-fixture",
        SOURCE_FIXTURE,
        "--replay-namespace",
        "unit-bad-repo",
        "--repo-full-name",
        repo_full_name,
        "--confirm-local-test-db",
        "--allow-network-read",
        source_replay_runner=source,
        http_get=http,
    )

    assert result.exit_code == 1
    assert "repo_full_name_invalid" in result.report["checks_failed"]
    assert source.calls == []
    assert http.calls == []


def test_refuses_artifact_repo_mismatch_before_network_call() -> None:
    mismatch = runner.ArtifactPreflightResult(
        source_candidate_replay_confirmed=True,
        candidate_group_loaded=True,
        github_artifact_loaded=True,
        artifact_matches_requested_repo=False,
        enrich_requested_event_found=True,
        artifact_id=ARTIFACT_ID,
        candidate_group_id=CANDIDATE_GROUP_ID,
        artifact_canonical_id="github:repo:other/repo",
        artifact_type="github_repo",
        checks_failed=("artifact_repo_mismatch",),
    )
    source = FakeSourceCandidateRunner()
    executor = FakeCanaryExecutor(preflight=mismatch)
    http = _success_http_get()

    result = _run(*_base_args(), source_replay_runner=source, executor=executor, http_get=http)

    assert result.exit_code == 1
    assert result.report["artifact_matches_requested_repo"] is False
    assert result.report["checks_failed"] == ["artifact_repo_mismatch"]
    assert len(source.calls) == 1
    assert len(executor.preflight_calls) == 1
    assert http.calls == []
    assert executor.write_calls == []


def test_maps_github_metadata_commit_and_readme_into_snapshot_plan() -> None:
    http = _success_http_get()
    executor = FakeCanaryExecutor()

    result = _run(*_base_args(), http_get=http, executor=executor)

    assert result.exit_code == 0
    assert result.report["status"] == "pass"
    assert result.report["live_github_read_called"] is False
    assert result.report["github_http_get_called"] is True
    assert [call[0] for call in http.calls] == [
        "https://api.github.com/repos/octocat/Hello-World",
        "https://api.github.com/repos/octocat/Hello-World/commits/master",
        "https://api.github.com/repos/octocat/Hello-World/readme",
    ]
    snapshot_plan = executor.write_calls[0][3]
    assert snapshot_plan.artifact_canonical_id == "github:repo:octocat/hello-world"
    assert snapshot_plan.repo_full_name == "octocat/Hello-World"
    assert snapshot_plan.status == "ready"
    assert snapshot_plan.content_anchor == f"commit:{COMMIT_SHA}"
    assert snapshot_plan.auth_mode == "anonymous_degraded"
    assert snapshot_plan.default_branch == "master"
    assert snapshot_plan.content_anchor_commit_sha == COMMIT_SHA
    assert snapshot_plan.readme_excerpt == "Hello World\nThis is a README excerpt."
    assert len(snapshot_plan.file_samples) == 1
    assert snapshot_plan.file_samples[0].path == "README.md"
    assert snapshot_plan.file_samples[0].role == "README"


@pytest.mark.parametrize(
    ("status_code", "expected_failure"),
    [
        (403, "github_repo_metadata_fetch_access_denied"),
        (404, "github_repo_metadata_fetch_access_denied"),
        (429, "github_repo_metadata_fetch_rate_limited"),
        (500, "github_repo_metadata_fetch_failed_transient"),
    ],
)
def test_handles_github_response_failures_without_snapshot_write(status_code: int, expected_failure: str) -> None:
    http = RecordingHttpGet([runner.GitHubHttpResponse(status_code=status_code, json_payload={})])
    executor = FakeCanaryExecutor()

    result = _run(*_base_args(), http_get=http, executor=executor)

    assert result.exit_code == 1
    assert expected_failure in result.report["checks_failed"]
    assert "github_snapshot_plan_missing" in result.report["checks_failed"]
    assert result.report["github_http_get_called"] is True
    assert result.report["github_write_called"] is False
    assert executor.write_calls == []
    assert len(http.calls) == 1


def test_redacts_database_url_and_token_like_values_from_json_output() -> None:
    http = RecordingHttpGet(
        [
            _repo_response(description=TOKEN_VALUE),
            _commit_response(),
            _readme_response(f"secret-like value {TOKEN_VALUE} must stay out of report"),
        ]
    )
    args = list(_base_args())
    args[1] = PASSWORD_URL

    result = _run(*args, http_get=http)
    text = runner.render_json(result.report)

    assert result.exit_code == 0
    assert PASSWORD_URL not in text
    assert SECRET_VALUE not in text
    assert TOKEN_VALUE not in text


def test_never_attempts_non_get_or_raw_download_url_fetch() -> None:
    http = _success_http_get()

    result = _run(*_base_args(), http_get=http)

    assert result.exit_code == 0
    called_urls = [call[0] for call in http.calls]
    assert all(url.startswith("https://api.github.com/repos/octocat/Hello-World") for url in called_urls)
    assert not any("raw.githubusercontent.com" in url for url in called_urls)
    assert not any("download_url" in url for url in called_urls)


def test_status_json_includes_stable_authority_flags() -> None:
    result = _run(*_base_args())
    text = runner.render_json(result.report)
    parsed = json.loads(text)

    assert list(parsed) == [
        "schema_version",
        "status",
        "database_url_guard_passed",
        "network_read_authorized",
        "github_api_base_url_allowed",
        "repo_full_name",
        "source_candidate_replay_confirmed",
        "candidate_group_loaded",
        "github_artifact_loaded",
        "artifact_matches_requested_repo",
        "github_repo_metadata_fetched",
        "github_default_branch_commit_fetched",
        "github_readme_fetched",
        "artifact_enrichment_run_created",
        "artifact_snapshot_created",
        "github_repo_child_snapshot_created",
        "github_readme_file_sample_created",
        "artifact_current_snapshot_updated",
        "artifact_snapshot_updated_event_created",
        "live_github_read_called",
        "github_http_get_called",
        "github_write_called",
        "telegram_called",
        "openai_called",
        "workers_started",
        "redis_mutation",
        "production_db_write",
        "alembic_or_ddl_ran",
        "checks_failed",
    ]
    assert parsed["network_read_authorized"] is True
    assert parsed["github_api_base_url_allowed"] is True
    assert parsed["github_write_called"] is False
    assert parsed["telegram_called"] is False
    assert parsed["openai_called"] is False
    assert parsed["workers_started"] is False
    assert parsed["redis_mutation"] is False
    assert parsed["production_db_write"] is False
    assert parsed["alembic_or_ddl_ran"] is False


def test_runner_source_has_no_forbidden_runtime_or_write_client_imports() -> None:
    source = (ROOT / "tools/local_db_restricted_github_read_canary_runner.py").read_text(encoding="utf-8")
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
    assert "download_url" not in source
