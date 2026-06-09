from __future__ import annotations

import argparse
import base64
import hashlib
import json
import os
import re
import sys
import urllib.error
import urllib.request
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Mapping, Protocol, Sequence
from urllib.parse import quote, urlsplit
from uuid import UUID

from tools import local_db_github_snapshot_fixture_replay_runner as snapshot_runner
from tools import local_db_source_candidate_replay_runner as source_candidate_runner


SCHEMA_VERSION = "local_db_restricted_github_read_canary_v1"
GITHUB_API_BASE_URL = "https://api.github.com"
SNAPSHOT_UPDATED_EVENT_TYPE = "artifact.snapshot.updated.v1"
ENRICHMENT_RUN_DEDUPE_PREFIX = "local-db-restricted-github-read"
MAX_HTTP_RESPONSE_BYTES = 1_000_000
README_EXCERPT_MAX_CHARS = 4096
HTTP_TIMEOUT_SECONDS = 10.0
REPO_FULL_NAME_RE = re.compile(
    r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,99}/[A-Za-z0-9][A-Za-z0-9_.-]{0,99}$"
)
PROOF_TRUE_KEYS = (
    "database_url_guard_passed",
    "network_read_authorized",
    "github_api_base_url_allowed",
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
    "github_http_get_called",
)
FALSE_RESULT_KEYS = (
    "github_write_called",
    "telegram_called",
    "openai_called",
    "workers_started",
    "redis_mutation",
    "production_db_write",
    "alembic_or_ddl_ran",
)


@dataclass(frozen=True, slots=True)
class GitHubHttpResponse:
    status_code: int
    json_payload: Mapping[str, Any] | None = None
    headers: Mapping[str, str] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class ArtifactPreflightResult:
    source_candidate_replay_confirmed: bool
    candidate_group_loaded: bool
    github_artifact_loaded: bool
    artifact_matches_requested_repo: bool
    enrich_requested_event_found: bool
    artifact_id: UUID | None
    candidate_group_id: UUID | None
    artifact_canonical_id: str | None
    artifact_type: str | None
    checks_failed: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class GitHubFetchResult:
    snapshot_plan: snapshot_runner.GitHubSnapshotFixture | None
    github_repo_metadata_fetched: bool
    github_default_branch_commit_fetched: bool
    github_readme_fetched: bool
    github_http_get_called: bool
    live_github_read_called: bool
    github_write_called: bool
    checks_failed: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class SnapshotWriteResult:
    artifact_enrichment_run_created: bool
    artifact_snapshot_created: bool
    github_repo_child_snapshot_created: bool
    github_readme_file_sample_created: bool
    artifact_current_snapshot_updated: bool
    artifact_snapshot_updated_event_created: bool
    checks_failed: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class RunnerResult:
    exit_code: int
    report: dict[str, Any]


class GitHubHttpGet(Protocol):
    def __call__(self, url: str, *, timeout_seconds: float) -> GitHubHttpResponse: ...


class SourceCandidateReplayRunner(Protocol):
    def run(
        self,
        *,
        database_url: str,
        source_fixture_path: Path,
        replay_namespace: str,
        env: Mapping[str, str],
        repo_root: Path,
    ) -> source_candidate_runner.RunnerResult: ...


class RestrictedGitHubReadCanaryExecutor(Protocol):
    def load_predecessor_state(
        self,
        *,
        database_url: str,
        source_fixture: source_candidate_runner.SourceFixture,
        replay_namespace: str,
        expected_artifact_canonical_id: str,
    ) -> ArtifactPreflightResult: ...

    def write_snapshot(
        self,
        *,
        database_url: str,
        artifact_id: UUID,
        candidate_group_id: UUID,
        snapshot_plan: snapshot_runner.GitHubSnapshotFixture,
        replay_namespace: str,
    ) -> SnapshotWriteResult: ...


class DefaultSourceCandidateReplayRunner:
    def run(
        self,
        *,
        database_url: str,
        source_fixture_path: Path,
        replay_namespace: str,
        env: Mapping[str, str],
        repo_root: Path,
    ) -> source_candidate_runner.RunnerResult:
        args = argparse.Namespace(
            database_url=database_url,
            fixture=str(source_fixture_path),
            replay_namespace=replay_namespace,
            confirm_local_test_db=True,
        )
        return source_candidate_runner.run(args, env=env, repo_root=repo_root)


class SqlAlchemyRestrictedGitHubReadCanaryExecutor:
    def load_predecessor_state(
        self,
        *,
        database_url: str,
        source_fixture: source_candidate_runner.SourceFixture,
        replay_namespace: str,
        expected_artifact_canonical_id: str,
    ) -> ArtifactPreflightResult:
        snapshot_runner._bootstrap_repo_imports()
        import sqlalchemy as sa

        engine = sa.create_engine(database_url, future=True)
        try:
            with engine.connect() as connection:
                return _load_predecessor_state(
                    connection,
                    source_fixture=source_fixture,
                    replay_namespace=replay_namespace,
                    expected_artifact_canonical_id=expected_artifact_canonical_id,
                )
        finally:
            engine.dispose()

    def write_snapshot(
        self,
        *,
        database_url: str,
        artifact_id: UUID,
        candidate_group_id: UUID,
        snapshot_plan: snapshot_runner.GitHubSnapshotFixture,
        replay_namespace: str,
    ) -> SnapshotWriteResult:
        snapshot_runner._bootstrap_repo_imports()
        import sqlalchemy as sa

        engine = sa.create_engine(database_url, future=True)
        try:
            with engine.begin() as connection:
                return _write_snapshot_rows(
                    connection,
                    artifact_id=artifact_id,
                    candidate_group_id=candidate_group_id,
                    snapshot_plan=snapshot_plan,
                    replay_namespace=replay_namespace,
                )
        finally:
            engine.dispose()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Run a one-shot local/test DB restricted public GitHub REST read canary "
            "and persist the resulting artifact snapshot."
        )
    )
    parser.add_argument("--database-url", required=True)
    parser.add_argument("--source-fixture", required=True)
    parser.add_argument("--replay-namespace", required=True)
    parser.add_argument("--repo-full-name", required=True)
    parser.add_argument("--github-api-base-url", default=GITHUB_API_BASE_URL)
    parser.add_argument("--confirm-local-test-db", action="store_true")
    parser.add_argument("--allow-network-read", action="store_true")
    return parser


def render_json(report: Mapping[str, Any]) -> str:
    return json.dumps(report, indent=2) + "\n"


def run(
    args: argparse.Namespace,
    *,
    env: Mapping[str, str] | None = None,
    http_get: GitHubHttpGet | None = None,
    executor: RestrictedGitHubReadCanaryExecutor | None = None,
    source_replay_runner: SourceCandidateReplayRunner | None = None,
    repo_root: Path | None = None,
) -> RunnerResult:
    effective_env = env if env is not None else os.environ
    root = repo_root or _repo_root()
    report = _base_report(args.repo_full_name)
    checks_failed: list[str] = []

    if not args.confirm_local_test_db:
        checks_failed.append("confirm_local_test_db_required")

    if effective_env.get("APP_ENV", "").strip().lower() != "test":
        checks_failed.append("app_env_test_required")

    if args.allow_network_read:
        report["network_read_authorized"] = True
    else:
        checks_failed.append("allow_network_read_required")

    base_url_ok, base_url_failures = validate_github_api_base_url(args.github_api_base_url)
    report["github_api_base_url_allowed"] = base_url_ok
    checks_failed.extend(base_url_failures)

    repo_ok, repo_failures = validate_repo_full_name(args.repo_full_name)
    checks_failed.extend(repo_failures)

    namespace_ok, namespace_failures = source_candidate_runner.validate_replay_namespace(args.replay_namespace)
    checks_failed.extend(namespace_failures)

    database_ok, database_failures, _ = source_candidate_runner.validate_database_url(args.database_url)
    report["database_url_guard_passed"] = database_ok
    checks_failed.extend(database_failures)

    source_fixture: source_candidate_runner.SourceFixture | None = None
    try:
        source_fixture = source_candidate_runner.load_source_fixture(Path(args.source_fixture), repo_root=root)
    except Exception:  # noqa: BLE001 - operator output must stay sanitized.
        checks_failed.append("source_fixture_load_failed")

    if checks_failed:
        return _finish(report, checks_failed)

    if source_fixture is None or not namespace_ok or not repo_ok:
        checks_failed.append("precondition_missing")
        return _finish(report, checks_failed)

    active_source_runner = source_replay_runner or DefaultSourceCandidateReplayRunner()
    try:
        source_result = active_source_runner.run(
            database_url=args.database_url,
            source_fixture_path=Path(args.source_fixture),
            replay_namespace=args.replay_namespace,
            env=effective_env,
            repo_root=root,
        )
    except Exception:  # noqa: BLE001 - never echo DB or runtime errors.
        checks_failed.append("source_candidate_replay_failed")
        return _finish(report, checks_failed)
    if source_result.exit_code != 0 or source_result.report.get("status") != "pass":
        checks_failed.append("source_candidate_replay_failed")
        return _finish(report, checks_failed)

    expected_canonical_id = build_expected_artifact_canonical_id(args.repo_full_name)
    active_executor = executor or SqlAlchemyRestrictedGitHubReadCanaryExecutor()
    try:
        preflight = active_executor.load_predecessor_state(
            database_url=args.database_url,
            source_fixture=source_fixture,
            replay_namespace=args.replay_namespace,
            expected_artifact_canonical_id=expected_canonical_id,
        )
    except Exception as exc:  # noqa: BLE001 - keep failure structured and sanitized.
        checks_failed.append(_safe_failure_code(exc))
        return _finish(report, checks_failed)

    report.update(
        {
            "source_candidate_replay_confirmed": preflight.source_candidate_replay_confirmed,
            "candidate_group_loaded": preflight.candidate_group_loaded,
            "github_artifact_loaded": preflight.github_artifact_loaded,
            "artifact_matches_requested_repo": preflight.artifact_matches_requested_repo,
        }
    )
    checks_failed.extend(preflight.checks_failed)
    if checks_failed:
        return _finish(report, checks_failed)
    if preflight.artifact_id is None or preflight.candidate_group_id is None:
        checks_failed.append("predecessor_state_missing")
        return _finish(report, checks_failed)

    try:
        fetch_result = fetch_github_snapshot_plan(
            repo_full_name=args.repo_full_name,
            artifact_canonical_id=expected_canonical_id,
            github_api_base_url=args.github_api_base_url,
            http_get=http_get or live_github_http_get,
            live_http=http_get is None,
        )
    except Exception as exc:  # noqa: BLE001 - expected errors are converted below.
        checks_failed.append(_safe_failure_code(exc))
        return _finish(report, checks_failed)

    report.update(
        {
            "github_repo_metadata_fetched": fetch_result.github_repo_metadata_fetched,
            "github_default_branch_commit_fetched": fetch_result.github_default_branch_commit_fetched,
            "github_readme_fetched": fetch_result.github_readme_fetched,
            "github_http_get_called": fetch_result.github_http_get_called,
            "live_github_read_called": fetch_result.live_github_read_called,
            "github_write_called": fetch_result.github_write_called,
        }
    )
    checks_failed.extend(fetch_result.checks_failed)
    if fetch_result.snapshot_plan is None:
        checks_failed.append("github_snapshot_plan_missing")
    if checks_failed:
        return _finish(report, checks_failed)

    try:
        write_result = active_executor.write_snapshot(
            database_url=args.database_url,
            artifact_id=preflight.artifact_id,
            candidate_group_id=preflight.candidate_group_id,
            snapshot_plan=fetch_result.snapshot_plan,
            replay_namespace=args.replay_namespace,
        )
    except Exception as exc:  # noqa: BLE001 - never echo DB errors.
        checks_failed.append(_safe_failure_code(exc))
        return _finish(report, checks_failed)

    report.update(
        {
            "artifact_enrichment_run_created": write_result.artifact_enrichment_run_created,
            "artifact_snapshot_created": write_result.artifact_snapshot_created,
            "github_repo_child_snapshot_created": write_result.github_repo_child_snapshot_created,
            "github_readme_file_sample_created": write_result.github_readme_file_sample_created,
            "artifact_current_snapshot_updated": write_result.artifact_current_snapshot_updated,
            "artifact_snapshot_updated_event_created": write_result.artifact_snapshot_updated_event_created,
        }
    )
    checks_failed.extend(write_result.checks_failed)
    checks_failed.extend(_proof_flag_failures(report))
    return _finish(report, checks_failed)


def validate_github_api_base_url(value: str | None) -> tuple[bool, list[str]]:
    if value != GITHUB_API_BASE_URL:
        return False, ["github_api_base_url_not_allowed"]
    parsed = urlsplit(value)
    if parsed.scheme != "https" or parsed.netloc != "api.github.com" or parsed.path:
        return False, ["github_api_base_url_not_allowed"]
    return True, []


def validate_repo_full_name(value: str | None) -> tuple[bool, list[str]]:
    repo = (value or "").strip()
    if not repo:
        return False, ["repo_full_name_required"]
    if not REPO_FULL_NAME_RE.fullmatch(repo):
        return False, ["repo_full_name_invalid"]
    owner, name = repo.split("/", 1)
    if ".." in owner or ".." in name:
        return False, ["repo_full_name_invalid"]
    return True, []


def build_expected_artifact_canonical_id(repo_full_name: str) -> str:
    return f"github:repo:{repo_full_name.lower()}"


def fetch_github_snapshot_plan(
    *,
    repo_full_name: str,
    artifact_canonical_id: str,
    github_api_base_url: str,
    http_get: GitHubHttpGet,
    live_http: bool,
) -> GitHubFetchResult:
    owner, repo = repo_full_name.split("/", 1)
    repo_url = _github_api_url(github_api_base_url, "repos", owner, repo)
    allowed_urls = {repo_url}
    http_get_called = False
    live_read_called = False

    repo_response = _call_allowed_get(
        http_get,
        repo_url,
        allowed_urls=allowed_urls,
    )
    http_get_called = True
    live_read_called = live_http
    if repo_response.status_code != 200:
        status = _snapshot_status_from_http(repo_response.status_code)
        return GitHubFetchResult(
            snapshot_plan=None,
            github_repo_metadata_fetched=False,
            github_default_branch_commit_fetched=False,
            github_readme_fetched=False,
            github_http_get_called=http_get_called,
            live_github_read_called=live_read_called,
            github_write_called=False,
            checks_failed=(f"github_repo_metadata_fetch_{status}",),
        )
    repo_json = _response_json_object(repo_response)
    if repo_json is None:
        return GitHubFetchResult(
            snapshot_plan=None,
            github_repo_metadata_fetched=False,
            github_default_branch_commit_fetched=False,
            github_readme_fetched=False,
            github_http_get_called=http_get_called,
            live_github_read_called=live_read_called,
            github_write_called=False,
            checks_failed=("github_repo_metadata_json_invalid",),
        )

    default_branch = _optional_str(repo_json.get("default_branch"))
    if default_branch is None:
        return GitHubFetchResult(
            snapshot_plan=None,
            github_repo_metadata_fetched=True,
            github_default_branch_commit_fetched=False,
            github_readme_fetched=False,
            github_http_get_called=http_get_called,
            live_github_read_called=live_read_called,
            github_write_called=False,
            checks_failed=("github_default_branch_missing",),
        )

    commit_url = _github_api_url(github_api_base_url, "repos", owner, repo, "commits", default_branch)
    allowed_urls.add(commit_url)
    commit_response = _call_allowed_get(http_get, commit_url, allowed_urls=allowed_urls)
    if commit_response.status_code != 200:
        status = _snapshot_status_from_http(commit_response.status_code)
        return GitHubFetchResult(
            snapshot_plan=None,
            github_repo_metadata_fetched=True,
            github_default_branch_commit_fetched=False,
            github_readme_fetched=False,
            github_http_get_called=True,
            live_github_read_called=live_read_called,
            github_write_called=False,
            checks_failed=(f"github_default_branch_commit_fetch_{status}",),
        )
    commit_json = _response_json_object(commit_response)
    commit_sha = _optional_str(commit_json.get("sha")) if commit_json else None
    if commit_sha is None:
        return GitHubFetchResult(
            snapshot_plan=None,
            github_repo_metadata_fetched=True,
            github_default_branch_commit_fetched=False,
            github_readme_fetched=False,
            github_http_get_called=True,
            live_github_read_called=live_read_called,
            github_write_called=False,
            checks_failed=("github_default_branch_commit_sha_missing",),
        )

    readme_url = _github_api_url(github_api_base_url, "repos", owner, repo, "readme")
    allowed_urls.add(readme_url)
    readme_response = _call_allowed_get(http_get, readme_url, allowed_urls=allowed_urls)
    readme_sample, readme_excerpt, readme_anomaly = _readme_sample_from_response(
        readme_response,
        repo_full_name=repo_full_name,
    )
    github_readme_fetched = readme_response.status_code == 200 and readme_sample is not None
    status = "ready" if github_readme_fetched else "partial_ready"
    fetch_anomalies = [readme_anomaly] if readme_anomaly else []
    snapshot_plan = _build_snapshot_plan(
        artifact_canonical_id=artifact_canonical_id,
        requested_repo_full_name=repo_full_name,
        repo_json=repo_json,
        default_branch=default_branch,
        commit_sha=commit_sha,
        status=status,
        readme_excerpt=readme_excerpt,
        file_samples=(readme_sample,) if readme_sample is not None else (),
        fetch_anomalies=fetch_anomalies,
    )
    return GitHubFetchResult(
        snapshot_plan=snapshot_plan,
        github_repo_metadata_fetched=True,
        github_default_branch_commit_fetched=True,
        github_readme_fetched=github_readme_fetched,
        github_http_get_called=True,
        live_github_read_called=live_read_called,
        github_write_called=False,
    )


def live_github_http_get(url: str, *, timeout_seconds: float) -> GitHubHttpResponse:
    request = urllib.request.Request(
        url,
        headers={
            "Accept": "application/vnd.github+json",
            "User-Agent": "github-ai-catchbot-restricted-read-canary",
        },
        method="GET",
    )
    opener = urllib.request.build_opener(_NoRedirectHandler)
    try:
        with opener.open(request, timeout=timeout_seconds) as response:
            raw = response.read(MAX_HTTP_RESPONSE_BYTES + 1)
            if len(raw) > MAX_HTTP_RESPONSE_BYTES:
                return GitHubHttpResponse(status_code=599)
            return GitHubHttpResponse(
                status_code=int(response.getcode()),
                json_payload=_loads_json_bytes(raw),
            )
    except urllib.error.HTTPError as exc:
        raw = exc.read(MAX_HTTP_RESPONSE_BYTES + 1)
        payload = None if len(raw) > MAX_HTTP_RESPONSE_BYTES else _loads_json_bytes(raw)
        return GitHubHttpResponse(status_code=int(exc.code), json_payload=payload)
    except urllib.error.URLError:
        return GitHubHttpResponse(status_code=599)


class _NoRedirectHandler(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):  # noqa: ANN001
        return None


def build_artifact_enrichment_run_dedupe_key(
    *,
    replay_namespace: str,
    artifact_id: UUID,
    content_anchor: str,
) -> str:
    return (
        f"{ENRICHMENT_RUN_DEDUPE_PREFIX}:{replay_namespace}:"
        f"artifact.enrichment:{artifact_id}:{content_anchor}"
    )


def build_snapshot_updated_dedupe_key(
    *,
    replay_namespace: str,
    artifact_id: UUID,
    snapshot_id: UUID,
) -> str:
    return (
        f"{ENRICHMENT_RUN_DEDUPE_PREFIX}:{replay_namespace}:"
        f"artifact.snapshot.updated:{artifact_id}:{snapshot_id}"
    )


def _github_api_url(base_url: str, *parts: str) -> str:
    encoded = "/".join(quote(part, safe="") for part in parts)
    return f"{base_url}/{encoded}"


def _call_allowed_get(
    http_get: GitHubHttpGet,
    url: str,
    *,
    allowed_urls: set[str],
) -> GitHubHttpResponse:
    if url not in allowed_urls:
        raise RuntimeError("github_http_url_not_allowed")
    response = http_get(url, timeout_seconds=HTTP_TIMEOUT_SECONDS)
    if not isinstance(response.status_code, int):
        raise RuntimeError("github_http_status_invalid")
    return response


def _loads_json_bytes(raw: bytes) -> Mapping[str, Any] | None:
    try:
        payload = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        return None
    return payload if isinstance(payload, Mapping) else None


def _response_json_object(response: GitHubHttpResponse) -> Mapping[str, Any] | None:
    payload = response.json_payload
    return payload if isinstance(payload, Mapping) else None


def _readme_sample_from_response(
    response: GitHubHttpResponse,
    *,
    repo_full_name: str,
) -> tuple[snapshot_runner.GitHubFileSampleFixture | None, str | None, str | None]:
    if response.status_code != 200:
        return None, None, f"github_readme_fetch_{_snapshot_status_from_http(response.status_code)}"
    payload = _response_json_object(response)
    if payload is None:
        return None, None, "github_readme_json_invalid"
    content = _optional_str(payload.get("content"))
    encoding = _optional_str(payload.get("encoding"))
    if content is None or encoding != "base64":
        return None, None, "github_readme_content_missing"
    try:
        decoded = base64.b64decode(content.encode("ascii"), validate=False)
    except Exception:  # noqa: BLE001 - payload content is not operator-facing.
        return None, None, "github_readme_content_decode_failed"
    text = decoded.decode("utf-8", errors="replace")
    excerpt = _cap_text(text, README_EXCERPT_MAX_CHARS)
    if not excerpt.strip():
        return None, None, "github_readme_excerpt_empty"
    path = _optional_str(payload.get("path")) or "README.md"
    sample = snapshot_runner.GitHubFileSampleFixture(
        path=path,
        role="README",
        size_bytes=_optional_int(payload.get("size")),
        content_hash=f"sha256:{hashlib.sha256(decoded).hexdigest()}",
        excerpt=excerpt,
        raw_blob_ref=None,
    )
    _ = repo_full_name
    return sample, excerpt, None


def _build_snapshot_plan(
    *,
    artifact_canonical_id: str,
    requested_repo_full_name: str,
    repo_json: Mapping[str, Any],
    default_branch: str,
    commit_sha: str,
    status: str,
    readme_excerpt: str | None,
    file_samples: tuple[snapshot_runner.GitHubFileSampleFixture, ...],
    fetch_anomalies: list[str],
) -> snapshot_runner.GitHubSnapshotFixture:
    repo_full_name = _optional_str(repo_json.get("full_name")) or requested_repo_full_name
    language = _optional_str(repo_json.get("language"))
    normalized_projection = {
        "artifact_type": "github_repo",
        "description": _optional_str(repo_json.get("description")),
        "focus_kind": "repo",
        "focus_path": None,
        "forks": _optional_int(repo_json.get("forks_count")),
        "open_issues": _optional_int(repo_json.get("open_issues_count")),
        "page_path": None,
        "pushed_at": _optional_str(repo_json.get("pushed_at")),
        "repo_homepage": _optional_str(repo_json.get("homepage")),
        "stars": _optional_int(repo_json.get("stargazers_count")),
        "tree_truncated": False,
        "watchers": _optional_int(repo_json.get("watchers_count")),
    }
    return snapshot_runner.GitHubSnapshotFixture(
        artifact_canonical_id=artifact_canonical_id,
        artifact_type="github_repo",
        provider="github",
        snapshot_type="github_repo",
        status=status,
        content_anchor=f"commit:{commit_sha}",
        auth_mode="anonymous_degraded",
        normalized_projection=normalized_projection,
        raw_payload_ref=None,
        evidence_limitations=[
            "restricted public GitHub REST read canary",
            "GET-only unauthenticated API reads",
            "README excerpt capped",
        ],
        fetch_anomalies=fetch_anomalies,
        repo_full_name=repo_full_name,
        default_branch=default_branch,
        resolved_ref=commit_sha,
        content_anchor_commit_sha=commit_sha,
        repo_flags_json={
            "archived": bool(repo_json.get("archived")),
            "fork": bool(repo_json.get("fork")),
            "template": bool(repo_json.get("is_template")),
        },
        license_spdx=_license_spdx(repo_json.get("license")),
        topics_json=_str_list_or_none(repo_json.get("topics")),
        readme_excerpt=readme_excerpt,
        detected_build_systems_json=None,
        detected_languages_json=[language] if language else None,
        key_paths_json=["README.md"] if readme_excerpt else None,
        test_paths_json=None,
        ci_paths_json=None,
        examples_paths_json=None,
        docs_paths_json=None,
        release_summary_json=None,
        file_samples=file_samples,
    )


def _load_predecessor_state(
    connection: Any,
    *,
    source_fixture: source_candidate_runner.SourceFixture,
    replay_namespace: str,
    expected_artifact_canonical_id: str,
) -> ArtifactPreflightResult:
    import sqlalchemy as sa

    failures: list[str] = []
    normalizer_version = source_candidate_runner.build_normalizer_version(replay_namespace)
    source_message_found = snapshot_runner._exists(
        connection,
        """
        SELECT 1
        FROM source_messages
        WHERE source_message_id = CAST(:source_message_id AS uuid)
          AND current_version_no = :source_version_no
        """,
        {
            "source_message_id": str(source_fixture.source_message_id),
            "source_version_no": source_fixture.source_version_no,
        },
    )
    source_version_found = snapshot_runner._exists(
        connection,
        """
        SELECT 1
        FROM source_message_versions
        WHERE source_message_id = CAST(:source_message_id AS uuid)
          AND version_no = :source_version_no
        """,
        {
            "source_message_id": str(source_fixture.source_message_id),
            "source_version_no": source_fixture.source_version_no,
        },
    )
    candidate_row = connection.execute(
        sa.text(
            """
            SELECT
              cgp.candidate_group_id,
              cgp.dedupe_subject_key,
              ar.artifact_id,
              ar.artifact_type,
              ar.canonical_id
            FROM candidate_group_proposals AS cgp
            JOIN artifact_registry AS ar
              ON ar.artifact_id = cgp.current_primary_artifact_id
            WHERE cgp.source_message_id = CAST(:source_message_id AS uuid)
              AND cgp.source_version_no = :source_version_no
              AND cgp.normalizer_version = :normalizer_version
            ORDER BY
              CASE WHEN cgp.dedupe_subject_key = :expected_canonical_id THEN 0 ELSE 1 END,
              cgp.updated_at DESC,
              cgp.candidate_group_id DESC
            LIMIT 1
            """
        ),
        {
            "source_message_id": str(source_fixture.source_message_id),
            "source_version_no": source_fixture.source_version_no,
            "normalizer_version": normalizer_version,
            "expected_canonical_id": expected_artifact_canonical_id,
        },
    ).mappings().first()

    candidate_group_id = UUID(str(candidate_row["candidate_group_id"])) if candidate_row else None
    artifact_id = UUID(str(candidate_row["artifact_id"])) if candidate_row else None
    artifact_type = str(candidate_row["artifact_type"]) if candidate_row else None
    artifact_canonical_id = str(candidate_row["canonical_id"]) if candidate_row else None
    candidate_group_loaded = candidate_group_id is not None
    github_artifact_loaded = artifact_type == "github_repo" and artifact_id is not None
    artifact_matches_requested_repo = artifact_canonical_id == expected_artifact_canonical_id

    candidate_member_found = False
    if candidate_group_id is not None and artifact_id is not None:
        candidate_member_found = snapshot_runner._exists(
            connection,
            """
            SELECT 1
            FROM candidate_group_members
            WHERE candidate_group_id = CAST(:candidate_group_id AS uuid)
              AND artifact_id = CAST(:artifact_id AS uuid)
              AND member_role = 'primary'
            """,
            {
                "candidate_group_id": str(candidate_group_id),
                "artifact_id": str(artifact_id),
            },
        )

    enrich_event_found = False
    if artifact_id is not None:
        enrich_event_found = snapshot_runner._exists(
            connection,
            """
            SELECT 1
            FROM event_outbox
            WHERE event_type = :event_type
              AND aggregate_type = 'artifact'
              AND aggregate_id = CAST(:artifact_id AS uuid)
              AND dedupe_key LIKE :dedupe_prefix
            """,
            {
                "event_type": source_candidate_runner.ENRICH_EVENT_TYPE,
                "artifact_id": str(artifact_id),
                "dedupe_prefix": f"local-db-source-candidate:{replay_namespace}:artifact.enrich:%",
            },
        )

    if not source_message_found:
        failures.append("source_message_missing")
    if not source_version_found:
        failures.append("source_message_version_missing")
    if not candidate_group_loaded:
        failures.append("candidate_group_missing")
    if not github_artifact_loaded:
        failures.append("github_artifact_missing")
    if not artifact_matches_requested_repo:
        failures.append("artifact_repo_mismatch")
    if not candidate_member_found:
        failures.append("candidate_group_member_missing")
    if not enrich_event_found:
        failures.append("artifact_enrich_requested_event_missing")

    return ArtifactPreflightResult(
        source_candidate_replay_confirmed=source_message_found and source_version_found,
        candidate_group_loaded=candidate_group_loaded,
        github_artifact_loaded=github_artifact_loaded,
        artifact_matches_requested_repo=artifact_matches_requested_repo,
        enrich_requested_event_found=enrich_event_found,
        artifact_id=artifact_id,
        candidate_group_id=candidate_group_id,
        artifact_canonical_id=artifact_canonical_id,
        artifact_type=artifact_type,
        checks_failed=tuple(dict.fromkeys(failures)),
    )


def _write_snapshot_rows(
    connection: Any,
    *,
    artifact_id: UUID,
    candidate_group_id: UUID,
    snapshot_plan: snapshot_runner.GitHubSnapshotFixture,
    replay_namespace: str,
) -> SnapshotWriteResult:
    _insert_or_reuse_artifact_enrichment_run(
        connection,
        artifact_id=artifact_id,
        status=snapshot_plan.status,
        content_anchor=snapshot_plan.content_anchor,
        replay_namespace=replay_namespace,
    )
    snapshot_id = snapshot_runner._insert_or_reuse_artifact_snapshot(
        connection,
        artifact_id=artifact_id,
        github_fixture=snapshot_plan,
    )
    snapshot_runner._insert_or_reuse_github_repo_snapshot(
        connection,
        snapshot_id=snapshot_id,
        github_fixture=snapshot_plan,
    )
    for sample in snapshot_plan.file_samples:
        snapshot_runner._insert_or_reuse_github_file_sample(
            connection,
            snapshot_id=snapshot_id,
            sample=sample,
        )
    snapshot_runner._update_artifact_current_snapshot(
        connection,
        artifact_id=artifact_id,
        snapshot_id=snapshot_id,
        status=snapshot_plan.status,
    )
    _insert_or_reuse_snapshot_updated_outbox(
        connection,
        artifact_id=artifact_id,
        snapshot_id=snapshot_id,
        status=snapshot_plan.status,
        content_anchor=snapshot_plan.content_anchor,
        replay_namespace=replay_namespace,
    )
    return _verify_snapshot_write(
        connection,
        artifact_id=artifact_id,
        candidate_group_id=candidate_group_id,
        snapshot_id=snapshot_id,
        snapshot_plan=snapshot_plan,
        replay_namespace=replay_namespace,
    )


def _insert_or_reuse_artifact_enrichment_run(
    connection: Any,
    *,
    artifact_id: UUID,
    status: str,
    content_anchor: str,
    replay_namespace: str,
) -> UUID:
    import sqlalchemy as sa

    result = connection.execute(
        sa.text(
            """
            INSERT INTO artifact_enrichment_runs (
                artifact_id,
                provider,
                refresh_mode,
                depth_budget,
                status,
                content_anchor,
                job_idempotency_key,
                started_at,
                finished_at
            )
            VALUES (
                CAST(:artifact_id AS uuid),
                'github',
                'restricted_read_canary',
                1,
                CAST(:status AS snapshot_status_enum),
                :content_anchor,
                :job_idempotency_key,
                now(),
                now()
            )
            ON CONFLICT ON CONSTRAINT uq_enrich_runs_job_idempotency_key
            DO UPDATE SET
                status = EXCLUDED.status,
                content_anchor = EXCLUDED.content_anchor
            RETURNING artifact_enrichment_run_id
            """
        ),
        {
            "artifact_id": str(artifact_id),
            "status": status,
            "content_anchor": content_anchor,
            "job_idempotency_key": build_artifact_enrichment_run_dedupe_key(
                replay_namespace=replay_namespace,
                artifact_id=artifact_id,
                content_anchor=content_anchor,
            ),
        },
    )
    return UUID(str(result.scalar_one()))


def _insert_or_reuse_snapshot_updated_outbox(
    connection: Any,
    *,
    artifact_id: UUID,
    snapshot_id: UUID,
    status: str,
    content_anchor: str,
    replay_namespace: str,
) -> None:
    import sqlalchemy as sa

    payload = {
        "artifact_id": str(artifact_id),
        "snapshot_id": str(snapshot_id),
        "provider": "github",
        "status": status,
        "content_anchor": content_anchor,
    }
    connection.execute(
        sa.text(
            """
            INSERT INTO event_outbox (
                event_type,
                aggregate_type,
                aggregate_id,
                dedupe_key,
                payload_json,
                status,
                created_at
            )
            VALUES (
                :event_type,
                'artifact',
                CAST(:artifact_id AS uuid),
                :dedupe_key,
                CAST(:payload_json AS jsonb),
                'pending'::outbox_status_enum,
                now()
            )
            ON CONFLICT (dedupe_key) DO NOTHING
            """
        ),
        {
            "event_type": SNAPSHOT_UPDATED_EVENT_TYPE,
            "artifact_id": str(artifact_id),
            "dedupe_key": build_snapshot_updated_dedupe_key(
                replay_namespace=replay_namespace,
                artifact_id=artifact_id,
                snapshot_id=snapshot_id,
            ),
            "payload_json": snapshot_runner._json_dumps(payload),
        },
    )


def _verify_snapshot_write(
    connection: Any,
    *,
    artifact_id: UUID,
    candidate_group_id: UUID,
    snapshot_id: UUID,
    snapshot_plan: snapshot_runner.GitHubSnapshotFixture,
    replay_namespace: str,
) -> SnapshotWriteResult:
    checks_failed: list[str] = []
    artifact_enrichment_run = snapshot_runner._exists(
        connection,
        """
        SELECT 1
        FROM artifact_enrichment_runs
        WHERE artifact_id = CAST(:artifact_id AS uuid)
          AND provider = 'github'
          AND refresh_mode = 'restricted_read_canary'
          AND content_anchor = :content_anchor
          AND job_idempotency_key = :job_idempotency_key
        """,
        {
            "artifact_id": str(artifact_id),
            "content_anchor": snapshot_plan.content_anchor,
            "job_idempotency_key": build_artifact_enrichment_run_dedupe_key(
                replay_namespace=replay_namespace,
                artifact_id=artifact_id,
                content_anchor=snapshot_plan.content_anchor,
            ),
        },
    )
    artifact_snapshot = snapshot_runner._exists(
        connection,
        """
        SELECT 1
        FROM artifact_snapshots
        WHERE snapshot_id = CAST(:snapshot_id AS uuid)
          AND artifact_id = CAST(:artifact_id AS uuid)
          AND provider = 'github'
          AND snapshot_type = :snapshot_type
          AND status = CAST(:status AS snapshot_status_enum)
          AND content_anchor = :content_anchor
        """,
        {
            "snapshot_id": str(snapshot_id),
            "artifact_id": str(artifact_id),
            "snapshot_type": snapshot_plan.snapshot_type,
            "status": snapshot_plan.status,
            "content_anchor": snapshot_plan.content_anchor,
        },
    )
    repo_child = snapshot_runner._exists(
        connection,
        """
        SELECT 1
        FROM artifact_snapshot_github_repo
        WHERE snapshot_id = CAST(:snapshot_id AS uuid)
          AND repo_full_name = :repo_full_name
        """,
        {"snapshot_id": str(snapshot_id), "repo_full_name": snapshot_plan.repo_full_name},
    )
    readme_sample = False
    if snapshot_plan.file_samples:
        readme_sample = snapshot_runner._exists(
            connection,
            """
            SELECT 1
            FROM artifact_snapshot_github_file_samples
            WHERE snapshot_id = CAST(:snapshot_id AS uuid)
              AND path = :path
              AND role = 'README'
            """,
            {
                "snapshot_id": str(snapshot_id),
                "path": snapshot_plan.file_samples[0].path,
            },
        )
    current_snapshot = snapshot_runner._exists(
        connection,
        """
        SELECT 1
        FROM artifact_registry
        WHERE artifact_id = CAST(:artifact_id AS uuid)
          AND current_snapshot_id = CAST(:snapshot_id AS uuid)
          AND current_status = CAST(:status AS snapshot_status_enum)
        """,
        {"artifact_id": str(artifact_id), "snapshot_id": str(snapshot_id), "status": snapshot_plan.status},
    )
    snapshot_outbox = snapshot_runner._exists(
        connection,
        """
        SELECT 1
        FROM event_outbox
        WHERE event_type = :event_type
          AND aggregate_type = 'artifact'
          AND aggregate_id = CAST(:artifact_id AS uuid)
          AND dedupe_key = :dedupe_key
        """,
        {
            "event_type": SNAPSHOT_UPDATED_EVENT_TYPE,
            "artifact_id": str(artifact_id),
            "dedupe_key": build_snapshot_updated_dedupe_key(
                replay_namespace=replay_namespace,
                artifact_id=artifact_id,
                snapshot_id=snapshot_id,
            ),
        },
    )
    downstream_created = snapshot_runner._exists(
        connection,
        """
        SELECT 1
        FROM candidate_evidence_bundles
        WHERE candidate_group_id = CAST(:candidate_group_id AS uuid)
        UNION ALL
        SELECT 1
        FROM event_outbox
        WHERE event_type IN (
            'analysis.requested.v1',
            'notification.plan.created.v1',
            'notification.delivery.result.v1'
        )
          AND dedupe_key LIKE :dedupe_prefix
        LIMIT 1
        """,
        {
            "candidate_group_id": str(candidate_group_id),
            "dedupe_prefix": f"{ENRICHMENT_RUN_DEDUPE_PREFIX}:{replay_namespace}:%",
        },
    )
    checks = {
        "artifact_enrichment_run_created": artifact_enrichment_run,
        "artifact_snapshot_created": artifact_snapshot,
        "github_repo_child_snapshot_created": repo_child,
        "github_readme_file_sample_created": readme_sample,
        "artifact_current_snapshot_updated": current_snapshot,
        "artifact_snapshot_updated_event_created": snapshot_outbox,
    }
    for key, value in checks.items():
        if value is not True:
            checks_failed.append(f"{key}:missing")
    if downstream_created:
        checks_failed.append("downstream_side_effect:unexpected")
    return SnapshotWriteResult(
        artifact_enrichment_run_created=artifact_enrichment_run,
        artifact_snapshot_created=artifact_snapshot,
        github_repo_child_snapshot_created=repo_child,
        github_readme_file_sample_created=readme_sample,
        artifact_current_snapshot_updated=current_snapshot,
        artifact_snapshot_updated_event_created=snapshot_outbox,
        checks_failed=tuple(dict.fromkeys(checks_failed)),
    )


def _snapshot_status_from_http(status_code: int) -> str:
    if status_code == 429:
        return "rate_limited"
    if status_code in {401, 403, 404}:
        return "access_denied"
    if status_code >= 500 or status_code == 599:
        return "failed_transient"
    return "failed_permanent"


def _license_spdx(value: Any) -> str | None:
    if isinstance(value, Mapping):
        return _optional_str(value.get("spdx_id"))
    return None


def _optional_str(value: Any) -> str | None:
    if isinstance(value, str) and value.strip():
        return value.strip()
    return None


def _optional_int(value: Any) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _str_list_or_none(value: Any) -> list[str] | None:
    if not isinstance(value, list):
        return None
    values = [str(item) for item in value if isinstance(item, str) and item.strip()]
    return values or None


def _cap_text(value: str, max_chars: int) -> str:
    normalized = value.replace("\x00", "").strip()
    if len(normalized) <= max_chars:
        return normalized
    return normalized[:max_chars]


def _proof_flag_failures(report: Mapping[str, Any]) -> list[str]:
    failures: list[str] = []
    for key in PROOF_TRUE_KEYS:
        if report.get(key) is not True:
            failures.append(f"{key}:missing")
    for key in FALSE_RESULT_KEYS:
        if report.get(key) is not False:
            failures.append(f"{key}:unexpected")
    return failures


def _base_report(repo_full_name: str) -> dict[str, Any]:
    return {
        "schema_version": SCHEMA_VERSION,
        "status": "pass",
        "database_url_guard_passed": False,
        "network_read_authorized": False,
        "github_api_base_url_allowed": False,
        "repo_full_name": repo_full_name,
        "source_candidate_replay_confirmed": False,
        "candidate_group_loaded": False,
        "github_artifact_loaded": False,
        "artifact_matches_requested_repo": False,
        "github_repo_metadata_fetched": False,
        "github_default_branch_commit_fetched": False,
        "github_readme_fetched": False,
        "artifact_enrichment_run_created": False,
        "artifact_snapshot_created": False,
        "github_repo_child_snapshot_created": False,
        "github_readme_file_sample_created": False,
        "artifact_current_snapshot_updated": False,
        "artifact_snapshot_updated_event_created": False,
        "live_github_read_called": False,
        "github_http_get_called": False,
        "github_write_called": False,
        "telegram_called": False,
        "openai_called": False,
        "workers_started": False,
        "redis_mutation": False,
        "production_db_write": False,
        "alembic_or_ddl_ran": False,
        "checks_failed": [],
    }


def _finish(report: dict[str, Any], checks_failed: Sequence[str]) -> RunnerResult:
    normalized_failures = list(dict.fromkeys(checks_failed))
    report["checks_failed"] = normalized_failures
    report["status"] = "fail" if normalized_failures else "pass"
    return RunnerResult(exit_code=0 if report["status"] == "pass" else 1, report=report)


def _safe_failure_code(exc: Exception) -> str:
    message = str(exc)
    safe_messages = {
        "github_http_url_not_allowed",
        "github_http_status_invalid",
        "predecessor_state_missing",
    }
    if message in safe_messages:
        return message
    return exc.__class__.__name__


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    result = run(args)
    sys.stdout.write(render_json(result.report))
    return result.exit_code


if __name__ == "__main__":
    raise SystemExit(main())
