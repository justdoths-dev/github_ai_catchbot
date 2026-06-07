from __future__ import annotations

import argparse
import json
import os
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Mapping, Protocol, Sequence
from uuid import UUID

from tools import local_db_source_candidate_replay_runner as source_candidate_runner


SCHEMA_VERSION = "local_db_github_snapshot_fixture_replay_v1"
FIXTURE_SCHEMA_VERSION = "github_repo_snapshot_fixture_v1"
ARTIFACT_CANONICAL_ID = "github:repo:example/example-tool"
ENRICH_EVENT_TYPE = "artifact.enrich.requested.v1"
SNAPSHOT_UPDATED_EVENT_TYPE = "artifact.snapshot.updated.v1"
SUPPORTED_GITHUB_ARTIFACT_TYPES = {
    "github_repo",
    "github_subpath",
    "github_repo_page",
    "github_gist",
}
TRUE_RESULT_KEYS = (
    "database_url_guard_passed",
    "source_candidate_replay_confirmed",
    "enrich_requested_event_found",
    "github_snapshot_fixture_loaded",
    "artifact_snapshot_created_or_reused",
    "github_repo_snapshot_created_or_reused",
    "github_file_samples_created_or_reused",
    "artifact_current_snapshot_updated",
    "snapshot_updated_outbox_event_created",
)
FALSE_RESULT_KEYS = (
    "production_db_write",
    "live_github_called",
    "live_telegram_called",
    "openai_called",
    "workers_started",
    "redis_mutation",
    "evidence_bundle_created",
    "analysis_requested_event_created",
    "notification_created",
)


@dataclass(frozen=True, slots=True)
class GitHubFileSampleFixture:
    path: str
    role: str
    size_bytes: int | None
    content_hash: str | None
    excerpt: str | None
    raw_blob_ref: str | None = None


@dataclass(frozen=True, slots=True)
class GitHubSnapshotFixture:
    artifact_canonical_id: str
    artifact_type: str
    provider: str
    snapshot_type: str
    status: str
    content_anchor: str
    auth_mode: str | None
    normalized_projection: dict[str, Any] | None
    raw_payload_ref: str | None
    evidence_limitations: list[str]
    fetch_anomalies: list[str]
    repo_full_name: str
    default_branch: str | None
    resolved_ref: str | None
    content_anchor_commit_sha: str | None
    repo_flags_json: dict[str, Any] | None
    license_spdx: str | None
    topics_json: list[str] | None
    readme_excerpt: str | None
    detected_build_systems_json: list[str] | None
    detected_languages_json: list[str] | None
    key_paths_json: list[str] | None
    test_paths_json: list[str] | None
    ci_paths_json: list[str] | None
    examples_paths_json: list[str] | None
    docs_paths_json: list[str] | None
    release_summary_json: dict[str, Any] | None
    file_samples: tuple[GitHubFileSampleFixture, ...] = field(default_factory=tuple)


@dataclass(frozen=True, slots=True)
class ReplayExecutionResult:
    source_candidate_replay_confirmed: bool
    enrich_requested_event_found: bool
    artifact_snapshot_created_or_reused: bool
    github_repo_snapshot_created_or_reused: bool
    github_file_samples_created_or_reused: bool
    artifact_current_snapshot_updated: bool
    snapshot_updated_outbox_event_created: bool
    evidence_bundle_created: bool
    analysis_requested_event_created: bool
    notification_created: bool
    checks_failed: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class RunnerResult:
    exit_code: int
    report: dict[str, Any]


@dataclass(frozen=True, slots=True)
class _PredecessorState:
    source_message_found: bool
    source_version_found: bool
    artifact_found: bool
    candidate_group_found: bool
    candidate_member_found: bool
    enrich_event_found: bool
    artifact_id: UUID | None
    artifact_type: str | None
    candidate_group_id: UUID | None
    enrich_event_id: UUID | None
    enrich_payload: dict[str, Any]
    checks_failed: tuple[str, ...]


class SnapshotReplayExecutor(Protocol):
    def execute(
        self,
        *,
        database_url: str,
        source_fixture: source_candidate_runner.SourceFixture,
        github_fixture: GitHubSnapshotFixture,
        replay_namespace: str,
    ) -> ReplayExecutionResult: ...


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


class SqlAlchemySnapshotReplayExecutor:
    def execute(
        self,
        *,
        database_url: str,
        source_fixture: source_candidate_runner.SourceFixture,
        github_fixture: GitHubSnapshotFixture,
        replay_namespace: str,
    ) -> ReplayExecutionResult:
        _bootstrap_repo_imports()
        import sqlalchemy as sa

        engine = sa.create_engine(database_url, future=True)
        try:
            with engine.begin() as connection:
                return _execute_snapshot_replay(
                    connection,
                    source_fixture=source_fixture,
                    github_fixture=github_fixture,
                    replay_namespace=replay_namespace,
                )
        finally:
            engine.dispose()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Replay a predecessor source-to-candidate fixture into a guarded "
            "local/test PostgreSQL database, then write a synthetic "
            "fixture-backed GitHub artifact snapshot without network calls."
        )
    )
    parser.add_argument("--database-url", required=True)
    parser.add_argument("--source-fixture", required=True)
    parser.add_argument("--github-snapshot-fixture", required=True)
    parser.add_argument("--replay-namespace", required=True)
    parser.add_argument("--confirm-local-test-db", action="store_true")
    return parser


def render_json(report: Mapping[str, Any]) -> str:
    return json.dumps(report, indent=2) + "\n"


def run(
    args: argparse.Namespace,
    *,
    env: Mapping[str, str] | None = None,
    executor: SnapshotReplayExecutor | None = None,
    source_replay_runner: SourceCandidateReplayRunner | None = None,
    repo_root: Path | None = None,
) -> RunnerResult:
    effective_env = env if env is not None else os.environ
    root = repo_root or _repo_root()
    report = _base_report()
    checks_failed: list[str] = []

    if not args.confirm_local_test_db:
        checks_failed.append("confirm_local_test_db_required")

    app_env = effective_env.get("APP_ENV", "").strip().lower()
    if app_env in {"prod", "production", "live"}:
        checks_failed.append("app_env_production_rejected")

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

    github_fixture: GitHubSnapshotFixture | None = None
    try:
        github_fixture = load_github_snapshot_fixture(Path(args.github_snapshot_fixture), repo_root=root)
        report["github_snapshot_fixture_loaded"] = True
    except Exception:  # noqa: BLE001 - operator output must stay sanitized.
        checks_failed.append("github_snapshot_fixture_load_failed")

    if checks_failed:
        return _finish(report, checks_failed)

    if source_fixture is None or github_fixture is None or not namespace_ok:
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
    except Exception:  # noqa: BLE001 - never echo DB or runtime error bodies.
        checks_failed.append("source_candidate_replay_failed")
        return _finish(report, checks_failed)

    if source_result.exit_code != 0 or source_result.report.get("status") != "pass":
        checks_failed.append("source_candidate_replay_failed")
        return _finish(report, checks_failed)

    active_executor = executor or SqlAlchemySnapshotReplayExecutor()
    try:
        execution = active_executor.execute(
            database_url=args.database_url,
            source_fixture=source_fixture,
            github_fixture=github_fixture,
            replay_namespace=args.replay_namespace,
        )
    except Exception as exc:  # noqa: BLE001 - never echo DB errors or URLs.
        checks_failed.append(_safe_failure_code(exc))
        return _finish(report, checks_failed)

    report.update(
        {
            "source_candidate_replay_confirmed": execution.source_candidate_replay_confirmed,
            "enrich_requested_event_found": execution.enrich_requested_event_found,
            "artifact_snapshot_created_or_reused": execution.artifact_snapshot_created_or_reused,
            "github_repo_snapshot_created_or_reused": execution.github_repo_snapshot_created_or_reused,
            "github_file_samples_created_or_reused": execution.github_file_samples_created_or_reused,
            "artifact_current_snapshot_updated": execution.artifact_current_snapshot_updated,
            "snapshot_updated_outbox_event_created": execution.snapshot_updated_outbox_event_created,
            "evidence_bundle_created": execution.evidence_bundle_created,
            "analysis_requested_event_created": execution.analysis_requested_event_created,
            "notification_created": execution.notification_created,
        }
    )
    checks_failed.extend(execution.checks_failed)

    for key in TRUE_RESULT_KEYS:
        if report.get(key) is not True:
            checks_failed.append(f"{key}:missing")
    for key in FALSE_RESULT_KEYS:
        if report.get(key) is not False:
            checks_failed.append(f"{key}:unexpected")

    return _finish(report, checks_failed)


def validate_database_url(
    database_url: str | None,
) -> tuple[bool, list[str], source_candidate_runner.ParsedDatabaseUrl | None]:
    return source_candidate_runner.validate_database_url(database_url)


def load_github_snapshot_fixture(path: Path, *, repo_root: Path | None = None) -> GitHubSnapshotFixture:
    fixture_path = path if path.is_absolute() else (repo_root or _repo_root()) / path
    payload = json.loads(fixture_path.read_text(encoding="utf-8"))
    if payload.get("schema_version") != FIXTURE_SCHEMA_VERSION:
        raise ValueError("github_snapshot_fixture_schema_version")
    if payload.get("provider") != "github":
        raise ValueError("github_snapshot_fixture_provider")
    artifact_canonical_id = _required_str(payload, "artifact_canonical_id")
    if artifact_canonical_id != ARTIFACT_CANONICAL_ID:
        raise ValueError("github_snapshot_fixture_artifact")
    artifact_type = _required_str(payload, "artifact_type")
    if artifact_type not in SUPPORTED_GITHUB_ARTIFACT_TYPES:
        raise ValueError("github_snapshot_fixture_artifact_type")

    repo = _required_dict(payload, "repo")
    file_samples = tuple(_load_file_sample(item) for item in _optional_list(payload.get("file_samples")))
    if not file_samples:
        raise ValueError("github_snapshot_fixture_file_samples_required")

    return GitHubSnapshotFixture(
        artifact_canonical_id=artifact_canonical_id,
        artifact_type=artifact_type,
        provider="github",
        snapshot_type=_required_str(payload, "snapshot_type"),
        status=_required_str(payload, "status"),
        content_anchor=_required_str(payload, "content_anchor"),
        auth_mode=_optional_str(payload.get("auth_mode")),
        normalized_projection=_optional_dict(payload.get("normalized_projection")),
        raw_payload_ref=_optional_str(payload.get("raw_payload_ref")),
        evidence_limitations=_str_list(payload.get("evidence_limitations")),
        fetch_anomalies=_str_list(payload.get("fetch_anomalies")),
        repo_full_name=_required_str(repo, "repo_full_name"),
        default_branch=_optional_str(repo.get("default_branch")),
        resolved_ref=_optional_str(repo.get("resolved_ref")),
        content_anchor_commit_sha=_optional_str(repo.get("content_anchor_commit_sha")),
        repo_flags_json=_optional_dict(repo.get("repo_flags_json")),
        license_spdx=_optional_str(repo.get("license_spdx")),
        topics_json=_str_list_or_none(repo.get("topics_json")),
        readme_excerpt=_optional_str(repo.get("readme_excerpt")),
        detected_build_systems_json=_str_list_or_none(repo.get("detected_build_systems_json")),
        detected_languages_json=_str_list_or_none(repo.get("detected_languages_json")),
        key_paths_json=_str_list_or_none(repo.get("key_paths_json")),
        test_paths_json=_str_list_or_none(repo.get("test_paths_json")),
        ci_paths_json=_str_list_or_none(repo.get("ci_paths_json")),
        examples_paths_json=_str_list_or_none(repo.get("examples_paths_json")),
        docs_paths_json=_str_list_or_none(repo.get("docs_paths_json")),
        release_summary_json=_optional_dict(repo.get("release_summary_json")),
        file_samples=file_samples,
    )


def build_snapshot_updated_dedupe_key(
    *,
    replay_namespace: str,
    artifact_id: UUID,
    snapshot_id: UUID,
) -> str:
    return (
        f"local-db-github-snapshot:{replay_namespace}:"
        f"artifact.snapshot.updated:{artifact_id}:{snapshot_id}"
    )


def _execute_snapshot_replay(
    connection: Any,
    *,
    source_fixture: source_candidate_runner.SourceFixture,
    github_fixture: GitHubSnapshotFixture,
    replay_namespace: str,
) -> ReplayExecutionResult:
    predecessor = _load_predecessor_state(
        connection,
        source_fixture=source_fixture,
        github_fixture=github_fixture,
        replay_namespace=replay_namespace,
    )
    if predecessor.checks_failed:
        return ReplayExecutionResult(
            source_candidate_replay_confirmed=False,
            enrich_requested_event_found=predecessor.enrich_event_found,
            artifact_snapshot_created_or_reused=False,
            github_repo_snapshot_created_or_reused=False,
            github_file_samples_created_or_reused=False,
            artifact_current_snapshot_updated=False,
            snapshot_updated_outbox_event_created=False,
            evidence_bundle_created=False,
            analysis_requested_event_created=False,
            notification_created=False,
            checks_failed=predecessor.checks_failed,
        )
    if predecessor.artifact_id is None or predecessor.candidate_group_id is None:
        raise RuntimeError("predecessor_state_missing")

    snapshot_id = _insert_or_reuse_artifact_snapshot(
        connection,
        artifact_id=predecessor.artifact_id,
        github_fixture=github_fixture,
    )
    _insert_or_reuse_github_repo_snapshot(
        connection,
        snapshot_id=snapshot_id,
        github_fixture=github_fixture,
    )
    for sample in github_fixture.file_samples:
        _insert_or_reuse_github_file_sample(connection, snapshot_id=snapshot_id, sample=sample)
    _update_artifact_current_snapshot(
        connection,
        artifact_id=predecessor.artifact_id,
        snapshot_id=snapshot_id,
        status=github_fixture.status,
    )
    _insert_or_reuse_snapshot_updated_outbox(
        connection,
        artifact_id=predecessor.artifact_id,
        snapshot_id=snapshot_id,
        status=github_fixture.status,
        content_anchor=github_fixture.content_anchor,
        replay_namespace=replay_namespace,
    )

    verification = _verify_snapshot_rows(
        connection,
        artifact_id=predecessor.artifact_id,
        candidate_group_id=predecessor.candidate_group_id,
        snapshot_id=snapshot_id,
        github_fixture=github_fixture,
        replay_namespace=replay_namespace,
    )
    return ReplayExecutionResult(
        source_candidate_replay_confirmed=True,
        enrich_requested_event_found=predecessor.enrich_event_found,
        artifact_snapshot_created_or_reused=verification["artifact_snapshot_created_or_reused"],
        github_repo_snapshot_created_or_reused=verification["github_repo_snapshot_created_or_reused"],
        github_file_samples_created_or_reused=verification["github_file_samples_created_or_reused"],
        artifact_current_snapshot_updated=verification["artifact_current_snapshot_updated"],
        snapshot_updated_outbox_event_created=verification["snapshot_updated_outbox_event_created"],
        evidence_bundle_created=verification["evidence_bundle_created"],
        analysis_requested_event_created=verification["analysis_requested_event_created"],
        notification_created=verification["notification_created"],
        checks_failed=tuple(verification["checks_failed"]),
    )


def _load_predecessor_state(
    connection: Any,
    *,
    source_fixture: source_candidate_runner.SourceFixture,
    github_fixture: GitHubSnapshotFixture,
    replay_namespace: str,
) -> _PredecessorState:
    import sqlalchemy as sa

    failures: list[str] = []
    normalizer_version = source_candidate_runner.build_normalizer_version(replay_namespace)
    source_message_found = _exists(
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
    source_version_found = _exists(
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
    artifact_row = connection.execute(
        sa.text(
            """
            SELECT artifact_id, artifact_type, canonical_id
            FROM artifact_registry
            WHERE canonical_id = :canonical_id
            """
        ),
        {"canonical_id": github_fixture.artifact_canonical_id},
    ).mappings().first()
    artifact_id = UUID(str(artifact_row["artifact_id"])) if artifact_row else None
    artifact_type = str(artifact_row["artifact_type"]) if artifact_row else None

    candidate_row = connection.execute(
        sa.text(
            """
            SELECT candidate_group_id
            FROM candidate_group_proposals
            WHERE source_message_id = CAST(:source_message_id AS uuid)
              AND source_version_no = :source_version_no
              AND normalizer_version = :normalizer_version
              AND dedupe_subject_key = :canonical_id
            """
        ),
        {
            "source_message_id": str(source_fixture.source_message_id),
            "source_version_no": source_fixture.source_version_no,
            "normalizer_version": normalizer_version,
            "canonical_id": github_fixture.artifact_canonical_id,
        },
    ).mappings().first()
    candidate_group_id = UUID(str(candidate_row["candidate_group_id"])) if candidate_row else None

    candidate_member_found = False
    if candidate_group_id is not None and artifact_id is not None:
        candidate_member_found = _exists(
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

    event_row = connection.execute(
        sa.text(
            """
            SELECT event_id, payload_json
            FROM event_outbox
            WHERE event_type = :event_type
              AND aggregate_type = 'artifact'
              AND aggregate_id = CAST(:artifact_id AS uuid)
              AND dedupe_key LIKE :dedupe_prefix
            ORDER BY created_at DESC, event_id DESC
            LIMIT 1
            """
        ),
        {
            "event_type": ENRICH_EVENT_TYPE,
            "artifact_id": str(artifact_id) if artifact_id is not None else "00000000-0000-0000-0000-000000000000",
            "dedupe_prefix": f"local-db-source-candidate:{replay_namespace}:artifact.enrich:%",
        },
    ).mappings().first()
    enrich_event_id = UUID(str(event_row["event_id"])) if event_row else None
    enrich_payload = _json_loads(event_row["payload_json"]) if event_row else {}

    if not source_message_found:
        failures.append("source_message_missing")
    if not source_version_found:
        failures.append("source_message_version_missing")
    if artifact_id is None:
        failures.append("artifact_registry_missing")
    if candidate_group_id is None:
        failures.append("candidate_group_missing")
    if not candidate_member_found:
        failures.append("candidate_group_member_missing")
    if enrich_event_id is None:
        failures.append("artifact_enrich_requested_event_missing")
    else:
        if enrich_payload.get("provider_route") != "github":
            failures.append("artifact_enrich_requested_provider_route")
        if enrich_payload.get("artifact_type") not in SUPPORTED_GITHUB_ARTIFACT_TYPES:
            failures.append("artifact_enrich_requested_artifact_type")
        if str(enrich_payload.get("artifact_id")) != str(artifact_id):
            failures.append("artifact_enrich_requested_artifact_id")
        if str(enrich_payload.get("candidate_group_id")) != str(candidate_group_id):
            failures.append("artifact_enrich_requested_candidate_group_id")
    if artifact_type != github_fixture.artifact_type:
        failures.append("artifact_registry_type_mismatch")

    return _PredecessorState(
        source_message_found=source_message_found,
        source_version_found=source_version_found,
        artifact_found=artifact_id is not None,
        candidate_group_found=candidate_group_id is not None,
        candidate_member_found=candidate_member_found,
        enrich_event_found=enrich_event_id is not None,
        artifact_id=artifact_id,
        artifact_type=artifact_type,
        candidate_group_id=candidate_group_id,
        enrich_event_id=enrich_event_id,
        enrich_payload=enrich_payload,
        checks_failed=tuple(dict.fromkeys(failures)),
    )


def _insert_or_reuse_artifact_snapshot(
    connection: Any,
    *,
    artifact_id: UUID,
    github_fixture: GitHubSnapshotFixture,
) -> UUID:
    import sqlalchemy as sa

    result = connection.execute(
        sa.text(
            """
            INSERT INTO artifact_snapshots (
                artifact_id,
                provider,
                snapshot_type,
                status,
                fetched_at,
                content_anchor,
                auth_mode,
                normalized_projection,
                raw_payload_ref,
                evidence_limitations,
                fetch_anomalies
            )
            VALUES (
                CAST(:artifact_id AS uuid),
                :provider,
                :snapshot_type,
                CAST(:status AS snapshot_status_enum),
                now(),
                :content_anchor,
                :auth_mode,
                CAST(:normalized_projection AS jsonb),
                :raw_payload_ref,
                CAST(:evidence_limitations AS jsonb),
                CAST(:fetch_anomalies AS jsonb)
            )
            ON CONFLICT ON CONSTRAINT uq_artifact_snapshots_artifact_provider_anchor_type
            DO UPDATE SET status = artifact_snapshots.status
            RETURNING snapshot_id
            """
        ),
        {
            "artifact_id": str(artifact_id),
            "provider": github_fixture.provider,
            "snapshot_type": github_fixture.snapshot_type,
            "status": github_fixture.status,
            "content_anchor": github_fixture.content_anchor,
            "auth_mode": github_fixture.auth_mode,
            "normalized_projection": _json_dumps_or_none(github_fixture.normalized_projection),
            "raw_payload_ref": github_fixture.raw_payload_ref,
            "evidence_limitations": _json_dumps_or_none(github_fixture.evidence_limitations),
            "fetch_anomalies": _json_dumps_or_none(github_fixture.fetch_anomalies),
        },
    )
    return UUID(str(result.scalar_one()))


def _insert_or_reuse_github_repo_snapshot(
    connection: Any,
    *,
    snapshot_id: UUID,
    github_fixture: GitHubSnapshotFixture,
) -> None:
    import sqlalchemy as sa

    connection.execute(
        sa.text(
            """
            INSERT INTO artifact_snapshot_github_repo (
                snapshot_id,
                repo_full_name,
                default_branch,
                resolved_ref,
                content_anchor_commit_sha,
                repo_flags_json,
                license_spdx,
                topics_json,
                readme_excerpt,
                detected_build_systems_json,
                detected_languages_json,
                key_paths_json,
                test_paths_json,
                ci_paths_json,
                examples_paths_json,
                docs_paths_json,
                release_summary_json
            )
            VALUES (
                CAST(:snapshot_id AS uuid),
                :repo_full_name,
                :default_branch,
                :resolved_ref,
                :content_anchor_commit_sha,
                CAST(:repo_flags_json AS jsonb),
                :license_spdx,
                CAST(:topics_json AS jsonb),
                :readme_excerpt,
                CAST(:detected_build_systems_json AS jsonb),
                CAST(:detected_languages_json AS jsonb),
                CAST(:key_paths_json AS jsonb),
                CAST(:test_paths_json AS jsonb),
                CAST(:ci_paths_json AS jsonb),
                CAST(:examples_paths_json AS jsonb),
                CAST(:docs_paths_json AS jsonb),
                CAST(:release_summary_json AS jsonb)
            )
            ON CONFLICT (snapshot_id) DO NOTHING
            """
        ),
        {
            "snapshot_id": str(snapshot_id),
            "repo_full_name": github_fixture.repo_full_name,
            "default_branch": github_fixture.default_branch,
            "resolved_ref": github_fixture.resolved_ref,
            "content_anchor_commit_sha": github_fixture.content_anchor_commit_sha,
            "repo_flags_json": _json_dumps_or_none(github_fixture.repo_flags_json),
            "license_spdx": github_fixture.license_spdx,
            "topics_json": _json_dumps_or_none(github_fixture.topics_json),
            "readme_excerpt": github_fixture.readme_excerpt,
            "detected_build_systems_json": _json_dumps_or_none(github_fixture.detected_build_systems_json),
            "detected_languages_json": _json_dumps_or_none(github_fixture.detected_languages_json),
            "key_paths_json": _json_dumps_or_none(github_fixture.key_paths_json),
            "test_paths_json": _json_dumps_or_none(github_fixture.test_paths_json),
            "ci_paths_json": _json_dumps_or_none(github_fixture.ci_paths_json),
            "examples_paths_json": _json_dumps_or_none(github_fixture.examples_paths_json),
            "docs_paths_json": _json_dumps_or_none(github_fixture.docs_paths_json),
            "release_summary_json": _json_dumps_or_none(github_fixture.release_summary_json),
        },
    )


def _insert_or_reuse_github_file_sample(
    connection: Any,
    *,
    snapshot_id: UUID,
    sample: GitHubFileSampleFixture,
) -> None:
    import sqlalchemy as sa

    connection.execute(
        sa.text(
            """
            INSERT INTO artifact_snapshot_github_file_samples (
                snapshot_id,
                path,
                role,
                size_bytes,
                content_hash,
                excerpt,
                raw_blob_ref
            )
            VALUES (
                CAST(:snapshot_id AS uuid),
                :path,
                :role,
                :size_bytes,
                :content_hash,
                :excerpt,
                :raw_blob_ref
            )
            ON CONFLICT (snapshot_id, path, role) DO NOTHING
            """
        ),
        {
            "snapshot_id": str(snapshot_id),
            "path": sample.path,
            "role": sample.role,
            "size_bytes": sample.size_bytes,
            "content_hash": sample.content_hash,
            "excerpt": sample.excerpt,
            "raw_blob_ref": sample.raw_blob_ref,
        },
    )


def _update_artifact_current_snapshot(
    connection: Any,
    *,
    artifact_id: UUID,
    snapshot_id: UUID,
    status: str,
) -> None:
    import sqlalchemy as sa

    connection.execute(
        sa.text(
            """
            UPDATE artifact_registry
            SET current_snapshot_id = CAST(:snapshot_id AS uuid),
                current_status = CAST(:status AS snapshot_status_enum),
                updated_at = now()
            WHERE artifact_id = CAST(:artifact_id AS uuid)
            """
        ),
        {"artifact_id": str(artifact_id), "snapshot_id": str(snapshot_id), "status": status},
    )


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
            "payload_json": _json_dumps(payload),
        },
    )


def _verify_snapshot_rows(
    connection: Any,
    *,
    artifact_id: UUID,
    candidate_group_id: UUID,
    snapshot_id: UUID,
    github_fixture: GitHubSnapshotFixture,
    replay_namespace: str,
) -> dict[str, Any]:
    checks_failed: list[str] = []
    artifact_snapshot = _exists(
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
            "snapshot_type": github_fixture.snapshot_type,
            "status": github_fixture.status,
            "content_anchor": github_fixture.content_anchor,
        },
    )
    repo_snapshot = _exists(
        connection,
        """
        SELECT 1
        FROM artifact_snapshot_github_repo
        WHERE snapshot_id = CAST(:snapshot_id AS uuid)
          AND repo_full_name = :repo_full_name
        """,
        {"snapshot_id": str(snapshot_id), "repo_full_name": github_fixture.repo_full_name},
    )
    file_sample_count = _count(
        connection,
        """
        SELECT count(*)
        FROM artifact_snapshot_github_file_samples
        WHERE snapshot_id = CAST(:snapshot_id AS uuid)
        """,
        {"snapshot_id": str(snapshot_id)},
    )
    current_snapshot = _exists(
        connection,
        """
        SELECT 1
        FROM artifact_registry
        WHERE artifact_id = CAST(:artifact_id AS uuid)
          AND current_snapshot_id = CAST(:snapshot_id AS uuid)
          AND current_status = CAST(:status AS snapshot_status_enum)
        """,
        {"artifact_id": str(artifact_id), "snapshot_id": str(snapshot_id), "status": github_fixture.status},
    )
    snapshot_outbox = _exists(
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
    evidence_bundle_created = _exists(
        connection,
        """
        SELECT 1
        FROM candidate_evidence_members
        WHERE snapshot_id = CAST(:snapshot_id AS uuid)
        """,
        {"snapshot_id": str(snapshot_id)},
    )
    analysis_requested_event_created = _exists(
        connection,
        """
        SELECT 1
        FROM event_outbox
        WHERE event_type = 'analysis.requested.v1'
          AND dedupe_key LIKE :dedupe_prefix
        """,
        {"dedupe_prefix": f"local-db-github-snapshot:{replay_namespace}:%"},
    )
    notification_created = _exists(
        connection,
        """
        SELECT 1
        FROM event_outbox
        WHERE event_type IN (
            'notification.plan.created.v1',
            'notification.delivery.result.v1'
        )
          AND dedupe_key LIKE :dedupe_prefix
        """,
        {"dedupe_prefix": f"local-db-github-snapshot:{replay_namespace}:%"},
    )

    file_samples = file_sample_count >= len(github_fixture.file_samples)
    checks = {
        "artifact_snapshot_created_or_reused": artifact_snapshot,
        "github_repo_snapshot_created_or_reused": repo_snapshot,
        "github_file_samples_created_or_reused": file_samples,
        "artifact_current_snapshot_updated": current_snapshot,
        "snapshot_updated_outbox_event_created": snapshot_outbox,
        "evidence_bundle_created": evidence_bundle_created,
        "analysis_requested_event_created": analysis_requested_event_created,
        "notification_created": notification_created,
    }
    for key, value in checks.items():
        if key in TRUE_RESULT_KEYS and value is not True:
            checks_failed.append(f"{key}:missing")
        if key in FALSE_RESULT_KEYS and value is not False:
            checks_failed.append(f"{key}:unexpected")
    return {**checks, "checks_failed": list(dict.fromkeys(checks_failed))}


def _exists(connection: Any, sql: str, params: dict[str, Any]) -> bool:
    import sqlalchemy as sa

    result = connection.execute(sa.text(sql), params)
    return result.first() is not None


def _count(connection: Any, sql: str, params: dict[str, Any]) -> int:
    import sqlalchemy as sa

    return int(connection.execute(sa.text(sql), params).scalar_one())


def _load_file_sample(payload: Any) -> GitHubFileSampleFixture:
    if not isinstance(payload, dict):
        raise ValueError("github_snapshot_fixture_file_sample")
    return GitHubFileSampleFixture(
        path=_required_str(payload, "path"),
        role=_required_str(payload, "role"),
        size_bytes=_optional_int(payload.get("size_bytes")),
        content_hash=_optional_str(payload.get("content_hash")),
        excerpt=_optional_str(payload.get("excerpt")),
        raw_blob_ref=_optional_str(payload.get("raw_blob_ref")),
    )


def _base_report() -> dict[str, Any]:
    return {
        "schema_version": SCHEMA_VERSION,
        "status": "pass",
        "database_url_guard_passed": False,
        "source_candidate_replay_confirmed": False,
        "enrich_requested_event_found": False,
        "github_snapshot_fixture_loaded": False,
        "artifact_snapshot_created_or_reused": False,
        "github_repo_snapshot_created_or_reused": False,
        "github_file_samples_created_or_reused": False,
        "artifact_current_snapshot_updated": False,
        "snapshot_updated_outbox_event_created": False,
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


def _finish(report: dict[str, Any], checks_failed: Sequence[str]) -> RunnerResult:
    normalized_failures = list(dict.fromkeys(checks_failed))
    report["checks_failed"] = normalized_failures
    report["status"] = "fail" if normalized_failures else "pass"
    return RunnerResult(exit_code=0 if report["status"] == "pass" else 1, report=report)


def _safe_failure_code(exc: Exception) -> str:
    message = str(exc)
    if message in {"predecessor_state_missing"}:
        return message
    return exc.__class__.__name__


def _required_str(payload: Mapping[str, Any], key: str) -> str:
    value = payload.get(key)
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{key}_required")
    return value


def _optional_str(value: Any) -> str | None:
    if isinstance(value, str) and value.strip():
        return value
    return None


def _optional_int(value: Any) -> int | None:
    if value is None:
        return None
    return int(value)


def _required_dict(payload: Mapping[str, Any], key: str) -> dict[str, Any]:
    value = payload.get(key)
    if not isinstance(value, dict):
        raise ValueError(f"{key}_dict_required")
    return value


def _optional_dict(value: Any) -> dict[str, Any] | None:
    if value is None:
        return None
    if not isinstance(value, dict):
        raise ValueError("json_dict_required")
    return value


def _optional_list(value: Any) -> list[Any]:
    if value is None:
        return []
    if not isinstance(value, list):
        raise ValueError("json_list_required")
    return value


def _str_list(value: Any) -> list[str]:
    if value is None:
        return []
    if not isinstance(value, list):
        raise ValueError("str_list_required")
    return [str(item) for item in value]


def _str_list_or_none(value: Any) -> list[str] | None:
    values = _str_list(value)
    return values if values else None


def _json_loads(value: Any) -> dict[str, Any]:
    if value is None:
        return {}
    if isinstance(value, str):
        parsed = json.loads(value)
    else:
        parsed = value
    return parsed if isinstance(parsed, dict) else {}


def _json_dumps(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True)


def _json_dumps_or_none(value: Any) -> str | None:
    if value is None:
        return None
    return _json_dumps(value)


def _bootstrap_repo_imports() -> None:
    repo_root = _repo_root()
    src_root = repo_root / "src"
    for path in (repo_root, src_root):
        value = str(path)
        if value not in sys.path:
            sys.path.insert(0, value)


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    result = run(args)
    sys.stdout.write(render_json(result.report))
    return result.exit_code


if __name__ == "__main__":
    raise SystemExit(main())
