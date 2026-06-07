from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping, Protocol, Sequence
from uuid import UUID

from tools import local_db_github_snapshot_fixture_replay_runner as github_snapshot_runner
from tools import local_db_source_candidate_replay_runner as source_candidate_runner


SCHEMA_VERSION = "local_db_evidence_bundle_fixture_replay_v1"
BUNDLE_PROFILE_VERSION = "local_db_evidence_bundle_fixture_v1"
TOKEN_BUDGET_PROFILE = "small"
SNAPSHOT_UPDATED_EVENT_TYPE = "artifact.snapshot.updated.v1"
ANALYSIS_REQUESTED_EVENT_TYPE = "analysis.requested.v1"
JUDGE_PROFILE_GITHUB = "github_primary"
READY_SNAPSHOT_STATUSES = {"ready", "partial_ready", "low_evidence"}
TRUE_RESULT_KEYS = (
    "database_url_guard_passed",
    "source_candidate_replay_confirmed",
    "artifact_snapshot_replay_confirmed",
    "snapshot_updated_event_found",
    "candidate_refresh_target_resolved",
    "current_artifact_snapshot_loaded",
    "evidence_bundle_created_or_reused",
    "candidate_evidence_members_created_or_reused",
    "candidate_current_bundle_updated",
    "analysis_requested_event_created",
    "judge_profile_resolved",
    "ready_for_analysis",
)
FALSE_RESULT_KEYS = (
    "production_db_write",
    "live_github_called",
    "live_telegram_called",
    "openai_called",
    "workers_started",
    "redis_mutation",
    "judge_output_created",
    "analysis_created",
    "notification_created",
)


@dataclass(frozen=True, slots=True)
class RunnerResult:
    exit_code: int
    report: dict[str, Any]


@dataclass(frozen=True, slots=True)
class ReplayExecutionResult:
    source_candidate_replay_confirmed: bool
    artifact_snapshot_replay_confirmed: bool
    snapshot_updated_event_found: bool
    candidate_refresh_target_resolved: bool
    current_artifact_snapshot_loaded: bool
    evidence_bundle_created_or_reused: bool
    candidate_evidence_members_created_or_reused: bool
    candidate_current_bundle_updated: bool
    analysis_requested_event_created: bool
    judge_profile_resolved: bool
    ready_for_analysis: bool
    checks_failed: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class SnapshotUpdatedEvent:
    event_id: UUID
    artifact_id: UUID
    snapshot_id: UUID
    provider: str
    status: str
    content_anchor: str


@dataclass(frozen=True, slots=True)
class CandidateGroupRecord:
    candidate_group_id: UUID
    initial_primary_artifact_id: UUID
    current_primary_artifact_id: UUID
    current_bundle_id: UUID | None


@dataclass(frozen=True, slots=True)
class CandidateMemberRecord:
    artifact_id: UUID
    artifact_type: str
    member_role: str
    member_order: int | None


@dataclass(frozen=True, slots=True)
class CurrentGitHubSnapshot:
    artifact_id: UUID
    artifact_type: str
    snapshot_id: UUID
    provider: str
    snapshot_type: str
    status: str
    content_anchor: str
    normalized_projection: dict[str, Any]
    evidence_limitations: list[str]
    repo_full_name: str
    readme_excerpt: str | None
    key_paths_json: list[str]
    test_paths_json: list[str]
    ci_paths_json: list[str]
    examples_paths_json: list[str]
    docs_paths_json: list[str]


class EvidenceBundleReplayExecutor(Protocol):
    def execute(
        self,
        *,
        database_url: str,
        source_fixture: source_candidate_runner.SourceFixture,
        github_fixture: github_snapshot_runner.GitHubSnapshotFixture,
        replay_namespace: str,
    ) -> ReplayExecutionResult: ...


class GitHubSnapshotReplayRunner(Protocol):
    def run(
        self,
        *,
        database_url: str,
        source_fixture_path: Path,
        github_snapshot_fixture_path: Path,
        replay_namespace: str,
        env: Mapping[str, str],
        repo_root: Path,
    ) -> github_snapshot_runner.RunnerResult: ...


class DefaultGitHubSnapshotReplayRunner:
    def run(
        self,
        *,
        database_url: str,
        source_fixture_path: Path,
        github_snapshot_fixture_path: Path,
        replay_namespace: str,
        env: Mapping[str, str],
        repo_root: Path,
    ) -> github_snapshot_runner.RunnerResult:
        args = argparse.Namespace(
            database_url=database_url,
            source_fixture=str(source_fixture_path),
            github_snapshot_fixture=str(github_snapshot_fixture_path),
            replay_namespace=replay_namespace,
            confirm_local_test_db=True,
        )
        predecessor_env = dict(env)
        predecessor_env["APP_ENV"] = "test"
        return github_snapshot_runner.run(args, env=predecessor_env, repo_root=repo_root)


class SqlAlchemyEvidenceBundleReplayExecutor:
    def execute(
        self,
        *,
        database_url: str,
        source_fixture: source_candidate_runner.SourceFixture,
        github_fixture: github_snapshot_runner.GitHubSnapshotFixture,
        replay_namespace: str,
    ) -> ReplayExecutionResult:
        _bootstrap_repo_imports()
        import sqlalchemy as sa

        engine = sa.create_engine(database_url, future=True)
        try:
            with engine.begin() as connection:
                return _execute_evidence_bundle_replay(
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
            "Replay source-to-candidate and GitHub snapshot fixtures into a guarded "
            "local/test PostgreSQL database, then write or reuse a deterministic "
            "fixture-backed EvidenceBundle and analysis.requested handoff."
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
    executor: EvidenceBundleReplayExecutor | None = None,
    predecessor_runner: GitHubSnapshotReplayRunner | None = None,
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

    database_ok, database_failures, _ = validate_database_url(args.database_url)
    report["database_url_guard_passed"] = database_ok
    checks_failed.extend(database_failures)

    source_fixture: source_candidate_runner.SourceFixture | None = None
    try:
        source_fixture = source_candidate_runner.load_source_fixture(Path(args.source_fixture), repo_root=root)
    except Exception:  # noqa: BLE001 - operator output must stay sanitized.
        checks_failed.append("source_fixture_load_failed")

    github_fixture: github_snapshot_runner.GitHubSnapshotFixture | None = None
    try:
        github_fixture = github_snapshot_runner.load_github_snapshot_fixture(
            Path(args.github_snapshot_fixture),
            repo_root=root,
        )
    except Exception:  # noqa: BLE001 - operator output must stay sanitized.
        checks_failed.append("github_snapshot_fixture_load_failed")

    if checks_failed:
        return _finish(report, checks_failed)

    if source_fixture is None or github_fixture is None or not namespace_ok:
        checks_failed.append("precondition_missing")
        return _finish(report, checks_failed)

    active_predecessor = predecessor_runner or DefaultGitHubSnapshotReplayRunner()
    try:
        predecessor = active_predecessor.run(
            database_url=args.database_url,
            source_fixture_path=Path(args.source_fixture),
            github_snapshot_fixture_path=Path(args.github_snapshot_fixture),
            replay_namespace=args.replay_namespace,
            env=effective_env,
            repo_root=root,
        )
    except Exception:  # noqa: BLE001 - never echo DB or runtime error bodies.
        checks_failed.append("github_snapshot_replay_failed")
        return _finish(report, checks_failed)

    if not _predecessor_result_acceptable(predecessor):
        checks_failed.append("github_snapshot_replay_failed")
        return _finish(report, checks_failed)
    if predecessor.report.get("source_candidate_replay_confirmed") is not True:
        checks_failed.append("source_candidate_replay_not_confirmed")
    if not _predecessor_snapshot_confirmed(predecessor.report):
        checks_failed.append("artifact_snapshot_replay_not_confirmed")
    if checks_failed:
        return _finish(report, checks_failed)

    active_executor = executor or SqlAlchemyEvidenceBundleReplayExecutor()
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
            "artifact_snapshot_replay_confirmed": execution.artifact_snapshot_replay_confirmed,
            "snapshot_updated_event_found": execution.snapshot_updated_event_found,
            "candidate_refresh_target_resolved": execution.candidate_refresh_target_resolved,
            "current_artifact_snapshot_loaded": execution.current_artifact_snapshot_loaded,
            "evidence_bundle_created_or_reused": execution.evidence_bundle_created_or_reused,
            "candidate_evidence_members_created_or_reused": execution.candidate_evidence_members_created_or_reused,
            "candidate_current_bundle_updated": execution.candidate_current_bundle_updated,
            "analysis_requested_event_created": execution.analysis_requested_event_created,
            "judge_profile_resolved": execution.judge_profile_resolved,
            "ready_for_analysis": execution.ready_for_analysis,
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
    return github_snapshot_runner.validate_database_url(database_url)


def build_bundle_input_hash(payload: Mapping[str, Any]) -> str:
    encoded = json.dumps(_canonicalize_for_hash(payload), ensure_ascii=False, sort_keys=True)
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def resolve_judge_profile(snapshot_type: str | None) -> str | None:
    if snapshot_type in {"github_repo", "github_subpath", "github_repo_page", "github_gist"}:
        return JUDGE_PROFILE_GITHUB
    if snapshot_type == "x_post":
        return "x_primary"
    if snapshot_type in {"web_article", "text_idea"}:
        return "text_idea_primary"
    return None


def build_analysis_requested_dedupe_key(
    *,
    replay_namespace: str,
    candidate_group_id: UUID,
    bundle_id: UUID,
) -> str:
    return f"local-db-evidence-bundle:{replay_namespace}:analysis.requested:{candidate_group_id}:{bundle_id}"


def _execute_evidence_bundle_replay(
    connection: Any,
    *,
    source_fixture: source_candidate_runner.SourceFixture,
    github_fixture: github_snapshot_runner.GitHubSnapshotFixture,
    replay_namespace: str,
) -> ReplayExecutionResult:
    checks_failed: list[str] = []

    source_candidate_confirmed = _source_candidate_output_exists(
        connection,
        source_fixture=source_fixture,
        github_fixture=github_fixture,
        replay_namespace=replay_namespace,
    )
    if not source_candidate_confirmed:
        checks_failed.append("source_candidate_replay_output_missing")

    event = _load_snapshot_updated_event(
        connection,
        github_fixture=github_fixture,
        replay_namespace=replay_namespace,
    )
    snapshot_event_found = event is not None
    if event is None:
        checks_failed.append("snapshot_updated_event_missing")
        return _execution_result(checks_failed=checks_failed)

    candidate_group_ids = _resolve_candidate_group_ids(connection, artifact_id=event.artifact_id)
    target_resolved = len(candidate_group_ids) == 1
    if not target_resolved:
        checks_failed.append("candidate_group_resolution_count_unexpected")
        return _execution_result(
            source_candidate_replay_confirmed=source_candidate_confirmed,
            snapshot_updated_event_found=snapshot_event_found,
            checks_failed=checks_failed,
        )

    candidate = _load_candidate_group(connection, candidate_group_ids[0])
    if candidate is None:
        checks_failed.append("candidate_group_missing")
        return _execution_result(
            source_candidate_replay_confirmed=source_candidate_confirmed,
            snapshot_updated_event_found=snapshot_event_found,
            candidate_refresh_target_resolved=target_resolved,
            checks_failed=checks_failed,
        )

    members = _load_candidate_members(connection, candidate.candidate_group_id)
    primary_members = [member for member in members if member.artifact_id == event.artifact_id]
    if len(primary_members) != 1:
        checks_failed.append("candidate_group_member_missing")

    snapshot = _load_current_github_snapshot(
        connection,
        artifact_id=event.artifact_id,
        snapshot_id=event.snapshot_id,
    )
    current_snapshot_loaded = snapshot is not None
    if snapshot is None:
        checks_failed.append("current_artifact_snapshot_missing")
        return _execution_result(
            source_candidate_replay_confirmed=source_candidate_confirmed,
            artifact_snapshot_replay_confirmed=False,
            snapshot_updated_event_found=snapshot_event_found,
            candidate_refresh_target_resolved=target_resolved,
            current_artifact_snapshot_loaded=current_snapshot_loaded,
            checks_failed=checks_failed,
        )
    if snapshot.provider != event.provider or snapshot.status != event.status:
        checks_failed.append("current_artifact_snapshot_event_mismatch")
    if snapshot.content_anchor != event.content_anchor:
        checks_failed.append("current_artifact_snapshot_anchor_mismatch")

    artifact_snapshot_confirmed = _github_snapshot_rows_exist(
        connection,
        artifact_id=event.artifact_id,
        snapshot_id=event.snapshot_id,
        github_fixture=github_fixture,
    )
    if not artifact_snapshot_confirmed:
        checks_failed.append("artifact_snapshot_replay_output_missing")

    judge_profile = resolve_judge_profile(snapshot.snapshot_type)
    judge_profile_resolved = judge_profile == JUDGE_PROFILE_GITHUB
    if not judge_profile_resolved:
        checks_failed.append("judge_profile_unresolved")

    ready_for_analysis = snapshot.status in READY_SNAPSHOT_STATUSES and bool(members) and judge_profile_resolved
    bundle_members = [
        {
            "artifact_id": str(event.artifact_id),
            "snapshot_id": str(event.snapshot_id),
            "member_role": "primary",
            "member_order": 0,
        }
    ]
    bundle_input_hash = build_bundle_input_hash(
        {
            "candidate_group_id": str(candidate.candidate_group_id),
            "current_primary_artifact_id": str(candidate.current_primary_artifact_id),
            "members": bundle_members,
            "snapshot_content_anchor": snapshot.content_anchor,
            "bundle_profile_version": BUNDLE_PROFILE_VERSION,
        }
    )
    primary_summary = _build_primary_summary(snapshot)

    bundle_id = _load_existing_bundle_id(
        connection,
        candidate_group_id=candidate.candidate_group_id,
        bundle_input_hash=bundle_input_hash,
    )
    if bundle_id is None:
        bundle_id = _insert_evidence_bundle(
            connection,
            candidate=candidate,
            bundle_input_hash=bundle_input_hash,
            primary_summary=primary_summary,
            evidence_limitations=snapshot.evidence_limitations,
            ready_for_analysis=ready_for_analysis,
        )
    _insert_or_reuse_evidence_member(
        connection,
        bundle_id=bundle_id,
        artifact_id=event.artifact_id,
        snapshot_id=event.snapshot_id,
    )
    _update_current_bundle(
        connection,
        candidate_group_id=candidate.candidate_group_id,
        bundle_id=bundle_id,
    )
    if ready_for_analysis and judge_profile is not None:
        _insert_or_reuse_analysis_requested_outbox(
            connection,
            replay_namespace=replay_namespace,
            candidate_group_id=candidate.candidate_group_id,
            bundle_id=bundle_id,
            judge_profile=judge_profile,
        )

    verification = _verify_evidence_rows(
        connection,
        candidate_group_id=candidate.candidate_group_id,
        bundle_id=bundle_id,
        artifact_id=event.artifact_id,
        snapshot_id=event.snapshot_id,
        replay_namespace=replay_namespace,
    )
    checks_failed.extend(verification["checks_failed"])
    return ReplayExecutionResult(
        source_candidate_replay_confirmed=source_candidate_confirmed,
        artifact_snapshot_replay_confirmed=artifact_snapshot_confirmed,
        snapshot_updated_event_found=snapshot_event_found,
        candidate_refresh_target_resolved=target_resolved,
        current_artifact_snapshot_loaded=current_snapshot_loaded,
        evidence_bundle_created_or_reused=verification["evidence_bundle_created_or_reused"],
        candidate_evidence_members_created_or_reused=verification["candidate_evidence_members_created_or_reused"],
        candidate_current_bundle_updated=verification["candidate_current_bundle_updated"],
        analysis_requested_event_created=verification["analysis_requested_event_created"],
        judge_profile_resolved=judge_profile_resolved,
        ready_for_analysis=ready_for_analysis,
        checks_failed=tuple(dict.fromkeys(checks_failed)),
    )


def _source_candidate_output_exists(
    connection: Any,
    *,
    source_fixture: source_candidate_runner.SourceFixture,
    github_fixture: github_snapshot_runner.GitHubSnapshotFixture,
    replay_namespace: str,
) -> bool:
    import sqlalchemy as sa

    normalizer_version = source_candidate_runner.build_normalizer_version(replay_namespace)
    row = connection.execute(
        sa.text(
            """
            SELECT cgp.candidate_group_id
            FROM candidate_group_proposals cgp
            JOIN candidate_group_members cgm
              ON cgm.candidate_group_id = cgp.candidate_group_id
            JOIN artifact_registry ar
              ON ar.artifact_id = cgm.artifact_id
            WHERE cgp.source_message_id = CAST(:source_message_id AS uuid)
              AND cgp.source_version_no = :source_version_no
              AND cgp.normalizer_version = :normalizer_version
              AND cgp.dedupe_subject_key = :canonical_id
              AND ar.canonical_id = :canonical_id
              AND cgm.member_role = 'primary'
            LIMIT 1
            """
        ),
        {
            "source_message_id": str(source_fixture.source_message_id),
            "source_version_no": source_fixture.source_version_no,
            "normalizer_version": normalizer_version,
            "canonical_id": github_fixture.artifact_canonical_id,
        },
    ).first()
    return row is not None


def _load_snapshot_updated_event(
    connection: Any,
    *,
    github_fixture: github_snapshot_runner.GitHubSnapshotFixture,
    replay_namespace: str,
) -> SnapshotUpdatedEvent | None:
    import sqlalchemy as sa

    row = connection.execute(
        sa.text(
            """
            SELECT event_id, payload_json
            FROM event_outbox
            WHERE event_type = :event_type
              AND dedupe_key LIKE :dedupe_prefix
            ORDER BY created_at DESC, event_id DESC
            LIMIT 1
            """
        ),
        {
            "event_type": SNAPSHOT_UPDATED_EVENT_TYPE,
            "dedupe_prefix": f"local-db-github-snapshot:{replay_namespace}:artifact.snapshot.updated:%",
        },
    ).mappings().first()
    if row is None:
        return None
    payload = _json_loads(row["payload_json"]) or {}
    required = {"artifact_id", "snapshot_id", "provider", "status", "content_anchor"}
    if not required.issubset(payload):
        return None
    if payload.get("provider") != "github":
        return None
    if payload.get("status") != github_fixture.status:
        return None
    if payload.get("content_anchor") != github_fixture.content_anchor:
        return None
    return SnapshotUpdatedEvent(
        event_id=UUID(str(row["event_id"])),
        artifact_id=UUID(str(payload["artifact_id"])),
        snapshot_id=UUID(str(payload["snapshot_id"])),
        provider=str(payload["provider"]),
        status=str(payload["status"]),
        content_anchor=str(payload["content_anchor"]),
    )


def _resolve_candidate_group_ids(connection: Any, *, artifact_id: UUID) -> list[UUID]:
    import sqlalchemy as sa

    result = connection.execute(
        sa.text(
            """
            SELECT DISTINCT candidate_group_id
            FROM candidate_group_members
            WHERE artifact_id = CAST(:artifact_id AS uuid)
            ORDER BY candidate_group_id
            """
        ),
        {"artifact_id": str(artifact_id)},
    )
    return [UUID(str(row["candidate_group_id"])) for row in result.mappings().all()]


def _load_candidate_group(connection: Any, candidate_group_id: UUID) -> CandidateGroupRecord | None:
    import sqlalchemy as sa

    row = connection.execute(
        sa.text(
            """
            SELECT candidate_group_id, initial_primary_artifact_id,
                   current_primary_artifact_id, current_bundle_id
            FROM candidate_group_proposals
            WHERE candidate_group_id = CAST(:candidate_group_id AS uuid)
            """
        ),
        {"candidate_group_id": str(candidate_group_id)},
    ).mappings().first()
    if row is None:
        return None
    return CandidateGroupRecord(
        candidate_group_id=UUID(str(row["candidate_group_id"])),
        initial_primary_artifact_id=UUID(str(row["initial_primary_artifact_id"])),
        current_primary_artifact_id=UUID(str(row["current_primary_artifact_id"])),
        current_bundle_id=_uuid_or_none(row["current_bundle_id"]),
    )


def _load_candidate_members(connection: Any, candidate_group_id: UUID) -> list[CandidateMemberRecord]:
    import sqlalchemy as sa

    result = connection.execute(
        sa.text(
            """
            SELECT cgm.artifact_id, ar.artifact_type, cgm.member_role, cgm.member_order
            FROM candidate_group_members cgm
            JOIN artifact_registry ar ON ar.artifact_id = cgm.artifact_id
            WHERE cgm.candidate_group_id = CAST(:candidate_group_id AS uuid)
            ORDER BY cgm.member_order NULLS LAST, cgm.artifact_id
            """
        ),
        {"candidate_group_id": str(candidate_group_id)},
    )
    return [
        CandidateMemberRecord(
            artifact_id=UUID(str(row["artifact_id"])),
            artifact_type=str(row["artifact_type"]),
            member_role=str(row["member_role"]),
            member_order=int(row["member_order"]) if row["member_order"] is not None else None,
        )
        for row in result.mappings().all()
    ]


def _load_current_github_snapshot(
    connection: Any,
    *,
    artifact_id: UUID,
    snapshot_id: UUID,
) -> CurrentGitHubSnapshot | None:
    import sqlalchemy as sa

    row = connection.execute(
        sa.text(
            """
            SELECT ar.artifact_id, ar.artifact_type, s.snapshot_id, s.provider,
                   s.snapshot_type, s.status, s.content_anchor, s.normalized_projection,
                   s.evidence_limitations, gh.repo_full_name, gh.readme_excerpt,
                   gh.key_paths_json, gh.test_paths_json, gh.ci_paths_json,
                   gh.examples_paths_json, gh.docs_paths_json
            FROM artifact_registry ar
            JOIN artifact_snapshots s ON s.snapshot_id = ar.current_snapshot_id
            JOIN artifact_snapshot_github_repo gh ON gh.snapshot_id = s.snapshot_id
            WHERE ar.artifact_id = CAST(:artifact_id AS uuid)
              AND s.snapshot_id = CAST(:snapshot_id AS uuid)
              AND s.provider = 'github'
            """
        ),
        {"artifact_id": str(artifact_id), "snapshot_id": str(snapshot_id)},
    ).mappings().first()
    if row is None:
        return None
    return CurrentGitHubSnapshot(
        artifact_id=UUID(str(row["artifact_id"])),
        artifact_type=str(row["artifact_type"]),
        snapshot_id=UUID(str(row["snapshot_id"])),
        provider=str(row["provider"]),
        snapshot_type=str(row["snapshot_type"]),
        status=str(row["status"]),
        content_anchor=str(row["content_anchor"]),
        normalized_projection=_json_loads(row["normalized_projection"]) or {},
        evidence_limitations=_json_loads(row["evidence_limitations"]) or [],
        repo_full_name=str(row["repo_full_name"]),
        readme_excerpt=str(row["readme_excerpt"]) if row["readme_excerpt"] else None,
        key_paths_json=_json_loads(row["key_paths_json"]) or [],
        test_paths_json=_json_loads(row["test_paths_json"]) or [],
        ci_paths_json=_json_loads(row["ci_paths_json"]) or [],
        examples_paths_json=_json_loads(row["examples_paths_json"]) or [],
        docs_paths_json=_json_loads(row["docs_paths_json"]) or [],
    )


def _github_snapshot_rows_exist(
    connection: Any,
    *,
    artifact_id: UUID,
    snapshot_id: UUID,
    github_fixture: github_snapshot_runner.GitHubSnapshotFixture,
) -> bool:
    import sqlalchemy as sa

    row = connection.execute(
        sa.text(
            """
            SELECT
              EXISTS (
                SELECT 1
                FROM artifact_snapshots
                WHERE snapshot_id = CAST(:snapshot_id AS uuid)
                  AND artifact_id = CAST(:artifact_id AS uuid)
                  AND provider = 'github'
                  AND snapshot_type = :snapshot_type
                  AND status = CAST(:status AS snapshot_status_enum)
                  AND content_anchor = :content_anchor
              ) AS parent_exists,
              EXISTS (
                SELECT 1
                FROM artifact_snapshot_github_repo
                WHERE snapshot_id = CAST(:snapshot_id AS uuid)
                  AND repo_full_name = :repo_full_name
              ) AS repo_exists,
              (
                SELECT count(*)
                FROM artifact_snapshot_github_file_samples
                WHERE snapshot_id = CAST(:snapshot_id AS uuid)
              ) AS file_sample_count
            """
        ),
        {
            "artifact_id": str(artifact_id),
            "snapshot_id": str(snapshot_id),
            "snapshot_type": github_fixture.snapshot_type,
            "status": github_fixture.status,
            "content_anchor": github_fixture.content_anchor,
            "repo_full_name": github_fixture.repo_full_name,
        },
    ).mappings().one()
    return bool(row["parent_exists"]) and bool(row["repo_exists"]) and int(row["file_sample_count"]) >= len(
        github_fixture.file_samples
    )


def _load_existing_bundle_id(
    connection: Any,
    *,
    candidate_group_id: UUID,
    bundle_input_hash: str,
) -> UUID | None:
    import sqlalchemy as sa

    row = connection.execute(
        sa.text(
            """
            SELECT bundle_id
            FROM candidate_evidence_bundles
            WHERE candidate_group_id = CAST(:candidate_group_id AS uuid)
              AND bundle_profile_version = :bundle_profile_version
              AND bundle_input_hash = :bundle_input_hash
            LIMIT 1
            """
        ),
        {
            "candidate_group_id": str(candidate_group_id),
            "bundle_profile_version": BUNDLE_PROFILE_VERSION,
            "bundle_input_hash": bundle_input_hash,
        },
    ).mappings().first()
    return UUID(str(row["bundle_id"])) if row else None


def _insert_evidence_bundle(
    connection: Any,
    *,
    candidate: CandidateGroupRecord,
    bundle_input_hash: str,
    primary_summary: dict[str, Any],
    evidence_limitations: list[str],
    ready_for_analysis: bool,
) -> UUID:
    import sqlalchemy as sa

    bundle_version = _next_bundle_version(connection, candidate.candidate_group_id)
    result = connection.execute(
        sa.text(
            """
            INSERT INTO candidate_evidence_bundles (
                candidate_group_id,
                initial_primary_artifact_id,
                current_primary_artifact_id,
                bundle_version,
                bundle_profile_version,
                bundle_input_hash,
                reroot_count,
                primary_summary,
                supporting_summaries_json,
                discovered_links_summary_json,
                evidence_limitations,
                ready_for_analysis,
                token_budget_profile,
                created_at
            ) VALUES (
                CAST(:candidate_group_id AS uuid),
                CAST(:initial_primary_artifact_id AS uuid),
                CAST(:current_primary_artifact_id AS uuid),
                :bundle_version,
                :bundle_profile_version,
                :bundle_input_hash,
                0,
                CAST(:primary_summary AS jsonb),
                CAST(:supporting_summaries_json AS jsonb),
                CAST(:discovered_links_summary_json AS jsonb),
                CAST(:evidence_limitations AS jsonb),
                :ready_for_analysis,
                :token_budget_profile,
                now()
            )
            ON CONFLICT (candidate_group_id, bundle_profile_version, bundle_input_hash)
            DO UPDATE SET bundle_id = candidate_evidence_bundles.bundle_id
            RETURNING bundle_id
            """
        ),
        {
            "candidate_group_id": str(candidate.candidate_group_id),
            "initial_primary_artifact_id": str(candidate.initial_primary_artifact_id),
            "current_primary_artifact_id": str(candidate.current_primary_artifact_id),
            "bundle_version": bundle_version,
            "bundle_profile_version": BUNDLE_PROFILE_VERSION,
            "bundle_input_hash": bundle_input_hash,
            "primary_summary": _json_dumps(primary_summary),
            "supporting_summaries_json": _json_dumps([]),
            "discovered_links_summary_json": _json_dumps([]),
            "evidence_limitations": _json_dumps(evidence_limitations),
            "ready_for_analysis": ready_for_analysis,
            "token_budget_profile": TOKEN_BUDGET_PROFILE,
        },
    )
    return UUID(str(result.scalar_one()))


def _next_bundle_version(connection: Any, candidate_group_id: UUID) -> int:
    import sqlalchemy as sa

    return int(
        connection.execute(
            sa.text(
                """
                SELECT COALESCE(MAX(bundle_version), 0) + 1
                FROM candidate_evidence_bundles
                WHERE candidate_group_id = CAST(:candidate_group_id AS uuid)
                """
            ),
            {"candidate_group_id": str(candidate_group_id)},
        ).scalar_one()
    )


def _insert_or_reuse_evidence_member(
    connection: Any,
    *,
    bundle_id: UUID,
    artifact_id: UUID,
    snapshot_id: UUID,
) -> None:
    import sqlalchemy as sa

    connection.execute(
        sa.text(
            """
            INSERT INTO candidate_evidence_members (
                candidate_evidence_member_id,
                bundle_id,
                artifact_id,
                snapshot_id,
                member_role,
                member_order
            ) VALUES (
                gen_random_uuid(),
                CAST(:bundle_id AS uuid),
                CAST(:artifact_id AS uuid),
                CAST(:snapshot_id AS uuid),
                'primary',
                0
            )
            ON CONFLICT (bundle_id, artifact_id, snapshot_id, member_role) DO NOTHING
            """
        ),
        {"bundle_id": str(bundle_id), "artifact_id": str(artifact_id), "snapshot_id": str(snapshot_id)},
    )


def _update_current_bundle(connection: Any, *, candidate_group_id: UUID, bundle_id: UUID) -> None:
    import sqlalchemy as sa

    connection.execute(
        sa.text(
            """
            UPDATE candidate_group_proposals
            SET current_bundle_id = CAST(:bundle_id AS uuid),
                updated_at = now()
            WHERE candidate_group_id = CAST(:candidate_group_id AS uuid)
            """
        ),
        {"candidate_group_id": str(candidate_group_id), "bundle_id": str(bundle_id)},
    )


def _insert_or_reuse_analysis_requested_outbox(
    connection: Any,
    *,
    replay_namespace: str,
    candidate_group_id: UUID,
    bundle_id: UUID,
    judge_profile: str,
) -> None:
    import sqlalchemy as sa

    payload = {
        "candidate_group_id": str(candidate_group_id),
        "bundle_id": str(bundle_id),
        "judge_profile": judge_profile,
        "escalation_allowed": True,
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
            ) VALUES (
                :event_type,
                'candidate_group',
                CAST(:candidate_group_id AS uuid),
                :dedupe_key,
                CAST(:payload_json AS jsonb),
                'pending'::outbox_status_enum,
                now()
            )
            ON CONFLICT (dedupe_key) DO NOTHING
            """
        ),
        {
            "event_type": ANALYSIS_REQUESTED_EVENT_TYPE,
            "candidate_group_id": str(candidate_group_id),
            "dedupe_key": build_analysis_requested_dedupe_key(
                replay_namespace=replay_namespace,
                candidate_group_id=candidate_group_id,
                bundle_id=bundle_id,
            ),
            "payload_json": _json_dumps(payload),
        },
    )


def _verify_evidence_rows(
    connection: Any,
    *,
    candidate_group_id: UUID,
    bundle_id: UUID,
    artifact_id: UUID,
    snapshot_id: UUID,
    replay_namespace: str,
) -> dict[str, Any]:
    import sqlalchemy as sa

    row = connection.execute(
        sa.text(
            """
            SELECT
              EXISTS (
                SELECT 1
                FROM candidate_evidence_bundles
                WHERE bundle_id = CAST(:bundle_id AS uuid)
                  AND candidate_group_id = CAST(:candidate_group_id AS uuid)
                  AND bundle_profile_version = :bundle_profile_version
                  AND ready_for_analysis IS TRUE
                  AND token_budget_profile = :token_budget_profile
              ) AS bundle_exists,
              (
                SELECT count(*)
                FROM candidate_evidence_members
                WHERE bundle_id = CAST(:bundle_id AS uuid)
                  AND artifact_id = CAST(:artifact_id AS uuid)
                  AND snapshot_id = CAST(:snapshot_id AS uuid)
                  AND member_role = 'primary'
              ) AS primary_member_count,
              EXISTS (
                SELECT 1
                FROM candidate_group_proposals
                WHERE candidate_group_id = CAST(:candidate_group_id AS uuid)
                  AND current_bundle_id = CAST(:bundle_id AS uuid)
              ) AS current_bundle_updated,
              EXISTS (
                SELECT 1
                FROM event_outbox
                WHERE event_type = :event_type
                  AND aggregate_type = 'candidate_group'
                  AND aggregate_id = CAST(:candidate_group_id AS uuid)
                  AND dedupe_key = :dedupe_key
              ) AS analysis_event_exists
            """
        ),
        {
            "bundle_id": str(bundle_id),
            "candidate_group_id": str(candidate_group_id),
            "artifact_id": str(artifact_id),
            "snapshot_id": str(snapshot_id),
            "bundle_profile_version": BUNDLE_PROFILE_VERSION,
            "token_budget_profile": TOKEN_BUDGET_PROFILE,
            "event_type": ANALYSIS_REQUESTED_EVENT_TYPE,
            "dedupe_key": build_analysis_requested_dedupe_key(
                replay_namespace=replay_namespace,
                candidate_group_id=candidate_group_id,
                bundle_id=bundle_id,
            ),
        },
    ).mappings().one()
    checks = {
        "evidence_bundle_created_or_reused": bool(row["bundle_exists"]),
        "candidate_evidence_members_created_or_reused": int(row["primary_member_count"]) == 1,
        "candidate_current_bundle_updated": bool(row["current_bundle_updated"]),
        "analysis_requested_event_created": bool(row["analysis_event_exists"]),
    }
    failures = [f"{key}:missing" for key, value in checks.items() if value is not True]
    return {**checks, "checks_failed": failures}


def _build_primary_summary(snapshot: CurrentGitHubSnapshot) -> dict[str, Any]:
    headline = snapshot.normalized_projection.get("description") or snapshot.readme_excerpt
    summary = {
        "artifact_id": str(snapshot.artifact_id),
        "snapshot_id": str(snapshot.snapshot_id),
        "provider": snapshot.provider,
        "snapshot_type": snapshot.snapshot_type,
        "status": snapshot.status,
        "content_anchor": snapshot.content_anchor,
        "repo_full_name": snapshot.repo_full_name,
    }
    if headline:
        summary["headline"] = headline
    for key in ("key_paths_json", "test_paths_json", "ci_paths_json", "examples_paths_json", "docs_paths_json"):
        value = getattr(snapshot, key)
        if value:
            summary[key.removesuffix("_json")] = value
    return summary


def _predecessor_snapshot_confirmed(report: Mapping[str, Any]) -> bool:
    return all(
        report.get(key) is True
        for key in (
            "artifact_snapshot_created_or_reused",
            "github_repo_snapshot_created_or_reused",
            "github_file_samples_created_or_reused",
            "artifact_current_snapshot_updated",
            "snapshot_updated_outbox_event_created",
        )
    )


def _predecessor_result_acceptable(predecessor: github_snapshot_runner.RunnerResult) -> bool:
    if predecessor.report.get("status") == "pass" and predecessor.exit_code == 0:
        return True
    allowed_successor_failures = {"evidence_bundle_created:unexpected"}
    checks_failed = set(predecessor.report.get("checks_failed") or [])
    return bool(checks_failed) and checks_failed.issubset(allowed_successor_failures)


def _execution_result(
    *,
    source_candidate_replay_confirmed: bool = False,
    artifact_snapshot_replay_confirmed: bool = False,
    snapshot_updated_event_found: bool = False,
    candidate_refresh_target_resolved: bool = False,
    current_artifact_snapshot_loaded: bool = False,
    checks_failed: list[str] | tuple[str, ...],
) -> ReplayExecutionResult:
    return ReplayExecutionResult(
        source_candidate_replay_confirmed=source_candidate_replay_confirmed,
        artifact_snapshot_replay_confirmed=artifact_snapshot_replay_confirmed,
        snapshot_updated_event_found=snapshot_updated_event_found,
        candidate_refresh_target_resolved=candidate_refresh_target_resolved,
        current_artifact_snapshot_loaded=current_artifact_snapshot_loaded,
        evidence_bundle_created_or_reused=False,
        candidate_evidence_members_created_or_reused=False,
        candidate_current_bundle_updated=False,
        analysis_requested_event_created=False,
        judge_profile_resolved=False,
        ready_for_analysis=False,
        checks_failed=tuple(dict.fromkeys(checks_failed)),
    )


def _canonicalize_for_hash(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {str(key): _canonicalize_for_hash(value[key]) for key in sorted(value)}
    if isinstance(value, tuple):
        return _canonicalize_for_hash(list(value))
    if isinstance(value, list):
        canonical_items = [_canonicalize_for_hash(item) for item in value]
        return sorted(canonical_items, key=lambda item: json.dumps(item, ensure_ascii=False, sort_keys=True))
    if isinstance(value, UUID):
        return str(value)
    return value


def _base_report() -> dict[str, Any]:
    return {
        "schema_version": SCHEMA_VERSION,
        "status": "pass",
        "database_url_guard_passed": False,
        "source_candidate_replay_confirmed": False,
        "artifact_snapshot_replay_confirmed": False,
        "snapshot_updated_event_found": False,
        "candidate_refresh_target_resolved": False,
        "current_artifact_snapshot_loaded": False,
        "evidence_bundle_created_or_reused": False,
        "candidate_evidence_members_created_or_reused": False,
        "candidate_current_bundle_updated": False,
        "analysis_requested_event_created": False,
        "judge_profile_resolved": False,
        "ready_for_analysis": False,
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


def _finish(report: dict[str, Any], checks_failed: Sequence[str]) -> RunnerResult:
    normalized_failures = list(dict.fromkeys(checks_failed))
    report["checks_failed"] = normalized_failures
    report["status"] = "fail" if normalized_failures else "pass"
    return RunnerResult(exit_code=0 if report["status"] == "pass" else 1, report=report)


def _json_loads(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, str):
        return json.loads(value)
    return value


def _json_dumps(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True)


def _uuid_or_none(value: Any) -> UUID | None:
    if value is None:
        return None
    return UUID(str(value))


def _safe_failure_code(exc: Exception) -> str:
    message = str(exc)
    if message in {"precondition_missing"}:
        return message
    return exc.__class__.__name__


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
