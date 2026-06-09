from __future__ import annotations

import base64
import json
import os
from pathlib import Path
from uuid import uuid4

import pytest
import sqlalchemy as sa

from tools import local_db_restricted_github_read_canary_runner as runner


ROOT = Path(__file__).resolve().parents[3]
SOURCE_FIXTURE = ROOT / "tests/fixtures/upstream/source_message_github_public_repo_octocat_hello_world.json"
REPO_FULL_NAME = "octocat/Hello-World"
COMMIT_SHA = "abcd1234abcd1234abcd1234abcd1234abcd1234"
EVENT_NAME_FILES = (
    "source_message.created.v1",
    "source_message.edited.v1",
    "source_message.deleted.v1",
    "source_message.reconciled.v1",
    "artifact.enrich.requested.v1",
    "artifact.snapshot.updated.v1",
    "candidate.bundle.refresh.v1",
    "analysis.requested.v1",
    "judge.call.requested.v1",
    "judge.output.ready.v1",
    "analysis.policy.apply.v1",
    "notification.plan.created.v1",
    "notification.delivery.result.v1",
)


pytestmark = pytest.mark.skipif(
    not os.environ.get("LOCAL_TEST_DATABASE_URL"),
    reason="LOCAL_TEST_DATABASE_URL is required for the restricted GitHub read canary component test",
)


class RecordingHttpGet:
    def __init__(self) -> None:
        self.calls: list[tuple[str, float]] = []

    def __call__(self, url: str, *, timeout_seconds: float) -> runner.GitHubHttpResponse:
        self.calls.append((url, timeout_seconds))
        if url == "https://api.github.com/repos/octocat/Hello-World":
            return _repo_response()
        if url == "https://api.github.com/repos/octocat/Hello-World/commits/master":
            return runner.GitHubHttpResponse(status_code=200, json_payload={"sha": COMMIT_SHA})
        if url == "https://api.github.com/repos/octocat/Hello-World/readme":
            return _readme_response()
        raise AssertionError(f"unexpected GitHub URL: {url}")


def test_restricted_github_read_canary_writes_snapshot_rows_idempotently_with_fake_http() -> None:
    database_url = os.environ["LOCAL_TEST_DATABASE_URL"]
    namespace = f"component-restricted-github-read-{uuid4().hex}"
    http = RecordingHttpGet()
    before = _fetch_downstream_counts(database_url)

    first = _run_canary(database_url=database_url, namespace=namespace, http=http)
    first_rows = _fetch_durable_rows(database_url=database_url, namespace=namespace)
    second = _run_canary(database_url=database_url, namespace=namespace, http=http)
    second_rows = _fetch_durable_rows(database_url=database_url, namespace=namespace)
    after = _fetch_downstream_counts(database_url)

    for result in (first, second):
        assert result.exit_code == 0
        assert database_url not in runner.render_json(result.report)
        assert result.report == {
            "schema_version": "local_db_restricted_github_read_canary_v1",
            "status": "pass",
            "database_url_guard_passed": True,
            "network_read_authorized": True,
            "github_api_base_url_allowed": True,
            "repo_full_name": "octocat/Hello-World",
            "source_candidate_replay_confirmed": True,
            "candidate_group_loaded": True,
            "github_artifact_loaded": True,
            "artifact_matches_requested_repo": True,
            "github_repo_metadata_fetched": True,
            "github_default_branch_commit_fetched": True,
            "github_readme_fetched": True,
            "artifact_enrichment_run_created": True,
            "artifact_snapshot_created": True,
            "github_repo_child_snapshot_created": True,
            "github_readme_file_sample_created": True,
            "artifact_current_snapshot_updated": True,
            "artifact_snapshot_updated_event_created": True,
            "live_github_read_called": False,
            "github_http_get_called": True,
            "github_write_called": False,
            "telegram_called": False,
            "openai_called": False,
            "workers_started": False,
            "redis_mutation": False,
            "production_db_write": False,
            "alembic_or_ddl_ran": False,
            "checks_failed": [],
        }

    assert [call[0] for call in http.calls] == [
        "https://api.github.com/repos/octocat/Hello-World",
        "https://api.github.com/repos/octocat/Hello-World/commits/master",
        "https://api.github.com/repos/octocat/Hello-World/readme",
        "https://api.github.com/repos/octocat/Hello-World",
        "https://api.github.com/repos/octocat/Hello-World/commits/master",
        "https://api.github.com/repos/octocat/Hello-World/readme",
    ]
    assert first_rows == {
        "source_messages": 1,
        "source_message_versions": 1,
        "artifact_registry": 1,
        "candidate_group_proposals": 1,
        "candidate_group_members": 1,
        "enrich_requested_events": 1,
        "artifact_enrichment_runs": 1,
        "artifact_snapshots": 1,
        "artifact_snapshot_github_repo": 1,
        "artifact_snapshot_github_file_samples": 1,
        "artifact_current_snapshot": 1,
        "snapshot_updated_outbox_events": 1,
        "candidate_evidence_bundles_for_group": 0,
        "namespace_analysis_requested_events": 0,
        "namespace_notification_events": 0,
    }
    assert second_rows == first_rows
    assert after == before
    _assert_no_event_name_files()


def _run_canary(*, database_url: str, namespace: str, http: RecordingHttpGet) -> runner.RunnerResult:
    args = runner.build_parser().parse_args(
        [
            "--database-url",
            database_url,
            "--source-fixture",
            str(SOURCE_FIXTURE.relative_to(ROOT)),
            "--replay-namespace",
            namespace,
            "--repo-full-name",
            REPO_FULL_NAME,
            "--confirm-local-test-db",
            "--allow-network-read",
        ]
    )
    return runner.run(args, env={"APP_ENV": "test"}, http_get=http, repo_root=ROOT)


def _repo_response() -> runner.GitHubHttpResponse:
    return runner.GitHubHttpResponse(
        status_code=200,
        json_payload={
            "full_name": REPO_FULL_NAME,
            "default_branch": "master",
            "description": "Public fixture response for restricted read canary.",
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
        },
    )


def _readme_response() -> runner.GitHubHttpResponse:
    text = "Hello World\nThis README excerpt is served by injected fake GitHub GET."
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


def _fetch_durable_rows(*, database_url: str, namespace: str) -> dict[str, int]:
    source_fixture = runner.source_candidate_runner.load_source_fixture(SOURCE_FIXTURE, repo_root=ROOT)
    canonical_id = runner.build_expected_artifact_canonical_id(REPO_FULL_NAME)
    normalizer_version = runner.source_candidate_runner.build_normalizer_version(namespace)
    content_anchor = f"commit:{COMMIT_SHA}"
    engine = sa.create_engine(database_url, future=True)
    try:
        with engine.connect() as connection:
            artifact_id = _scalar(
                connection,
                """
                SELECT artifact_id
                FROM artifact_registry
                WHERE canonical_id = :canonical_id
                """,
                {"canonical_id": canonical_id},
            )
            candidate_group_id = _scalar(
                connection,
                """
                SELECT candidate_group_id
                FROM candidate_group_proposals
                WHERE source_message_id = CAST(:source_message_id AS uuid)
                  AND source_version_no = :source_version_no
                  AND normalizer_version = :normalizer_version
                  AND dedupe_subject_key = :canonical_id
                """,
                {
                    "source_message_id": str(source_fixture.source_message_id),
                    "source_version_no": source_fixture.source_version_no,
                    "normalizer_version": normalizer_version,
                    "canonical_id": canonical_id,
                },
            )
            snapshot_id = _scalar(
                connection,
                """
                SELECT snapshot_id
                FROM artifact_snapshots
                WHERE artifact_id = CAST(:artifact_id AS uuid)
                  AND provider = 'github'
                  AND snapshot_type = 'github_repo'
                  AND content_anchor = :content_anchor
                """,
                {"artifact_id": str(artifact_id), "content_anchor": content_anchor},
            )
            return {
                "source_messages": _count(
                    connection,
                    """
                    SELECT count(*)
                    FROM source_messages
                    WHERE source_message_id = CAST(:source_message_id AS uuid)
                      AND current_version_no = :source_version_no
                    """,
                    {
                        "source_message_id": str(source_fixture.source_message_id),
                        "source_version_no": source_fixture.source_version_no,
                    },
                ),
                "source_message_versions": _count(
                    connection,
                    """
                    SELECT count(*)
                    FROM source_message_versions
                    WHERE source_message_id = CAST(:source_message_id AS uuid)
                      AND version_no = :source_version_no
                    """,
                    {
                        "source_message_id": str(source_fixture.source_message_id),
                        "source_version_no": source_fixture.source_version_no,
                    },
                ),
                "artifact_registry": 1 if artifact_id else 0,
                "candidate_group_proposals": 1 if candidate_group_id else 0,
                "candidate_group_members": _count(
                    connection,
                    """
                    SELECT count(*)
                    FROM candidate_group_members
                    WHERE candidate_group_id = CAST(:candidate_group_id AS uuid)
                      AND artifact_id = CAST(:artifact_id AS uuid)
                      AND member_role = 'primary'
                    """,
                    {"candidate_group_id": str(candidate_group_id), "artifact_id": str(artifact_id)},
                ),
                "enrich_requested_events": _count(
                    connection,
                    """
                    SELECT count(*)
                    FROM event_outbox
                    WHERE event_type = :event_type
                      AND aggregate_id = CAST(:artifact_id AS uuid)
                      AND dedupe_key LIKE :dedupe_prefix
                    """,
                    {
                        "event_type": runner.source_candidate_runner.ENRICH_EVENT_TYPE,
                        "artifact_id": str(artifact_id),
                        "dedupe_prefix": f"local-db-source-candidate:{namespace}:artifact.enrich:%",
                    },
                ),
                "artifact_enrichment_runs": _count(
                    connection,
                    """
                    SELECT count(*)
                    FROM artifact_enrichment_runs
                    WHERE artifact_id = CAST(:artifact_id AS uuid)
                      AND provider = 'github'
                      AND refresh_mode = 'restricted_read_canary'
                      AND job_idempotency_key = :dedupe_key
                    """,
                    {
                        "artifact_id": str(artifact_id),
                        "dedupe_key": runner.build_artifact_enrichment_run_dedupe_key(
                            replay_namespace=namespace,
                            artifact_id=artifact_id,
                            content_anchor=content_anchor,
                        ),
                    },
                ),
                "artifact_snapshots": 1 if snapshot_id else 0,
                "artifact_snapshot_github_repo": _count(
                    connection,
                    """
                    SELECT count(*)
                    FROM artifact_snapshot_github_repo
                    WHERE snapshot_id = CAST(:snapshot_id AS uuid)
                      AND repo_full_name = :repo_full_name
                    """,
                    {"snapshot_id": str(snapshot_id), "repo_full_name": REPO_FULL_NAME},
                ),
                "artifact_snapshot_github_file_samples": _count(
                    connection,
                    """
                    SELECT count(*)
                    FROM artifact_snapshot_github_file_samples
                    WHERE snapshot_id = CAST(:snapshot_id AS uuid)
                      AND path = 'README.md'
                      AND role = 'README'
                    """,
                    {"snapshot_id": str(snapshot_id)},
                ),
                "artifact_current_snapshot": _count(
                    connection,
                    """
                    SELECT count(*)
                    FROM artifact_registry
                    WHERE artifact_id = CAST(:artifact_id AS uuid)
                      AND current_snapshot_id = CAST(:snapshot_id AS uuid)
                      AND current_status = 'ready'::snapshot_status_enum
                    """,
                    {"artifact_id": str(artifact_id), "snapshot_id": str(snapshot_id)},
                ),
                "snapshot_updated_outbox_events": _count(
                    connection,
                    """
                    SELECT count(*)
                    FROM event_outbox
                    WHERE event_type = :event_type
                      AND aggregate_type = 'artifact'
                      AND aggregate_id = CAST(:artifact_id AS uuid)
                      AND dedupe_key = :dedupe_key
                    """,
                    {
                        "event_type": runner.SNAPSHOT_UPDATED_EVENT_TYPE,
                        "artifact_id": str(artifact_id),
                        "dedupe_key": runner.build_snapshot_updated_dedupe_key(
                            replay_namespace=namespace,
                            artifact_id=artifact_id,
                            snapshot_id=snapshot_id,
                        ),
                    },
                ),
                "candidate_evidence_bundles_for_group": _count(
                    connection,
                    """
                    SELECT count(*)
                    FROM candidate_evidence_bundles
                    WHERE candidate_group_id = CAST(:candidate_group_id AS uuid)
                    """,
                    {"candidate_group_id": str(candidate_group_id)},
                ),
                "namespace_analysis_requested_events": _count(
                    connection,
                    """
                    SELECT count(*)
                    FROM event_outbox
                    WHERE event_type = 'analysis.requested.v1'
                      AND dedupe_key LIKE :dedupe_prefix
                    """,
                    {"dedupe_prefix": f"{runner.ENRICHMENT_RUN_DEDUPE_PREFIX}:{namespace}:%"},
                ),
                "namespace_notification_events": _count(
                    connection,
                    """
                    SELECT count(*)
                    FROM event_outbox
                    WHERE event_type IN (
                        'notification.plan.created.v1',
                        'notification.delivery.result.v1'
                    )
                      AND dedupe_key LIKE :dedupe_prefix
                    """,
                    {"dedupe_prefix": f"{runner.ENRICHMENT_RUN_DEDUPE_PREFIX}:{namespace}:%"},
                ),
            }
    finally:
        engine.dispose()


def _fetch_downstream_counts(database_url: str) -> dict[str, int]:
    engine = sa.create_engine(database_url, future=True)
    try:
        with engine.connect() as connection:
            return {
                "candidate_evidence_bundles": _count(connection, "SELECT count(*) FROM candidate_evidence_bundles", {}),
                "candidate_evidence_members": _count(connection, "SELECT count(*) FROM candidate_evidence_members", {}),
                "analysis_requested_events": _count(
                    connection,
                    "SELECT count(*) FROM event_outbox WHERE event_type = 'analysis.requested.v1'",
                    {},
                ),
                "judge_runs": _count(connection, "SELECT count(*) FROM judge_runs", {}),
                "judge_outputs": _count(connection, "SELECT count(*) FROM judge_outputs", {}),
                "analyses": _count(connection, "SELECT count(*) FROM analyses", {}),
                "notification_plans": _count(connection, "SELECT count(*) FROM notification_plans", {}),
                "notification_renders": _count(connection, "SELECT count(*) FROM notification_renders", {}),
                "notification_delivery_records": _count(
                    connection,
                    "SELECT count(*) FROM notification_delivery_records",
                    {},
                ),
            }
    finally:
        engine.dispose()


def _scalar(connection: sa.Connection, sql: str, params: dict[str, object]):
    return connection.execute(sa.text(sql), params).scalar_one_or_none()


def _count(connection: sa.Connection, sql: str, params: dict[str, object]) -> int:
    return int(connection.execute(sa.text(sql), params).scalar_one())


def _assert_no_event_name_files() -> None:
    for name in EVENT_NAME_FILES:
        assert not (ROOT / name).exists(), name
