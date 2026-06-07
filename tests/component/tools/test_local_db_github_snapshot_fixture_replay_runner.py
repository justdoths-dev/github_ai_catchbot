from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path
from uuid import uuid4

import pytest
import sqlalchemy as sa

from tools import local_db_github_snapshot_fixture_replay_runner as runner
from tools import local_db_source_candidate_replay_runner as source_runner


ROOT = Path(__file__).resolve().parents[3]
SOURCE_FIXTURE = ROOT / "tests/fixtures/upstream/source_message_github_repo_signal.json"
GITHUB_FIXTURE = ROOT / "tests/fixtures/upstream/github_repo_snapshot_example_tool.json"
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
    reason="LOCAL_TEST_DATABASE_URL is required for the DB-backed GitHub snapshot replay component test",
)


def test_cli_writes_fixture_backed_github_snapshot_after_source_candidate_replay() -> None:
    database_url = os.environ["LOCAL_TEST_DATABASE_URL"]
    namespace = f"component-local-db-github-snapshot-{uuid4().hex}"
    before = _fetch_downstream_counts(database_url)

    first = _run_cli(database_url=database_url, namespace=namespace)
    first_rows = _fetch_durable_rows(database_url=database_url, namespace=namespace)
    second = _run_cli(database_url=database_url, namespace=namespace)
    second_rows = _fetch_durable_rows(database_url=database_url, namespace=namespace)
    after = _fetch_downstream_counts(database_url)

    for result in (first, second):
        assert result.returncode == 0
        assert result.stderr == ""
        assert database_url not in result.stdout
        report = json.loads(result.stdout)
        assert report == {
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

    assert first_rows == {
        "source_messages": 1,
        "source_message_versions": 1,
        "artifact_registry": 1,
        "candidate_group_proposals": 1,
        "candidate_group_members": 1,
        "enrich_requested_events": 1,
        "artifact_snapshots": 1,
        "artifact_snapshot_github_repo": 1,
        "artifact_snapshot_github_file_samples": 3,
        "artifact_current_snapshot": 1,
        "snapshot_updated_outbox_events": 1,
        "candidate_evidence_members_for_snapshot": 0,
        "namespace_analysis_requested_events": 0,
        "namespace_notification_events": 0,
    }
    assert second_rows == first_rows
    assert after == before
    _assert_no_event_name_files()


def _run_cli(*, database_url: str, namespace: str) -> subprocess.CompletedProcess[str]:
    env = dict(os.environ)
    env["APP_ENV"] = "test"
    return subprocess.run(
        [
            sys.executable,
            "-m",
            "tools.local_db_github_snapshot_fixture_replay_runner",
            "--database-url",
            database_url,
            "--source-fixture",
            str(SOURCE_FIXTURE.relative_to(ROOT)),
            "--github-snapshot-fixture",
            str(GITHUB_FIXTURE.relative_to(ROOT)),
            "--replay-namespace",
            namespace,
            "--confirm-local-test-db",
        ],
        check=False,
        capture_output=True,
        cwd=ROOT,
        env=env,
        text=True,
        timeout=45,
    )


def _fetch_durable_rows(*, database_url: str, namespace: str) -> dict[str, int]:
    source_fixture = source_runner.load_source_fixture(SOURCE_FIXTURE, repo_root=ROOT)
    github_fixture = runner.load_github_snapshot_fixture(GITHUB_FIXTURE, repo_root=ROOT)
    normalizer_version = source_runner.build_normalizer_version(namespace)
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
                {"canonical_id": github_fixture.artifact_canonical_id},
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
                    "canonical_id": github_fixture.artifact_canonical_id,
                },
            )
            snapshot_id = _scalar(
                connection,
                """
                SELECT snapshot_id
                FROM artifact_snapshots
                WHERE artifact_id = CAST(:artifact_id AS uuid)
                  AND provider = 'github'
                  AND snapshot_type = :snapshot_type
                  AND content_anchor = :content_anchor
                """,
                {
                    "artifact_id": str(artifact_id),
                    "snapshot_type": github_fixture.snapshot_type,
                    "content_anchor": github_fixture.content_anchor,
                },
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
                        "event_type": runner.ENRICH_EVENT_TYPE,
                        "artifact_id": str(artifact_id),
                        "dedupe_prefix": f"local-db-source-candidate:{namespace}:artifact.enrich:%",
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
                    {"snapshot_id": str(snapshot_id), "repo_full_name": github_fixture.repo_full_name},
                ),
                "artifact_snapshot_github_file_samples": _count(
                    connection,
                    """
                    SELECT count(*)
                    FROM artifact_snapshot_github_file_samples
                    WHERE snapshot_id = CAST(:snapshot_id AS uuid)
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
                      AND current_status = CAST(:status AS snapshot_status_enum)
                    """,
                    {
                        "artifact_id": str(artifact_id),
                        "snapshot_id": str(snapshot_id),
                        "status": github_fixture.status,
                    },
                ),
                "snapshot_updated_outbox_events": _count(
                    connection,
                    """
                    SELECT count(*)
                    FROM event_outbox
                    WHERE event_type = :event_type
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
                "candidate_evidence_members_for_snapshot": _count(
                    connection,
                    """
                    SELECT count(*)
                    FROM candidate_evidence_members
                    WHERE snapshot_id = CAST(:snapshot_id AS uuid)
                    """,
                    {"snapshot_id": str(snapshot_id)},
                ),
                "namespace_analysis_requested_events": _count(
                    connection,
                    """
                    SELECT count(*)
                    FROM event_outbox
                    WHERE event_type = 'analysis.requested.v1'
                      AND dedupe_key LIKE :dedupe_prefix
                    """,
                    {"dedupe_prefix": f"local-db-github-snapshot:{namespace}:%"},
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
                    {"dedupe_prefix": f"local-db-github-snapshot:{namespace}:%"},
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
