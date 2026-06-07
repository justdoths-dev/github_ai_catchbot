from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path
from uuid import uuid4

import pytest
import sqlalchemy as sa

from tools import local_db_source_candidate_replay_runner as runner


ROOT = Path(__file__).resolve().parents[3]
FIXTURE = ROOT / "tests/fixtures/upstream/source_message_github_repo_signal.json"
EVENT_NAME_FILES = (
    "source_message.created.v1",
    "source_message.edited.v1",
    "source_message.deleted.v1",
    "source_message.reconciled.v1",
    "artifact.enrich.requested.v1",
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
    reason="LOCAL_TEST_DATABASE_URL is required for the DB-backed replay component test",
)


def test_cli_replays_fixture_into_durable_local_db_rows_without_runtime_calls() -> None:
    database_url = os.environ["LOCAL_TEST_DATABASE_URL"]
    namespace = f"component-local-db-source-candidate-{uuid4().hex}"

    first = _run_cli(database_url=database_url, namespace=namespace)
    second = _run_cli(database_url=database_url, namespace=namespace)

    for result in (first, second):
        assert result.returncode == 0
        assert result.stderr == ""
        assert database_url not in result.stdout
        report = json.loads(result.stdout)
        assert report == {
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

    rows = _fetch_durable_rows(database_url=database_url, namespace=namespace)
    assert rows == {
        "source_messages": 1,
        "source_message_versions": 1,
        "source_outbox_events": 1,
        "normalization_runs": 1,
        "artifact_registry": 1,
        "artifact_observations": 1,
        "candidate_group_proposals": 1,
        "candidate_group_members": 1,
        "enrich_requested_events": 1,
    }
    _assert_no_event_name_files()


def _run_cli(*, database_url: str, namespace: str) -> subprocess.CompletedProcess[str]:
    env = dict(os.environ)
    env["APP_ENV"] = "test"
    return subprocess.run(
        [
            sys.executable,
            "-m",
            "tools.local_db_source_candidate_replay_runner",
            "--database-url",
            database_url,
            "--fixture",
            str(FIXTURE.relative_to(ROOT)),
            "--replay-namespace",
            namespace,
            "--confirm-local-test-db",
        ],
        check=False,
        capture_output=True,
        cwd=ROOT,
        env=env,
        text=True,
        timeout=30,
    )


def _fetch_durable_rows(*, database_url: str, namespace: str) -> dict[str, int]:
    fixture = runner.load_source_fixture(FIXTURE, repo_root=ROOT)
    source_dedupe_key = runner.build_source_event_dedupe_key(fixture, namespace)
    normalizer_version = runner.build_normalizer_version(namespace)
    engine = sa.create_engine(database_url, future=True)
    try:
        with engine.connect() as connection:
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
                        "source_message_id": str(fixture.source_message_id),
                        "source_version_no": fixture.source_version_no,
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
                        "source_message_id": str(fixture.source_message_id),
                        "source_version_no": fixture.source_version_no,
                    },
                ),
                "source_outbox_events": _count(
                    connection,
                    """
                    SELECT count(*)
                    FROM event_outbox
                    WHERE event_type = :event_type
                      AND dedupe_key = :dedupe_key
                    """,
                    {
                        "event_type": runner.SOURCE_EVENT_TYPE,
                        "dedupe_key": source_dedupe_key,
                    },
                ),
                "normalization_runs": _count(
                    connection,
                    """
                    SELECT count(*)
                    FROM normalization_runs
                    WHERE source_message_id = CAST(:source_message_id AS uuid)
                      AND source_version_no = :source_version_no
                      AND normalizer_version = :normalizer_version
                      AND candidate_eligible IS TRUE
                    """,
                    {
                        "source_message_id": str(fixture.source_message_id),
                        "source_version_no": fixture.source_version_no,
                        "normalizer_version": normalizer_version,
                    },
                ),
                "artifact_registry": _count(
                    connection,
                    """
                    SELECT count(*)
                    FROM artifact_registry
                    WHERE canonical_id = 'github:repo:example/example-tool'
                    """,
                    {},
                ),
                "artifact_observations": _count(
                    connection,
                    """
                    SELECT count(*)
                    FROM artifact_observations AS ao
                    JOIN artifact_registry AS ar
                      ON ar.artifact_id = ao.artifact_id
                    WHERE ao.source_message_id = CAST(:source_message_id AS uuid)
                      AND ao.source_version_no = :source_version_no
                      AND ar.canonical_id = 'github:repo:example/example-tool'
                    """,
                    {
                        "source_message_id": str(fixture.source_message_id),
                        "source_version_no": fixture.source_version_no,
                    },
                ),
                "candidate_group_proposals": _count(
                    connection,
                    """
                    SELECT count(*)
                    FROM candidate_group_proposals
                    WHERE source_message_id = CAST(:source_message_id AS uuid)
                      AND source_version_no = :source_version_no
                      AND dedupe_subject_key = 'github:repo:example/example-tool'
                    """,
                    {
                        "source_message_id": str(fixture.source_message_id),
                        "source_version_no": fixture.source_version_no,
                    },
                ),
                "candidate_group_members": _count(
                    connection,
                    """
                    SELECT count(*)
                    FROM candidate_group_members AS cgm
                    JOIN candidate_group_proposals AS cgp
                      ON cgp.candidate_group_id = cgm.candidate_group_id
                    JOIN artifact_registry AS ar
                      ON ar.artifact_id = cgm.artifact_id
                    WHERE cgp.source_message_id = CAST(:source_message_id AS uuid)
                      AND cgp.source_version_no = :source_version_no
                      AND cgp.dedupe_subject_key = 'github:repo:example/example-tool'
                      AND cgm.member_role = 'primary'
                      AND ar.canonical_id = 'github:repo:example/example-tool'
                    """,
                    {
                        "source_message_id": str(fixture.source_message_id),
                        "source_version_no": fixture.source_version_no,
                    },
                ),
                "enrich_requested_events": _count(
                    connection,
                    """
                    SELECT count(*)
                    FROM event_outbox AS eo
                    JOIN artifact_registry AS ar
                      ON ar.artifact_id = eo.aggregate_id
                    WHERE eo.event_type = :event_type
                      AND eo.dedupe_key LIKE :dedupe_prefix
                      AND ar.canonical_id = 'github:repo:example/example-tool'
                    """,
                    {
                        "event_type": runner.ENRICH_EVENT_TYPE,
                        "dedupe_prefix": f"local-db-source-candidate:{namespace}:artifact.enrich:%",
                    },
                ),
            }
    finally:
        engine.dispose()


def _count(connection: sa.Connection, sql: str, params: dict[str, object]) -> int:
    return int(connection.execute(sa.text(sql), params).scalar_one())


def _assert_no_event_name_files() -> None:
    for name in EVENT_NAME_FILES:
        assert not (ROOT / name).exists(), name
