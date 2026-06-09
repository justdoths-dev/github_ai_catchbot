from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path
from uuid import uuid4

import pytest
import sqlalchemy as sa

from tools import local_db_analysis_router_fixture_replay_runner as runner
from tools import local_db_evidence_bundle_fixture_replay_runner as evidence_runner
from tools import local_db_github_snapshot_fixture_replay_runner as github_runner
from tools import local_db_source_candidate_replay_runner as source_runner


ROOT = Path(__file__).resolve().parents[3]
SOURCE_FIXTURE = ROOT / "tests/fixtures/upstream/source_message_github_repo_signal.json"
GITHUB_FIXTURE = ROOT / "tests/fixtures/upstream/github_repo_snapshot_example_tool.json"
ZERO_UUID = "00000000-0000-0000-0000-000000000000"
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
    reason="LOCAL_TEST_DATABASE_URL is required for the DB-backed analysis-router replay component test",
)


def test_cli_writes_fixture_backed_judge_call_after_evidence_bundle_replay() -> None:
    database_url = os.environ["LOCAL_TEST_DATABASE_URL"]
    namespace = f"component-local-db-analysis-router-{uuid4().hex}"
    before_global = _fetch_global_downstream_counts(database_url, namespace=namespace)

    first = _run_cli(database_url=database_url, namespace=namespace)
    first_rows = _fetch_durable_rows(database_url=database_url, namespace=namespace)
    first_judge = _fetch_judge_run_fields(database_url=database_url, namespace=namespace)
    first_payload = _fetch_judge_call_payload(database_url=database_url, namespace=namespace)
    second = _run_cli(database_url=database_url, namespace=namespace)
    second_rows = _fetch_durable_rows(database_url=database_url, namespace=namespace)
    second_judge = _fetch_judge_run_fields(database_url=database_url, namespace=namespace)
    second_payload = _fetch_judge_call_payload(database_url=database_url, namespace=namespace)
    after_global = _fetch_global_downstream_counts(database_url, namespace=namespace)

    for result in (first, second):
        assert result.returncode == 0
        assert result.stderr == ""
        assert database_url not in result.stdout
        assert json.loads(result.stdout) == _expected_pass_report()

    assert first_rows == {
        "source_messages": 1,
        "source_message_versions": 1,
        "artifact_registry": 1,
        "candidate_group_proposals": 1,
        "candidate_group_members": 1,
        "artifact_snapshots": 1,
        "artifact_snapshot_github_repo": 1,
        "artifact_snapshot_github_file_samples": 3,
        "candidate_evidence_bundles": 1,
        "candidate_evidence_members": 1,
        "candidate_current_bundle": 1,
        "analysis_requested_events": 1,
        "judge_runs": 1,
        "judge_call_requested_events": 1,
        "judge_outputs": 0,
        "analyses": 0,
        "notification_plans": 0,
        "notification_renders": 0,
        "notification_delivery_records": 0,
    }
    assert second_rows == first_rows
    assert first_judge == {
        "status": "pending",
        "model": "gpt-5.4-mini",
        "reasoning_effort": "low",
        "prompt_version": "judge_github_primary_v1",
        "schema_version": "judge_output_v1",
        "policy_version": "verdict_policy_v1",
        "prompt_cache_key": "judge:github_primary:judge_github_primary_v1:judge_output_v1:verdict_policy_v1",
    }
    assert second_judge == first_judge
    assert first_payload == second_payload
    assert first_payload["model"] == "gpt-5.4-mini"
    assert first_payload["reasoning_effort"] == "low"
    assert first_payload["prompt_version"] == "judge_github_primary_v1"
    assert (
        first_payload["prompt_cache_key"]
        == "judge:github_primary:judge_github_primary_v1:judge_output_v1:verdict_policy_v1"
    )
    assert after_global["judge_runs"] - before_global["judge_runs"] in {0, 1}
    assert after_global["namespace_judge_call_requested_events"] == 1
    assert before_global["namespace_judge_call_requested_events"] == 0
    for table in (
        "judge_outputs",
        "analyses",
        "notification_plans",
        "notification_renders",
        "notification_delivery_records",
    ):
        assert after_global[table] == before_global[table]
    _assert_no_event_name_files()


def _run_cli(*, database_url: str, namespace: str) -> subprocess.CompletedProcess[str]:
    env = dict(os.environ)
    env["APP_ENV"] = "test"
    return subprocess.run(
        [
            sys.executable,
            "-m",
            "tools.local_db_analysis_router_fixture_replay_runner",
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
        timeout=60,
    )


def _fetch_durable_rows(*, database_url: str, namespace: str) -> dict[str, int]:
    ids = _fetch_ids(database_url=database_url, namespace=namespace)
    source_fixture = source_runner.load_source_fixture(SOURCE_FIXTURE, repo_root=ROOT)
    github_fixture = github_runner.load_github_snapshot_fixture(GITHUB_FIXTURE, repo_root=ROOT)
    analysis_dedupe = evidence_runner.build_analysis_requested_dedupe_key(
        replay_namespace=namespace,
        candidate_group_id=ids["candidate_group_id"],
        bundle_id=ids["bundle_id"],
    )
    judge_dedupe = runner.build_judge_call_requested_dedupe_key(
        replay_namespace=namespace,
        bundle_id=ids["bundle_id"],
        model="gpt-5.4-mini",
        reasoning_effort="low",
        prompt_version="judge_github_primary_v1",
    )
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
                "artifact_registry": 1 if ids["artifact_id"] != ZERO_UUID else 0,
                "candidate_group_proposals": 1 if ids["candidate_group_id"] != ZERO_UUID else 0,
                "candidate_group_members": _count(
                    connection,
                    """
                    SELECT count(*)
                    FROM candidate_group_members
                    WHERE candidate_group_id = CAST(:candidate_group_id AS uuid)
                      AND artifact_id = CAST(:artifact_id AS uuid)
                      AND member_role = 'primary'
                    """,
                    {
                        "candidate_group_id": ids["candidate_group_id"],
                        "artifact_id": ids["artifact_id"],
                    },
                ),
                "artifact_snapshots": 1 if ids["snapshot_id"] != ZERO_UUID else 0,
                "artifact_snapshot_github_repo": _count(
                    connection,
                    """
                    SELECT count(*)
                    FROM artifact_snapshot_github_repo
                    WHERE snapshot_id = CAST(:snapshot_id AS uuid)
                      AND repo_full_name = :repo_full_name
                    """,
                    {"snapshot_id": ids["snapshot_id"], "repo_full_name": github_fixture.repo_full_name},
                ),
                "artifact_snapshot_github_file_samples": _count(
                    connection,
                    """
                    SELECT count(*)
                    FROM artifact_snapshot_github_file_samples
                    WHERE snapshot_id = CAST(:snapshot_id AS uuid)
                    """,
                    {"snapshot_id": ids["snapshot_id"]},
                ),
                "candidate_evidence_bundles": _count(
                    connection,
                    """
                    SELECT count(*)
                    FROM candidate_evidence_bundles
                    WHERE bundle_id = CAST(:bundle_id AS uuid)
                      AND candidate_group_id = CAST(:candidate_group_id AS uuid)
                      AND ready_for_analysis IS TRUE
                    """,
                    {
                        "bundle_id": ids["bundle_id"],
                        "candidate_group_id": ids["candidate_group_id"],
                    },
                ),
                "candidate_evidence_members": _count(
                    connection,
                    """
                    SELECT count(*)
                    FROM candidate_evidence_members
                    WHERE bundle_id = CAST(:bundle_id AS uuid)
                      AND artifact_id = CAST(:artifact_id AS uuid)
                      AND snapshot_id = CAST(:snapshot_id AS uuid)
                      AND member_role = 'primary'
                    """,
                    {
                        "bundle_id": ids["bundle_id"],
                        "artifact_id": ids["artifact_id"],
                        "snapshot_id": ids["snapshot_id"],
                    },
                ),
                "candidate_current_bundle": _count(
                    connection,
                    """
                    SELECT count(*)
                    FROM candidate_group_proposals
                    WHERE candidate_group_id = CAST(:candidate_group_id AS uuid)
                      AND current_bundle_id = CAST(:bundle_id AS uuid)
                    """,
                    {
                        "candidate_group_id": ids["candidate_group_id"],
                        "bundle_id": ids["bundle_id"],
                    },
                ),
                "analysis_requested_events": _count(
                    connection,
                    """
                    SELECT count(*)
                    FROM event_outbox
                    WHERE event_type = :event_type
                      AND dedupe_key = :dedupe_key
                    """,
                    {"event_type": evidence_runner.ANALYSIS_REQUESTED_EVENT_TYPE, "dedupe_key": analysis_dedupe},
                ),
                "judge_runs": _count(
                    connection,
                    """
                    SELECT count(*)
                    FROM judge_runs
                    WHERE judge_run_id = CAST(:judge_run_id AS uuid)
                      AND bundle_id = CAST(:bundle_id AS uuid)
                    """,
                    {
                        "judge_run_id": ids["judge_run_id"],
                        "bundle_id": ids["bundle_id"],
                    },
                ),
                "judge_call_requested_events": _count(
                    connection,
                    """
                    SELECT count(*)
                    FROM event_outbox
                    WHERE event_type = :event_type
                      AND aggregate_id = CAST(:judge_run_id AS uuid)
                      AND dedupe_key = :dedupe_key
                    """,
                    {
                        "event_type": runner.JUDGE_CALL_REQUESTED_EVENT_TYPE,
                        "judge_run_id": ids["judge_run_id"],
                        "dedupe_key": judge_dedupe,
                    },
                ),
                "judge_outputs": _count(
                    connection,
                    """
                    SELECT count(*)
                    FROM judge_outputs
                    WHERE judge_run_id = CAST(:judge_run_id AS uuid)
                    """,
                    {"judge_run_id": ids["judge_run_id"]},
                ),
                "analyses": _count(
                    connection,
                    """
                    SELECT count(*)
                    FROM analyses
                    WHERE candidate_group_id = CAST(:candidate_group_id AS uuid)
                    """,
                    {"candidate_group_id": ids["candidate_group_id"]},
                ),
                "notification_plans": _count(
                    connection,
                    """
                    SELECT count(*)
                    FROM notification_plans
                    WHERE candidate_group_id = CAST(:candidate_group_id AS uuid)
                    """,
                    {"candidate_group_id": ids["candidate_group_id"]},
                ),
                "notification_renders": _count(
                    connection,
                    """
                    SELECT count(*)
                    FROM notification_renders nr
                    JOIN notification_plans np
                      ON np.notification_plan_id = nr.notification_plan_id
                    WHERE np.candidate_group_id = CAST(:candidate_group_id AS uuid)
                    """,
                    {"candidate_group_id": ids["candidate_group_id"]},
                ),
                "notification_delivery_records": _count(
                    connection,
                    """
                    SELECT count(*)
                    FROM notification_delivery_records ndr
                    JOIN notification_plans np
                      ON np.notification_plan_id = ndr.notification_plan_id
                    WHERE np.candidate_group_id = CAST(:candidate_group_id AS uuid)
                    """,
                    {"candidate_group_id": ids["candidate_group_id"]},
                ),
            }
    finally:
        engine.dispose()


def _fetch_ids(*, database_url: str, namespace: str) -> dict[str, str]:
    source_fixture = source_runner.load_source_fixture(SOURCE_FIXTURE, repo_root=ROOT)
    github_fixture = github_runner.load_github_snapshot_fixture(GITHUB_FIXTURE, repo_root=ROOT)
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
                    "artifact_id": str(artifact_id or ZERO_UUID),
                    "snapshot_type": github_fixture.snapshot_type,
                    "content_anchor": github_fixture.content_anchor,
                },
            )
            bundle_id = _scalar(
                connection,
                """
                SELECT current_bundle_id
                FROM candidate_group_proposals
                WHERE candidate_group_id = CAST(:candidate_group_id AS uuid)
                """,
                {"candidate_group_id": str(candidate_group_id or ZERO_UUID)},
            )
            judge_run_id = _scalar(
                connection,
                """
                SELECT judge_run_id
                FROM judge_runs
                WHERE bundle_id = CAST(:bundle_id AS uuid)
                  AND model = 'gpt-5.4-mini'
                  AND reasoning_effort = 'low'
                  AND prompt_version = 'judge_github_primary_v1'
                """,
                {"bundle_id": str(bundle_id or ZERO_UUID)},
            )
    finally:
        engine.dispose()
    return {
        "artifact_id": str(artifact_id or ZERO_UUID),
        "candidate_group_id": str(candidate_group_id or ZERO_UUID),
        "snapshot_id": str(snapshot_id or ZERO_UUID),
        "bundle_id": str(bundle_id or ZERO_UUID),
        "judge_run_id": str(judge_run_id or ZERO_UUID),
    }


def _fetch_judge_run_fields(*, database_url: str, namespace: str) -> dict[str, str]:
    ids = _fetch_ids(database_url=database_url, namespace=namespace)
    engine = sa.create_engine(database_url, future=True)
    try:
        with engine.connect() as connection:
            row = connection.execute(
                sa.text(
                    """
                    SELECT status, model, reasoning_effort, prompt_version,
                           schema_version, policy_version, prompt_cache_key
                    FROM judge_runs
                    WHERE judge_run_id = CAST(:judge_run_id AS uuid)
                    """
                ),
                {"judge_run_id": ids["judge_run_id"]},
            ).mappings().one()
            return {key: str(row[key]) for key in row.keys()}
    finally:
        engine.dispose()


def _fetch_judge_call_payload(*, database_url: str, namespace: str) -> dict[str, str]:
    ids = _fetch_ids(database_url=database_url, namespace=namespace)
    dedupe_key = runner.build_judge_call_requested_dedupe_key(
        replay_namespace=namespace,
        bundle_id=ids["bundle_id"],
        model="gpt-5.4-mini",
        reasoning_effort="low",
        prompt_version="judge_github_primary_v1",
    )
    engine = sa.create_engine(database_url, future=True)
    try:
        with engine.connect() as connection:
            payload = _scalar(
                connection,
                """
                SELECT payload_json
                FROM event_outbox
                WHERE event_type = :event_type
                  AND dedupe_key = :dedupe_key
                """,
                {
                    "event_type": runner.JUDGE_CALL_REQUESTED_EVENT_TYPE,
                    "dedupe_key": dedupe_key,
                },
            )
            if isinstance(payload, str):
                return json.loads(payload)
            return dict(payload or {})
    finally:
        engine.dispose()


def _fetch_global_downstream_counts(database_url: str, *, namespace: str) -> dict[str, int]:
    engine = sa.create_engine(database_url, future=True)
    try:
        with engine.connect() as connection:
            return {
                "judge_runs": _count(connection, "SELECT count(*) FROM judge_runs", {}),
                "namespace_judge_call_requested_events": _count(
                    connection,
                    """
                    SELECT count(*)
                    FROM event_outbox
                    WHERE event_type = 'judge.call.requested.v1'
                      AND dedupe_key LIKE :dedupe_prefix
                    """,
                    {"dedupe_prefix": f"local-db-analysis-router:{namespace}:judge.call.requested:%"},
                ),
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


def _expected_pass_report() -> dict[str, object]:
    return {
        "schema_version": "local_db_analysis_router_fixture_replay_v1",
        "status": "pass",
        "database_url_guard_passed": True,
        "source_candidate_replay_confirmed": True,
        "artifact_snapshot_replay_confirmed": True,
        "evidence_bundle_replay_confirmed": True,
        "analysis_requested_event_found": True,
        "candidate_current_bundle_confirmed": True,
        "evidence_bundle_ready_confirmed": True,
        "judge_profile_allowed": True,
        "routing_policy_applied": True,
        "judge_run_created_or_reused": True,
        "judge_call_requested_event_created": True,
        "default_model_selected": True,
        "prompt_cache_key_created": True,
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
