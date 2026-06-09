from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path
from uuid import uuid4

import pytest
import sqlalchemy as sa

from tools import local_db_analysis_router_fixture_replay_runner as analysis_runner
from tools import local_db_analysis_validator_fixture_replay_runner as validator_runner
from tools import local_db_evidence_bundle_fixture_replay_runner as evidence_runner
from tools import local_db_fake_judge_output_fixture_replay_runner as fake_judge_runner
from tools import local_db_github_snapshot_fixture_replay_runner as github_runner
from tools import local_db_policy_engine_fixture_replay_runner as runner
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
    reason="LOCAL_TEST_DATABASE_URL is required for the DB-backed policy-engine replay component test",
)


def test_cli_applies_policy_and_emits_notification_plan_intent_idempotently() -> None:
    database_url = os.environ["LOCAL_TEST_DATABASE_URL"]
    namespace = f"component-local-db-policy-engine-{uuid4().hex}"
    before_global = _fetch_global_counts(database_url, namespace=namespace)

    first = _run_cli(database_url=database_url, namespace=namespace)
    first_report = json.loads(first.stdout)
    first_rows = _fetch_durable_rows(database_url=database_url, namespace=namespace)
    first_analysis = _fetch_analysis_row(database_url=database_url, namespace=namespace)
    first_transition = _fetch_policy_transition(database_url=database_url, namespace=namespace)
    first_intent = _fetch_notification_plan_intent_payload(database_url=database_url, namespace=namespace)
    first_output = _fetch_judge_output_fields(database_url=database_url, namespace=namespace)
    first_bundle = _fetch_bundle_fields(database_url=database_url, namespace=namespace)
    first_candidate = _fetch_candidate_fields(database_url=database_url, namespace=namespace)

    second = _run_cli(database_url=database_url, namespace=namespace)
    second_report = json.loads(second.stdout)
    second_rows = _fetch_durable_rows(database_url=database_url, namespace=namespace)
    second_analysis = _fetch_analysis_row(database_url=database_url, namespace=namespace)
    second_transition = _fetch_policy_transition(database_url=database_url, namespace=namespace)
    second_intent = _fetch_notification_plan_intent_payload(database_url=database_url, namespace=namespace)
    second_output = _fetch_judge_output_fields(database_url=database_url, namespace=namespace)
    second_bundle = _fetch_bundle_fields(database_url=database_url, namespace=namespace)
    second_candidate = _fetch_candidate_fields(database_url=database_url, namespace=namespace)
    after_global = _fetch_global_counts(database_url, namespace=namespace)

    for completed, report in ((first, first_report), (second, second_report)):
        assert completed.returncode == 0
        assert completed.stderr == ""
        assert database_url not in completed.stdout
        assert report["status"] == "pass"
        assert report["checks_failed"] == []
        assert report["openai_called"] is False
        assert report["live_github_called"] is False
        assert report["live_telegram_called"] is False
        assert report["workers_started"] is False
        assert report["redis_mutation"] is False
        assert report["production_db_write"] is False
        assert report["notification_plan_created"] is False
        assert report["notification_render_created"] is False
        assert report["notification_delivery_created"] is False

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
        "judge_outputs": 1,
        "judge_output_ready_events": 1,
        "analysis_validation_state_transitions": 1,
        "analysis_policy_apply_events": 1,
        "analyses": 1,
        "analysis_policy_state_transitions": 1,
        "notification_plan_intent_events": 1,
        "notification_plans": 0,
        "notification_renders": 0,
        "notification_delivery_records": 0,
    }
    assert second_rows == first_rows
    assert second_analysis == first_analysis
    assert second_transition == first_transition
    assert second_intent == first_intent
    assert second_output == first_output
    assert second_bundle == first_bundle
    assert second_candidate == first_candidate

    assert first_analysis["schema_version"] == "analysis_v1"
    assert first_analysis["policy_version"] == "verdict_policy_v1"
    assert first_analysis["delivery_policy_version"] == "delivery_policy_v1"
    assert first_analysis["verdict"] == "later"
    assert first_analysis["delivery_decision"] == "send_now"
    assert first_analysis["model_proposed_verdict"] == "later"
    assert first_analysis["policy_reconciled_flag"] is True
    assert first_analysis["scores_json"]["evidence_strength"] == 62
    assert first_analysis["scores_json"]["practical_usefulness"] == 58
    assert "policy_threshold_later" in first_analysis["reason_codes_json"]
    assert first_transition == {
        "from_state": "analysis_validated",
        "to_state": "analysis_policy_applied",
        "reason_code": "policy_applied:later:send_now",
    }
    assert first_intent["analysis_id"] == first_analysis["analysis_id"]
    assert first_intent["candidate_group_id"] == first_analysis["candidate_group_id"]
    assert first_intent["delivery_decision"] == "send_now"
    assert first_intent["urgency_profile"] == "normal_silent"
    assert first_intent["target_chat_id"] == runner.LOCAL_TEST_TARGET_CHAT_ID
    assert first_intent["target_thread_id"] is None
    assert first_intent["render_profile"] == runner.RENDER_PROFILE_NORMAL
    assert first_intent["suppress_reason_code"] is None
    assert first_candidate["current_analysis_id"] is None
    assert first_output["model_proposed_verdict"] == "later"
    assert first_output["model_confidence_band"] == "medium"
    assert first_bundle["ready_for_analysis"] is True

    assert after_global["namespace_notification_plan_intent_events"] == 1
    assert before_global["namespace_notification_plan_intent_events"] == 0
    assert after_global["namespace_delivery_result_events"] == 0
    assert after_global["analyses"] - before_global["analyses"] in {0, 1}
    assert after_global["notification_plans"] == before_global["notification_plans"]
    assert after_global["notification_renders"] == before_global["notification_renders"]
    assert after_global["notification_delivery_records"] == before_global["notification_delivery_records"]
    _assert_no_event_name_files()


def _run_cli(*, database_url: str, namespace: str) -> subprocess.CompletedProcess[str]:
    env = dict(os.environ)
    env["APP_ENV"] = "test"
    return subprocess.run(
        [
            sys.executable,
            "-m",
            "tools.local_db_policy_engine_fixture_replay_runner",
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
    judge_dedupe = analysis_runner.build_judge_call_requested_dedupe_key(
        replay_namespace=namespace,
        bundle_id=ids["bundle_id"],
        model="gpt-5.4-mini",
        reasoning_effort="low",
        prompt_version="judge_github_primary_v1",
    )
    ready_dedupe = fake_judge_runner.build_judge_output_ready_dedupe_key(
        replay_namespace=namespace,
        judge_run_id=ids["judge_run_id"],
        judge_output_id=ids["judge_output_id"],
    )
    policy_dedupe = validator_runner.build_analysis_policy_apply_dedupe_key(
        replay_namespace=namespace,
        judge_run_id=ids["judge_run_id"],
        judge_output_id=ids["judge_output_id"],
    )
    notify_prefix = f"local-db-policy-engine:{namespace}:notification.plan.created:%"
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
                        "event_type": analysis_runner.JUDGE_CALL_REQUESTED_EVENT_TYPE,
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
                "judge_output_ready_events": _count(
                    connection,
                    """
                    SELECT count(*)
                    FROM event_outbox
                    WHERE event_type = :event_type
                      AND aggregate_id = CAST(:judge_run_id AS uuid)
                      AND dedupe_key = :dedupe_key
                    """,
                    {
                        "event_type": fake_judge_runner.JUDGE_OUTPUT_READY_EVENT_TYPE,
                        "judge_run_id": ids["judge_run_id"],
                        "dedupe_key": ready_dedupe,
                    },
                ),
                "analysis_validation_state_transitions": _count(
                    connection,
                    """
                    SELECT count(*)
                    FROM state_transitions
                    WHERE object_type = 'judge_run'
                      AND object_id = CAST(:judge_run_id AS uuid)
                      AND from_state = 'succeeded'
                      AND to_state = 'analysis_validated'
                      AND reason_code = 'judge_output_validated'
                    """,
                    {"judge_run_id": ids["judge_run_id"]},
                ),
                "analysis_policy_apply_events": _count(
                    connection,
                    """
                    SELECT count(*)
                    FROM event_outbox
                    WHERE event_type = :event_type
                      AND aggregate_id = CAST(:judge_run_id AS uuid)
                      AND dedupe_key = :dedupe_key
                    """,
                    {
                        "event_type": validator_runner.ANALYSIS_POLICY_APPLY_EVENT_TYPE,
                        "judge_run_id": ids["judge_run_id"],
                        "dedupe_key": policy_dedupe,
                    },
                ),
                "analyses": _count(
                    connection,
                    """
                    SELECT count(*)
                    FROM analyses
                    WHERE analysis_id = CAST(:analysis_id AS uuid)
                      AND candidate_group_id = CAST(:candidate_group_id AS uuid)
                      AND judge_output_id = CAST(:judge_output_id AS uuid)
                    """,
                    {
                        "analysis_id": ids["analysis_id"],
                        "candidate_group_id": ids["candidate_group_id"],
                        "judge_output_id": ids["judge_output_id"],
                    },
                ),
                "analysis_policy_state_transitions": _count(
                    connection,
                    """
                    SELECT count(*)
                    FROM state_transitions
                    WHERE object_type = 'analysis'
                      AND object_id = CAST(:analysis_id AS uuid)
                      AND from_state = 'analysis_validated'
                      AND to_state = 'analysis_policy_applied'
                      AND reason_code = 'policy_applied:later:send_now'
                    """,
                    {"analysis_id": ids["analysis_id"]},
                ),
                "notification_plan_intent_events": _count(
                    connection,
                    """
                    SELECT count(*)
                    FROM event_outbox
                    WHERE event_type = :event_type
                      AND aggregate_type = 'analysis'
                      AND aggregate_id = CAST(:analysis_id AS uuid)
                      AND dedupe_key LIKE :dedupe_prefix
                    """,
                    {
                        "event_type": runner.NOTIFICATION_PLAN_CREATED_EVENT_TYPE,
                        "analysis_id": ids["analysis_id"],
                        "dedupe_prefix": notify_prefix,
                    },
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
            judge_output_id = _scalar(
                connection,
                """
                SELECT judge_output_id
                FROM judge_outputs
                WHERE judge_run_id = CAST(:judge_run_id AS uuid)
                  AND judge_schema_version = 'judge_output_v1'
                ORDER BY created_at, judge_output_id
                LIMIT 1
                """,
                {"judge_run_id": str(judge_run_id or ZERO_UUID)},
            )
            analysis_id = _scalar(
                connection,
                """
                SELECT analysis_id
                FROM analyses
                WHERE judge_output_id = CAST(:judge_output_id AS uuid)
                  AND policy_version = 'verdict_policy_v1'
                  AND delivery_policy_version = 'delivery_policy_v1'
                ORDER BY created_at, analysis_id
                LIMIT 1
                """,
                {"judge_output_id": str(judge_output_id or ZERO_UUID)},
            )
    finally:
        engine.dispose()
    return {
        "artifact_id": str(artifact_id or ZERO_UUID),
        "candidate_group_id": str(candidate_group_id or ZERO_UUID),
        "snapshot_id": str(snapshot_id or ZERO_UUID),
        "bundle_id": str(bundle_id or ZERO_UUID),
        "judge_run_id": str(judge_run_id or ZERO_UUID),
        "judge_output_id": str(judge_output_id or ZERO_UUID),
        "analysis_id": str(analysis_id or ZERO_UUID),
    }


def _fetch_analysis_row(*, database_url: str, namespace: str) -> dict[str, object]:
    ids = _fetch_ids(database_url=database_url, namespace=namespace)
    engine = sa.create_engine(database_url, future=True)
    try:
        with engine.connect() as connection:
            row = connection.execute(
                sa.text(
                    """
                    SELECT analysis_id, candidate_group_id, judge_output_id, schema_version,
                           policy_version, prompt_version, delivery_policy_version, verdict,
                           delivery_decision, scores_json, reason_codes_json,
                           evidence_limitations_ko, recommended_action_ko, freshness_note_ko,
                           model_proposed_verdict, policy_reconciled_flag
                    FROM analyses
                    WHERE analysis_id = CAST(:analysis_id AS uuid)
                    """
                ),
                {"analysis_id": ids["analysis_id"]},
            ).mappings().one()
    finally:
        engine.dispose()
    return {
        "analysis_id": str(row["analysis_id"]),
        "candidate_group_id": str(row["candidate_group_id"]),
        "judge_output_id": str(row["judge_output_id"]),
        "schema_version": str(row["schema_version"]),
        "policy_version": str(row["policy_version"]),
        "prompt_version": str(row["prompt_version"]),
        "delivery_policy_version": str(row["delivery_policy_version"]),
        "verdict": str(row["verdict"]),
        "delivery_decision": str(row["delivery_decision"]),
        "scores_json": _json_obj(row["scores_json"]),
        "reason_codes_json": _json_obj(row["reason_codes_json"]),
        "evidence_limitations_ko": row["evidence_limitations_ko"],
        "recommended_action_ko": row["recommended_action_ko"],
        "freshness_note_ko": row["freshness_note_ko"],
        "model_proposed_verdict": str(row["model_proposed_verdict"]),
        "policy_reconciled_flag": bool(row["policy_reconciled_flag"]),
    }


def _fetch_policy_transition(*, database_url: str, namespace: str) -> dict[str, str | None]:
    ids = _fetch_ids(database_url=database_url, namespace=namespace)
    engine = sa.create_engine(database_url, future=True)
    try:
        with engine.connect() as connection:
            row = connection.execute(
                sa.text(
                    """
                    SELECT from_state, to_state, reason_code
                    FROM state_transitions
                    WHERE object_type = 'analysis'
                      AND object_id = CAST(:analysis_id AS uuid)
                      AND to_state = 'analysis_policy_applied'
                    ORDER BY created_at, state_transition_id
                    """
                ),
                {"analysis_id": ids["analysis_id"]},
            ).mappings().one()
            return dict(row)
    finally:
        engine.dispose()


def _fetch_notification_plan_intent_payload(*, database_url: str, namespace: str) -> dict[str, object]:
    ids = _fetch_ids(database_url=database_url, namespace=namespace)
    engine = sa.create_engine(database_url, future=True)
    try:
        with engine.connect() as connection:
            row = connection.execute(
                sa.text(
                    """
                    SELECT payload_json
                    FROM event_outbox
                    WHERE event_type = 'notification.plan.created.v1'
                      AND aggregate_type = 'analysis'
                      AND aggregate_id = CAST(:analysis_id AS uuid)
                      AND dedupe_key LIKE :dedupe_prefix
                    """
                ),
                {
                    "analysis_id": ids["analysis_id"],
                    "dedupe_prefix": f"local-db-policy-engine:{namespace}:notification.plan.created:%",
                },
            ).mappings().one()
            return _json_obj(row["payload_json"])
    finally:
        engine.dispose()


def _fetch_judge_output_fields(*, database_url: str, namespace: str) -> dict[str, object]:
    ids = _fetch_ids(database_url=database_url, namespace=namespace)
    engine = sa.create_engine(database_url, future=True)
    try:
        with engine.connect() as connection:
            row = connection.execute(
                sa.text(
                    """
                    SELECT judge_output_id, judge_run_id, candidate_group_id,
                           judge_schema_version, payload_json,
                           model_proposed_verdict, model_confidence_band
                    FROM judge_outputs
                    WHERE judge_output_id = CAST(:judge_output_id AS uuid)
                    """
                ),
                {"judge_output_id": ids["judge_output_id"]},
            ).mappings().one()
    finally:
        engine.dispose()
    return {
        "judge_output_id": str(row["judge_output_id"]),
        "judge_run_id": str(row["judge_run_id"]),
        "candidate_group_id": str(row["candidate_group_id"]),
        "judge_schema_version": str(row["judge_schema_version"]),
        "payload_json": _json_obj(row["payload_json"]),
        "model_proposed_verdict": str(row["model_proposed_verdict"]),
        "model_confidence_band": str(row["model_confidence_band"]),
    }


def _fetch_bundle_fields(*, database_url: str, namespace: str) -> dict[str, object]:
    ids = _fetch_ids(database_url=database_url, namespace=namespace)
    engine = sa.create_engine(database_url, future=True)
    try:
        with engine.connect() as connection:
            row = connection.execute(
                sa.text(
                    """
                    SELECT bundle_id, candidate_group_id, current_primary_artifact_id,
                           primary_summary, evidence_limitations, ready_for_analysis
                    FROM candidate_evidence_bundles
                    WHERE bundle_id = CAST(:bundle_id AS uuid)
                    """
                ),
                {"bundle_id": ids["bundle_id"]},
            ).mappings().one()
    finally:
        engine.dispose()
    return {
        "bundle_id": str(row["bundle_id"]),
        "candidate_group_id": str(row["candidate_group_id"]),
        "current_primary_artifact_id": str(row["current_primary_artifact_id"]),
        "primary_summary": _json_obj(row["primary_summary"]),
        "evidence_limitations": _json_obj(row["evidence_limitations"]),
        "ready_for_analysis": bool(row["ready_for_analysis"]),
    }


def _fetch_candidate_fields(*, database_url: str, namespace: str) -> dict[str, object]:
    ids = _fetch_ids(database_url=database_url, namespace=namespace)
    engine = sa.create_engine(database_url, future=True)
    try:
        with engine.connect() as connection:
            row = connection.execute(
                sa.text(
                    """
                    SELECT candidate_group_id, current_bundle_id, current_analysis_id, current_primary_artifact_id
                    FROM candidate_group_proposals
                    WHERE candidate_group_id = CAST(:candidate_group_id AS uuid)
                    """
                ),
                {"candidate_group_id": ids["candidate_group_id"]},
            ).mappings().one()
    finally:
        engine.dispose()
    return {
        "candidate_group_id": str(row["candidate_group_id"]),
        "current_bundle_id": str(row["current_bundle_id"]),
        "current_analysis_id": str(row["current_analysis_id"]) if row["current_analysis_id"] else None,
        "current_primary_artifact_id": str(row["current_primary_artifact_id"]),
    }


def _fetch_global_counts(database_url: str, *, namespace: str) -> dict[str, int]:
    engine = sa.create_engine(database_url, future=True)
    try:
        with engine.connect() as connection:
            return {
                "analyses": _count(connection, "SELECT count(*) FROM analyses", {}),
                "namespace_notification_plan_intent_events": _count(
                    connection,
                    """
                    SELECT count(*)
                    FROM event_outbox
                    WHERE event_type = 'notification.plan.created.v1'
                      AND dedupe_key LIKE :dedupe_prefix
                    """,
                    {"dedupe_prefix": f"local-db-policy-engine:{namespace}:notification.plan.created:%"},
                ),
                "namespace_delivery_result_events": _count(
                    connection,
                    """
                    SELECT count(*)
                    FROM event_outbox
                    WHERE event_type = 'notification.delivery.result.v1'
                      AND dedupe_key LIKE :dedupe_prefix
                    """,
                    {"dedupe_prefix": f"local-db-policy-engine:{namespace}:%"},
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


def _json_obj(value):
    return json.loads(value) if isinstance(value, str) else value


def _assert_no_event_name_files() -> None:
    for name in EVENT_NAME_FILES:
        assert not (ROOT / name).exists(), name
