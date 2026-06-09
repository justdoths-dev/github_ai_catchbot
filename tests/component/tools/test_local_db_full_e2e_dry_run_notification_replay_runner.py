from __future__ import annotations

import hashlib
import json
import os
import subprocess
import sys
from pathlib import Path
from uuid import uuid4

import pytest
import sqlalchemy as sa

from tests.component.tools import test_local_db_notifier_fixture_replay_runner as notifier_component
from tests.component.tools import test_local_db_policy_engine_fixture_replay_runner as policy_component
from tools import local_db_full_e2e_dry_run_notification_replay_runner as runner


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
    reason="LOCAL_TEST_DATABASE_URL is required for the DB-backed full E2E dry-run notification replay component test",
)


def test_cli_proves_full_chain_to_dry_run_delivery_result_idempotently() -> None:
    database_url = os.environ["LOCAL_TEST_DATABASE_URL"]
    namespace = f"component-local-db-full-e2e-{uuid4().hex}"
    try:
        before_global = _fetch_global_counts(database_url, namespace=namespace)

        first = _run_cli(database_url=database_url, namespace=namespace)
        first_report = json.loads(first.stdout)
        first_rows = policy_component._fetch_durable_rows(database_url=database_url, namespace=namespace)
        first_intent = policy_component._fetch_notification_plan_intent_payload(
            database_url=database_url,
            namespace=namespace,
        )
        first_plan = notifier_component._fetch_notification_plan(
            database_url=database_url,
            notification_plan_id=first_intent["notification_plan_id"],
        )
        first_render = notifier_component._fetch_notification_render(
            database_url=database_url,
            notification_plan_id=first_intent["notification_plan_id"],
        )
        first_delivery = notifier_component._fetch_delivery_record(
            database_url=database_url,
            notification_plan_id=first_intent["notification_plan_id"],
        )
        first_result_event = notifier_component._fetch_delivery_result_event(
            database_url=database_url,
            namespace=namespace,
            notification_plan_id=first_intent["notification_plan_id"],
        )
        first_stable_digest = _fetch_upstream_stable_digest(database_url=database_url, namespace=namespace)

        second = _run_cli(database_url=database_url, namespace=namespace)
        second_report = json.loads(second.stdout)
        second_rows = policy_component._fetch_durable_rows(database_url=database_url, namespace=namespace)
        second_intent = policy_component._fetch_notification_plan_intent_payload(
            database_url=database_url,
            namespace=namespace,
        )
        second_plan = notifier_component._fetch_notification_plan(
            database_url=database_url,
            notification_plan_id=second_intent["notification_plan_id"],
        )
        second_render = notifier_component._fetch_notification_render(
            database_url=database_url,
            notification_plan_id=second_intent["notification_plan_id"],
        )
        second_delivery = notifier_component._fetch_delivery_record(
            database_url=database_url,
            notification_plan_id=second_intent["notification_plan_id"],
        )
        second_result_event = notifier_component._fetch_delivery_result_event(
            database_url=database_url,
            namespace=namespace,
            notification_plan_id=second_intent["notification_plan_id"],
        )
        second_counts = _fetch_full_terminal_counts(
            database_url=database_url,
            namespace=namespace,
            notification_plan_id=second_intent["notification_plan_id"],
        )
        second_stable_digest = _fetch_upstream_stable_digest(database_url=database_url, namespace=namespace)
        after_global = _fetch_global_counts(database_url, namespace=namespace)

        for completed, report in ((first, first_report), (second, second_report)):
            assert completed.returncode == 0
            assert completed.stderr == ""
            assert database_url not in completed.stdout
            assert report == _expected_pass_report()
            for key in runner.SIDE_EFFECT_FALSE_KEYS:
                assert report[key] is False

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
            "notification_plans": 1,
            "notification_renders": 1,
            "notification_delivery_records": 1,
        }
        assert second_rows == first_rows
        assert second_counts == {
            "notification_plans": 1,
            "notification_renders": 1,
            "notification_delivery_records": 1,
            "notification_delivery_state_transitions": 1,
            "notification_delivery_result_events": 1,
        }

        assert first_intent == second_intent
        assert first_plan == second_plan
        assert first_render == second_render
        assert first_delivery == second_delivery
        assert first_result_event == second_result_event
        assert first_stable_digest == second_stable_digest

        assert first_plan["notification_plan_id"] == first_intent["notification_plan_id"]
        assert first_plan["analysis_id"] == first_intent["analysis_id"]
        assert first_plan["candidate_group_id"] == first_intent["candidate_group_id"]
        assert first_plan["delivery_decision"] == "send_now"
        assert first_plan["urgency_profile"] == "normal_silent"
        assert first_plan["status"] == "suppressed"

        assert first_delivery["delivery_status"] == "suppressed"
        assert first_delivery["telegram_message_id"] is None
        assert first_delivery["attempt_count"] == 0
        assert first_delivery["transport_error_code"] == "dry_run_skip_transport"
        assert first_delivery["telegram_response_json"]["dry_run"] is True
        assert first_delivery["telegram_response_json"]["local_fixture"] is True

        payload = first_result_event["payload_json"]
        assert payload["notification_plan_id"] == first_intent["notification_plan_id"]
        assert payload["delivery_status"] == "suppressed"
        assert payload["telegram_message_id"] is None
        assert payload["notification_delivery_record_id"] == first_delivery["notification_delivery_record_id"]
        assert payload["attempt_count"] == 0
        assert payload["dry_run"] is True
        assert payload["local_fixture"] is True

        assert before_global["namespace_delivery_result_events"] == 0
        assert after_global["namespace_notification_plan_intent_events"] == 1
        assert after_global["namespace_delivery_result_events"] == 1
        assert after_global["notification_plans"] - before_global["notification_plans"] in {0, 1}
        assert after_global["notification_renders"] - before_global["notification_renders"] in {0, 1}
        assert after_global["notification_delivery_records"] - before_global["notification_delivery_records"] in {0, 1}
        _assert_no_event_name_files()
    finally:
        notifier_component._cleanup_notifier_rows(database_url=database_url, namespace=namespace)


def _run_cli(*, database_url: str, namespace: str) -> subprocess.CompletedProcess[str]:
    env = dict(os.environ)
    env["APP_ENV"] = "test"
    return subprocess.run(
        [
            sys.executable,
            "-m",
            "tools.local_db_full_e2e_dry_run_notification_replay_runner",
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
        timeout=90,
    )


def _fetch_full_terminal_counts(*, database_url: str, namespace: str, notification_plan_id: str) -> dict[str, int]:
    engine = sa.create_engine(database_url, future=True)
    try:
        with engine.connect() as connection:
            row = connection.execute(
                sa.text(
                    """
                    SELECT
                      (SELECT count(*) FROM notification_plans
                       WHERE notification_plan_id = CAST(:notification_plan_id AS uuid)) AS notification_plans,
                      (SELECT count(*) FROM notification_renders
                       WHERE notification_plan_id = CAST(:notification_plan_id AS uuid)) AS notification_renders,
                      (SELECT count(*) FROM notification_delivery_records
                       WHERE notification_plan_id = CAST(:notification_plan_id AS uuid)) AS notification_delivery_records,
                      (SELECT count(*) FROM state_transitions
                       WHERE object_type = 'notification_plan'
                         AND object_id = CAST(:notification_plan_id AS uuid)
                         AND to_state = 'suppressed'
                         AND reason_code = 'dry_run_skip_transport') AS notification_delivery_state_transitions,
                      (SELECT count(*) FROM event_outbox
                       WHERE event_type = 'notification.delivery.result.v1'
                         AND dedupe_key LIKE :delivery_dedupe_prefix) AS notification_delivery_result_events
                    """
                ),
                {
                    "notification_plan_id": notification_plan_id,
                    "delivery_dedupe_prefix": f"local-db-notifier:{namespace}:notification.delivery.result:%",
                },
            ).mappings().one()
    finally:
        engine.dispose()
    return {key: int(row[key]) for key in row.keys()}


def _fetch_global_counts(database_url: str, *, namespace: str) -> dict[str, int]:
    engine = sa.create_engine(database_url, future=True)
    try:
        with engine.connect() as connection:
            return {
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
                    {"dedupe_prefix": f"local-db-notifier:{namespace}:notification.delivery.result:%"},
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


def _fetch_upstream_stable_digest(*, database_url: str, namespace: str) -> str:
    ids = policy_component._fetch_ids(database_url=database_url, namespace=namespace)
    source_fixture = runner.source_candidate_runner.load_source_fixture(SOURCE_FIXTURE, repo_root=ROOT)
    engine = sa.create_engine(database_url, future=True)
    try:
        with engine.connect() as connection:
            source_row = connection.execute(
                sa.text(
                    """
                    SELECT source_message_id, current_version_no, logical_post_key, content_type
                    FROM source_messages
                    WHERE source_message_id = CAST(:source_message_id AS uuid)
                    """
                ),
                {"source_message_id": str(source_fixture.source_message_id)},
            ).mappings().one()
            payload = {
                "source_message": dict(source_row),
                "candidate": policy_component._fetch_candidate_fields(
                    database_url=database_url,
                    namespace=namespace,
                ),
                "bundle": policy_component._fetch_bundle_fields(
                    database_url=database_url,
                    namespace=namespace,
                ),
                "judge_output": policy_component._fetch_judge_output_fields(
                    database_url=database_url,
                    namespace=namespace,
                ),
                "analysis": policy_component._fetch_analysis_row(
                    database_url=database_url,
                    namespace=namespace,
                ),
                "ids": ids,
            }
    finally:
        engine.dispose()
    encoded = json.dumps(payload, ensure_ascii=False, sort_keys=True, default=str)
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def _count(connection: sa.Connection, sql: str, params: dict[str, object]) -> int:
    return int(connection.execute(sa.text(sql), params).scalar_one())


def _assert_no_event_name_files() -> None:
    for name in EVENT_NAME_FILES:
        assert not (ROOT / name).exists(), name


def _expected_pass_report() -> dict[str, object]:
    return {
        "schema_version": "local_db_full_e2e_dry_run_notification_replay_v1",
        "status": "pass",
        "database_url_guard_passed": True,
        "source_message_created": True,
        "artifact_created": True,
        "candidate_group_created": True,
        "artifact_snapshot_created": True,
        "evidence_bundle_created": True,
        "analysis_requested_event_created": True,
        "judge_run_created": True,
        "judge_call_requested_event_created": True,
        "judge_output_created": True,
        "judge_output_ready_event_created": True,
        "analysis_validated_state_transition_created": True,
        "analysis_policy_apply_event_created": True,
        "analysis_created": True,
        "notification_plan_intent_event_created": True,
        "notification_plan_created": True,
        "notification_render_created": True,
        "notification_delivery_record_created": True,
        "notification_delivery_state_transition_created": True,
        "notification_delivery_result_event_created": True,
        "telegram_called": False,
        "send_message_called": False,
        "edit_message_called": False,
        "openai_called": False,
        "live_github_called": False,
        "live_telegram_called": False,
        "workers_started": False,
        "redis_mutation": False,
        "production_db_write": False,
        "alembic_or_ddl_ran": False,
        "checks_failed": [],
    }
