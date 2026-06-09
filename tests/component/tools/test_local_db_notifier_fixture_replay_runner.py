from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path
from uuid import uuid4

import pytest
import sqlalchemy as sa

from tests.component.tools import test_local_db_policy_engine_fixture_replay_runner as policy_component
from tools import local_db_notifier_fixture_replay_runner as runner


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
    reason="LOCAL_TEST_DATABASE_URL is required for the DB-backed notifier replay component test",
)


def test_cli_concretizes_dry_run_delivery_result_idempotently() -> None:
    database_url = os.environ["LOCAL_TEST_DATABASE_URL"]
    namespace = f"component-local-db-notifier-{uuid4().hex}"
    try:
        before_global = _fetch_global_counts(database_url, namespace=namespace)

        first = _run_cli(database_url=database_url, namespace=namespace)
        first_report = json.loads(first.stdout)
        first_rows = policy_component._fetch_durable_rows(database_url=database_url, namespace=namespace)
        first_intent = policy_component._fetch_notification_plan_intent_payload(
            database_url=database_url,
            namespace=namespace,
        )
        first_plan = _fetch_notification_plan(
            database_url=database_url,
            notification_plan_id=first_intent["notification_plan_id"],
        )
        first_render = _fetch_notification_render(
            database_url=database_url,
            notification_plan_id=first_intent["notification_plan_id"],
        )
        first_delivery = _fetch_delivery_record(
            database_url=database_url,
            notification_plan_id=first_intent["notification_plan_id"],
        )
        first_result_event = _fetch_delivery_result_event(
            database_url=database_url,
            namespace=namespace,
            notification_plan_id=first_intent["notification_plan_id"],
        )
        first_analysis = policy_component._fetch_analysis_row(database_url=database_url, namespace=namespace)
        first_output = policy_component._fetch_judge_output_fields(database_url=database_url, namespace=namespace)
        first_bundle = policy_component._fetch_bundle_fields(database_url=database_url, namespace=namespace)
        first_candidate = policy_component._fetch_candidate_fields(database_url=database_url, namespace=namespace)

        second = _run_cli(database_url=database_url, namespace=namespace)
        second_report = json.loads(second.stdout)
        second_rows = policy_component._fetch_durable_rows(database_url=database_url, namespace=namespace)
        second_intent = policy_component._fetch_notification_plan_intent_payload(
            database_url=database_url,
            namespace=namespace,
        )
        second_plan = _fetch_notification_plan(
            database_url=database_url,
            notification_plan_id=second_intent["notification_plan_id"],
        )
        second_render = _fetch_notification_render(
            database_url=database_url,
            notification_plan_id=second_intent["notification_plan_id"],
        )
        second_delivery = _fetch_delivery_record(
            database_url=database_url,
            notification_plan_id=second_intent["notification_plan_id"],
        )
        second_result_event = _fetch_delivery_result_event(
            database_url=database_url,
            namespace=namespace,
            notification_plan_id=second_intent["notification_plan_id"],
        )
        second_analysis = policy_component._fetch_analysis_row(database_url=database_url, namespace=namespace)
        second_output = policy_component._fetch_judge_output_fields(database_url=database_url, namespace=namespace)
        second_bundle = policy_component._fetch_bundle_fields(database_url=database_url, namespace=namespace)
        second_candidate = policy_component._fetch_candidate_fields(database_url=database_url, namespace=namespace)
        second_counts = _fetch_notifier_counts(
            database_url=database_url,
            namespace=namespace,
            notification_plan_id=second_intent["notification_plan_id"],
        )
        after_global = _fetch_global_counts(database_url, namespace=namespace)

        for completed, report in ((first, first_report), (second, second_report)):
            assert completed.returncode == 0
            assert completed.stderr == ""
            assert database_url not in completed.stdout
            assert report["status"] == "pass"
            assert report["checks_failed"] == []
            assert report["telegram_called"] is False
            assert report["send_message_called"] is False
            assert report["edit_message_called"] is False
            assert report["openai_called"] is False
            assert report["live_github_called"] is False
            assert report["live_telegram_called"] is False
            assert report["workers_started"] is False
            assert report["redis_mutation"] is False
            assert report["production_db_write"] is False
            assert report["analysis_mutated"] is False
            assert report["judge_output_mutated"] is False
            assert report["candidate_group_mutated"] is False

        assert first_rows["analysis_requested_events"] == 1
        assert first_rows["judge_call_requested_events"] == 1
        assert first_rows["judge_outputs"] == 1
        assert first_rows["judge_output_ready_events"] == 1
        assert first_rows["analysis_policy_apply_events"] == 1
        assert first_rows["analyses"] == 1
        assert first_rows["analysis_policy_state_transitions"] == 1
        assert first_rows["notification_plan_intent_events"] == 1
        assert first_rows["notification_plans"] == 1
        assert first_rows["notification_renders"] == 1
        assert first_rows["notification_delivery_records"] == 1
        assert second_rows == first_rows

        assert first_intent == second_intent
        assert first_plan == second_plan
        assert first_render == second_render
        assert first_delivery == second_delivery
        assert first_result_event == second_result_event
        assert first_analysis == second_analysis
        assert first_output == second_output
        assert first_bundle == second_bundle
        assert first_candidate == second_candidate

        assert first_plan["notification_plan_id"] == first_intent["notification_plan_id"]
        assert first_plan["analysis_id"] == first_intent["analysis_id"]
        assert first_plan["candidate_group_id"] == first_intent["candidate_group_id"]
        assert first_plan["delivery_decision"] == first_intent["delivery_decision"] == "send_now"
        assert first_plan["urgency_profile"] == first_intent["urgency_profile"] == "normal_silent"
        assert first_plan["target_chat_id"] == first_intent["target_chat_id"]
        assert first_plan["target_thread_id"] == first_intent["target_thread_id"]
        assert first_plan["render_profile"] == first_intent["render_profile"]
        assert first_plan["dedupe_subject_key"] == first_intent["dedupe_subject_key"]
        assert first_plan["material_change_hash"] == first_intent["material_change_hash"]
        assert first_plan["suppress_reason_code"] == first_intent["suppress_reason_code"]
        assert first_plan["status"] == "suppressed"

        assert first_render["notification_plan_id"] == first_intent["notification_plan_id"]
        assert first_render["message_text"].startswith("[GitHub AI] later / send_now")
        assert "Reason:" in first_render["message_text"]
        assert first_render["entities_json"] == []
        assert first_render["link_preview_options_json"] == {"is_disabled": True}
        assert first_render["reply_markup_json"] is None
        assert first_render["disable_notification"] is True
        assert first_render["protect_content"] is False
        assert first_render["parse_strategy"] == "entities"
        assert first_render["render_hash"]

        assert first_delivery["notification_plan_id"] == first_intent["notification_plan_id"]
        assert first_delivery["delivery_status"] == "suppressed"
        assert first_delivery["telegram_chat_id"] == first_intent["target_chat_id"]
        assert first_delivery["telegram_message_id"] is None
        assert first_delivery["attempt_count"] == 0
        assert first_delivery["transport_error_code"] == "dry_run_skip_transport"
        assert first_delivery["transport_error_class"] is None
        assert first_delivery["telegram_response_json"]["dry_run"] is True
        assert first_delivery["telegram_response_json"]["local_fixture"] is True

        payload = first_result_event["payload_json"]
        assert payload["notification_plan_id"] == first_intent["notification_plan_id"]
        assert payload["delivery_status"] == "suppressed"
        assert payload["telegram_chat_id"] == first_intent["target_chat_id"]
        assert payload["telegram_message_id"] is None
        assert payload["notification_delivery_record_id"] == first_delivery["notification_delivery_record_id"]
        assert payload["attempt_count"] == 0
        assert payload["transport_error_code"] is None
        assert payload["transport_error_class"] is None
        assert payload["edited"] is False
        assert payload["dry_run"] is True
        assert payload["local_fixture"] is True

        assert second_counts == {
            "notification_plans": 1,
            "notification_renders": 1,
            "notification_delivery_records": 1,
            "notification_delivery_state_transitions": 1,
            "notification_delivery_result_events": 1,
        }
        assert after_global["namespace_notification_plan_intent_events"] == 1
        assert after_global["namespace_delivery_result_events"] == 1
        assert after_global["notification_plans"] - before_global["notification_plans"] in {0, 1}
        assert after_global["notification_renders"] - before_global["notification_renders"] in {0, 1}
        assert after_global["notification_delivery_records"] - before_global["notification_delivery_records"] in {0, 1}
        _assert_no_event_name_files()
    finally:
        _cleanup_notifier_rows(database_url=database_url, namespace=namespace)


def _run_cli(*, database_url: str, namespace: str) -> subprocess.CompletedProcess[str]:
    env = dict(os.environ)
    env["APP_ENV"] = "test"
    return subprocess.run(
        [
            sys.executable,
            "-m",
            "tools.local_db_notifier_fixture_replay_runner",
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


def _fetch_notification_plan(*, database_url: str, notification_plan_id: str) -> dict[str, object]:
    engine = sa.create_engine(database_url, future=True)
    try:
        with engine.connect() as connection:
            row = connection.execute(
                sa.text(
                    """
                    SELECT notification_plan_id, analysis_id, candidate_group_id,
                           delivery_decision, urgency_profile, target_chat_id,
                           target_thread_id, render_profile, dedupe_subject_key,
                           material_change_hash, send_after, suppress_reason_code,
                           status
                    FROM notification_plans
                    WHERE notification_plan_id = CAST(:notification_plan_id AS uuid)
                    """
                ),
                {"notification_plan_id": notification_plan_id},
            ).mappings().one()
    finally:
        engine.dispose()
    return {
        "notification_plan_id": str(row["notification_plan_id"]),
        "analysis_id": str(row["analysis_id"]),
        "candidate_group_id": str(row["candidate_group_id"]),
        "delivery_decision": str(row["delivery_decision"]),
        "urgency_profile": str(row["urgency_profile"]),
        "target_chat_id": int(row["target_chat_id"]),
        "target_thread_id": row["target_thread_id"],
        "render_profile": row["render_profile"],
        "dedupe_subject_key": row["dedupe_subject_key"],
        "material_change_hash": row["material_change_hash"],
        "send_after": row["send_after"].isoformat() if row["send_after"] else None,
        "suppress_reason_code": row["suppress_reason_code"],
        "status": str(row["status"]),
    }


def _fetch_notification_render(*, database_url: str, notification_plan_id: str) -> dict[str, object]:
    engine = sa.create_engine(database_url, future=True)
    try:
        with engine.connect() as connection:
            row = connection.execute(
                sa.text(
                    """
                    SELECT notification_render_id, notification_plan_id, message_text,
                           entities_json, link_preview_options_json, reply_markup_json,
                           disable_notification, protect_content, parse_strategy, render_hash
                    FROM notification_renders
                    WHERE notification_plan_id = CAST(:notification_plan_id AS uuid)
                    """
                ),
                {"notification_plan_id": notification_plan_id},
            ).mappings().one()
    finally:
        engine.dispose()
    return {
        "notification_render_id": str(row["notification_render_id"]),
        "notification_plan_id": str(row["notification_plan_id"]),
        "message_text": str(row["message_text"]),
        "entities_json": _json_obj(row["entities_json"]),
        "link_preview_options_json": _json_obj(row["link_preview_options_json"]),
        "reply_markup_json": _json_obj(row["reply_markup_json"]),
        "disable_notification": bool(row["disable_notification"]),
        "protect_content": bool(row["protect_content"]),
        "parse_strategy": str(row["parse_strategy"]),
        "render_hash": str(row["render_hash"]),
    }


def _fetch_delivery_record(*, database_url: str, notification_plan_id: str) -> dict[str, object]:
    engine = sa.create_engine(database_url, future=True)
    try:
        with engine.connect() as connection:
            row = connection.execute(
                sa.text(
                    """
                    SELECT notification_delivery_record_id, notification_plan_id,
                           telegram_chat_id, telegram_message_id, delivery_status,
                           attempt_count, transport_error_code, transport_error_class,
                           telegram_response_json
                    FROM notification_delivery_records
                    WHERE notification_plan_id = CAST(:notification_plan_id AS uuid)
                    """
                ),
                {"notification_plan_id": notification_plan_id},
            ).mappings().one()
    finally:
        engine.dispose()
    return {
        "notification_delivery_record_id": str(row["notification_delivery_record_id"]),
        "notification_plan_id": str(row["notification_plan_id"]),
        "telegram_chat_id": int(row["telegram_chat_id"]) if row["telegram_chat_id"] is not None else None,
        "telegram_message_id": int(row["telegram_message_id"]) if row["telegram_message_id"] is not None else None,
        "delivery_status": str(row["delivery_status"]),
        "attempt_count": int(row["attempt_count"]),
        "transport_error_code": row["transport_error_code"],
        "transport_error_class": row["transport_error_class"],
        "telegram_response_json": _json_obj(row["telegram_response_json"]),
    }


def _fetch_delivery_result_event(*, database_url: str, namespace: str, notification_plan_id: str) -> dict[str, object]:
    engine = sa.create_engine(database_url, future=True)
    try:
        with engine.connect() as connection:
            row = connection.execute(
                sa.text(
                    """
                    SELECT event_id, aggregate_type, aggregate_id, dedupe_key, payload_json
                    FROM event_outbox
                    WHERE event_type = :event_type
                      AND aggregate_type = 'notification_plan'
                      AND aggregate_id = CAST(:notification_plan_id AS uuid)
                      AND dedupe_key LIKE :dedupe_prefix
                    """
                ),
                {
                    "event_type": runner.NOTIFICATION_DELIVERY_RESULT_EVENT_TYPE,
                    "notification_plan_id": notification_plan_id,
                    "dedupe_prefix": f"local-db-notifier:{namespace}:notification.delivery.result:%",
                },
            ).mappings().one()
    finally:
        engine.dispose()
    return {
        "event_id": str(row["event_id"]),
        "aggregate_type": str(row["aggregate_type"]),
        "aggregate_id": str(row["aggregate_id"]),
        "dedupe_key": str(row["dedupe_key"]),
        "payload_json": _json_obj(row["payload_json"]),
    }


def _fetch_notifier_counts(*, database_url: str, namespace: str, notification_plan_id: str) -> dict[str, int]:
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
                       WHERE event_type = :event_type
                         AND dedupe_key LIKE :dedupe_prefix) AS notification_delivery_result_events
                    """
                ),
                {
                    "notification_plan_id": notification_plan_id,
                    "event_type": runner.NOTIFICATION_DELIVERY_RESULT_EVENT_TYPE,
                    "dedupe_prefix": f"local-db-notifier:{namespace}:notification.delivery.result:%",
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


def _cleanup_notifier_rows(*, database_url: str, namespace: str) -> None:
    engine = sa.create_engine(database_url, future=True)
    try:
        with engine.begin() as connection:
            connection.execute(
                sa.text(
                    """
                    WITH plan_ids AS (
                        SELECT CAST(payload_json->>'notification_plan_id' AS uuid) AS notification_plan_id
                        FROM event_outbox
                        WHERE event_type = 'notification.plan.created.v1'
                          AND dedupe_key LIKE :plan_dedupe_prefix
                        UNION
                        SELECT aggregate_id AS notification_plan_id
                        FROM event_outbox
                        WHERE event_type = 'notification.delivery.result.v1'
                          AND dedupe_key LIKE :delivery_dedupe_prefix
                    )
                    DELETE FROM event_outbox
                    WHERE event_type = 'notification.delivery.result.v1'
                      AND dedupe_key LIKE :delivery_dedupe_prefix
                    """
                ),
                {
                    "plan_dedupe_prefix": f"local-db-policy-engine:{namespace}:notification.plan.created:%",
                    "delivery_dedupe_prefix": f"local-db-notifier:{namespace}:notification.delivery.result:%",
                },
            )
            for sql in (
                """
                WITH plan_ids AS (
                    SELECT CAST(payload_json->>'notification_plan_id' AS uuid) AS notification_plan_id
                    FROM event_outbox
                    WHERE event_type = 'notification.plan.created.v1'
                      AND dedupe_key LIKE :plan_dedupe_prefix
                    UNION
                    SELECT aggregate_id AS notification_plan_id
                    FROM event_outbox
                    WHERE event_type = 'notification.delivery.result.v1'
                      AND dedupe_key LIKE :delivery_dedupe_prefix
                )
                DELETE FROM state_transitions
                WHERE object_type = 'notification_plan'
                  AND object_id IN (SELECT notification_plan_id FROM plan_ids)
                """,
                """
                WITH plan_ids AS (
                    SELECT CAST(payload_json->>'notification_plan_id' AS uuid) AS notification_plan_id
                    FROM event_outbox
                    WHERE event_type = 'notification.plan.created.v1'
                      AND dedupe_key LIKE :plan_dedupe_prefix
                    UNION
                    SELECT aggregate_id AS notification_plan_id
                    FROM event_outbox
                    WHERE event_type = 'notification.delivery.result.v1'
                      AND dedupe_key LIKE :delivery_dedupe_prefix
                )
                DELETE FROM notification_delivery_records
                WHERE notification_plan_id IN (SELECT notification_plan_id FROM plan_ids)
                """,
                """
                WITH plan_ids AS (
                    SELECT CAST(payload_json->>'notification_plan_id' AS uuid) AS notification_plan_id
                    FROM event_outbox
                    WHERE event_type = 'notification.plan.created.v1'
                      AND dedupe_key LIKE :plan_dedupe_prefix
                    UNION
                    SELECT aggregate_id AS notification_plan_id
                    FROM event_outbox
                    WHERE event_type = 'notification.delivery.result.v1'
                      AND dedupe_key LIKE :delivery_dedupe_prefix
                )
                DELETE FROM notification_renders
                WHERE notification_plan_id IN (SELECT notification_plan_id FROM plan_ids)
                """,
                """
                WITH plan_ids AS (
                    SELECT CAST(payload_json->>'notification_plan_id' AS uuid) AS notification_plan_id
                    FROM event_outbox
                    WHERE event_type = 'notification.plan.created.v1'
                      AND dedupe_key LIKE :plan_dedupe_prefix
                    UNION
                    SELECT aggregate_id AS notification_plan_id
                    FROM event_outbox
                    WHERE event_type = 'notification.delivery.result.v1'
                      AND dedupe_key LIKE :delivery_dedupe_prefix
                )
                DELETE FROM notification_plans
                WHERE notification_plan_id IN (SELECT notification_plan_id FROM plan_ids)
                """,
            ):
                connection.execute(
                    sa.text(sql),
                    {
                        "plan_dedupe_prefix": f"local-db-policy-engine:{namespace}:notification.plan.created:%",
                        "delivery_dedupe_prefix": f"local-db-notifier:{namespace}:notification.delivery.result:%",
                    },
                )
    finally:
        engine.dispose()


def _count(connection: sa.Connection, sql: str, params: dict[str, object]) -> int:
    return int(connection.execute(sa.text(sql), params).scalar_one())


def _json_obj(value):
    return json.loads(value) if isinstance(value, str) else value


def _assert_no_event_name_files() -> None:
    for name in EVENT_NAME_FILES:
        assert not (ROOT / name).exists(), name
