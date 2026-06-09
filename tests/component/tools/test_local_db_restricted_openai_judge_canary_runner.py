from __future__ import annotations

import json
import os
from pathlib import Path
from uuid import UUID, uuid4

import sqlalchemy as sa

from tools import local_db_analysis_router_fixture_replay_runner as analysis_runner
from tools import local_db_evidence_bundle_fixture_replay_runner as evidence_runner
from tools import local_db_github_snapshot_fixture_replay_runner as github_runner
from tools import local_db_restricted_openai_judge_canary_runner as runner
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


class FakeResponsesClient:
    def __init__(self) -> None:
        self.calls = []

    def create(self, **request):
        self.calls.append(request)
        user_context = json.loads(request["input"][1]["content"][0]["text"])
        bundle = runner.BundleContext(
            bundle_id=UUID(user_context["bundle_id"]),
            candidate_group_id=UUID(user_context["candidate_group_id"]),
            current_primary_artifact_id=UUID(user_context["current_primary_artifact_id"]),
            current_bundle_id=UUID(user_context["bundle_id"]),
            primary_summary=user_context["primary_summary"],
            supporting_summaries_json=user_context["supporting_summaries_json"],
            discovered_links_summary_json=user_context["discovered_links_summary_json"],
            evidence_limitations=user_context["evidence_limitations"],
            token_budget_profile=user_context["token_budget_profile"],
            reroot_count=int(user_context["reroot_count"]),
            ready_for_analysis=True,
        )
        payload = runner.build_fake_judge_output_payload(bundle)
        return {
            "status": "completed",
            "output_text": json.dumps(payload),
            "usage": {
                "input_tokens": 111,
                "input_tokens_details": {"cached_tokens": 22},
                "output_tokens": 88,
                "output_tokens_details": {"reasoning_tokens": 7},
            },
        }


class FakeOpenAIClient:
    def __init__(self) -> None:
        self.responses = FakeResponsesClient()


def test_local_db_restricted_openai_judge_canary_uses_injected_client_and_is_idempotent() -> None:
    database_url = os.environ.get("LOCAL_TEST_DATABASE_URL")
    assert database_url, "LOCAL_TEST_DATABASE_URL is required for this non-skipped DB component proof"
    namespace = f"component-local-db-restricted-openai-judge-{uuid4().hex}"
    openai_client = FakeOpenAIClient()
    before_global = _fetch_global_downstream_counts(database_url, namespace=namespace)

    first = _run_canary(database_url=database_url, namespace=namespace, openai_client=openai_client)
    first_rows = _fetch_durable_rows(database_url=database_url, namespace=namespace)
    first_ids = _fetch_ids(database_url=database_url, namespace=namespace)
    first_judge = _fetch_judge_run_fields(database_url=database_url, namespace=namespace)
    first_output = _fetch_judge_output_fields(database_url=database_url, namespace=namespace)
    first_ready = _fetch_ready_event_payload(database_url=database_url, namespace=namespace)
    second = _run_canary(database_url=database_url, namespace=namespace, openai_client=openai_client)
    second_rows = _fetch_durable_rows(database_url=database_url, namespace=namespace)
    second_judge = _fetch_judge_run_fields(database_url=database_url, namespace=namespace)
    second_output = _fetch_judge_output_fields(database_url=database_url, namespace=namespace)
    after_global = _fetch_global_downstream_counts(database_url, namespace=namespace)

    assert first.exit_code == 0
    assert second.exit_code == 0
    assert first.report == _expected_pass_report()
    assert second.report == _expected_pass_report()
    assert len(openai_client.responses.calls) == 1

    request = openai_client.responses.calls[0]
    user_context = json.loads(request["input"][1]["content"][0]["text"])
    assert request["model"] == first_judge["model"] == "gpt-5.4-mini"
    assert request["reasoning"]["effort"] == first_judge["reasoning_effort"] == "low"
    assert request["prompt_cache_key"] == first_judge["prompt_cache_key"]
    assert request["tools"] == []
    assert request["text"]["format"]["type"] == "json_schema"
    assert request["text"]["format"]["name"] == "judge_output_v1"
    assert request["text"]["format"]["strict"] is True
    assert set(user_context) == set(runner.REQUEST_CONTEXT_KEYS)
    assert "source_messages" not in json.dumps(request)
    assert "artifact_snapshots" not in json.dumps(request)
    assert "web_search" not in json.dumps(request)
    assert "file_search" not in json.dumps(request)

    expected_upstream_rows = {
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
    }
    for key, value in expected_upstream_rows.items():
        assert first_rows[key] == value
    assert second_rows == first_rows
    assert second_judge == first_judge
    assert second_output == first_output
    assert first_judge["status"] == "succeeded"
    assert first_judge["schema_version"] == "judge_output_v1"
    assert first_judge["policy_version"] == "verdict_policy_v1"
    assert first_judge["finish_reason"] == "stop"
    assert first_judge["refusal_detected"] is False
    assert first_judge["input_tokens"] == 111
    assert first_judge["cached_input_tokens"] == 22
    assert first_judge["output_tokens"] == 88
    assert first_judge["reasoning_tokens"] == 7
    assert first_output["judge_schema_version"] == "judge_output_v1"
    assert first_output["model_proposed_verdict"] == "later"
    assert first_output["model_confidence_band"] == "medium"
    assert first_output["payload_json"]["judge_schema_version"] == "judge_output_v1"
    assert first_output["payload_json"]["model_proposed_verdict"] == "later"
    assert first_output["payload_json"]["model_confidence_band"] == "medium"
    assert first_ready == {
        "judge_run_id": first_ids["judge_run_id"],
        "judge_output_id": first_ids["judge_output_id"],
        "finish_reason": "stop",
        "refusal_detected": False,
    }
    assert after_global["judge_outputs"] - before_global["judge_outputs"] in {0, 1}
    assert after_global["namespace_judge_output_ready_events"] == 1
    assert before_global["namespace_judge_output_ready_events"] == 0
    assert after_global["namespace_forbidden_downstream_events"] == 0
    for table in (
        "analyses",
        "notification_plans",
        "notification_renders",
        "notification_delivery_records",
    ):
        assert after_global[table] == before_global[table]
    _assert_no_event_name_files()


def _run_canary(*, database_url: str, namespace: str, openai_client: FakeOpenAIClient) -> runner.RunnerResult:
    args = runner.build_parser().parse_args(
        [
            "--database-url",
            database_url,
            "--source-fixture",
            str(SOURCE_FIXTURE.relative_to(ROOT)),
            "--github-snapshot-fixture",
            str(GITHUB_FIXTURE.relative_to(ROOT)),
            "--replay-namespace",
            namespace,
            "--confirm-local-test-db",
        ]
    )
    return runner.run(
        args,
        env={"APP_ENV": "test"},
        openai_client=openai_client,
        repo_root=ROOT,
    )


def _fetch_durable_rows(*, database_url: str, namespace: str) -> dict[str, int | str]:
    ids = _fetch_ids(database_url=database_url, namespace=namespace)
    source_fixture = source_runner.load_source_fixture(SOURCE_FIXTURE, repo_root=ROOT)
    github_fixture = github_runner.load_github_snapshot_fixture(GITHUB_FIXTURE, repo_root=ROOT)
    analysis_dedupe = evidence_runner.build_analysis_requested_dedupe_key(
        replay_namespace=namespace,
        candidate_group_id=UUID(ids["candidate_group_id"]),
        bundle_id=UUID(ids["bundle_id"]),
    )
    judge_dedupe = analysis_runner.build_judge_call_requested_dedupe_key(
        replay_namespace=namespace,
        bundle_id=UUID(ids["bundle_id"]),
        model="gpt-5.4-mini",
        reasoning_effort="low",
        prompt_version="judge_github_primary_v1",
    )
    ready_dedupe = runner.build_judge_output_ready_dedupe_key(
        replay_namespace=namespace,
        judge_run_id=UUID(ids["judge_run_id"]),
        judge_output_id=UUID(ids["judge_output_id"]),
    )
    engine = sa.create_engine(database_url, future=True)
    try:
        with engine.connect() as connection:
            counts: dict[str, int | str] = {
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
                    {"candidate_group_id": ids["candidate_group_id"], "artifact_id": ids["artifact_id"]},
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
                    {"bundle_id": ids["bundle_id"], "candidate_group_id": ids["candidate_group_id"]},
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
                    {"candidate_group_id": ids["candidate_group_id"], "bundle_id": ids["bundle_id"]},
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
                    {"judge_run_id": ids["judge_run_id"], "bundle_id": ids["bundle_id"]},
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
                    WHERE judge_output_id = CAST(:judge_output_id AS uuid)
                    """,
                    {"judge_output_id": ids["judge_output_id"]},
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
                        "event_type": runner.JUDGE_OUTPUT_READY_EVENT_TYPE,
                        "judge_run_id": ids["judge_run_id"],
                        "dedupe_key": ready_dedupe,
                    },
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
            return counts
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
            ready_payload = connection.execute(
                sa.text(
                    """
                    SELECT payload_json
                    FROM event_outbox
                    WHERE event_type = 'judge.output.ready.v1'
                      AND aggregate_id = CAST(:judge_run_id AS uuid)
                      AND dedupe_key LIKE :dedupe_prefix
                    ORDER BY created_at DESC, event_id DESC
                    LIMIT 1
                    """
                ),
                {
                    "judge_run_id": str(judge_run_id or ZERO_UUID),
                    "dedupe_prefix": f"local-db-restricted-openai-judge:{namespace}:judge.output.ready:%",
                },
            ).scalar_one_or_none()
            ready_payload_json = (
                json.loads(ready_payload) if isinstance(ready_payload, str) else dict(ready_payload or {})
            )
            judge_output_id = ready_payload_json.get("judge_output_id")
            if not judge_output_id:
                judge_output_id = _scalar(
                    connection,
                    """
                    SELECT judge_output_id
                    FROM judge_outputs
                    WHERE judge_run_id = CAST(:judge_run_id AS uuid)
                      AND judge_schema_version = 'judge_output_v1'
                    ORDER BY created_at DESC, judge_output_id DESC
                    LIMIT 1
                    """,
                    {"judge_run_id": str(judge_run_id or ZERO_UUID)},
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
    }


def _fetch_judge_run_fields(*, database_url: str, namespace: str) -> dict[str, object]:
    ids = _fetch_ids(database_url=database_url, namespace=namespace)
    engine = sa.create_engine(database_url, future=True)
    try:
        with engine.connect() as connection:
            row = connection.execute(
                sa.text(
                    """
                    SELECT status, model, reasoning_effort, prompt_version,
                           schema_version, policy_version, prompt_cache_key,
                           finish_reason, refusal_detected, input_tokens,
                           cached_input_tokens, output_tokens, reasoning_tokens,
                           latency_ms
                    FROM judge_runs
                    WHERE judge_run_id = CAST(:judge_run_id AS uuid)
                    """
                ),
                {"judge_run_id": ids["judge_run_id"]},
            ).mappings().one()
            return dict(row)
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
                    SELECT judge_schema_version, payload_json,
                           model_proposed_verdict, model_confidence_band
                    FROM judge_outputs
                    WHERE judge_output_id = CAST(:judge_output_id AS uuid)
                    """
                ),
                {"judge_output_id": ids["judge_output_id"]},
            ).mappings().one()
            payload = row["payload_json"]
            return {
                "judge_schema_version": str(row["judge_schema_version"]),
                "payload_json": json.loads(payload) if isinstance(payload, str) else dict(payload),
                "model_proposed_verdict": str(row["model_proposed_verdict"]),
                "model_confidence_band": str(row["model_confidence_band"]),
            }
    finally:
        engine.dispose()


def _fetch_ready_event_payload(*, database_url: str, namespace: str) -> dict[str, object]:
    ids = _fetch_ids(database_url=database_url, namespace=namespace)
    dedupe_key = runner.build_judge_output_ready_dedupe_key(
        replay_namespace=namespace,
        judge_run_id=UUID(ids["judge_run_id"]),
        judge_output_id=UUID(ids["judge_output_id"]),
    )
    engine = sa.create_engine(database_url, future=True)
    try:
        with engine.connect() as connection:
            row = connection.execute(
                sa.text(
                    """
                    SELECT payload_json
                    FROM event_outbox
                    WHERE event_type = 'judge.output.ready.v1'
                      AND dedupe_key = :dedupe_key
                    """
                ),
                {"dedupe_key": dedupe_key},
            ).mappings().one()
            payload = row["payload_json"]
            return json.loads(payload) if isinstance(payload, str) else dict(payload)
    finally:
        engine.dispose()


def _fetch_global_downstream_counts(database_url: str, *, namespace: str) -> dict[str, int]:
    engine = sa.create_engine(database_url, future=True)
    try:
        with engine.connect() as connection:
            return {
                "namespace_judge_output_ready_events": _count(
                    connection,
                    """
                    SELECT count(*)
                    FROM event_outbox
                    WHERE event_type = 'judge.output.ready.v1'
                      AND dedupe_key LIKE :dedupe_prefix
                    """,
                    {"dedupe_prefix": f"local-db-restricted-openai-judge:{namespace}:judge.output.ready:%"},
                ),
                "namespace_forbidden_downstream_events": _count(
                    connection,
                    """
                    SELECT count(*)
                    FROM event_outbox
                    WHERE event_type IN (
                        'analysis.policy.apply.v1',
                        'notification.plan.created.v1',
                        'notification.delivery.result.v1'
                    )
                      AND dedupe_key LIKE :dedupe_prefix
                    """,
                    {"dedupe_prefix": f"local-db-restricted-openai-judge:{namespace}:%"},
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
        "schema_version": "local_db_restricted_openai_judge_canary_v1",
        "status": "pass",
        "database_url_guard_passed": True,
        "openai_live_call_authorized": False,
        "openai_client_injected": True,
        "analysis_router_replay_confirmed": True,
        "judge_call_requested_event_found": True,
        "judge_run_loaded": True,
        "evidence_bundle_loaded": True,
        "judge_request_built": True,
        "judge_request_uses_bundle_only": True,
        "openai_responses_request_shape_valid": True,
        "openai_structured_output_received": True,
        "judge_output_created": True,
        "judge_run_updated": True,
        "judge_output_ready_event_created": True,
        "live_openai_called": False,
        "telegram_called": False,
        "live_github_called": False,
        "workers_started": False,
        "redis_mutation": False,
        "production_db_write": False,
        "alembic_or_ddl_ran": False,
        "analysis_created": False,
        "notification_created": False,
        "checks_failed": [],
    }
