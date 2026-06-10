from __future__ import annotations

import ast
import json
from pathlib import Path
from uuid import UUID

from tools import local_db_operator_approved_dead_letter_delivery_replay_request_fixture_runner as runner


ROOT = Path(__file__).resolve().parents[3]
SOURCE_FIXTURE = "tests/fixtures/upstream/source_message_github_repo_signal.json"
GITHUB_FIXTURE = "tests/fixtures/upstream/github_repo_snapshot_example_tool.json"
PG_SCHEME = "postgresql+psycopg"
SAFE_DATABASE_NAME = "github_ai_catchbot_test"
SOCKET_HOST = "/var/run/postgresql"
SAFE_SOCKET_URL = f"{PG_SCHEME}:///{SAFE_DATABASE_NAME}?host={SOCKET_HOST}"
SECRET_VALUE = "local" + "_" + "secret"
AUTHORITY_WITH_PASSWORD = "local_user" + ":" + SECRET_VALUE + "@" + "127.0.0.1:5432"
PASSWORD_URL = f"{PG_SCHEME}://" + AUTHORITY_WITH_PASSWORD + f"/{SAFE_DATABASE_NAME}"
REMOTE_UNSAFE_URL = f"{PG_SCHEME}://db.example.invalid/prod"
DEAD_LETTER_ID = UUID("10000000-0000-4000-8000-000000000001")
PLAN_ID = UUID("20000000-0000-4000-8000-000000000002")
REPLAY_REQUEST_ID = UUID("30000000-0000-4000-8000-000000000003")
APPROVAL = "operator-approved-delivery-replay-request"


class FakeResolver:
    def __init__(
        self,
        *,
        dead_letter_entry_id: UUID | None = DEAD_LETTER_ID,
        prepared: bool = True,
        checks_failed: tuple[str, ...] = (),
    ) -> None:
        self.calls = []
        self.dead_letter_entry_id = dead_letter_entry_id
        self.prepared = prepared
        self.checks_failed = checks_failed

    def resolve(
        self,
        *,
        database_url: str,
        source_fixture_path: Path,
        github_snapshot_fixture_path: Path,
        replay_namespace: str,
        max_attempts: int,
        env,
        repo_root: Path,
    ) -> runner.FixtureResolutionResult:
        self.calls.append(
            {
                "database_url": database_url,
                "source_fixture_path": source_fixture_path,
                "github_snapshot_fixture_path": github_snapshot_fixture_path,
                "replay_namespace": replay_namespace,
                "max_attempts": max_attempts,
                "env": dict(env),
                "repo_root": repo_root,
            }
        )
        return runner.FixtureResolutionResult(
            dead_letter_entry_id=self.dead_letter_entry_id,
            dead_letter_fixture_prepared=self.prepared,
            checks_failed=self.checks_failed,
        )


class FakeExecutor:
    def __init__(self, *, execution: runner.ReplayRequestExecutionResult | None = None) -> None:
        self.calls = []
        self.execution = execution or _successful_execution()

    def execute(
        self,
        *,
        database_url: str,
        dead_letter_entry_id: UUID | None,
        notification_plan_id: UUID | None,
    ) -> runner.ReplayRequestExecutionResult:
        self.calls.append(
            {
                "database_url": database_url,
                "dead_letter_entry_id": dead_letter_entry_id,
                "notification_plan_id": notification_plan_id,
            }
        )
        return self.execution


def _parse_args(*args: str):
    return runner.build_parser().parse_args(args)


def _run(*args: str, env=None, resolver=None, executor=None) -> runner.RunnerResult:
    return runner.run(
        _parse_args(*args),
        env=env or {"APP_ENV": "test"},
        resolver=resolver,
        executor=executor,
        repo_root=ROOT,
    )


def test_cli_requires_app_env_test() -> None:
    executor = FakeExecutor()

    result = _run(
        "--database-url",
        SAFE_SOCKET_URL,
        "--dead-letter-entry-id",
        str(DEAD_LETTER_ID),
        "--operator-approval",
        APPROVAL,
        "--confirm-local-test-db",
        env={"APP_ENV": "dev"},
        executor=executor,
    )

    assert result.exit_code == 1
    assert result.report["checks_failed"] == ["app_env_test_required"]
    assert executor.calls == []


def test_cli_requires_confirm_local_test_db() -> None:
    executor = FakeExecutor()

    result = _run(
        "--database-url",
        SAFE_SOCKET_URL,
        "--dead-letter-entry-id",
        str(DEAD_LETTER_ID),
        "--operator-approval",
        APPROVAL,
        executor=executor,
    )

    assert result.exit_code == 1
    assert result.report["database_url_guard_passed"] is True
    assert result.report["checks_failed"] == ["confirm_local_test_db_required"]
    assert executor.calls == []


def test_cli_requires_exact_operator_approval_token() -> None:
    executor = FakeExecutor()

    missing = _run(
        "--database-url",
        SAFE_SOCKET_URL,
        "--dead-letter-entry-id",
        str(DEAD_LETTER_ID),
        "--confirm-local-test-db",
        executor=executor,
    )
    wrong = _run(
        "--database-url",
        SAFE_SOCKET_URL,
        "--dead-letter-entry-id",
        str(DEAD_LETTER_ID),
        "--operator-approval",
        "operator-approved",
        "--confirm-local-test-db",
        executor=executor,
    )

    assert missing.report["operator_approval_status"] == "missing"
    assert missing.report["checks_failed"] == ["operator_approval_required"]
    assert wrong.report["operator_approval_status"] == "rejected"
    assert wrong.report["checks_failed"] == ["operator_approval_invalid"]
    assert executor.calls == []


def test_unsafe_database_url_is_rejected_by_existing_guard() -> None:
    executor = FakeExecutor()

    result = _run(
        "--database-url",
        REMOTE_UNSAFE_URL,
        "--dead-letter-entry-id",
        str(DEAD_LETTER_ID),
        "--operator-approval",
        APPROVAL,
        "--confirm-local-test-db",
        executor=executor,
    )

    assert result.exit_code == 1
    assert result.report["database_url_guard_passed"] is False
    assert "database_url_remote_host_rejected" in result.report["checks_failed"]
    assert executor.calls == []


def test_invalid_dead_letter_entry_id_is_rejected() -> None:
    executor = FakeExecutor()

    result = _run(
        "--database-url",
        SAFE_SOCKET_URL,
        "--dead-letter-entry-id",
        "not-a-uuid",
        "--operator-approval",
        APPROVAL,
        "--confirm-local-test-db",
        executor=executor,
    )

    assert result.exit_code == 1
    assert "dead_letter_entry_id_invalid" in result.report["checks_failed"]
    assert executor.calls == []


def test_invalid_notification_plan_id_is_rejected() -> None:
    executor = FakeExecutor()

    result = _run(
        "--database-url",
        SAFE_SOCKET_URL,
        "--notification-plan-id",
        "not-a-uuid",
        "--operator-approval",
        APPROVAL,
        "--confirm-local-test-db",
        executor=executor,
    )

    assert result.exit_code == 1
    assert "notification_plan_id_invalid" in result.report["checks_failed"]
    assert executor.calls == []


def test_selector_ambiguity_is_rejected() -> None:
    result = _run(
        "--database-url",
        SAFE_SOCKET_URL,
        "--dead-letter-entry-id",
        str(DEAD_LETTER_ID),
        "--notification-plan-id",
        str(PLAN_ID),
        "--operator-approval",
        APPROVAL,
        "--confirm-local-test-db",
        executor=FakeExecutor(),
    )

    assert result.exit_code == 1
    assert result.report["checks_failed"] == ["selector_mode_ambiguous"]


def test_fixture_chain_incomplete_is_rejected() -> None:
    result = _run(
        "--database-url",
        SAFE_SOCKET_URL,
        "--source-fixture",
        SOURCE_FIXTURE,
        "--github-snapshot-fixture",
        GITHUB_FIXTURE,
        "--replay-namespace",
        "unit-incomplete-fixture",
        "--operator-approval",
        APPROVAL,
        "--confirm-local-test-db",
        resolver=FakeResolver(),
        executor=FakeExecutor(),
    )

    assert result.exit_code == 1
    assert result.report["checks_failed"] == ["fixture_selector_incomplete"]


def test_no_selector_mode_is_rejected_with_stable_code() -> None:
    result = _run(
        "--database-url",
        SAFE_SOCKET_URL,
        "--operator-approval",
        APPROVAL,
        "--confirm-local-test-db",
        executor=FakeExecutor(),
    )

    assert result.exit_code == 1
    assert result.report["checks_failed"] == ["selector_mode_required"]


def test_explicit_dead_letter_mode_loads_dlq_row_and_bypasses_fixture_resolver() -> None:
    resolver = FakeResolver()
    executor = FakeExecutor()

    result = _run(
        "--database-url",
        SAFE_SOCKET_URL,
        "--dead-letter-entry-id",
        str(DEAD_LETTER_ID),
        "--operator-approval",
        APPROVAL,
        "--confirm-local-test-db",
        resolver=resolver,
        executor=executor,
    )

    assert result.exit_code == 0
    assert result.report == _expected_pass_report(dead_letter_fixture_prepared=False)
    assert resolver.calls == []
    assert executor.calls == [
        {"database_url": SAFE_SOCKET_URL, "dead_letter_entry_id": DEAD_LETTER_ID, "notification_plan_id": None}
    ]


def test_explicit_notification_plan_mode_locates_dlq_row_in_executor() -> None:
    executor = FakeExecutor()

    result = _run(
        "--database-url",
        SAFE_SOCKET_URL,
        "--notification-plan-id",
        str(PLAN_ID),
        "--operator-approval",
        APPROVAL,
        "--confirm-local-test-db",
        executor=executor,
    )

    assert result.exit_code == 0
    assert executor.calls == [
        {"database_url": SAFE_SOCKET_URL, "dead_letter_entry_id": None, "notification_plan_id": PLAN_ID}
    ]


def test_fixture_chain_mode_runs_predecessor_resolver_and_passes_max_attempts() -> None:
    resolver = FakeResolver()
    executor = FakeExecutor()

    result = _run(
        "--database-url",
        SAFE_SOCKET_URL,
        "--source-fixture",
        SOURCE_FIXTURE,
        "--github-snapshot-fixture",
        GITHUB_FIXTURE,
        "--replay-namespace",
        "unit-operator-approved-dlq-replay",
        "--prepare-failed-retryable-fixture",
        "--max-attempts",
        "6",
        "--operator-approval",
        APPROVAL,
        "--confirm-local-test-db",
        resolver=resolver,
        executor=executor,
    )

    assert result.exit_code == 0
    assert result.report == _expected_pass_report(dead_letter_fixture_prepared=True)
    assert resolver.calls[0]["max_attempts"] == 6
    assert executor.calls[0]["dead_letter_entry_id"] == DEAD_LETTER_ID


def test_missing_dlq_row_fails_with_stable_failure_code() -> None:
    result = _run(
        "--database-url",
        SAFE_SOCKET_URL,
        "--dead-letter-entry-id",
        str(DEAD_LETTER_ID),
        "--operator-approval",
        APPROVAL,
        "--confirm-local-test-db",
        executor=FakeExecutor(execution=_execution_with_failures(("dead_letter_missing_or_invalid",))),
    )

    assert result.exit_code == 1
    assert result.report["checks_failed"][0] == "dead_letter_missing_or_invalid"
    assert "dead_letter_loaded:missing" in result.report["checks_failed"]


def test_wrong_replay_hint_fails() -> None:
    result = _run(
        "--database-url",
        SAFE_SOCKET_URL,
        "--dead-letter-entry-id",
        str(DEAD_LETTER_ID),
        "--operator-approval",
        APPROVAL,
        "--confirm-local-test-db",
        executor=FakeExecutor(
            execution=_execution_with_failures(
                ("dead_letter_replay_hint_invalid",),
                dead_letter_loaded=True,
                dead_letter_replay_hint_valid=False,
            )
        ),
    )

    assert result.exit_code == 1
    assert "dead_letter_replay_hint_invalid" in result.report["checks_failed"]


def test_non_notification_plan_root_fails() -> None:
    result = _run(
        "--database-url",
        SAFE_SOCKET_URL,
        "--dead-letter-entry-id",
        str(DEAD_LETTER_ID),
        "--operator-approval",
        APPROVAL,
        "--confirm-local-test-db",
        executor=FakeExecutor(
            execution=_execution_with_failures(
                ("dead_letter_root_object_invalid",),
                dead_letter_loaded=True,
                dead_letter_replay_hint_valid=True,
            )
        ),
    )

    assert result.exit_code == 1
    assert "dead_letter_root_object_invalid" in result.report["checks_failed"]


def test_missing_notification_plan_fails() -> None:
    result = _run(
        "--database-url",
        SAFE_SOCKET_URL,
        "--dead-letter-entry-id",
        str(DEAD_LETTER_ID),
        "--operator-approval",
        APPROVAL,
        "--confirm-local-test-db",
        executor=FakeExecutor(
            execution=_execution_with_failures(
                ("notification_plan_missing_or_invalid",),
                dead_letter_loaded=True,
                dead_letter_replay_hint_valid=True,
                notification_plan_loaded=False,
            )
        ),
    )

    assert result.exit_code == 1
    assert "notification_plan_missing_or_invalid" in result.report["checks_failed"]


def test_operator_approved_path_creates_or_reuses_one_replay_request_and_outbox_event() -> None:
    result = _run(
        "--database-url",
        SAFE_SOCKET_URL,
        "--dead-letter-entry-id",
        str(DEAD_LETTER_ID),
        "--operator-approval",
        APPROVAL,
        "--confirm-local-test-db",
        executor=FakeExecutor(),
    )

    assert result.exit_code == 0
    assert result.report["delivery_replay_request_created"] is True
    assert result.report["delivery_replay_request_count_before"] == 0
    assert result.report["delivery_replay_request_count_after"] == 1
    assert result.report["replay_requested_event_created"] is True
    assert result.report["replay_requested_event_count_before"] == 0
    assert result.report["replay_requested_event_count_after"] == 1


def test_replay_requested_payload_includes_source_dead_letter_entry_id() -> None:
    payload = runner.build_replay_requested_payload(
        replay_request_id=REPLAY_REQUEST_ID,
        notification_plan_id=PLAN_ID,
        dead_letter_entry_id=DEAD_LETTER_ID,
    )

    assert payload == {
        "replay_request_id": str(REPLAY_REQUEST_ID),
        "replay_type": "delivery",
        "root_object_type": "notification_plan",
        "root_object_id": str(PLAN_ID),
        "operator_approval": APPROVAL,
        "replay_reason": "operator_approved_dead_letter_delivery_replay",
        "source_dead_letter_entry_id": str(DEAD_LETTER_ID),
    }


def test_no_notification_plan_created_replay_intent_or_notifier_side_effects_created() -> None:
    result = _run(
        "--database-url",
        SAFE_SOCKET_URL,
        "--dead-letter-entry-id",
        str(DEAD_LETTER_ID),
        "--operator-approval",
        APPROVAL,
        "--confirm-local-test-db",
        executor=FakeExecutor(),
    )

    assert result.exit_code == 0
    assert result.report["notification_plan_created_replay_intent_created"] is False
    assert result.report["notification_plan_created_event_count_before"] == 0
    assert result.report["notification_plan_created_event_count_after"] == 0
    assert result.report["notifier_render_created"] is False
    assert result.report["notification_delivery_record_created"] is False


def test_mutation_boundary_booleans_are_false() -> None:
    result = _run(
        "--database-url",
        SAFE_SOCKET_URL,
        "--dead-letter-entry-id",
        str(DEAD_LETTER_ID),
        "--operator-approval",
        APPROVAL,
        "--confirm-local-test-db",
        executor=FakeExecutor(),
    )

    assert result.exit_code == 0
    assert result.report["dead_letter_mutated"] is False
    assert result.report["notification_plan_mutated"] is False
    assert result.report["notification_render_mutated"] is False
    assert result.report["notification_delivery_record_mutated"] is False
    assert result.report["state_transition_mutated"] is False
    assert result.report["analysis_mutated"] is False
    assert result.report["judge_output_mutated"] is False
    assert result.report["candidate_group_mutated"] is False
    assert result.report["evidence_bundle_mutated"] is False
    assert result.report["artifact_mutated"] is False
    assert result.report["source_message_mutated"] is False


def test_idempotent_rerun_is_bounded() -> None:
    execution = _successful_execution(
        delivery_replay_request_count_before=1,
        delivery_replay_request_count_after=1,
        replay_requested_event_count_before=1,
        replay_requested_event_count_after=1,
        maintenance_pipeline_run_count_before=1,
        maintenance_pipeline_run_count_after=1,
        maintenance_job_attempt_count_before=1,
        maintenance_job_attempt_count_after=1,
    )

    result = _run(
        "--database-url",
        SAFE_SOCKET_URL,
        "--dead-letter-entry-id",
        str(DEAD_LETTER_ID),
        "--operator-approval",
        APPROVAL,
        "--confirm-local-test-db",
        executor=FakeExecutor(execution=execution),
    )

    assert result.exit_code == 0
    assert result.report["delivery_replay_request_count_before"] == 1
    assert result.report["delivery_replay_request_count_after"] == 1
    assert result.report["replay_request_dedupe_or_uniqueness_stable"] is True
    assert result.report["replay_requested_event_dedupe_key_stable"] is True


def test_report_output_is_stable_and_sanitized() -> None:
    result = _run(
        "--database-url",
        PASSWORD_URL,
        "--dead-letter-entry-id",
        str(DEAD_LETTER_ID),
        "--operator-approval",
        APPROVAL,
        "--confirm-local-test-db",
        executor=FakeExecutor(),
    )

    text = runner.render_json(result.report)
    parsed = json.loads(text)
    assert list(parsed) == list(_expected_pass_report(dead_letter_fixture_prepared=False))
    assert PASSWORD_URL not in text
    assert SECRET_VALUE not in text
    assert "Traceback" not in text


def test_runner_source_has_no_forbidden_runtime_or_network_imports() -> None:
    source = (
        ROOT / "tools/local_db_operator_approved_dead_letter_delivery_replay_request_fixture_runner.py"
    ).read_text(encoding="utf-8")
    tree = ast.parse(source)
    imported_roots = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imported_roots.update(alias.name.split(".", 1)[0] for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            imported_roots.add(node.module.split(".", 1)[0])

    forbidden = {"redis", "openai", "telegram", "docker", "systemd", "requests", "httpx", "aiohttp"}
    assert forbidden.isdisjoint(imported_roots)


def test_runner_source_contains_no_ddl_strings() -> None:
    source = (
        ROOT / "tools/local_db_operator_approved_dead_letter_delivery_replay_request_fixture_runner.py"
    ).read_text(encoding="utf-8")

    assert "CREATE TABLE" not in source
    assert "ALTER TABLE" not in source
    assert "DROP TABLE" not in source


def _successful_execution(
    *,
    delivery_replay_request_count_before: int = 0,
    delivery_replay_request_count_after: int = 1,
    replay_requested_event_count_before: int = 0,
    replay_requested_event_count_after: int = 1,
    maintenance_pipeline_run_count_before: int = 0,
    maintenance_pipeline_run_count_after: int = 1,
    maintenance_job_attempt_count_before: int = 0,
    maintenance_job_attempt_count_after: int = 1,
) -> runner.ReplayRequestExecutionResult:
    return runner.ReplayRequestExecutionResult(
        dead_letter_loaded=True,
        dead_letter_replay_hint_valid=True,
        notification_plan_loaded=True,
        delivery_replay_request_created=True,
        delivery_replay_request_payload_matches_dead_letter=True,
        replay_request_dedupe_or_uniqueness_stable=True,
        replay_requested_event_created=True,
        replay_requested_event_payload_matches_request=True,
        replay_requested_event_dedupe_key_stable=True,
        maintenance_pipeline_run_recorded=True,
        maintenance_job_attempt_recorded=True,
        notification_plan_created_replay_intent_created=False,
        notifier_render_created=False,
        notification_delivery_record_created=False,
        dead_letter_mutated=False,
        notification_plan_mutated=False,
        notification_render_mutated=False,
        notification_delivery_record_mutated=False,
        state_transition_mutated=False,
        analysis_mutated=False,
        judge_output_mutated=False,
        candidate_group_mutated=False,
        evidence_bundle_mutated=False,
        artifact_mutated=False,
        source_message_mutated=False,
        delivery_replay_request_count_before=delivery_replay_request_count_before,
        delivery_replay_request_count_after=delivery_replay_request_count_after,
        replay_requested_event_count_before=replay_requested_event_count_before,
        replay_requested_event_count_after=replay_requested_event_count_after,
        notification_plan_created_event_count_before=0,
        notification_plan_created_event_count_after=0,
        maintenance_pipeline_run_count_before=maintenance_pipeline_run_count_before,
        maintenance_pipeline_run_count_after=maintenance_pipeline_run_count_after,
        maintenance_job_attempt_count_before=maintenance_job_attempt_count_before,
        maintenance_job_attempt_count_after=maintenance_job_attempt_count_after,
    )


def _execution_with_failures(
    checks_failed: tuple[str, ...],
    *,
    dead_letter_loaded: bool = False,
    dead_letter_replay_hint_valid: bool = False,
    notification_plan_loaded: bool = False,
) -> runner.ReplayRequestExecutionResult:
    return runner.ReplayRequestExecutionResult(
        dead_letter_loaded=dead_letter_loaded,
        dead_letter_replay_hint_valid=dead_letter_replay_hint_valid,
        notification_plan_loaded=notification_plan_loaded,
        delivery_replay_request_created=False,
        delivery_replay_request_payload_matches_dead_letter=False,
        replay_request_dedupe_or_uniqueness_stable=False,
        replay_requested_event_created=False,
        replay_requested_event_payload_matches_request=False,
        replay_requested_event_dedupe_key_stable=False,
        maintenance_pipeline_run_recorded=False,
        maintenance_job_attempt_recorded=False,
        checks_failed=checks_failed,
    )


def _expected_pass_report(*, dead_letter_fixture_prepared: bool) -> dict[str, object]:
    return {
        "schema_version": "local_db_operator_approved_dead_letter_delivery_replay_request_fixture_runner_v1",
        "status": "pass",
        "database_url_guard_passed": True,
        "operator_approval_status": "approved",
        "dead_letter_fixture_prepared": dead_letter_fixture_prepared,
        "dead_letter_loaded": True,
        "dead_letter_replay_hint_valid": True,
        "notification_plan_loaded": True,
        "delivery_replay_request_created": True,
        "delivery_replay_request_payload_matches_dead_letter": True,
        "replay_request_dedupe_or_uniqueness_stable": True,
        "replay_requested_event_created": True,
        "replay_requested_event_payload_matches_request": True,
        "replay_requested_event_dedupe_key_stable": True,
        "maintenance_pipeline_run_recorded": True,
        "maintenance_job_attempt_recorded": True,
        "notification_plan_created_replay_intent_created": False,
        "notifier_render_created": False,
        "notification_delivery_record_created": False,
        "dead_letter_mutated": False,
        "notification_plan_mutated": False,
        "notification_render_mutated": False,
        "notification_delivery_record_mutated": False,
        "state_transition_mutated": False,
        "analysis_mutated": False,
        "judge_output_mutated": False,
        "candidate_group_mutated": False,
        "evidence_bundle_mutated": False,
        "artifact_mutated": False,
        "source_message_mutated": False,
        "openai_called": False,
        "telegram_called": False,
        "live_github_called": False,
        "workers_started": False,
        "redis_mutation": False,
        "production_db_write": False,
        "alembic_or_ddl_ran": False,
        "delivery_replay_request_count_before": 0,
        "delivery_replay_request_count_after": 1,
        "replay_requested_event_count_before": 0,
        "replay_requested_event_count_after": 1,
        "notification_plan_created_event_count_before": 0,
        "notification_plan_created_event_count_after": 0,
        "maintenance_pipeline_run_count_before": 0,
        "maintenance_pipeline_run_count_after": 1,
        "maintenance_job_attempt_count_before": 0,
        "maintenance_job_attempt_count_after": 1,
        "checks_failed": [],
    }
