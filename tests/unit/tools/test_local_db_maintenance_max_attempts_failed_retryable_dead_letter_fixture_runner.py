from __future__ import annotations

import ast
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from uuid import UUID

from tools import local_db_maintenance_max_attempts_failed_retryable_dead_letter_fixture_runner as runner


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
PLAN_ID = UUID("10000000-0000-4000-8000-000000000001")
ANALYSIS_ID = UUID("20000000-0000-4000-8000-000000000002")
GROUP_ID = UUID("30000000-0000-4000-8000-000000000003")


class FakeResolver:
    def __init__(
        self,
        *,
        notification_plan_id: UUID | None = PLAN_ID,
        prepared: bool = True,
        checks_failed: tuple[str, ...] = (),
    ) -> None:
        self.calls = []
        self.notification_plan_id = notification_plan_id
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
            notification_plan_id=self.notification_plan_id,
            failed_retryable_fixture_prepared=self.prepared,
            checks_failed=self.checks_failed,
        )


class FakeExecutor:
    def __init__(self, *, execution: runner.MaintenanceExecutionResult | None = None) -> None:
        self.calls = []
        self.execution = execution or _successful_execution()

    def execute(
        self,
        *,
        database_url: str,
        notification_plan_id: UUID,
        max_attempts: int,
    ) -> runner.MaintenanceExecutionResult:
        self.calls.append(
            {
                "database_url": database_url,
                "notification_plan_id": notification_plan_id,
                "max_attempts": max_attempts,
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
        "--notification-plan-id",
        str(PLAN_ID),
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
        "--notification-plan-id",
        str(PLAN_ID),
        executor=executor,
    )

    assert result.exit_code == 1
    assert result.report["database_url_guard_passed"] is True
    assert result.report["checks_failed"] == ["confirm_local_test_db_required"]
    assert executor.calls == []


def test_unsafe_database_url_is_rejected_by_existing_guard() -> None:
    executor = FakeExecutor()

    result = _run(
        "--database-url",
        REMOTE_UNSAFE_URL,
        "--notification-plan-id",
        str(PLAN_ID),
        "--confirm-local-test-db",
        executor=executor,
    )

    assert result.exit_code == 1
    assert result.report["database_url_guard_passed"] is False
    assert "database_url_remote_host_rejected" in result.report["checks_failed"]
    assert executor.calls == []


def test_invalid_notification_plan_id_is_rejected() -> None:
    executor = FakeExecutor()

    result = _run(
        "--database-url",
        SAFE_SOCKET_URL,
        "--notification-plan-id",
        "not-a-uuid",
        "--confirm-local-test-db",
        executor=executor,
    )

    assert result.exit_code == 1
    assert "notification_plan_id_invalid" in result.report["checks_failed"]
    assert executor.calls == []


def test_invalid_max_attempts_is_rejected() -> None:
    executor = FakeExecutor()

    result = _run(
        "--database-url",
        SAFE_SOCKET_URL,
        "--notification-plan-id",
        str(PLAN_ID),
        "--max-attempts",
        "0",
        "--confirm-local-test-db",
        executor=executor,
    )

    assert result.exit_code == 1
    assert result.report["checks_failed"] == ["max_attempts_invalid"]
    assert executor.calls == []


def test_default_max_attempts_prefers_notification_env_over_delivery_env() -> None:
    executor = FakeExecutor()

    result = _run(
        "--database-url",
        SAFE_SOCKET_URL,
        "--notification-plan-id",
        str(PLAN_ID),
        "--confirm-local-test-db",
        env={"APP_ENV": "test", "NOTIFICATION_RETRY_MAX_ATTEMPTS": "7", "DELIVERY_RETRY_MAX_ATTEMPTS": "9"},
        executor=executor,
    )

    assert result.exit_code == 0
    assert executor.calls == [{"database_url": SAFE_SOCKET_URL, "notification_plan_id": PLAN_ID, "max_attempts": 7}]


def test_selector_ambiguity_is_rejected() -> None:
    result = _run(
        "--database-url",
        SAFE_SOCKET_URL,
        "--notification-plan-id",
        str(PLAN_ID),
        "--source-fixture",
        SOURCE_FIXTURE,
        "--github-snapshot-fixture",
        GITHUB_FIXTURE,
        "--replay-namespace",
        "unit-ambiguous-selector",
        "--prepare-failed-retryable-fixture",
        "--confirm-local-test-db",
        resolver=FakeResolver(),
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
        "--confirm-local-test-db",
        resolver=FakeResolver(),
        executor=FakeExecutor(),
    )

    assert result.exit_code == 1
    assert "fixture_selector_required" in result.report["checks_failed"]
    assert "fixture_selector_incomplete" in result.report["checks_failed"]


def test_explicit_plan_mode_bypasses_fixture_resolver() -> None:
    resolver = FakeResolver()
    executor = FakeExecutor()

    result = _run(
        "--database-url",
        SAFE_SOCKET_URL,
        "--notification-plan-id",
        str(PLAN_ID),
        "--confirm-local-test-db",
        resolver=resolver,
        executor=executor,
    )

    assert result.exit_code == 0
    assert result.report == _expected_pass_report(failed_retryable_fixture_prepared=False)
    assert resolver.calls == []
    assert executor.calls == [{"database_url": SAFE_SOCKET_URL, "notification_plan_id": PLAN_ID, "max_attempts": 5}]


def test_fixture_chain_mode_passes_max_attempts_to_failed_retryable_fixture_preparation() -> None:
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
        "unit-maintenance-retry-ceiling",
        "--prepare-failed-retryable-fixture",
        "--max-attempts",
        "6",
        "--confirm-local-test-db",
        resolver=resolver,
        executor=executor,
    )

    assert result.exit_code == 0
    assert result.report == _expected_pass_report(failed_retryable_fixture_prepared=True)
    assert resolver.calls[0]["max_attempts"] == 6
    assert executor.calls == [{"database_url": SAFE_SOCKET_URL, "notification_plan_id": PLAN_ID, "max_attempts": 6}]


def test_attempt_count_below_max_is_not_accepted_by_retry_ceiling_runner() -> None:
    plan = _plan()

    decision = runner.evaluate_retry_ceiling(
        delivery_status="failed_retryable",
        plan=plan,
        latest_attempt_count=4,
        max_attempts=5,
        now=datetime.now(timezone.utc),
    )

    assert decision["action"] == "emit_retry_intent"
    assert decision["reason_code"] == "due_retry_promotion"


def test_suppressed_dry_run_row_is_not_auto_dead_lettered() -> None:
    plan = _plan(status="suppressed", suppress_reason_code="dry_run_skip_transport")

    decision = runner.evaluate_retry_ceiling(
        delivery_status="suppressed",
        plan=plan,
        latest_attempt_count=5,
        max_attempts=5,
        now=datetime.now(timezone.utc),
    )

    assert decision["action"] == "noop"
    assert decision["reason_code"] == "delivery_status_not_retryable"


def test_send_disabled_suppressed_row_is_not_auto_dead_lettered() -> None:
    plan = _plan(status="suppressed", suppress_reason_code="notification_send_flag_disabled")

    decision = runner.evaluate_retry_ceiling(
        delivery_status="suppressed",
        plan=plan,
        latest_attempt_count=5,
        max_attempts=5,
        now=datetime.now(timezone.utc),
    )

    assert decision["action"] == "noop"
    assert decision["reason_code"] == "delivery_status_not_retryable"


def test_terminal_failures_are_out_of_scope_for_retry_ceiling_runner() -> None:
    plan = _plan(status="failed_terminal")

    decision = runner.evaluate_retry_ceiling(
        delivery_status="failed_terminal",
        plan=plan,
        latest_attempt_count=5,
        max_attempts=5,
        now=datetime.now(timezone.utc),
    )

    assert decision["action"] == "noop"
    assert decision["reason_code"] == "delivery_status_not_retryable"


def test_max_attempts_exceeded_creates_or_reuses_exactly_one_matching_dlq_row() -> None:
    result = _run(
        "--database-url",
        SAFE_SOCKET_URL,
        "--notification-plan-id",
        str(PLAN_ID),
        "--confirm-local-test-db",
        executor=FakeExecutor(execution=_successful_execution(dead_letter_count_before=0, dead_letter_count_after=1)),
    )

    assert result.exit_code == 0
    assert result.report["max_attempts_exceeded"] is True
    assert result.report["dead_letter_created"] is True
    assert result.report["dead_letter_count_before"] == 0
    assert result.report["dead_letter_count_after"] == 1


def test_no_retry_intent_event_or_replay_request_is_created() -> None:
    result = _run(
        "--database-url",
        SAFE_SOCKET_URL,
        "--notification-plan-id",
        str(PLAN_ID),
        "--confirm-local-test-db",
        executor=FakeExecutor(),
    )

    assert result.exit_code == 0
    assert result.report["retry_intent_event_created"] is False
    assert result.report["retry_intent_event_count_before"] == 0
    assert result.report["retry_intent_event_count_after"] == 0
    assert result.report["replay_request_created"] is False


def test_mutation_boundary_booleans_are_false() -> None:
    result = _run(
        "--database-url",
        SAFE_SOCKET_URL,
        "--notification-plan-id",
        str(PLAN_ID),
        "--confirm-local-test-db",
        executor=FakeExecutor(),
    )

    assert result.exit_code == 0
    assert result.report["notification_plan_mutated_by_maintenance"] is False
    assert result.report["notification_delivery_record_mutated_by_maintenance"] is False
    assert result.report["analysis_mutated"] is False
    assert result.report["judge_output_mutated"] is False
    assert result.report["candidate_group_mutated"] is False
    assert result.report["evidence_bundle_mutated"] is False
    assert result.report["artifact_mutated"] is False
    assert result.report["source_message_mutated"] is False


def test_idempotent_rerun_is_bounded() -> None:
    execution = _successful_execution(
        dead_letter_count_before=1,
        dead_letter_count_after=1,
        maintenance_pipeline_run_count_before=1,
        maintenance_pipeline_run_count_after=1,
        maintenance_job_attempt_count_before=1,
        maintenance_job_attempt_count_after=1,
    )

    result = _run(
        "--database-url",
        SAFE_SOCKET_URL,
        "--notification-plan-id",
        str(PLAN_ID),
        "--confirm-local-test-db",
        executor=FakeExecutor(execution=execution),
    )

    assert result.exit_code == 0
    assert result.report["dead_letter_count_before"] == 1
    assert result.report["dead_letter_count_after"] == 1
    assert result.report["dead_letter_dedupe_or_uniqueness_stable"] is True


def test_report_shape_is_stable_and_sanitized() -> None:
    result = _run(
        "--database-url",
        PASSWORD_URL,
        "--notification-plan-id",
        str(PLAN_ID),
        "--confirm-local-test-db",
        executor=FakeExecutor(),
    )

    text = runner.render_json(result.report)
    parsed = json.loads(text)
    assert list(parsed) == list(_expected_pass_report(failed_retryable_fixture_prepared=False))
    assert PASSWORD_URL not in text
    assert SECRET_VALUE not in text


def test_runner_source_has_no_forbidden_runtime_or_network_imports() -> None:
    source = (
        ROOT / "tools/local_db_maintenance_max_attempts_failed_retryable_dead_letter_fixture_runner.py"
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


def test_runner_source_contains_no_ddl() -> None:
    source = (
        ROOT / "tools/local_db_maintenance_max_attempts_failed_retryable_dead_letter_fixture_runner.py"
    ).read_text(encoding="utf-8")

    assert "CREATE TABLE" not in source
    assert "ALTER TABLE" not in source
    assert "DROP TABLE" not in source


def _successful_execution(
    *,
    dead_letter_count_before: int = 0,
    dead_letter_count_after: int = 1,
    maintenance_pipeline_run_count_before: int = 0,
    maintenance_pipeline_run_count_after: int = 1,
    maintenance_job_attempt_count_before: int = 0,
    maintenance_job_attempt_count_after: int = 1,
) -> runner.MaintenanceExecutionResult:
    return runner.MaintenanceExecutionResult(
        due_failed_retryable_plan_loaded=True,
        latest_failed_retryable_delivery_record_loaded=True,
        retry_ceiling_candidate_valid=True,
        max_attempts_exceeded=True,
        retry_intent_event_created=False,
        dead_letter_created=True,
        dead_letter_payload_matches_plan=True,
        dead_letter_dedupe_or_uniqueness_stable=True,
        maintenance_pipeline_run_recorded=True,
        maintenance_job_attempt_recorded=True,
        replay_request_created=False,
        notification_plan_mutated_by_maintenance=False,
        notification_delivery_record_mutated_by_maintenance=False,
        analysis_mutated=False,
        judge_output_mutated=False,
        candidate_group_mutated=False,
        evidence_bundle_mutated=False,
        artifact_mutated=False,
        source_message_mutated=False,
        retry_intent_event_count_before=0,
        retry_intent_event_count_after=0,
        dead_letter_count_before=dead_letter_count_before,
        dead_letter_count_after=dead_letter_count_after,
        maintenance_pipeline_run_count_before=maintenance_pipeline_run_count_before,
        maintenance_pipeline_run_count_after=maintenance_pipeline_run_count_after,
        maintenance_job_attempt_count_before=maintenance_job_attempt_count_before,
        maintenance_job_attempt_count_after=maintenance_job_attempt_count_after,
    )


def _plan(
    *,
    status: str = "failed_retryable",
    send_after=None,
    delivery_decision: str = "send_now",
    urgency_profile: str = "high",
    suppress_reason_code: str | None = None,
) -> runner.NotificationPlanRecord:
    now = datetime.now(timezone.utc)
    return runner.NotificationPlanRecord(
        notification_plan_id=PLAN_ID,
        analysis_id=ANALYSIS_ID,
        candidate_group_id=GROUP_ID,
        delivery_decision=delivery_decision,
        urgency_profile=urgency_profile,
        target_chat_id=123,
        target_thread_id=None,
        render_profile="telegram_single_alert_high_v1",
        dedupe_subject_key="unit-subject",
        material_change_hash="unit-material",
        send_after=send_after if send_after is not None else now - timedelta(minutes=1),
        suppress_reason_code=suppress_reason_code,
        status=status,
    )


def _expected_pass_report(*, failed_retryable_fixture_prepared: bool) -> dict[str, object]:
    return {
        "schema_version": "local_db_maintenance_max_attempts_failed_retryable_dead_letter_fixture_runner_v1",
        "status": "pass",
        "database_url_guard_passed": True,
        "failed_retryable_fixture_prepared": failed_retryable_fixture_prepared,
        "due_failed_retryable_plan_loaded": True,
        "latest_failed_retryable_delivery_record_loaded": True,
        "retry_ceiling_candidate_valid": True,
        "max_attempts_exceeded": True,
        "retry_intent_event_created": False,
        "dead_letter_created": True,
        "dead_letter_payload_matches_plan": True,
        "dead_letter_dedupe_or_uniqueness_stable": True,
        "maintenance_pipeline_run_recorded": True,
        "maintenance_job_attempt_recorded": True,
        "replay_request_created": False,
        "notification_plan_mutated_by_maintenance": False,
        "notification_delivery_record_mutated_by_maintenance": False,
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
        "retry_intent_event_count_before": 0,
        "retry_intent_event_count_after": 0,
        "dead_letter_count_before": 0,
        "dead_letter_count_after": 1,
        "maintenance_pipeline_run_count_before": 0,
        "maintenance_pipeline_run_count_after": 1,
        "maintenance_job_attempt_count_before": 0,
        "maintenance_job_attempt_count_after": 1,
        "checks_failed": [],
    }
