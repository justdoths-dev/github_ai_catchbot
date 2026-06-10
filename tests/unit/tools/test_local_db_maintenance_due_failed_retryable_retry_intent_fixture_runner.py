from __future__ import annotations

import ast
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from uuid import UUID

from tools import local_db_maintenance_due_failed_retryable_retry_intent_fixture_runner as runner


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
        env,
        repo_root: Path,
    ) -> runner.FixtureResolutionResult:
        self.calls.append(
            {
                "database_url": database_url,
                "source_fixture_path": source_fixture_path,
                "github_snapshot_fixture_path": github_snapshot_fixture_path,
                "replay_namespace": replay_namespace,
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


def test_explicit_notification_plan_id_mode_bypasses_fixture_setup_resolver() -> None:
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


def test_fixture_mode_requires_source_github_snapshot_namespace_and_prepare_flag() -> None:
    result = _run(
        "--database-url",
        SAFE_SOCKET_URL,
        "--source-fixture",
        SOURCE_FIXTURE,
        "--github-snapshot-fixture",
        GITHUB_FIXTURE,
        "--replay-namespace",
        "unit-maintenance-retry",
        "--confirm-local-test-db",
        resolver=FakeResolver(),
        executor=FakeExecutor(),
    )

    assert result.exit_code == 1
    assert "fixture_selector_required" in result.report["checks_failed"]
    assert "fixture_selector_incomplete" in result.report["checks_failed"]


def test_fixture_mode_prepares_failed_retryable_fixture_before_maintenance_execution() -> None:
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
        "unit-maintenance-retry",
        "--prepare-failed-retryable-fixture",
        "--confirm-local-test-db",
        resolver=resolver,
        executor=executor,
    )

    assert result.exit_code == 0
    assert result.report == _expected_pass_report(failed_retryable_fixture_prepared=True)
    assert len(resolver.calls) == 1
    assert executor.calls == [{"database_url": SAFE_SOCKET_URL, "notification_plan_id": PLAN_ID, "max_attempts": 5}]


def test_ambiguous_selector_mode_is_rejected() -> None:
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


def test_due_failed_retryable_candidate_emits_retry_intent() -> None:
    result = _run(
        "--database-url",
        SAFE_SOCKET_URL,
        "--notification-plan-id",
        str(PLAN_ID),
        "--confirm-local-test-db",
        executor=FakeExecutor(),
    )

    assert result.exit_code == 0
    assert result.report["due_retry_candidate_valid"] is True
    assert result.report["retry_intent_event_created"] is True
    assert result.report["retry_intent_event_count_before"] == 0
    assert result.report["retry_intent_event_count_after"] == 1


def test_future_send_after_is_not_due_and_emits_no_retry_intent() -> None:
    plan = _plan(send_after=datetime.now(timezone.utc) + timedelta(minutes=5))

    decision = runner._evaluate_retry_promotion(  # noqa: SLF001 - runner boundary policy proof.
        delivery_status="failed_retryable",
        plan=plan,
        latest_attempt_count=1,
        max_attempts=5,
        now=datetime.now(timezone.utc),
    )

    assert decision["action"] == "noop"
    assert decision["reason_code"] == "notification_plan_not_due"
    assert decision["payload"] is None


def test_suppressed_dry_run_row_is_not_auto_retried() -> None:
    plan = _plan(status="suppressed", suppress_reason_code="dry_run_skip_transport")

    decision = runner._evaluate_retry_promotion(  # noqa: SLF001 - runner boundary policy proof.
        delivery_status="suppressed",
        plan=plan,
        latest_attempt_count=0,
        max_attempts=5,
        now=datetime.now(timezone.utc),
    )

    assert decision["action"] == "noop"
    assert decision["reason_code"] == "delivery_status_not_retryable"
    assert decision["payload"] is None


def test_send_disabled_suppressed_row_is_not_auto_retried() -> None:
    plan = _plan(status="suppressed", suppress_reason_code="notification_send_flag_disabled")

    decision = runner._evaluate_retry_promotion(  # noqa: SLF001 - runner boundary policy proof.
        delivery_status="suppressed",
        plan=plan,
        latest_attempt_count=0,
        max_attempts=5,
        now=datetime.now(timezone.utc),
    )

    assert decision["action"] == "noop"
    assert decision["reason_code"] == "delivery_status_not_retryable"
    assert decision["payload"] is None


def test_retry_intent_dedupe_key_is_deterministic() -> None:
    send_after = datetime(2026, 1, 1, 0, 0, tzinfo=timezone.utc)

    assert runner.build_retry_intent_dedupe_key(
        notification_plan_id=PLAN_ID,
        latest_attempt_count=1,
        send_after=send_after,
    ) == runner.build_retry_intent_dedupe_key(
        notification_plan_id=PLAN_ID,
        latest_attempt_count=1,
        send_after=send_after,
    )
    assert runner.build_retry_intent_dedupe_key(
        notification_plan_id=PLAN_ID,
        latest_attempt_count=1,
        send_after=send_after,
    ) == f"notify:retry-intent:{PLAN_ID}:1:1767225600"


def test_retry_intent_payload_preserves_upstream_delivery_decision_without_recompute() -> None:
    plan = _plan(delivery_decision="send_digest", urgency_profile="digest")

    decision = runner._evaluate_retry_promotion(  # noqa: SLF001 - runner boundary policy proof.
        delivery_status="failed_retryable",
        plan=plan,
        latest_attempt_count=1,
        max_attempts=5,
        now=datetime.now(timezone.utc),
    )

    assert decision["action"] == "emit_retry_intent"
    assert decision["payload"]["delivery_decision"] == "send_digest"
    assert decision["payload"]["urgency_profile"] == "digest"
    assert decision["payload"]["retry_reason"] == "due_retry_promotion"
    assert decision["payload"]["previous_attempt_count"] == 1
    assert decision["payload"]["send_after"] is None


def test_maintenance_does_not_mutate_notification_plan_or_delivery_record_after_fixture_preparation() -> None:
    result = _run(
        "--database-url",
        SAFE_SOCKET_URL,
        "--source-fixture",
        SOURCE_FIXTURE,
        "--github-snapshot-fixture",
        GITHUB_FIXTURE,
        "--replay-namespace",
        "unit-maintenance-retry",
        "--prepare-failed-retryable-fixture",
        "--confirm-local-test-db",
        resolver=FakeResolver(),
        executor=FakeExecutor(),
    )

    assert result.exit_code == 0
    assert result.report["failed_retryable_fixture_prepared"] is True
    assert result.report["notification_plan_mutated_by_maintenance"] is False
    assert result.report["notification_delivery_record_mutated_by_maintenance"] is False


def test_replay_request_is_not_created_in_this_due_retry_task() -> None:
    result = _run(
        "--database-url",
        SAFE_SOCKET_URL,
        "--notification-plan-id",
        str(PLAN_ID),
        "--confirm-local-test-db",
        executor=FakeExecutor(),
    )

    assert result.exit_code == 0
    assert result.report["replay_request_created"] is False


def test_dead_letter_is_not_created_when_attempt_count_is_below_max() -> None:
    result = _run(
        "--database-url",
        SAFE_SOCKET_URL,
        "--notification-plan-id",
        str(PLAN_ID),
        "--confirm-local-test-db",
        executor=FakeExecutor(),
    )

    assert result.exit_code == 0
    assert result.report["dead_letter_created"] is False


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
    source = (ROOT / "tools/local_db_maintenance_due_failed_retryable_retry_intent_fixture_runner.py").read_text(
        encoding="utf-8"
    )
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
    source = (ROOT / "tools/local_db_maintenance_due_failed_retryable_retry_intent_fixture_runner.py").read_text(
        encoding="utf-8"
    )

    assert "CREATE TABLE" not in source
    assert "ALTER TABLE" not in source
    assert "DROP TABLE" not in source


def _successful_execution() -> runner.MaintenanceExecutionResult:
    return runner.MaintenanceExecutionResult(
        due_failed_retryable_plan_loaded=True,
        latest_failed_retryable_delivery_record_loaded=True,
        due_retry_candidate_valid=True,
        retry_intent_event_created=True,
        retry_intent_payload_matches_plan=True,
        retry_intent_dedupe_key_stable=True,
        maintenance_pipeline_run_recorded=True,
        maintenance_job_attempt_recorded=True,
        replay_request_created=False,
        dead_letter_created=False,
        notification_plan_mutated_by_maintenance=False,
        notification_delivery_record_mutated_by_maintenance=False,
        analysis_mutated=False,
        judge_output_mutated=False,
        candidate_group_mutated=False,
        retry_intent_event_count_before=0,
        retry_intent_event_count_after=1,
        maintenance_pipeline_run_count_before=0,
        maintenance_pipeline_run_count_after=1,
        maintenance_job_attempt_count_before=0,
        maintenance_job_attempt_count_after=1,
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
        "schema_version": "local_db_maintenance_due_failed_retryable_retry_intent_fixture_runner_v1",
        "status": "pass",
        "database_url_guard_passed": True,
        "failed_retryable_fixture_prepared": failed_retryable_fixture_prepared,
        "due_failed_retryable_plan_loaded": True,
        "latest_failed_retryable_delivery_record_loaded": True,
        "due_retry_candidate_valid": True,
        "retry_intent_event_created": True,
        "retry_intent_payload_matches_plan": True,
        "retry_intent_dedupe_key_stable": True,
        "maintenance_pipeline_run_recorded": True,
        "maintenance_job_attempt_recorded": True,
        "replay_request_created": False,
        "dead_letter_created": False,
        "notification_plan_mutated_by_maintenance": False,
        "notification_delivery_record_mutated_by_maintenance": False,
        "analysis_mutated": False,
        "judge_output_mutated": False,
        "candidate_group_mutated": False,
        "openai_called": False,
        "telegram_called": False,
        "live_github_called": False,
        "workers_started": False,
        "redis_mutation": False,
        "production_db_write": False,
        "alembic_or_ddl_ran": False,
        "retry_intent_event_count_before": 0,
        "retry_intent_event_count_after": 1,
        "maintenance_pipeline_run_count_before": 0,
        "maintenance_pipeline_run_count_after": 1,
        "maintenance_job_attempt_count_before": 0,
        "maintenance_job_attempt_count_after": 1,
        "checks_failed": [],
    }
