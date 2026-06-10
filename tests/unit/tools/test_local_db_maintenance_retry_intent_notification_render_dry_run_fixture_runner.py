from __future__ import annotations

import ast
import json
from pathlib import Path
from uuid import UUID

from tools import local_db_maintenance_retry_intent_notification_render_dry_run_fixture_runner as runner


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
EVENT_ID = UUID("10000000-0000-4000-8000-000000000001")


class FakeResolver:
    def __init__(
        self,
        *,
        event_id: UUID | None = EVENT_ID,
        checks_failed: tuple[str, ...] = (),
        order: list[str] | None = None,
    ) -> None:
        self.calls = []
        self.event_id = event_id
        self.checks_failed = checks_failed
        self.order = order

    def resolve(
        self,
        *,
        database_url: str,
        source_fixture_path: Path,
        github_snapshot_fixture_path: Path,
        replay_namespace: str,
        env,
        repo_root: Path,
    ) -> runner.RetryIntentResolutionResult:
        if self.order is not None:
            self.order.append("due_retry_intent")
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
        return runner.RetryIntentResolutionResult(
            notification_plan_created_event_id=self.event_id,
            retry_intent_event_found=self.event_id is not None,
            retry_intent_event_count_before=0,
            retry_intent_event_count_after=1 if self.event_id is not None else 0,
            checks_failed=self.checks_failed,
        )


class FakeExecutor:
    def __init__(
        self,
        *,
        execution: runner.RetryIntentRenderExecutionResult | None = None,
        order: list[str] | None = None,
    ) -> None:
        self.calls = []
        self.execution = execution or _successful_execution()
        self.order = order

    def execute(
        self,
        *,
        database_url: str,
        notification_plan_created_event_id: UUID,
        delivery_dedupe_namespace: str,
        env,
        repo_root: Path,
    ) -> runner.RetryIntentRenderExecutionResult:
        if self.order is not None:
            self.order.append("notifier_render_dry_run")
        self.calls.append(
            {
                "database_url": database_url,
                "notification_plan_created_event_id": notification_plan_created_event_id,
                "delivery_dedupe_namespace": delivery_dedupe_namespace,
                "env": dict(env),
                "repo_root": repo_root,
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
        "--notification-plan-created-event-id",
        str(EVENT_ID),
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
        "--notification-plan-created-event-id",
        str(EVENT_ID),
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
        "--notification-plan-created-event-id",
        str(EVENT_ID),
        "--confirm-local-test-db",
        executor=executor,
    )

    assert result.exit_code == 1
    assert result.report["database_url_guard_passed"] is False
    assert "database_url_remote_host_rejected" in result.report["checks_failed"]
    assert executor.calls == []


def test_selector_ambiguity_is_rejected() -> None:
    result = _run(
        "--database-url",
        SAFE_SOCKET_URL,
        "--notification-plan-created-event-id",
        str(EVENT_ID),
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


def test_fixture_chain_mode_requires_complete_selector() -> None:
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


def test_explicit_retry_intent_event_mode_bypasses_fixture_chain_resolver() -> None:
    resolver = FakeResolver()
    executor = FakeExecutor()

    result = _run(
        "--database-url",
        SAFE_SOCKET_URL,
        "--notification-plan-created-event-id",
        str(EVENT_ID),
        "--confirm-local-test-db",
        resolver=resolver,
        executor=executor,
    )

    assert result.exit_code == 0
    assert resolver.calls == []
    assert executor.calls[0]["notification_plan_created_event_id"] == EVENT_ID
    assert executor.calls[0]["delivery_dedupe_namespace"] == f"explicit-retry-{EVENT_ID}"


def test_fixture_chain_mode_calls_due_retry_intent_then_notifier_dry_run_in_order() -> None:
    order: list[str] = []
    resolver = FakeResolver(order=order)
    executor = FakeExecutor(order=order)

    result = _run(
        "--database-url",
        SAFE_SOCKET_URL,
        "--source-fixture",
        SOURCE_FIXTURE,
        "--github-snapshot-fixture",
        GITHUB_FIXTURE,
        "--replay-namespace",
        "unit-retry-chain",
        "--prepare-failed-retryable-fixture",
        "--confirm-local-test-db",
        resolver=resolver,
        executor=executor,
    )

    assert result.exit_code == 0
    assert order == ["due_retry_intent", "notifier_render_dry_run"]
    assert resolver.calls[0]["replay_namespace"] == "unit-retry-chain"
    assert executor.calls[0]["notification_plan_created_event_id"] == EVENT_ID
    assert executor.calls[0]["delivery_dedupe_namespace"] == f"unit-retry-chain-retry-{EVENT_ID}"


def test_retry_intent_payload_is_validated_as_due_retry_promotion() -> None:
    result = _run(
        "--database-url",
        SAFE_SOCKET_URL,
        "--notification-plan-created-event-id",
        str(EVENT_ID),
        "--confirm-local-test-db",
        executor=FakeExecutor(),
    )

    assert result.exit_code == 0
    assert result.report["retry_intent_event_is_due_retry_promotion"] is True
    assert result.report["retry_intent_payload_matches_plan"] is True


def test_notifier_consumes_actual_retry_intent_event_id_not_fabricated_event() -> None:
    executor = FakeExecutor()

    result = _run(
        "--database-url",
        SAFE_SOCKET_URL,
        "--notification-plan-created-event-id",
        str(EVENT_ID),
        "--confirm-local-test-db",
        executor=executor,
    )

    assert result.exit_code == 0
    assert executor.calls == [
        {
            "database_url": SAFE_SOCKET_URL,
            "notification_plan_created_event_id": EVENT_ID,
            "delivery_dedupe_namespace": f"explicit-retry-{EVENT_ID}",
            "env": {"APP_ENV": "test"},
            "repo_root": ROOT,
        }
    ]
    assert result.report["notifier_retry_intent_rehydrated"] is True


def test_same_notification_plan_is_reused_and_delivery_result_matches_retry_dry_run_record() -> None:
    result = _run(
        "--database-url",
        SAFE_SOCKET_URL,
        "--notification-plan-created-event-id",
        str(EVENT_ID),
        "--confirm-local-test-db",
        executor=FakeExecutor(),
    )

    assert result.exit_code == 0
    assert result.report["same_notification_plan_reused"] is True
    assert result.report["delivery_result_matches_retry_dry_run_record"] is True


def test_dry_run_path_does_not_call_telegram_or_live_services() -> None:
    result = _run(
        "--database-url",
        SAFE_SOCKET_URL,
        "--notification-plan-created-event-id",
        str(EVENT_ID),
        "--confirm-local-test-db",
        executor=FakeExecutor(),
    )

    assert result.exit_code == 0
    for key in (
        "openai_called",
        "telegram_called",
        "live_github_called",
        "workers_started",
        "redis_mutation",
        "production_db_write",
        "alembic_or_ddl_ran",
    ):
        assert result.report[key] is False


def test_no_replay_request_or_dlq_is_created() -> None:
    result = _run(
        "--database-url",
        SAFE_SOCKET_URL,
        "--notification-plan-created-event-id",
        str(EVENT_ID),
        "--confirm-local-test-db",
        executor=FakeExecutor(),
    )

    assert result.exit_code == 0
    assert result.report["replay_request_created"] is False
    assert result.report["dead_letter_created"] is False


def test_mutation_boundary_booleans_are_false() -> None:
    result = _run(
        "--database-url",
        SAFE_SOCKET_URL,
        "--notification-plan-created-event-id",
        str(EVENT_ID),
        "--confirm-local-test-db",
        executor=FakeExecutor(),
    )

    assert result.exit_code == 0
    for key in (
        "notification_plan_mutated_by_maintenance",
        "notification_delivery_record_mutated_by_maintenance",
        "analysis_mutated",
        "judge_output_mutated",
        "candidate_group_mutated",
        "evidence_bundle_mutated",
        "artifact_mutated",
        "source_message_mutated",
        "verdict_recomputed",
        "delivery_decision_overridden",
    ):
        assert result.report[key] is False


def test_idempotent_rerun_report_is_bounded() -> None:
    execution = _successful_execution(
        retry_intent_before=1,
        retry_intent_after=1,
        render_before=1,
        render_after=1,
        dry_run_before=1,
        dry_run_after=1,
        delivery_result_before=1,
        delivery_result_after=1,
    )

    result = _run(
        "--database-url",
        SAFE_SOCKET_URL,
        "--notification-plan-created-event-id",
        str(EVENT_ID),
        "--confirm-local-test-db",
        executor=FakeExecutor(execution=execution),
    )

    assert result.exit_code == 0
    assert result.report["retry_intent_event_count_before"] == 1
    assert result.report["retry_intent_event_count_after"] == 1
    assert result.report["notification_render_count_before"] == 1
    assert result.report["notification_render_count_after"] == 1
    assert result.report["dry_run_delivery_record_count_before"] == 1
    assert result.report["dry_run_delivery_record_count_after"] == 1
    assert result.report["notification_delivery_result_event_count_before"] == 1
    assert result.report["notification_delivery_result_event_count_after"] == 1


def test_retry_intent_render_accepts_only_send_after_plan_intent_mismatch() -> None:
    report = {
        "status": "fail",
        "database_url_guard_passed": True,
        "notification_plan_created_event_found": True,
        "analysis_loaded": True,
        "judge_output_loaded": True,
        "candidate_group_loaded": True,
        "primary_artifact_loaded": True,
        "notification_plan_concretized": True,
        "notification_render_created": True,
        "dry_run_delivery_record_created": True,
        "notification_delivery_result_event_created": True,
        "openai_called": False,
        "telegram_called": False,
        "live_github_called": False,
        "workers_started": False,
        "redis_mutation": False,
        "production_db_write": False,
        "alembic_or_ddl_ran": False,
        "verdict_recomputed": False,
        "delivery_decision_overridden": False,
        "checks_failed": ["notification_plan_intent_mismatch"],
    }

    assert runner._render_fixture_result_acceptable(runner.render_runner.RunnerResult(1, report)) is True


def test_report_output_is_stable_and_sanitized() -> None:
    result = _run(
        "--database-url",
        PASSWORD_URL,
        "--notification-plan-created-event-id",
        str(EVENT_ID),
        "--confirm-local-test-db",
        executor=FakeExecutor(),
    )

    text = runner.render_json(result.report)
    parsed = json.loads(text)
    assert list(parsed) == list(_expected_pass_report())
    assert PASSWORD_URL not in text
    assert SECRET_VALUE not in text


def test_runner_source_has_no_forbidden_runtime_or_network_imports() -> None:
    source = (
        ROOT / "tools/local_db_maintenance_retry_intent_notification_render_dry_run_fixture_runner.py"
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
        ROOT / "tools/local_db_maintenance_retry_intent_notification_render_dry_run_fixture_runner.py"
    ).read_text(encoding="utf-8")

    assert "CREATE TABLE" not in source
    assert "ALTER TABLE" not in source
    assert "DROP TABLE" not in source


def _successful_execution(
    *,
    retry_intent_before: int = 1,
    retry_intent_after: int = 1,
    render_before: int = 0,
    render_after: int = 1,
    dry_run_before: int = 0,
    dry_run_after: int = 1,
    delivery_result_before: int = 0,
    delivery_result_after: int = 1,
) -> runner.RetryIntentRenderExecutionResult:
    return runner.RetryIntentRenderExecutionResult(
        retry_intent_event_found=True,
        retry_intent_event_is_due_retry_promotion=True,
        retry_intent_payload_matches_plan=True,
        notifier_retry_intent_rehydrated=True,
        same_notification_plan_reused=True,
        notification_render_created_or_reused=True,
        dry_run_delivery_record_created=True,
        notification_delivery_result_event_created=True,
        delivery_result_matches_retry_dry_run_record=True,
        retry_intent_event_count_before=retry_intent_before,
        retry_intent_event_count_after=retry_intent_after,
        notification_render_count_before=render_before,
        notification_render_count_after=render_after,
        dry_run_delivery_record_count_before=dry_run_before,
        dry_run_delivery_record_count_after=dry_run_after,
        notification_delivery_result_event_count_before=delivery_result_before,
        notification_delivery_result_event_count_after=delivery_result_after,
        replay_request_created=False,
        dead_letter_created=False,
        analysis_mutated=False,
        judge_output_mutated=False,
        candidate_group_mutated=False,
        evidence_bundle_mutated=False,
        artifact_mutated=False,
        source_message_mutated=False,
        verdict_recomputed=False,
        delivery_decision_overridden=False,
        openai_called=False,
        telegram_called=False,
        live_github_called=False,
        workers_started=False,
        redis_mutation=False,
        production_db_write=False,
        alembic_or_ddl_ran=False,
    )


def _expected_pass_report() -> dict[str, object]:
    return {
        "schema_version": "local_db_maintenance_retry_intent_notification_render_dry_run_fixture_runner_v1",
        "status": "pass",
        "database_url_guard_passed": True,
        "retry_intent_event_found": True,
        "retry_intent_event_is_due_retry_promotion": True,
        "retry_intent_payload_matches_plan": True,
        "notifier_retry_intent_rehydrated": True,
        "same_notification_plan_reused": True,
        "notification_render_created_or_reused": True,
        "dry_run_delivery_record_created": True,
        "notification_delivery_result_event_created": True,
        "delivery_result_matches_retry_dry_run_record": True,
        "retry_intent_event_count_before": 1,
        "retry_intent_event_count_after": 1,
        "notification_render_count_before": 0,
        "notification_render_count_after": 1,
        "dry_run_delivery_record_count_before": 0,
        "dry_run_delivery_record_count_after": 1,
        "notification_delivery_result_event_count_before": 0,
        "notification_delivery_result_event_count_after": 1,
        "replay_request_created": False,
        "dead_letter_created": False,
        "notification_plan_mutated_by_maintenance": False,
        "notification_delivery_record_mutated_by_maintenance": False,
        "analysis_mutated": False,
        "judge_output_mutated": False,
        "candidate_group_mutated": False,
        "evidence_bundle_mutated": False,
        "artifact_mutated": False,
        "source_message_mutated": False,
        "verdict_recomputed": False,
        "delivery_decision_overridden": False,
        "openai_called": False,
        "telegram_called": False,
        "live_github_called": False,
        "workers_started": False,
        "redis_mutation": False,
        "production_db_write": False,
        "alembic_or_ddl_ran": False,
        "checks_failed": [],
    }
