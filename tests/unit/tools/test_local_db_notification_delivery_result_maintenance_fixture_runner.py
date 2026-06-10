from __future__ import annotations

import ast
import json
from pathlib import Path
from uuid import UUID

from tools import local_db_notification_delivery_result_maintenance_fixture_runner as runner


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
    def __init__(self, *, event_id: UUID | None = EVENT_ID, checks_failed: tuple[str, ...] = ()) -> None:
        self.calls = []
        self.event_id = event_id
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
    ) -> runner.DeliveryResultResolutionResult:
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
        return runner.DeliveryResultResolutionResult(
            notification_delivery_result_event_id=self.event_id,
            notification_delivery_result_event_found=self.event_id is not None,
            checks_failed=self.checks_failed,
        )


class FakeExecutor:
    def __init__(
        self,
        *,
        checks_failed: tuple[str, ...] = (),
        retry_intent_event_created: bool = False,
        replay_request_created: bool = False,
    ) -> None:
        self.calls = []
        self.checks_failed = checks_failed
        self.retry_intent_event_created = retry_intent_event_created
        self.replay_request_created = replay_request_created

    def execute(
        self,
        *,
        database_url: str,
        notification_delivery_result_event_id: UUID,
    ) -> runner.MaintenanceExecutionResult:
        self.calls.append((database_url, notification_delivery_result_event_id))
        return runner.MaintenanceExecutionResult(
            notification_delivery_result_event_found=True,
            notification_plan_loaded=True,
            latest_delivery_record_loaded=True,
            delivery_result_matches_latest_record=True,
            dry_run_suppressed_classified_logical_noop=True,
            maintenance_pipeline_run_recorded=True,
            maintenance_job_attempt_recorded=True,
            retry_intent_event_created=self.retry_intent_event_created,
            replay_request_created=self.replay_request_created,
            checks_failed=self.checks_failed,
        )


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


def test_fixture_chain_mode_returns_expected_pass_report() -> None:
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
        "unit-maintenance-delivery-result",
        "--confirm-local-test-db",
        resolver=resolver,
        executor=executor,
    )

    assert result.exit_code == 0
    assert result.report == _expected_pass_report()
    assert len(resolver.calls) == 1
    assert executor.calls == [(SAFE_SOCKET_URL, EVENT_ID)]


def test_explicit_event_id_mode_bypasses_fixture_resolver() -> None:
    resolver = FakeResolver()
    executor = FakeExecutor()

    result = _run(
        "--database-url",
        SAFE_SOCKET_URL,
        "--notification-delivery-result-event-id",
        str(EVENT_ID),
        "--confirm-local-test-db",
        resolver=resolver,
        executor=executor,
    )

    assert result.exit_code == 0
    assert resolver.calls == []
    assert executor.calls == [(SAFE_SOCKET_URL, EVENT_ID)]


def test_cli_requires_app_env_test_before_executor() -> None:
    executor = FakeExecutor()

    result = _run(
        "--database-url",
        SAFE_SOCKET_URL,
        "--notification-delivery-result-event-id",
        str(EVENT_ID),
        "--confirm-local-test-db",
        env={"APP_ENV": "prod"},
        executor=executor,
    )

    assert result.exit_code == 1
    assert result.report["checks_failed"] == ["app_env_test_required"]
    assert executor.calls == []


def test_cli_requires_confirm_local_test_db_before_executor() -> None:
    executor = FakeExecutor()

    result = _run(
        "--database-url",
        SAFE_SOCKET_URL,
        "--notification-delivery-result-event-id",
        str(EVENT_ID),
        executor=executor,
    )

    assert result.exit_code == 1
    assert result.report["database_url_guard_passed"] is True
    assert result.report["checks_failed"] == ["confirm_local_test_db_required"]
    assert executor.calls == []


def test_unsafe_database_url_is_rejected_by_existing_guard_before_executor() -> None:
    executor = FakeExecutor()

    result = _run(
        "--database-url",
        REMOTE_UNSAFE_URL,
        "--notification-delivery-result-event-id",
        str(EVENT_ID),
        "--confirm-local-test-db",
        executor=executor,
    )

    assert result.exit_code == 1
    assert result.report["database_url_guard_passed"] is False
    assert "database_url_remote_host_rejected" in result.report["checks_failed"]
    assert executor.calls == []


def test_fixture_mode_requires_source_github_snapshot_and_replay_namespace() -> None:
    result = _run(
        "--database-url",
        SAFE_SOCKET_URL,
        "--source-fixture",
        SOURCE_FIXTURE,
        "--confirm-local-test-db",
        resolver=FakeResolver(),
        executor=FakeExecutor(),
    )

    assert result.exit_code == 1
    assert "fixture_selector_required" in result.report["checks_failed"]
    assert "fixture_selector_incomplete" in result.report["checks_failed"]


def test_ambiguous_selector_mode_is_rejected() -> None:
    result = _run(
        "--database-url",
        SAFE_SOCKET_URL,
        "--notification-delivery-result-event-id",
        str(EVENT_ID),
        "--source-fixture",
        SOURCE_FIXTURE,
        "--github-snapshot-fixture",
        GITHUB_FIXTURE,
        "--replay-namespace",
        "unit-ambiguous-selector",
        "--confirm-local-test-db",
        resolver=FakeResolver(),
        executor=FakeExecutor(),
    )

    assert result.exit_code == 1
    assert result.report["checks_failed"] == ["selector_mode_ambiguous"]


def test_fixture_chain_ambiguity_returns_sanitized_stable_reason() -> None:
    result = _run(
        "--database-url",
        SAFE_SOCKET_URL,
        "--source-fixture",
        SOURCE_FIXTURE,
        "--github-snapshot-fixture",
        GITHUB_FIXTURE,
        "--replay-namespace",
        "unit-ambiguous-db",
        "--confirm-local-test-db",
        resolver=FakeResolver(event_id=None, checks_failed=("notification_delivery_result_event_ambiguous",)),
        executor=FakeExecutor(),
    )

    assert result.exit_code == 1
    assert result.report["notification_delivery_result_event_found"] is False
    assert result.report["checks_failed"] == [
        "notification_delivery_result_event_ambiguous",
        "notification_delivery_result_event_missing_or_invalid",
    ]


def test_dry_run_suppressed_delivery_result_classifies_as_logical_noop_success() -> None:
    assert runner.classify_dry_run_suppressed_logical_noop(
        delivery_status="suppressed",
        delivery_reason="dry_run_skip_transport",
    )
    assert not runner.classify_dry_run_suppressed_logical_noop(
        delivery_status="failed_retryable",
        delivery_reason="telegram_retryable_5xx",
    )


def test_retry_intent_is_not_emitted_for_dry_run_skip_transport() -> None:
    result = _run(
        "--database-url",
        SAFE_SOCKET_URL,
        "--notification-delivery-result-event-id",
        str(EVENT_ID),
        "--confirm-local-test-db",
        executor=FakeExecutor(),
    )

    assert result.exit_code == 0
    assert result.report["dry_run_suppressed_classified_logical_noop"] is True
    assert result.report["retry_intent_event_created"] is False


def test_replay_request_is_not_created_for_dry_run_skip_transport() -> None:
    result = _run(
        "--database-url",
        SAFE_SOCKET_URL,
        "--notification-delivery-result-event-id",
        str(EVENT_ID),
        "--confirm-local-test-db",
        executor=FakeExecutor(),
    )

    assert result.exit_code == 0
    assert result.report["replay_request_created"] is False
    assert result.report["dead_letter_created"] is False


def test_runtime_network_and_forbidden_mutation_boundaries_stay_false() -> None:
    result = _run(
        "--database-url",
        SAFE_SOCKET_URL,
        "--notification-delivery-result-event-id",
        str(EVENT_ID),
        "--confirm-local-test-db",
        executor=FakeExecutor(),
    )

    for key in (
        "openai_called",
        "telegram_called",
        "live_github_called",
        "workers_started",
        "redis_mutation",
        "production_db_write",
        "alembic_or_ddl_ran",
        "notification_plan_mutated",
        "notification_delivery_record_mutated",
        "analysis_mutated",
        "judge_output_mutated",
        "candidate_group_mutated",
    ):
        assert result.report[key] is False


def test_report_shape_is_stable_and_sanitized() -> None:
    result = _run(
        "--database-url",
        PASSWORD_URL,
        "--notification-delivery-result-event-id",
        str(EVENT_ID),
        "--confirm-local-test-db",
        executor=FakeExecutor(),
    )

    text = runner.render_json(result.report)
    parsed = json.loads(text)
    assert list(parsed) == list(_expected_pass_report())
    assert PASSWORD_URL not in text
    assert SECRET_VALUE not in text


def test_unexpected_retry_or_replay_side_effect_fails_report() -> None:
    result = _run(
        "--database-url",
        SAFE_SOCKET_URL,
        "--notification-delivery-result-event-id",
        str(EVENT_ID),
        "--confirm-local-test-db",
        executor=FakeExecutor(retry_intent_event_created=True, replay_request_created=True),
    )

    assert result.exit_code == 1
    assert "retry_intent_event_created:unexpected" in result.report["checks_failed"]
    assert "replay_request_created:unexpected" in result.report["checks_failed"]


def test_runner_source_has_no_forbidden_runtime_or_network_imports() -> None:
    source = (ROOT / "tools/local_db_notification_delivery_result_maintenance_fixture_runner.py").read_text(
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
    source = (ROOT / "tools/local_db_notification_delivery_result_maintenance_fixture_runner.py").read_text(
        encoding="utf-8"
    )

    assert "CREATE TABLE" not in source
    assert "ALTER TABLE" not in source
    assert "DROP TABLE" not in source


def _expected_pass_report() -> dict[str, object]:
    return {
        "schema_version": "local_db_notification_delivery_result_maintenance_fixture_runner_v1",
        "status": "pass",
        "database_url_guard_passed": True,
        "notification_delivery_result_event_found": True,
        "notification_plan_loaded": True,
        "latest_delivery_record_loaded": True,
        "delivery_result_matches_latest_record": True,
        "dry_run_suppressed_classified_logical_noop": True,
        "maintenance_pipeline_run_recorded": True,
        "maintenance_job_attempt_recorded": True,
        "retry_intent_event_created": False,
        "replay_request_created": False,
        "dead_letter_created": False,
        "notification_plan_mutated": False,
        "notification_delivery_record_mutated": False,
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
        "checks_failed": [],
    }
