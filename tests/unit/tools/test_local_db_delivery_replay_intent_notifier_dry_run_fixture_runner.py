from __future__ import annotations

import ast
import json
from pathlib import Path
from uuid import UUID

from tools import local_db_delivery_replay_intent_notifier_dry_run_fixture_runner as runner


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
REPLAY_REQUEST_ID = UUID("20000000-0000-4000-8000-000000000002")
PLAN_ID = UUID("30000000-0000-4000-8000-000000000003")


class FakeResolver:
    def __init__(
        self,
        *,
        event_id: UUID | None = EVENT_ID,
        loaded: bool = True,
        prepared: bool = False,
        checks_failed: tuple[str, ...] = (),
        order: list[str] | None = None,
    ) -> None:
        self.calls = []
        self.event_id = event_id
        self.loaded = loaded
        self.prepared = prepared
        self.checks_failed = checks_failed
        self.order = order

    def resolve(
        self,
        *,
        database_url: str,
        selector_mode: str,
        notification_plan_created_event_id: UUID | None,
        replay_request_id: UUID | None,
        notification_plan_id: UUID | None,
        source_fixture_path: Path | None,
        github_snapshot_fixture_path: Path | None,
        replay_namespace: str | None,
        max_attempts: int,
        env,
        repo_root: Path,
    ) -> runner.ReplayIntentResolutionResult:
        if self.order is not None:
            self.order.append("replay_intent")
        self.calls.append(
            {
                "database_url": database_url,
                "selector_mode": selector_mode,
                "notification_plan_created_event_id": notification_plan_created_event_id,
                "replay_request_id": replay_request_id,
                "notification_plan_id": notification_plan_id,
                "source_fixture_path": source_fixture_path,
                "github_snapshot_fixture_path": github_snapshot_fixture_path,
                "replay_namespace": replay_namespace,
                "max_attempts": max_attempts,
                "env": dict(env),
                "repo_root": repo_root,
            }
        )
        return runner.ReplayIntentResolutionResult(
            notification_plan_created_event_id=self.event_id,
            notification_plan_created_event_loaded=self.loaded,
            replay_intent_fixture_prepared=self.prepared,
            checks_failed=self.checks_failed,
        )


class FakeExecutor:
    def __init__(
        self,
        *,
        execution: runner.ReplayIntentNotifierDryRunExecutionResult | None = None,
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
    ) -> runner.ReplayIntentNotifierDryRunExecutionResult:
        if self.order is not None:
            self.order.append("notifier_dry_run")
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


def test_invalid_event_uuid_rejected() -> None:
    result = _run(
        "--database-url",
        SAFE_SOCKET_URL,
        "--notification-plan-created-event-id",
        "not-a-uuid",
        "--confirm-local-test-db",
        resolver=FakeResolver(),
        executor=FakeExecutor(),
    )

    assert result.exit_code == 1
    assert "notification_plan_created_event_id_invalid" in result.report["checks_failed"]


def test_selector_ambiguity_rejected() -> None:
    result = _run(
        "--database-url",
        SAFE_SOCKET_URL,
        "--notification-plan-created-event-id",
        str(EVENT_ID),
        "--replay-request-id",
        str(REPLAY_REQUEST_ID),
        "--confirm-local-test-db",
        resolver=FakeResolver(),
        executor=FakeExecutor(),
    )

    assert result.exit_code == 1
    assert result.report["checks_failed"] == ["selector_mode_ambiguous"]


def test_no_selector_rejected() -> None:
    result = _run(
        "--database-url",
        SAFE_SOCKET_URL,
        "--confirm-local-test-db",
        resolver=FakeResolver(),
        executor=FakeExecutor(),
    )

    assert result.exit_code == 1
    assert result.report["checks_failed"] == ["selector_mode_required"]


def test_explicit_notification_plan_created_event_id_mode_consumes_event() -> None:
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
    assert resolver.calls[0]["selector_mode"] == "notification_plan_created_event"
    assert resolver.calls[0]["notification_plan_created_event_id"] == EVENT_ID
    assert executor.calls[0]["notification_plan_created_event_id"] == EVENT_ID
    assert executor.calls[0]["delivery_dedupe_namespace"] == f"delivery-replay-intent-{EVENT_ID}"


def test_explicit_replay_request_id_mode_resolves_exactly_one_replay_intent_event() -> None:
    resolver = FakeResolver()
    executor = FakeExecutor()

    result = _run(
        "--database-url",
        SAFE_SOCKET_URL,
        "--replay-request-id",
        str(REPLAY_REQUEST_ID),
        "--confirm-local-test-db",
        resolver=resolver,
        executor=executor,
    )

    assert result.exit_code == 0
    assert resolver.calls[0]["selector_mode"] == "replay_request"
    assert resolver.calls[0]["replay_request_id"] == REPLAY_REQUEST_ID
    assert executor.calls[0]["notification_plan_created_event_id"] == EVENT_ID


def test_notification_plan_selector_resolves_exactly_one_replay_intent_event() -> None:
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
    assert resolver.calls[0]["selector_mode"] == "notification_plan"
    assert resolver.calls[0]["notification_plan_id"] == PLAN_ID
    assert executor.calls[0]["notification_plan_created_event_id"] == EVENT_ID


def test_fixture_chain_mode_invokes_previous_replay_intent_runner_then_notifier_dry_run() -> None:
    order: list[str] = []
    resolver = FakeResolver(prepared=True, order=order)
    executor = FakeExecutor(order=order)

    result = _run(
        "--database-url",
        SAFE_SOCKET_URL,
        "--source-fixture",
        SOURCE_FIXTURE,
        "--github-snapshot-fixture",
        GITHUB_FIXTURE,
        "--replay-namespace",
        "unit-delivery-replay-intent-notifier",
        "--prepare-delivery-replay-intent-fixture",
        "--max-attempts",
        "6",
        "--confirm-local-test-db",
        resolver=resolver,
        executor=executor,
    )

    assert result.exit_code == 0
    assert order == ["replay_intent", "notifier_dry_run"]
    assert result.report["replay_intent_fixture_prepared"] is True
    assert resolver.calls[0]["selector_mode"] == "fixture_chain"
    assert resolver.calls[0]["max_attempts"] == 6
    assert executor.calls[0]["delivery_dedupe_namespace"] == "unit-delivery-replay-intent-notifier"


def test_missing_replay_intent_event_fails() -> None:
    result = _run(
        "--database-url",
        SAFE_SOCKET_URL,
        "--replay-request-id",
        str(REPLAY_REQUEST_ID),
        "--confirm-local-test-db",
        resolver=FakeResolver(
            event_id=None,
            loaded=False,
            checks_failed=("notification_plan_created_replay_intent_missing_or_invalid",),
        ),
        executor=FakeExecutor(),
    )

    assert result.exit_code == 1
    assert "notification_plan_created_replay_intent_missing_or_invalid" in result.report["checks_failed"]


def test_malformed_replay_intent_payload_fails_before_dry_run() -> None:
    executor = FakeExecutor()

    result = _run(
        "--database-url",
        SAFE_SOCKET_URL,
        "--notification-plan-created-event-id",
        str(EVENT_ID),
        "--confirm-local-test-db",
        resolver=FakeResolver(
            event_id=None,
            loaded=False,
            checks_failed=("notification_plan_created_replay_intent_payload_invalid",),
        ),
        executor=executor,
    )

    assert result.exit_code == 1
    assert "notification_plan_created_replay_intent_payload_invalid" in result.report["checks_failed"]
    assert executor.calls == []


def test_missing_analysis_fails() -> None:
    result = _run(
        "--database-url",
        SAFE_SOCKET_URL,
        "--notification-plan-created-event-id",
        str(EVENT_ID),
        "--confirm-local-test-db",
        resolver=FakeResolver(),
        executor=FakeExecutor(execution=_execution_with_failures(("analysis_missing",), analysis_loaded=False)),
    )

    assert result.exit_code == 1
    assert "analysis_missing" in result.report["checks_failed"]
    assert "analysis_loaded:missing" in result.report["checks_failed"]


def test_missing_judge_output_or_render_source_fails() -> None:
    result = _run(
        "--database-url",
        SAFE_SOCKET_URL,
        "--notification-plan-created-event-id",
        str(EVENT_ID),
        "--confirm-local-test-db",
        resolver=FakeResolver(),
        executor=FakeExecutor(
            execution=_execution_with_failures(
                ("judge_output_missing", "primary_artifact_missing", "source_message_missing"),
                judge_output_loaded=False,
                artifact_loaded=False,
                source_message_loaded=False,
            )
        ),
    )

    assert result.exit_code == 1
    assert "judge_output_missing" in result.report["checks_failed"]
    assert "primary_artifact_missing" in result.report["checks_failed"]
    assert "source_message_missing" in result.report["checks_failed"]


def test_dry_run_creates_or_reuses_render_delivery_record_and_delivery_result_event() -> None:
    result = _run(
        "--database-url",
        SAFE_SOCKET_URL,
        "--notification-plan-created-event-id",
        str(EVENT_ID),
        "--confirm-local-test-db",
        resolver=FakeResolver(),
        executor=FakeExecutor(),
    )

    assert result.exit_code == 0
    assert result.report["notification_render_created_or_reused"] is True
    assert result.report["dry_run_delivery_record_created_or_reused"] is True
    assert result.report["notification_delivery_result_event_created_or_reused"] is True
    assert result.report["delivery_result_matches_dry_run_record"] is True


def test_idempotent_rerun_bounded_counts_remain_one_to_one() -> None:
    result = _run(
        "--database-url",
        SAFE_SOCKET_URL,
        "--notification-plan-created-event-id",
        str(EVENT_ID),
        "--confirm-local-test-db",
        resolver=FakeResolver(),
        executor=FakeExecutor(
            execution=_successful_execution(
                render_before=1,
                render_after=1,
                dry_run_before=1,
                dry_run_after=1,
                delivery_result_before=1,
                delivery_result_after=1,
            )
        ),
    )

    assert result.exit_code == 0
    assert result.report["notification_render_count_before"] == 1
    assert result.report["notification_render_count_after"] == 1
    assert result.report["dry_run_delivery_record_count_before"] == 1
    assert result.report["dry_run_delivery_record_count_after"] == 1
    assert result.report["notification_delivery_result_event_count_before"] == 1
    assert result.report["notification_delivery_result_event_count_after"] == 1


def test_no_telegram_openai_redis_network_or_worker_authority() -> None:
    result = _run(
        "--database-url",
        SAFE_SOCKET_URL,
        "--notification-plan-created-event-id",
        str(EVENT_ID),
        "--confirm-local-test-db",
        resolver=FakeResolver(),
        executor=FakeExecutor(),
    )

    assert result.exit_code == 0
    for key in (
        "telegram_called",
        "openai_called",
        "redis_mutation",
        "live_github_called",
        "workers_started",
        "production_db_write",
        "alembic_or_ddl_ran",
        "real_transport_attempted",
    ):
        assert result.report[key] is False


def test_no_upstream_mutation_booleans() -> None:
    result = _run(
        "--database-url",
        SAFE_SOCKET_URL,
        "--notification-plan-created-event-id",
        str(EVENT_ID),
        "--confirm-local-test-db",
        resolver=FakeResolver(),
        executor=FakeExecutor(),
    )

    assert result.exit_code == 0
    for key in (
        "analysis_mutated",
        "judge_output_mutated",
        "candidate_group_mutated",
        "evidence_bundle_mutated",
        "artifact_mutated",
        "source_message_mutated",
    ):
        assert result.report[key] is False


def test_no_replay_request_mutation_or_new_notification_plan_created_replay_intent() -> None:
    result = _run(
        "--database-url",
        SAFE_SOCKET_URL,
        "--notification-plan-created-event-id",
        str(EVENT_ID),
        "--confirm-local-test-db",
        resolver=FakeResolver(),
        executor=FakeExecutor(),
    )

    assert result.exit_code == 0
    assert result.report["replay_request_created"] is False
    assert result.report["replay_requested_event_created"] is False
    assert result.report["replay_request_status_mutated"] is False
    assert result.report["notification_plan_created_replay_intent_created"] is False


def test_enable_notification_send_true_is_rejected_before_mutation() -> None:
    executor = FakeExecutor()

    result = _run(
        "--database-url",
        SAFE_SOCKET_URL,
        "--notification-plan-created-event-id",
        str(EVENT_ID),
        "--confirm-local-test-db",
        env={"APP_ENV": "test", "ENABLE_NOTIFICATION_SEND": "true"},
        resolver=FakeResolver(),
        executor=executor,
    )

    assert result.exit_code == 1
    assert "enable_notification_send_must_be_false" in result.report["checks_failed"]
    assert executor.calls == []


def test_report_output_sanitized_no_db_url_password_or_traceback() -> None:
    result = _run(
        "--database-url",
        PASSWORD_URL,
        "--notification-plan-created-event-id",
        str(EVENT_ID),
        "--confirm-local-test-db",
        resolver=FakeResolver(),
        executor=FakeExecutor(),
    )

    text = runner.render_json(result.report)
    parsed = json.loads(text)
    assert parsed["status"] == "pass"
    assert PASSWORD_URL not in text
    assert SECRET_VALUE not in text
    assert "Trace" + "back" not in text


def test_runner_source_has_no_forbidden_runtime_or_network_imports() -> None:
    source = (ROOT / "tools/local_db_delivery_replay_intent_notifier_dry_run_fixture_runner.py").read_text(
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


def test_runner_source_contains_no_ddl_strings() -> None:
    source = (ROOT / "tools/local_db_delivery_replay_intent_notifier_dry_run_fixture_runner.py").read_text(
        encoding="utf-8"
    )

    assert "CREATE TABLE" not in source
    assert "ALTER TABLE" not in source
    assert "DROP TABLE" not in source


def _successful_execution(
    *,
    render_before: int = 0,
    render_after: int = 1,
    dry_run_before: int = 0,
    dry_run_after: int = 1,
    delivery_result_before: int = 0,
    delivery_result_after: int = 1,
) -> runner.ReplayIntentNotifierDryRunExecutionResult:
    return runner.ReplayIntentNotifierDryRunExecutionResult(
        notification_plan_created_event_loaded=True,
        notification_plan_loaded_or_concretized=True,
        analysis_loaded=True,
        judge_output_loaded=True,
        candidate_group_loaded=True,
        artifact_loaded=True,
        source_message_loaded=True,
        notification_render_created_or_reused=True,
        dry_run_delivery_record_created_or_reused=True,
        notification_delivery_result_event_created_or_reused=True,
        notification_render_dedupe_stable=True,
        dry_run_delivery_record_dedupe_stable=True,
        delivery_result_event_dedupe_stable=True,
        delivery_result_matches_dry_run_record=True,
        notification_render_count_before=render_before,
        notification_render_count_after=render_after,
        dry_run_delivery_record_count_before=dry_run_before,
        dry_run_delivery_record_count_after=dry_run_after,
        notification_delivery_result_event_count_before=delivery_result_before,
        notification_delivery_result_event_count_after=delivery_result_after,
        notification_plan_created_replay_intent_count_before=1,
        notification_plan_created_replay_intent_count_after=1,
        replay_request_count_before=1,
        replay_request_count_after=1,
        replay_requested_event_count_before=1,
        replay_requested_event_count_after=1,
        telegram_called=False,
        openai_called=False,
        redis_mutation=False,
        live_github_called=False,
        workers_started=False,
        production_db_write=False,
        alembic_or_ddl_ran=False,
        real_transport_attempted=False,
        notification_plan_created_replay_intent_created=False,
        replay_request_created=False,
        replay_requested_event_created=False,
        replay_request_status_mutated=False,
        analysis_mutated=False,
        judge_output_mutated=False,
        candidate_group_mutated=False,
        evidence_bundle_mutated=False,
        artifact_mutated=False,
        source_message_mutated=False,
    )


def _execution_with_failures(
    checks_failed: tuple[str, ...],
    *,
    analysis_loaded: bool = True,
    judge_output_loaded: bool = True,
    artifact_loaded: bool = True,
    source_message_loaded: bool = True,
) -> runner.ReplayIntentNotifierDryRunExecutionResult:
    return runner.ReplayIntentNotifierDryRunExecutionResult(
        notification_plan_created_event_loaded=True,
        notification_plan_loaded_or_concretized=True,
        analysis_loaded=analysis_loaded,
        judge_output_loaded=judge_output_loaded,
        candidate_group_loaded=True,
        artifact_loaded=artifact_loaded,
        source_message_loaded=source_message_loaded,
        notification_render_created_or_reused=False,
        dry_run_delivery_record_created_or_reused=False,
        notification_delivery_result_event_created_or_reused=False,
        notification_render_dedupe_stable=False,
        dry_run_delivery_record_dedupe_stable=False,
        delivery_result_event_dedupe_stable=False,
        delivery_result_matches_dry_run_record=False,
        checks_failed=checks_failed,
    )
