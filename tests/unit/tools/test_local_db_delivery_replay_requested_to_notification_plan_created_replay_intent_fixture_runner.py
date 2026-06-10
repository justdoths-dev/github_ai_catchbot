from __future__ import annotations

import ast
import json
from pathlib import Path
from uuid import UUID

from tools import local_db_delivery_replay_requested_to_notification_plan_created_replay_intent_fixture_runner as runner


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
REPLAY_REQUEST_ID = UUID("10000000-0000-4000-8000-000000000001")
PLAN_ID = UUID("20000000-0000-4000-8000-000000000002")
ANALYSIS_ID = UUID("30000000-0000-4000-8000-000000000003")
CANDIDATE_GROUP_ID = UUID("40000000-0000-4000-8000-000000000004")


class FakeResolver:
    def __init__(
        self,
        *,
        replay_request_id: UUID | None = REPLAY_REQUEST_ID,
        prepared: bool = True,
        checks_failed: tuple[str, ...] = (),
    ) -> None:
        self.calls = []
        self.replay_request_id = replay_request_id
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
            replay_request_id=self.replay_request_id,
            replay_request_fixture_prepared=self.prepared,
            checks_failed=self.checks_failed,
        )


class FakeExecutor:
    def __init__(self, *, execution: runner.ReplayDispatchExecutionResult | None = None) -> None:
        self.calls = []
        self.execution = execution or _successful_execution()

    def execute(
        self,
        *,
        database_url: str,
        replay_request_id: UUID | None,
        notification_plan_id: UUID | None,
    ) -> runner.ReplayDispatchExecutionResult:
        self.calls.append(
            {
                "database_url": database_url,
                "replay_request_id": replay_request_id,
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
        "--replay-request-id",
        str(REPLAY_REQUEST_ID),
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
        "--replay-request-id",
        str(REPLAY_REQUEST_ID),
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
        "--replay-request-id",
        str(REPLAY_REQUEST_ID),
        "--confirm-local-test-db",
        executor=executor,
    )

    assert result.exit_code == 1
    assert result.report["database_url_guard_passed"] is False
    assert "database_url_remote_host_rejected" in result.report["checks_failed"]
    assert executor.calls == []


def test_invalid_replay_request_uuid_is_rejected() -> None:
    executor = FakeExecutor()

    result = _run(
        "--database-url",
        SAFE_SOCKET_URL,
        "--replay-request-id",
        "not-a-uuid",
        "--confirm-local-test-db",
        executor=executor,
    )

    assert result.exit_code == 1
    assert "replay_request_id_invalid" in result.report["checks_failed"]
    assert executor.calls == []


def test_selector_ambiguity_is_rejected() -> None:
    result = _run(
        "--database-url",
        SAFE_SOCKET_URL,
        "--replay-request-id",
        str(REPLAY_REQUEST_ID),
        "--notification-plan-id",
        str(PLAN_ID),
        "--confirm-local-test-db",
        executor=FakeExecutor(),
    )

    assert result.exit_code == 1
    assert result.report["checks_failed"] == ["selector_mode_ambiguous"]


def test_no_selector_mode_is_rejected_with_stable_code() -> None:
    result = _run(
        "--database-url",
        SAFE_SOCKET_URL,
        "--confirm-local-test-db",
        executor=FakeExecutor(),
    )

    assert result.exit_code == 1
    assert result.report["checks_failed"] == ["selector_mode_required"]


def test_explicit_replay_request_mode_consumes_that_request() -> None:
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
    assert result.report["status"] == "pass"
    assert result.report["replay_request_fixture_prepared"] is False
    assert resolver.calls == []
    assert executor.calls == [
        {"database_url": SAFE_SOCKET_URL, "replay_request_id": REPLAY_REQUEST_ID, "notification_plan_id": None}
    ]


def test_notification_plan_selector_mode_delegates_exactly_one_open_request_resolution_to_executor() -> None:
    executor = FakeExecutor()

    result = _run(
        "--database-url",
        SAFE_SOCKET_URL,
        "--notification-plan-id",
        str(PLAN_ID),
        "--confirm-local-test-db",
        executor=executor,
    )

    assert result.exit_code == 0
    assert executor.calls == [
        {"database_url": SAFE_SOCKET_URL, "replay_request_id": None, "notification_plan_id": PLAN_ID}
    ]


def test_fixture_chain_mode_invokes_predecessor_resolver_and_consumes_replay_request() -> None:
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
        "unit-delivery-replay-request-dispatch",
        "--prepare-delivery-replay-request-fixture",
        "--max-attempts",
        "6",
        "--confirm-local-test-db",
        resolver=resolver,
        executor=executor,
    )

    assert result.exit_code == 0
    assert result.report["replay_request_fixture_prepared"] is True
    assert resolver.calls[0]["max_attempts"] == 6
    assert executor.calls == [
        {"database_url": SAFE_SOCKET_URL, "replay_request_id": REPLAY_REQUEST_ID, "notification_plan_id": None}
    ]


def test_missing_replay_request_fails_with_stable_code() -> None:
    result = _run(
        "--database-url",
        SAFE_SOCKET_URL,
        "--replay-request-id",
        str(REPLAY_REQUEST_ID),
        "--confirm-local-test-db",
        executor=FakeExecutor(execution=_execution_with_failures(("replay_request_missing_or_invalid",))),
    )

    assert result.exit_code == 1
    assert result.report["checks_failed"][0] == "replay_request_missing_or_invalid"
    assert "replay_request_loaded:missing" in result.report["checks_failed"]


def test_missing_replay_requested_event_fails() -> None:
    result = _run(
        "--database-url",
        SAFE_SOCKET_URL,
        "--replay-request-id",
        str(REPLAY_REQUEST_ID),
        "--confirm-local-test-db",
        executor=FakeExecutor(
            execution=_execution_with_failures(
                ("replay_requested_event_missing_or_invalid",),
                replay_request_loaded=True,
                notification_plan_loaded=True,
            )
        ),
    )

    assert result.exit_code == 1
    assert "replay_requested_event_missing_or_invalid" in result.report["checks_failed"]


def test_mismatched_replay_requested_event_fails() -> None:
    result = _run(
        "--database-url",
        SAFE_SOCKET_URL,
        "--replay-request-id",
        str(REPLAY_REQUEST_ID),
        "--confirm-local-test-db",
        executor=FakeExecutor(
            execution=_execution_with_failures(
                ("replay_requested_event_payload_mismatch",),
                replay_request_loaded=True,
                replay_requested_event_loaded=True,
                notification_plan_loaded=True,
            )
        ),
    )

    assert result.exit_code == 1
    assert "replay_requested_event_payload_mismatch" in result.report["checks_failed"]


def test_non_delivery_replay_request_rejected() -> None:
    result = _run(
        "--database-url",
        SAFE_SOCKET_URL,
        "--replay-request-id",
        str(REPLAY_REQUEST_ID),
        "--confirm-local-test-db",
        executor=FakeExecutor(
            execution=_execution_with_failures(
                ("replay_request_replay_type_invalid",),
                replay_request_loaded=True,
            )
        ),
    )

    assert result.exit_code == 1
    assert "replay_request_replay_type_invalid" in result.report["checks_failed"]


def test_non_notification_plan_replay_request_rejected() -> None:
    result = _run(
        "--database-url",
        SAFE_SOCKET_URL,
        "--replay-request-id",
        str(REPLAY_REQUEST_ID),
        "--confirm-local-test-db",
        executor=FakeExecutor(
            execution=_execution_with_failures(
                ("replay_request_root_object_invalid",),
                replay_request_loaded=True,
            )
        ),
    )

    assert result.exit_code == 1
    assert "replay_request_root_object_invalid" in result.report["checks_failed"]


def test_missing_notification_plan_rejected() -> None:
    result = _run(
        "--database-url",
        SAFE_SOCKET_URL,
        "--replay-request-id",
        str(REPLAY_REQUEST_ID),
        "--confirm-local-test-db",
        executor=FakeExecutor(
            execution=_execution_with_failures(
                ("notification_plan_missing_or_invalid",),
                replay_request_loaded=True,
                notification_plan_loaded=False,
            )
        ),
    )

    assert result.exit_code == 1
    assert "notification_plan_missing_or_invalid" in result.report["checks_failed"]


def test_replay_intent_payload_includes_required_fields() -> None:
    payload = runner.build_notification_plan_created_replay_intent_payload(
        plan=_sample_plan(),
        replay_request_id=REPLAY_REQUEST_ID,
    )

    required = {
        "notification_plan_id",
        "analysis_id",
        "candidate_group_id",
        "delivery_decision",
        "urgency_profile",
        "target_chat_id",
        "target_thread_id",
        "render_profile",
        "dedupe_subject_key",
        "material_change_hash",
        "send_after",
        "replay_reason",
        "replay_request_id",
    }
    assert required.issubset(payload)
    assert payload["notification_plan_id"] == str(PLAN_ID)
    assert payload["analysis_id"] == str(ANALYSIS_ID)
    assert payload["candidate_group_id"] == str(CANDIDATE_GROUP_ID)
    assert payload["target_chat_id"] == 123
    assert payload["target_thread_id"] is None
    assert payload["send_after"] is None
    assert payload["replay_reason"] == "explicit_delivery_replay"
    assert payload["replay_request_id"] == str(REPLAY_REQUEST_ID)


def test_replay_intent_dedupe_key_stable() -> None:
    assert (
        runner.build_notification_plan_created_replay_intent_dedupe_key(REPLAY_REQUEST_ID)
        == f"notify:replay-intent:{REPLAY_REQUEST_ID}"
    )


def test_replay_request_status_completed_or_reused_reported() -> None:
    result = _run(
        "--database-url",
        SAFE_SOCKET_URL,
        "--replay-request-id",
        str(REPLAY_REQUEST_ID),
        "--confirm-local-test-db",
        executor=FakeExecutor(),
    )

    assert result.exit_code == 0
    assert result.report["replay_request_status_completed_or_reused"] is True


def test_idempotent_rerun_is_bounded() -> None:
    execution = _successful_execution(
        notification_plan_created_event_count_before=1,
        notification_plan_created_event_count_after=1,
        maintenance_pipeline_run_count_before=1,
        maintenance_pipeline_run_count_after=1,
        maintenance_job_attempt_count_before=1,
        maintenance_job_attempt_count_after=1,
    )

    result = _run(
        "--database-url",
        SAFE_SOCKET_URL,
        "--replay-request-id",
        str(REPLAY_REQUEST_ID),
        "--confirm-local-test-db",
        executor=FakeExecutor(execution=execution),
    )

    assert result.exit_code == 0
    assert result.report["notification_plan_created_event_count_before"] == 1
    assert result.report["notification_plan_created_event_count_after"] == 1
    assert result.report["maintenance_pipeline_run_count_before"] == 1
    assert result.report["maintenance_pipeline_run_count_after"] == 1
    assert result.report["maintenance_job_attempt_count_before"] == 1
    assert result.report["maintenance_job_attempt_count_after"] == 1


def test_no_notifier_render_or_delivery_record_created() -> None:
    result = _run(
        "--database-url",
        SAFE_SOCKET_URL,
        "--replay-request-id",
        str(REPLAY_REQUEST_ID),
        "--confirm-local-test-db",
        executor=FakeExecutor(),
    )

    assert result.exit_code == 0
    assert result.report["notifier_render_created"] is False
    assert result.report["notification_delivery_record_created"] is False


def test_no_upstream_mutation_booleans() -> None:
    result = _run(
        "--database-url",
        SAFE_SOCKET_URL,
        "--replay-request-id",
        str(REPLAY_REQUEST_ID),
        "--confirm-local-test-db",
        executor=FakeExecutor(),
    )

    assert result.exit_code == 0
    assert result.report["analysis_mutated"] is False
    assert result.report["judge_output_mutated"] is False
    assert result.report["candidate_group_mutated"] is False
    assert result.report["evidence_bundle_mutated"] is False
    assert result.report["artifact_mutated"] is False
    assert result.report["source_message_mutated"] is False


def test_report_output_is_stable_and_sanitized() -> None:
    result = _run(
        "--database-url",
        PASSWORD_URL,
        "--replay-request-id",
        str(REPLAY_REQUEST_ID),
        "--confirm-local-test-db",
        executor=FakeExecutor(),
    )

    text = runner.render_json(result.report)
    parsed = json.loads(text)
    assert parsed["status"] == "pass"
    assert PASSWORD_URL not in text
    assert SECRET_VALUE not in text
    assert "Trace" + "back" not in text


def test_runner_source_has_no_forbidden_runtime_or_network_imports() -> None:
    source = (
        ROOT / "tools/local_db_delivery_replay_requested_to_notification_plan_created_replay_intent_fixture_runner.py"
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
        ROOT / "tools/local_db_delivery_replay_requested_to_notification_plan_created_replay_intent_fixture_runner.py"
    ).read_text(encoding="utf-8")

    assert "CREATE TABLE" not in source
    assert "ALTER TABLE" not in source
    assert "DROP TABLE" not in source


def _sample_plan() -> runner.NotificationPlanRecord:
    return runner.NotificationPlanRecord(
        notification_plan_id=PLAN_ID,
        analysis_id=ANALYSIS_ID,
        candidate_group_id=CANDIDATE_GROUP_ID,
        delivery_decision="send_now",
        urgency_profile="normal_silent",
        target_chat_id=123,
        target_thread_id=None,
        render_profile="single_alert_v1",
        dedupe_subject_key="subject-key",
        material_change_hash="material-hash",
        send_after=None,
        suppress_reason_code=None,
        status="queued",
    )


def _successful_execution(
    *,
    notification_plan_created_event_count_before: int = 0,
    notification_plan_created_event_count_after: int = 1,
    maintenance_pipeline_run_count_before: int = 0,
    maintenance_pipeline_run_count_after: int = 1,
    maintenance_job_attempt_count_before: int = 0,
    maintenance_job_attempt_count_after: int = 1,
) -> runner.ReplayDispatchExecutionResult:
    return runner.ReplayDispatchExecutionResult(
        replay_request_loaded=True,
        replay_requested_event_loaded=True,
        notification_plan_loaded=True,
        notification_plan_created_replay_intent_created=True,
        notification_plan_created_replay_intent_payload_matches_request=True,
        notification_plan_created_replay_intent_dedupe_key_stable=True,
        replay_request_status_completed_or_reused=True,
        maintenance_pipeline_run_recorded=True,
        maintenance_job_attempt_recorded=True,
        notifier_render_created=False,
        notification_delivery_record_created=False,
        telegram_called=False,
        openai_called=False,
        redis_mutation=False,
        live_github_called=False,
        workers_started=False,
        production_db_write=False,
        alembic_or_ddl_ran=False,
        notification_plan_mutated=False,
        notification_render_mutated=False,
        notification_delivery_record_mutated=False,
        state_transition_mutated=False,
        dead_letter_mutated=False,
        analysis_mutated=False,
        judge_output_mutated=False,
        candidate_group_mutated=False,
        evidence_bundle_mutated=False,
        artifact_mutated=False,
        source_message_mutated=False,
        notification_plan_created_event_count_before=notification_plan_created_event_count_before,
        notification_plan_created_event_count_after=notification_plan_created_event_count_after,
        maintenance_pipeline_run_count_before=maintenance_pipeline_run_count_before,
        maintenance_pipeline_run_count_after=maintenance_pipeline_run_count_after,
        maintenance_job_attempt_count_before=maintenance_job_attempt_count_before,
        maintenance_job_attempt_count_after=maintenance_job_attempt_count_after,
    )


def _execution_with_failures(
    checks_failed: tuple[str, ...],
    *,
    replay_request_loaded: bool = False,
    replay_requested_event_loaded: bool = False,
    notification_plan_loaded: bool = False,
) -> runner.ReplayDispatchExecutionResult:
    return runner.ReplayDispatchExecutionResult(
        replay_request_loaded=replay_request_loaded,
        replay_requested_event_loaded=replay_requested_event_loaded,
        notification_plan_loaded=notification_plan_loaded,
        notification_plan_created_replay_intent_created=False,
        notification_plan_created_replay_intent_payload_matches_request=False,
        notification_plan_created_replay_intent_dedupe_key_stable=False,
        replay_request_status_completed_or_reused=False,
        maintenance_pipeline_run_recorded=False,
        maintenance_job_attempt_recorded=False,
        checks_failed=checks_failed,
    )
