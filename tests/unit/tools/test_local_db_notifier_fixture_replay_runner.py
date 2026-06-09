from __future__ import annotations

import ast
import json
from datetime import datetime, timezone
from pathlib import Path
from uuid import UUID

from tools import local_db_notifier_fixture_replay_runner as runner
from tools import local_db_policy_engine_fixture_replay_runner as policy_runner


ROOT = Path(__file__).resolve().parents[3]
SOURCE_FIXTURE = "tests/fixtures/upstream/source_message_github_repo_signal.json"
GITHUB_FIXTURE = "tests/fixtures/upstream/github_repo_snapshot_example_tool.json"
PG_SCHEME = "postgresql+psycopg"
SAFE_DATABASE_NAME = "github_ai_catchbot_test"
SOCKET_HOST = "/var/run/postgresql"
SAFE_SOCKET_URL = f"{PG_SCHEME}:///{SAFE_DATABASE_NAME}?host={SOCKET_HOST}"
SECRET_VALUE = "local" + "_" + "secret"
PASSWORD_URL = f"{PG_SCHEME}://local_user:{SECRET_VALUE}@127.0.0.1:5432/{SAFE_DATABASE_NAME}"
PLAN_ID = UUID("11111111-1111-4111-8111-111111111111")
ANALYSIS_ID = UUID("22222222-2222-4222-8222-222222222222")
GROUP_ID = UUID("33333333-3333-4333-8333-333333333333")
OUTPUT_ID = UUID("44444444-4444-4444-8444-444444444444")
DELIVERY_ID = UUID("55555555-5555-4555-8555-555555555555")
ARTIFACT_ID = UUID("66666666-6666-4666-8666-666666666666")


class FakePolicyRunner:
    def __init__(self, *, report_overrides=None, exit_code: int = 0) -> None:
        self.calls = []
        self.report_overrides = report_overrides or {}
        self.exit_code = exit_code

    def run(
        self,
        *,
        database_url: str,
        source_fixture_path: Path,
        github_snapshot_fixture_path: Path,
        replay_namespace: str,
        env,
        repo_root: Path,
    ):
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
        report = {
            "schema_version": "local_db_policy_engine_fixture_replay_v1",
            "status": "pass",
            "source_candidate_replay_confirmed": True,
            "artifact_snapshot_replay_confirmed": True,
            "evidence_bundle_replay_confirmed": True,
            "analysis_router_replay_confirmed": True,
            "judge_output_replay_confirmed": True,
            "analysis_validator_replay_confirmed": True,
            "analysis_policy_apply_event_found": True,
            "judge_run_succeeded_confirmed": True,
            "judge_output_loaded": True,
            "bundle_context_confirmed": True,
            "analysis_validation_state_transition_found": True,
            "analysis_created": True,
            "policy_state_transition_recorded": True,
            "notification_plan_intent_event_created": True,
            "checks_failed": [],
        }
        report.update(self.report_overrides)
        return policy_runner.RunnerResult(exit_code=self.exit_code, report=report)


class FakeNotifierExecutor:
    def __init__(self, *, checks_failed: tuple[str, ...] = ()) -> None:
        self.calls = []
        self.checks_failed = checks_failed

    def execute(
        self,
        *,
        database_url: str,
        replay_namespace: str,
    ) -> runner.ReplayExecutionResult:
        self.calls.append((database_url, replay_namespace))
        return runner.ReplayExecutionResult(
            notification_plan_created_event_found=True,
            analysis_loaded=True,
            judge_output_loaded=True,
            candidate_context_loaded=True,
            notification_plan_created=True,
            notification_render_created=True,
            notification_delivery_record_created=True,
            notification_delivery_state_transition_recorded=True,
            notification_delivery_result_event_created=True,
            analysis_mutated=False,
            judge_output_mutated=False,
            candidate_group_mutated=False,
            telegram_called=False,
            send_message_called=False,
            edit_message_called=False,
            checks_failed=self.checks_failed,
        )


def _parse_args(*args: str):
    return runner.build_parser().parse_args(args)


def _run(*args: str, env=None, executor=None, predecessor_runner=None) -> runner.RunnerResult:
    return runner.run(
        _parse_args(*args),
        env=env or {"APP_ENV": "test"},
        executor=executor,
        predecessor_runner=predecessor_runner,
        repo_root=ROOT,
    )


def test_fake_predecessor_and_executor_return_expected_pass_output() -> None:
    predecessor = FakePolicyRunner()
    executor = FakeNotifierExecutor()

    result = _run(
        "--database-url",
        SAFE_SOCKET_URL,
        "--source-fixture",
        SOURCE_FIXTURE,
        "--github-snapshot-fixture",
        GITHUB_FIXTURE,
        "--replay-namespace",
        "operator-local-db-source-candidate",
        "--confirm-local-test-db",
        predecessor_runner=predecessor,
        executor=executor,
    )

    assert result.exit_code == 0
    assert result.report == _expected_pass_report()
    assert len(predecessor.calls) == 1
    assert len(executor.calls) == 1


def test_required_output_shape_is_stable_json_without_raw_url_or_password() -> None:
    result = _run(
        "--database-url",
        PASSWORD_URL,
        "--source-fixture",
        SOURCE_FIXTURE,
        "--github-snapshot-fixture",
        GITHUB_FIXTURE,
        "--replay-namespace",
        "unit-output-shape",
        "--confirm-local-test-db",
        predecessor_runner=FakePolicyRunner(),
        executor=FakeNotifierExecutor(),
    )

    text = runner.render_json(result.report)
    parsed = json.loads(text)
    assert list(parsed) == list(_expected_pass_report())
    assert PASSWORD_URL not in text
    assert SECRET_VALUE not in text


def test_rejects_missing_confirmation_before_predecessor_runs() -> None:
    predecessor = FakePolicyRunner()
    executor = FakeNotifierExecutor()

    result = _run(
        "--database-url",
        SAFE_SOCKET_URL,
        "--source-fixture",
        SOURCE_FIXTURE,
        "--github-snapshot-fixture",
        GITHUB_FIXTURE,
        "--replay-namespace",
        "unit-missing-confirm",
        predecessor_runner=predecessor,
        executor=executor,
    )

    assert result.exit_code == 1
    assert result.report["database_url_guard_passed"] is True
    assert result.report["checks_failed"] == ["confirm_local_test_db_required"]
    assert predecessor.calls == []
    assert executor.calls == []


def test_rejects_app_env_prod_before_predecessor_runs() -> None:
    predecessor = FakePolicyRunner()
    executor = FakeNotifierExecutor()

    result = _run(
        "--database-url",
        SAFE_SOCKET_URL,
        "--source-fixture",
        SOURCE_FIXTURE,
        "--github-snapshot-fixture",
        GITHUB_FIXTURE,
        "--replay-namespace",
        "unit-prod-env",
        "--confirm-local-test-db",
        env={"APP_ENV": "prod"},
        predecessor_runner=predecessor,
        executor=executor,
    )

    assert result.exit_code == 1
    assert result.report["checks_failed"] == ["app_env_production_rejected"]
    assert predecessor.calls == []
    assert executor.calls == []


def test_database_url_guard_delegates_to_predecessor_guard(monkeypatch) -> None:
    calls = []
    unsafe_url = f"{PG_SCHEME}:///unsafe"

    def fake_validate(database_url):
        calls.append(database_url)
        return False, ["delegated_failure"], None

    monkeypatch.setattr(runner.policy_runner, "validate_database_url", fake_validate)

    ok, failures, parsed = runner.validate_database_url(unsafe_url)

    assert ok is False
    assert failures == ["delegated_failure"]
    assert parsed is None
    assert calls == [unsafe_url]


def test_accepts_predecessor_expected_successor_row_failures() -> None:
    predecessor = FakePolicyRunner(
        report_overrides={
            "status": "fail",
            "checks_failed": [
                "notification_plan_created:unexpected",
                "notification_render_created:unexpected",
                "notification_delivery_created:unexpected",
            ],
        },
        exit_code=1,
    )

    assert runner._predecessor_policy_confirmed(predecessor.run(  # noqa: SLF001 - unit-level contract check.
        database_url=SAFE_SOCKET_URL,
        source_fixture_path=Path(SOURCE_FIXTURE),
        github_snapshot_fixture_path=Path(GITHUB_FIXTURE),
        replay_namespace="unit-predecessor-successor-rows",
        env={"APP_ENV": "test"},
        repo_root=ROOT,
    ))


def test_notification_intent_payload_validation_rejects_missing_required_fields() -> None:
    payload = _intent_payload()
    payload.pop("notification_plan_id")
    payload.pop("material_change_hash")

    ok, failures = runner.validate_notification_intent_payload(payload)

    assert ok is False
    assert "missing:notification_plan_id" in failures
    assert "missing:material_change_hash" in failures


def test_notification_intent_payload_validation_rejects_invalid_uuid_and_integer_fields() -> None:
    payload = _intent_payload()
    payload["analysis_id"] = "not-a-uuid"
    payload["target_chat_id"] = "not-an-int"
    payload["target_thread_id"] = "also-not-an-int"

    ok, failures = runner.validate_notification_intent_payload(payload)

    assert ok is False
    assert "analysis_id:invalid_uuid" in failures
    assert "target_chat_id:invalid_integer" in failures
    assert "target_thread_id:invalid_integer" in failures


def test_render_builder_is_deterministic_and_uses_existing_truth_without_changing_verdict() -> None:
    intent = _intent()
    analysis = _analysis(verdict="later", delivery_decision="send_now")
    judge_output = _judge_output(headline="example/example-tool")
    candidate = _candidate(primary_canonical_id="github:example/example-tool")

    left = runner.build_notification_render(
        intent=intent,
        analysis=analysis,
        judge_output=judge_output,
        candidate=candidate,
    )
    right = runner.build_notification_render(
        intent=intent,
        analysis=analysis,
        judge_output=judge_output,
        candidate=candidate,
    )

    assert left == right
    assert "[GitHub AI] later / send_now" in left.message_text
    assert "example/example-tool" in left.message_text
    assert "policy_threshold_later" in left.message_text
    assert "github:example/example-tool" in left.message_text
    assert analysis.verdict == "later"
    assert analysis.delivery_decision == "send_now"


def test_render_hash_is_stable() -> None:
    payload = {
        "message_text": "same",
        "entities_json": [],
        "link_preview_options_json": {"is_disabled": True},
        "reply_markup_json": None,
    }

    assert runner.stable_render_hash(payload) == runner.stable_render_hash(dict(reversed(list(payload.items()))))


def test_notification_plan_payload_preserves_upstream_intent_fields() -> None:
    intent = _intent()

    payload = runner.notification_plan_payload(intent)

    assert payload == _intent_payload()


def test_delivery_result_payload_builder_is_stable_and_complete() -> None:
    result = _delivery_result_payload()

    left = runner.build_delivery_result_payload(result)
    right = runner.build_delivery_result_payload(result)

    assert left == right
    assert set(left) == {
        "notification_plan_id",
        "delivery_status",
        "telegram_chat_id",
        "telegram_message_id",
        "notification_delivery_record_id",
        "attempt_count",
        "transport_error_code",
        "transport_error_class",
        "edited",
        "dry_run",
        "local_fixture",
        "reason_code",
    }
    assert left["delivery_status"] == "suppressed"
    assert left["telegram_chat_id"] == 424242001
    assert left["telegram_message_id"] is None
    assert left["dry_run"] is True
    assert left["local_fixture"] is True


def test_dry_run_delivery_path_does_not_call_telegram_send_or_edit() -> None:
    result = _run(
        "--database-url",
        SAFE_SOCKET_URL,
        "--source-fixture",
        SOURCE_FIXTURE,
        "--github-snapshot-fixture",
        GITHUB_FIXTURE,
        "--replay-namespace",
        "unit-dry-run",
        "--confirm-local-test-db",
        predecessor_runner=FakePolicyRunner(),
        executor=FakeNotifierExecutor(),
    )

    assert result.report["telegram_called"] is False
    assert result.report["send_message_called"] is False
    assert result.report["edit_message_called"] is False
    assert runner.transport_would_be_called_for_fixture(intent=_intent()) is False


def test_same_plan_material_idempotency_helpers_and_dedupe_keys_are_stable() -> None:
    left = runner.build_delivery_result_event_dedupe_key(
        replay_namespace="unit-idempotent",
        notification_plan_id=PLAN_ID,
        notification_delivery_record_id=DELIVERY_ID,
    )
    right = runner.build_delivery_result_event_dedupe_key(
        replay_namespace="unit-idempotent",
        notification_plan_id=PLAN_ID,
        notification_delivery_record_id=DELIVERY_ID,
    )

    assert left == right
    assert left.startswith("local-db-notifier:unit-idempotent:notification.delivery.result:")


def test_suppression_or_future_send_after_path_does_not_call_telegram() -> None:
    suppress_intent = _intent(delivery_decision="suppress", urgency_profile="suppressed", suppress_reason_code="verdict_skip")
    future_intent = _intent(send_after=datetime(2099, 1, 1, tzinfo=timezone.utc))

    assert runner.transport_would_be_called_for_fixture(intent=suppress_intent) is False
    assert runner.transport_would_be_called_for_fixture(intent=future_intent) is False


def test_runner_source_has_no_forbidden_runtime_or_network_imports() -> None:
    source = (ROOT / "tools/local_db_notifier_fixture_replay_runner.py").read_text(encoding="utf-8")
    tree = ast.parse(source)
    imported_roots = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imported_roots.update(alias.name.split(".", 1)[0] for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            imported_roots.add(node.module.split(".", 1)[0])

    assert {"redis", "openai", "telegram", "docker", "systemd", "requests", "httpx", "aiohttp"}.isdisjoint(
        imported_roots
    )


def _intent(
    *,
    delivery_decision: str = "send_now",
    urgency_profile: str = "normal_silent",
    send_after: datetime | None = None,
    suppress_reason_code: str | None = None,
) -> runner.NotificationPlanIntent:
    return runner.NotificationPlanIntent(
        notification_plan_id=PLAN_ID,
        analysis_id=ANALYSIS_ID,
        candidate_group_id=GROUP_ID,
        delivery_decision=delivery_decision,
        urgency_profile=urgency_profile,
        target_chat_id=424242001,
        target_thread_id=None,
        render_profile="telegram_single_alert_normal_v1",
        dedupe_subject_key=str(GROUP_ID),
        material_change_hash="material-hash",
        send_after=send_after,
        suppress_reason_code=suppress_reason_code,
    )


def _intent_payload() -> dict[str, object]:
    return {
        "notification_plan_id": str(PLAN_ID),
        "analysis_id": str(ANALYSIS_ID),
        "candidate_group_id": str(GROUP_ID),
        "delivery_decision": "send_now",
        "urgency_profile": "normal_silent",
        "target_chat_id": 424242001,
        "target_thread_id": None,
        "render_profile": "telegram_single_alert_normal_v1",
        "dedupe_subject_key": str(GROUP_ID),
        "material_change_hash": "material-hash",
        "send_after": None,
        "suppress_reason_code": None,
    }


def _analysis(*, verdict: str = "later", delivery_decision: str = "send_now") -> runner.AnalysisRecord:
    return runner.AnalysisRecord(
        analysis_id=ANALYSIS_ID,
        candidate_group_id=GROUP_ID,
        judge_output_id=OUTPUT_ID,
        verdict=verdict,
        delivery_decision=delivery_decision,
        reason_codes_json=["github_repo_fixture_evidence", "policy_threshold_later"],
        evidence_limitations_ko="synthetic local fixture; no GitHub API call",
        recommended_action_ko="inspect later",
        freshness_note_ko="local fixture",
        policy_reconciled_flag=True,
    )


def _judge_output(*, headline: str = "example/example-tool") -> runner.JudgeOutputRecord:
    return runner.JudgeOutputRecord(
        judge_output_id=OUTPUT_ID,
        candidate_group_id=GROUP_ID,
        payload_json={"headline": headline, "model_proposed_verdict": "later"},
        model_proposed_verdict="later",
        model_confidence_band="medium",
    )


def _candidate(*, primary_canonical_id: str = "github:example/example-tool") -> runner.CandidateContext:
    return runner.CandidateContext(
        candidate_group_id=GROUP_ID,
        current_primary_artifact_id=ARTIFACT_ID,
        primary_artifact_type="github_repo",
        primary_canonical_id=primary_canonical_id,
        primary_canonical_url="https://example.invalid/example/example-tool",
    )


def _delivery_result_payload() -> runner.DeliveryResultPayload:
    return runner.DeliveryResultPayload(
        notification_plan_id=PLAN_ID,
        delivery_status="suppressed",
        telegram_chat_id=424242001,
        telegram_message_id=None,
        notification_delivery_record_id=DELIVERY_ID,
        attempt_count=0,
        transport_error_code=None,
        transport_error_class=None,
        edited=False,
    )


def _expected_pass_report() -> dict[str, object]:
    return {
        "schema_version": "local_db_notifier_fixture_replay_v1",
        "status": "pass",
        "database_url_guard_passed": True,
        "source_candidate_replay_confirmed": True,
        "artifact_snapshot_replay_confirmed": True,
        "evidence_bundle_replay_confirmed": True,
        "analysis_router_replay_confirmed": True,
        "judge_output_replay_confirmed": True,
        "analysis_validator_replay_confirmed": True,
        "policy_engine_replay_confirmed": True,
        "notification_plan_created_event_found": True,
        "analysis_loaded": True,
        "judge_output_loaded": True,
        "candidate_context_loaded": True,
        "notification_plan_created": True,
        "notification_render_created": True,
        "notification_delivery_record_created": True,
        "notification_delivery_state_transition_recorded": True,
        "notification_delivery_result_event_created": True,
        "analysis_mutated": False,
        "judge_output_mutated": False,
        "candidate_group_mutated": False,
        "telegram_called": False,
        "send_message_called": False,
        "edit_message_called": False,
        "production_db_write": False,
        "live_github_called": False,
        "live_telegram_called": False,
        "openai_called": False,
        "workers_started": False,
        "redis_mutation": False,
        "checks_failed": [],
    }
