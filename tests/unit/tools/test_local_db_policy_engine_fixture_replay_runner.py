from __future__ import annotations

import ast
import json
from pathlib import Path
from uuid import UUID, uuid4

from tools import local_db_analysis_validator_fixture_replay_runner as validator_runner
from tools import local_db_fake_judge_output_fixture_replay_runner as fake_judge_runner
from tools import local_db_policy_engine_fixture_replay_runner as runner


ROOT = Path(__file__).resolve().parents[3]
SOURCE_FIXTURE = "tests/fixtures/upstream/source_message_github_repo_signal.json"
GITHUB_FIXTURE = "tests/fixtures/upstream/github_repo_snapshot_example_tool.json"
PG_SCHEME = "postgresql+psycopg"
SAFE_DATABASE_NAME = "github_ai_catchbot_test"
SOCKET_HOST = "/var/run/postgresql"
SAFE_SOCKET_URL = f"{PG_SCHEME}:///{SAFE_DATABASE_NAME}?host={SOCKET_HOST}"
SECRET_VALUE = "local" + "_" + "secret"
PASSWORD_URL = f"{PG_SCHEME}://local_user:{SECRET_VALUE}@127.0.0.1:5432/{SAFE_DATABASE_NAME}"
GROUP_ID = UUID("11111111-1111-4111-8111-111111111111")
BUNDLE_ID = UUID("22222222-2222-4222-8222-222222222222")
RUN_ID = UUID("33333333-3333-4333-8333-333333333333")
OUTPUT_ID = UUID("44444444-4444-4444-8444-444444444444")
ANALYSIS_ID = UUID("55555555-5555-4555-8555-555555555555")


class FakeAnalysisValidatorRunner:
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
            "schema_version": "local_db_analysis_validator_fixture_replay_v1",
            "status": "pass",
            "source_candidate_replay_confirmed": True,
            "artifact_snapshot_replay_confirmed": True,
            "evidence_bundle_replay_confirmed": True,
            "analysis_router_replay_confirmed": True,
            "judge_output_replay_confirmed": True,
            "judge_output_ready_event_found": True,
            "judge_run_succeeded_confirmed": True,
            "judge_output_loaded": True,
            "bundle_context_confirmed": True,
            "structured_output_schema_valid": True,
            "semantic_validation_passed": True,
            "refusal_not_detected": True,
            "validation_state_transition_recorded": True,
            "analysis_policy_apply_event_created": True,
            "checks_failed": [],
        }
        report.update(self.report_overrides)
        return validator_runner.RunnerResult(exit_code=self.exit_code, report=report)


class FakePolicyExecutor:
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
            analysis_policy_apply_event_found=True,
            judge_run_succeeded_confirmed=True,
            judge_output_loaded=True,
            bundle_context_confirmed=True,
            analysis_validation_state_transition_found=True,
            analysis_created=True,
            policy_state_transition_recorded=True,
            notification_plan_intent_event_created=True,
            judge_output_mutated=False,
            bundle_mutated=False,
            candidate_group_mutated=False,
            notification_plan_created=False,
            notification_render_created=False,
            notification_delivery_created=False,
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
    predecessor = FakeAnalysisValidatorRunner()
    executor = FakePolicyExecutor()

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
        predecessor_runner=FakeAnalysisValidatorRunner(),
        executor=FakePolicyExecutor(),
    )

    text = runner.render_json(result.report)
    parsed = json.loads(text)
    assert list(parsed) == list(_expected_pass_report())
    assert PASSWORD_URL not in text
    assert SECRET_VALUE not in text


def test_rejects_missing_confirmation_before_predecessor_runs() -> None:
    predecessor = FakeAnalysisValidatorRunner()
    executor = FakePolicyExecutor()

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
    predecessor = FakeAnalysisValidatorRunner()
    executor = FakePolicyExecutor()

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

    def fake_validate(database_url):
        calls.append(database_url)
        return False, ["delegated_failure"], None

    monkeypatch.setattr(runner.validator_runner, "validate_database_url", fake_validate)

    ok, failures, parsed = runner.validate_database_url("postgresql+psycopg:///unsafe")

    assert ok is False
    assert failures == ["delegated_failure"]
    assert parsed is None
    assert calls == ["postgresql+psycopg:///unsafe"]


def test_policy_apply_event_payload_validation_rejects_missing_ids() -> None:
    ok, failures = runner.validate_policy_apply_payload(
        {
            "judge_run_id": str(RUN_ID),
            "candidate_group_id": str(GROUP_ID),
            "bundle_id": str(BUNDLE_ID),
        }
    )

    assert ok is False
    assert "missing:judge_output_id" in failures


def test_deterministic_verdict_policy_returns_later_for_fake_judge_payload() -> None:
    payload = _fake_payload()

    verdict, reason_codes, scores = runner.evaluate_verdict_policy(
        payload=payload,
        current_primary_artifact_type="github_repo",
    )

    assert verdict == "later"
    assert reason_codes == ["policy_threshold_later"]
    assert scores["evidence_strength"] == 62
    assert scores["practical_usefulness"] == 58
    assert scores["confidence"] == 55
    assert scores["code_quality"] == 58


def test_deterministic_delivery_policy_returns_fixture_default_send_now() -> None:
    delivery_decision, urgency_profile, suppress_reason_code = runner.evaluate_delivery_policy(verdict="later")

    assert delivery_decision == "send_now"
    assert urgency_profile == "normal_silent"
    assert suppress_reason_code is None


def test_notification_plan_created_payload_builder_is_stable_and_complete() -> None:
    decision = _policy_decision()

    left = runner.build_notification_plan_intent(
        analysis_id=ANALYSIS_ID,
        candidate_group_id=GROUP_ID,
        decision=decision,
    )
    right = runner.build_notification_plan_intent(
        analysis_id=ANALYSIS_ID,
        candidate_group_id=GROUP_ID,
        decision=decision,
    )

    assert left == right
    assert left is not None
    payload = runner.notification_plan_intent_payload(left)
    assert set(payload) == {
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
        "suppress_reason_code",
    }
    assert payload["analysis_id"] == str(ANALYSIS_ID)
    assert payload["candidate_group_id"] == str(GROUP_ID)
    assert payload["delivery_decision"] == "send_now"
    assert payload["urgency_profile"] == "normal_silent"
    assert payload["target_chat_id"] == runner.LOCAL_TEST_TARGET_CHAT_ID


def test_runner_source_has_no_forbidden_runtime_or_network_imports() -> None:
    source = (ROOT / "tools/local_db_policy_engine_fixture_replay_runner.py").read_text(encoding="utf-8")
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


def test_suppression_path_does_not_build_notification_plan_intent() -> None:
    decision = _policy_decision(verdict="skip", delivery_decision="suppress", urgency_profile="suppressed")

    intent = runner.build_notification_plan_intent(
        analysis_id=ANALYSIS_ID,
        candidate_group_id=GROUP_ID,
        decision=decision,
    )

    assert intent is None


def test_stale_bundle_current_mismatch_noops_without_analysis_or_notification(monkeypatch) -> None:
    inserted = {"analysis": False, "notification": False, "candidate_transition": False}
    stale_bundle_id = uuid4()

    monkeypatch.setattr(
        runner,
        "_load_analysis_policy_apply_event",
        lambda connection, *, replay_namespace: runner.PolicyApplyEvent(
            event_id=uuid4(),
            judge_run_id=RUN_ID,
            judge_output_id=OUTPUT_ID,
            candidate_group_id=GROUP_ID,
            bundle_id=BUNDLE_ID,
        ),
    )
    monkeypatch.setattr(
        runner,
        "_load_candidate_context",
        lambda connection, candidate_group_id: runner.CandidateContext(
            candidate_group_id=GROUP_ID,
            current_bundle_id=stale_bundle_id,
            current_analysis_id=None,
            current_primary_artifact_id=uuid4(),
        ),
    )
    monkeypatch.setattr(
        runner,
        "_load_judge_run",
        lambda connection, judge_run_id: runner.JudgeRunRecord(
            judge_run_id=RUN_ID,
            bundle_id=BUNDLE_ID,
            prompt_version="judge_github_primary_v1",
            policy_version="verdict_policy_v1",
            status="succeeded",
        ),
    )
    monkeypatch.setattr(
        runner,
        "_load_judge_output",
        lambda connection, judge_output_id: runner.JudgeOutputRecord(
            judge_output_id=OUTPUT_ID,
            judge_run_id=RUN_ID,
            candidate_group_id=GROUP_ID,
            judge_schema_version="judge_output_v1",
            payload_json=_fake_payload(),
            model_proposed_verdict="later",
            model_confidence_band="medium",
        ),
    )
    monkeypatch.setattr(
        runner,
        "_load_bundle_context",
        lambda connection, bundle_id: runner.BundleContext(
            bundle_id=BUNDLE_ID,
            candidate_group_id=GROUP_ID,
            current_primary_artifact_id=uuid4(),
            current_primary_artifact_type="github_repo",
            ready_for_analysis=True,
        ),
    )
    monkeypatch.setattr(runner, "_analysis_validation_transition_exists", lambda connection, *, judge_run_id: True)

    def record_candidate_transition(*args, **kwargs) -> None:
        inserted["candidate_transition"] = True

    def fail_analysis_insert(*args, **kwargs):
        inserted["analysis"] = True
        raise AssertionError("stale bundle must not insert analysis")

    def fail_notification_insert(*args, **kwargs):
        inserted["notification"] = True
        raise AssertionError("stale bundle must not insert notification intent")

    monkeypatch.setattr(runner, "_insert_or_reuse_candidate_state_transition", record_candidate_transition)
    monkeypatch.setattr(runner, "_insert_or_reuse_analysis", fail_analysis_insert)
    monkeypatch.setattr(runner, "_insert_or_reuse_notification_plan_intent_event", fail_notification_insert)

    result = runner._execute_policy_engine_replay(object(), replay_namespace="unit-stale")

    assert result.analysis_created is False
    assert result.notification_plan_intent_event_created is False
    assert result.checks_failed == ("candidate_current_bundle_mismatch",)
    assert inserted == {"analysis": False, "notification": False, "candidate_transition": True}


def test_idempotency_helpers_and_dedupe_keys_are_stable() -> None:
    decision = _policy_decision()
    intent = runner.build_notification_plan_intent(
        analysis_id=ANALYSIS_ID,
        candidate_group_id=GROUP_ID,
        decision=decision,
    )
    assert intent is not None

    left = runner.build_notification_plan_created_dedupe_key(
        replay_namespace="unit-idempotent",
        analysis_id=ANALYSIS_ID,
        target_chat_id=runner.LOCAL_TEST_TARGET_CHAT_ID,
        material_change_hash=intent.material_change_hash,
    )
    right = runner.build_notification_plan_created_dedupe_key(
        replay_namespace="unit-idempotent",
        analysis_id=ANALYSIS_ID,
        target_chat_id=runner.LOCAL_TEST_TARGET_CHAT_ID,
        material_change_hash=intent.material_change_hash,
    )

    assert left == right
    assert intent.notification_plan_id == runner.build_notification_plan_id(
        analysis_id=ANALYSIS_ID,
        target_chat_id=runner.LOCAL_TEST_TARGET_CHAT_ID,
        material_change_hash=intent.material_change_hash,
    )


def _fake_payload() -> dict:
    return fake_judge_runner.build_fake_judge_output_payload(
        fake_judge_runner.BundleContext(
            bundle_id=BUNDLE_ID,
            candidate_group_id=GROUP_ID,
            current_bundle_id=BUNDLE_ID,
            primary_summary={
                "repo_full_name": "example/example-tool",
                "headline": "Synthetic local fixture for a developer workflow helper.",
                "test_paths": ["tests/test_example_tool.py"],
                "ci_paths": [".github/workflows/test.yml"],
                "docs_paths": ["docs/usage.md"],
            },
            evidence_limitations=["synthetic local fixture; no GitHub API call"],
            ready_for_analysis=True,
        )
    )


def _policy_decision(
    *,
    verdict: str = "later",
    delivery_decision: str = "send_now",
    urgency_profile: str = "normal_silent",
) -> runner.PolicyDecision:
    return runner.PolicyDecision(
        verdict=verdict,
        delivery_decision=delivery_decision,
        urgency_profile=urgency_profile,
        scores_json={"practical_usefulness": 58, "evidence_strength": 62, "confidence": 55},
        reason_codes_json=["github_repo_fixture_evidence", "policy_threshold_later"],
        policy_reconciled_flag=True,
        evidence_limitations_ko="synthetic local fixture; no GitHub API call",
        recommended_action_ko="inspect later",
        freshness_note_ko="local fixture",
        model_proposed_verdict=verdict,
        suppress_reason_code="verdict_skip" if delivery_decision == "suppress" else None,
    )


def _expected_pass_report() -> dict[str, object]:
    return {
        "schema_version": "local_db_policy_engine_fixture_replay_v1",
        "status": "pass",
        "database_url_guard_passed": True,
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
        "judge_output_mutated": False,
        "bundle_mutated": False,
        "candidate_group_mutated": False,
        "notification_plan_created": False,
        "notification_render_created": False,
        "notification_delivery_created": False,
        "production_db_write": False,
        "live_github_called": False,
        "live_telegram_called": False,
        "openai_called": False,
        "workers_started": False,
        "redis_mutation": False,
        "checks_failed": [],
    }
