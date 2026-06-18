from __future__ import annotations

import ast
import json
from pathlib import Path
from uuid import UUID

from tools import local_db_judge_output_ready_policy_apply_fixture_runner as runner


ROOT = Path(__file__).resolve().parents[3]
SOURCE_FIXTURE = "tests/fixtures/upstream/source_message_github_repo_signal.json"
GITHUB_FIXTURE = "tests/fixtures/upstream/github_repo_snapshot_example_tool.json"
PG_SCHEME = "postgresql" + "+psycopg"
SAFE_DATABASE_NAME = "github_ai_catchbot_test"
SOCKET_HOST = "/var/run/postgresql"
SAFE_SOCKET_URL = f"{PG_SCHEME}:///{SAFE_DATABASE_NAME}?host={SOCKET_HOST}"
PASSWORD_VALUE = "local" + "_" + "password"
PASSWORD_URL = f"{PG_SCHEME}://local_user:{PASSWORD_VALUE}@127.0.0.1:5432/{SAFE_DATABASE_NAME}"
READY_EVENT_ID = UUID("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa")
GROUP_ID = UUID("11111111-1111-4111-8111-111111111111")
BUNDLE_ID = UUID("22222222-2222-4222-8222-222222222222")
RUN_ID = UUID("33333333-3333-4333-8333-333333333333")
OUTPUT_ID = UUID("44444444-4444-4444-8444-444444444444")
ANALYSIS_ID = UUID("55555555-5555-4555-8555-555555555555")


class FakeReadyEventResolver:
    def __init__(
        self,
        *,
        ready_event_id: UUID | None = READY_EVENT_ID,
        checks_failed: tuple[str, ...] = (),
    ) -> None:
        self.ready_event_id = ready_event_id
        self.checks_failed = checks_failed
        self.calls = []

    def resolve(
        self,
        *,
        database_url: str,
        replay_namespace: str,
        source_fixture_path: Path | None,
        github_snapshot_fixture_path: Path | None,
        env,
        repo_root: Path,
    ) -> runner.ReadyEventResolutionResult:
        self.calls.append(
            {
                "database_url": database_url,
                "replay_namespace": replay_namespace,
                "source_fixture_path": source_fixture_path,
                "github_snapshot_fixture_path": github_snapshot_fixture_path,
                "env": dict(env),
                "repo_root": repo_root,
            }
        )
        return runner.ReadyEventResolutionResult(
            judge_output_ready_event_id=self.ready_event_id,
            judge_output_ready_event_found=self.ready_event_id is not None,
            checks_failed=self.checks_failed,
        )


class FakeExecutor:
    def __init__(self, execution: runner.ReplayExecutionResult | None = None) -> None:
        self.execution = execution or _valid_non_suppress_execution()
        self.calls = []

    def execute(
        self,
        *,
        database_url: str,
        replay_namespace: str,
        judge_output_ready_event_id: UUID,
    ) -> runner.ReplayExecutionResult:
        self.calls.append(
            {
                "database_url": database_url,
                "replay_namespace": replay_namespace,
                "judge_output_ready_event_id": judge_output_ready_event_id,
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


def test_explicit_ready_event_id_valid_path_reports_policy_apply_analysis_and_plan_intent() -> None:
    resolver = FakeReadyEventResolver()
    executor = FakeExecutor()

    result = _run(
        "--database-url",
        SAFE_SOCKET_URL,
        "--judge-output-ready-event-id",
        str(READY_EVENT_ID),
        "--confirm-local-test-db",
        resolver=resolver,
        executor=executor,
    )

    assert result.exit_code == 0
    assert result.report == _expected_pass_report()
    assert resolver.calls == []
    assert executor.calls == [
        {
            "database_url": SAFE_SOCKET_URL,
            "replay_namespace": f"event-{READY_EVENT_ID}",
            "judge_output_ready_event_id": READY_EVENT_ID,
        }
    ]


def test_replay_namespace_fixture_path_resolves_one_ready_event_then_runs_boundaries() -> None:
    resolver = FakeReadyEventResolver()
    executor = FakeExecutor()

    result = _run(
        "--database-url",
        SAFE_SOCKET_URL,
        "--source-fixture",
        SOURCE_FIXTURE,
        "--github-snapshot-fixture",
        GITHUB_FIXTURE,
        "--replay-namespace",
        "unit-policy-apply-fixture",
        "--confirm-local-test-db",
        resolver=resolver,
        executor=executor,
    )

    assert result.exit_code == 0
    assert result.report["analysis_policy_apply_event_created"] is True
    assert result.report["analysis_created"] is True
    assert result.report["notification_plan_created_event_created"] is True
    assert len(resolver.calls) == 1
    assert resolver.calls[0]["source_fixture_path"] == Path(SOURCE_FIXTURE)
    assert resolver.calls[0]["github_snapshot_fixture_path"] == Path(GITHUB_FIXTURE)
    assert executor.calls[0]["replay_namespace"] == "unit-policy-apply-fixture"


def test_skip_suppress_writes_one_analysis_row_without_notification_plan_intent() -> None:
    executor = FakeExecutor(
        _valid_non_suppress_execution(
            notification_plan_created_event_created=False,
            notification_plan_created_event_count=0,
        )
    )

    result = _run(
        "--database-url",
        SAFE_SOCKET_URL,
        "--judge-output-ready-event-id",
        str(READY_EVENT_ID),
        "--confirm-local-test-db",
        executor=executor,
    )

    assert result.exit_code == 0
    assert result.report["analysis_created"] is True
    assert result.report["notification_plan_created_event_created"] is False
    assert result.report["notification_created"] is False


def test_refusal_envelope_stops_at_validator_without_policy_apply_or_analysis() -> None:
    executor = FakeExecutor(
        runner.ReplayExecutionResult(
            judge_output_ready_event_found=True,
            judge_run_loaded=True,
            judge_output_loaded=True,
            evidence_bundle_loaded=True,
            candidate_group_loaded=True,
            analysis_validator_passed=False,
            refusal_stopped_at_validator=True,
            analysis_policy_apply_event_created=False,
            policy_engine_applied=False,
            analysis_created=False,
            notification_plan_created_event_created=False,
            notification_created=False,
            analysis_policy_apply_event_count=0,
            analysis_row_count=0,
            notification_plan_created_event_count=0,
        )
    )

    result = _run(
        "--database-url",
        SAFE_SOCKET_URL,
        "--judge-output-ready-event-id",
        str(READY_EVENT_ID),
        "--confirm-local-test-db",
        executor=executor,
    )

    assert result.exit_code == 0
    assert result.report["refusal_stopped_at_validator"] is True
    assert result.report["analysis_policy_apply_event_created"] is False
    assert result.report["analysis_created"] is False
    assert result.report["notification_plan_created_event_created"] is False


def test_stale_or_mismatched_bundle_context_fails_without_analysis() -> None:
    executor = FakeExecutor(
        runner.ReplayExecutionResult(
            judge_output_ready_event_found=True,
            judge_run_loaded=True,
            judge_output_loaded=True,
            evidence_bundle_loaded=True,
            candidate_group_loaded=True,
            analysis_validator_passed=False,
            refusal_stopped_at_validator=False,
            analysis_policy_apply_event_created=False,
            policy_engine_applied=False,
            analysis_created=False,
            notification_plan_created_event_created=False,
            notification_created=False,
            checks_failed=("candidate_current_bundle_mismatch",),
        )
    )

    result = _run(
        "--database-url",
        SAFE_SOCKET_URL,
        "--judge-output-ready-event-id",
        str(READY_EVENT_ID),
        "--confirm-local-test-db",
        executor=executor,
    )

    assert result.exit_code == 1
    assert "candidate_current_bundle_mismatch" in result.report["checks_failed"]
    assert result.report["analysis_created"] is False
    assert result.report["notification_plan_created_event_created"] is False


def test_idempotency_requires_single_policy_event_analysis_row_and_plan_event() -> None:
    executor = FakeExecutor(_valid_non_suppress_execution(analysis_row_count=2))

    result = _run(
        "--database-url",
        SAFE_SOCKET_URL,
        "--judge-output-ready-event-id",
        str(READY_EVENT_ID),
        "--confirm-local-test-db",
        executor=executor,
    )

    assert result.exit_code == 1
    assert "analysis_row_count_not_one" in result.report["checks_failed"]


def test_ambiguous_namespace_ready_events_refuse_before_executor_runs() -> None:
    resolver = FakeReadyEventResolver(
        ready_event_id=None,
        checks_failed=("judge_output_ready_event_ambiguous",),
    )
    executor = FakeExecutor()

    result = _run(
        "--database-url",
        SAFE_SOCKET_URL,
        "--replay-namespace",
        "unit-ambiguous",
        "--confirm-local-test-db",
        resolver=resolver,
        executor=executor,
    )

    assert result.exit_code == 1
    assert "judge_output_ready_event_ambiguous" in result.report["checks_failed"]
    assert executor.calls == []


def test_requires_app_env_test_and_confirmation_before_boundary_execution() -> None:
    executor = FakeExecutor()

    result = _run(
        "--database-url",
        SAFE_SOCKET_URL,
        "--judge-output-ready-event-id",
        str(READY_EVENT_ID),
        env={"APP_ENV": "dev"},
        executor=executor,
    )

    assert result.exit_code == 1
    assert result.report["checks_failed"] == ["confirm_local_test_db_required", "app_env_test_required"]
    assert executor.calls == []


def test_report_output_is_stable_json_without_raw_secrets_or_payload_text() -> None:
    raw_prompt = "developer prompt " + "raw body"
    raw_evidence = "raw evidence " + "text"
    raw_response = "raw openai " + "response body"
    api_key = "sk-" + "test-" + "secret"

    result = _run(
        "--database-url",
        PASSWORD_URL,
        "--judge-output-ready-event-id",
        str(READY_EVENT_ID),
        "--confirm-local-test-db",
        executor=FakeExecutor(),
    )

    text = runner.render_json(result.report)
    parsed = json.loads(text)
    assert list(parsed) == list(_expected_pass_report())
    for forbidden in (PASSWORD_URL, PASSWORD_VALUE, raw_prompt, raw_evidence, raw_response, api_key):
        assert forbidden not in text


def test_source_has_no_forbidden_runtime_network_or_transport_imports() -> None:
    source = (ROOT / "tools/local_db_judge_output_ready_policy_apply_fixture_runner.py").read_text(encoding="utf-8")
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


def test_closed_authority_side_effect_flags_remain_false() -> None:
    result = _run(
        "--database-url",
        SAFE_SOCKET_URL,
        "--judge-output-ready-event-id",
        str(READY_EVENT_ID),
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
        "notification_created",
    ):
        assert result.report[key] is False


def test_live_output_like_payload_without_legacy_scores_reaches_policy_apply(monkeypatch) -> None:
    calls = _install_direct_success_path(monkeypatch, payload=_locked_judge_output_payload())

    result = runner._execute_judge_output_ready_policy_apply(
        object(),
        replay_namespace="unit-live-like",
        judge_output_ready_event_id=READY_EVENT_ID,
    )

    assert result.judge_output_ready_event_found is True
    assert result.judge_run_loaded is True
    assert result.judge_output_loaded is True
    assert result.evidence_bundle_loaded is True
    assert result.candidate_group_loaded is True
    assert result.analysis_validator_passed is True
    assert result.analysis_policy_apply_event_created is True
    assert result.policy_engine_applied is True
    assert result.analysis_created is True
    assert result.notification_plan_created_event_created is True
    assert result.notification_created is False
    assert result.analysis_policy_apply_event_count == 1
    assert result.analysis_row_count == 1
    assert result.notification_plan_created_event_count == 1
    assert result.checks_failed == ()
    assert calls == {"state_transition": 1, "policy_event": 1, "policy_engine": 1}


def test_direct_ready_event_path_rejects_missing_locked_required_score(monkeypatch) -> None:
    payload = _locked_judge_output_payload()
    del payload["scores"]["practical_usefulness"]
    calls = _install_direct_success_path(monkeypatch, payload=payload)

    result = runner._execute_judge_output_ready_policy_apply(
        object(),
        replay_namespace="unit-missing-score",
        judge_output_ready_event_id=READY_EVENT_ID,
    )

    assert result.analysis_validator_passed is False
    assert result.analysis_policy_apply_event_created is False
    assert result.policy_engine_applied is False
    assert result.analysis_created is False
    assert "scores.practical_usefulness:missing" in result.checks_failed
    assert calls == {"state_transition": 1, "policy_event": 0, "policy_engine": 0}


def test_direct_ready_event_path_rerun_reuses_single_policy_event_analysis_and_plan(monkeypatch) -> None:
    calls = _install_direct_success_path(monkeypatch, payload=_locked_judge_output_payload())

    first = runner._execute_judge_output_ready_policy_apply(
        object(),
        replay_namespace="unit-idempotent",
        judge_output_ready_event_id=READY_EVENT_ID,
    )
    second = runner._execute_judge_output_ready_policy_apply(
        object(),
        replay_namespace="unit-idempotent",
        judge_output_ready_event_id=READY_EVENT_ID,
    )

    for result in (first, second):
        assert result.analysis_validator_passed is True
        assert result.analysis_policy_apply_event_count == 1
        assert result.analysis_row_count == 1
        assert result.notification_plan_created_event_count == 1
        assert result.checks_failed == ()
    assert calls == {"state_transition": 2, "policy_event": 2, "policy_engine": 2}


def test_direct_path_does_not_treat_preexisting_notification_rows_as_created(monkeypatch) -> None:
    calls = _install_direct_success_path(
        monkeypatch,
        payload=_locked_judge_output_payload(),
        policy_notification_flags={
            "notification_plan_created": True,
            "notification_render_created": True,
            "notification_delivery_created": True,
        },
        policy_failures=(
            "notification_plan_created:unexpected",
            "notification_render_created:unexpected",
            "notification_delivery_created:unexpected",
        ),
        notification_side_effect_flags={
            "notification_plan_created": True,
            "notification_render_created": True,
            "notification_delivery_created": True,
        },
    )

    result = runner._execute_judge_output_ready_policy_apply(
        object(),
        replay_namespace="unit-preexisting-notifications",
        judge_output_ready_event_id=READY_EVENT_ID,
    )

    assert result.analysis_validator_passed is True
    assert result.policy_engine_applied is True
    assert result.analysis_created is True
    assert result.notification_created is False
    assert result.checks_failed == ()
    assert calls == {"state_transition": 1, "policy_event": 1, "policy_engine": 1}


def _valid_non_suppress_execution(
    *,
    analysis_row_count: int = 1,
    notification_plan_created_event_created: bool = True,
    notification_plan_created_event_count: int = 1,
) -> runner.ReplayExecutionResult:
    return runner.ReplayExecutionResult(
        judge_output_ready_event_found=True,
        judge_run_loaded=True,
        judge_output_loaded=True,
        evidence_bundle_loaded=True,
        candidate_group_loaded=True,
        analysis_validator_passed=True,
        refusal_stopped_at_validator=False,
        analysis_policy_apply_event_created=True,
        policy_engine_applied=True,
        analysis_created=True,
        notification_plan_created_event_created=notification_plan_created_event_created,
        notification_created=False,
        analysis_policy_apply_event_count=1,
        analysis_row_count=analysis_row_count,
        notification_plan_created_event_count=notification_plan_created_event_count,
    )


def _locked_judge_output_payload() -> dict:
    return {
        "judge_schema_version": "judge_output_v1",
        "candidate_group_id": str(GROUP_ID),
        "headline": "example/example-tool",
        "summary_one_line_ko": "GitHub 저장소 기반 개발 도구 후보다.",
        "skeptical_take_ko": "증거는 local fixture 기반이다.",
        "why_it_might_matter_ko": "개발자 워크플로우 보조 도구로 검토할 수 있다.",
        "comparables": [],
        "scores": {
            "novelty": 41,
            "practical_usefulness": 58,
            "evidence_strength": 45,
            "hype_penalty": 20,
            "confidence": 45,
            "code_quality": 58,
            "maintenance_signal": 57,
            "specificity": None,
            "reproducibility_signal": None,
        },
        "reason_codes": ["github_repo_fixture_evidence", "comparison_gap", "insufficient_comparables"],
        "red_flags_ko": ["실제 GitHub API 호출 결과가 아니다."],
        "evidence_limitations_ko": [
            "synthetic local fixture; no GitHub API call",
            "comparison_gap: insufficient_comparables in local fixture",
        ],
        "recommended_action_ko": "local pipeline 검증용 후보로 취급한다.",
        "freshness_note_ko": "fixture 기준으로 고정된 local snapshot이다.",
        "model_proposed_verdict": "later",
        "model_confidence_band": "medium",
    }


def _install_direct_success_path(
    monkeypatch,
    *,
    payload: dict,
    policy_notification_flags: dict[str, bool] | None = None,
    policy_failures: tuple[str, ...] = (),
    notification_side_effect_flags: dict[str, bool] | None = None,
) -> dict[str, int]:
    calls = {"state_transition": 0, "policy_event": 0, "policy_engine": 0}
    policy_notification_flags = policy_notification_flags or {
        "notification_plan_created": False,
        "notification_render_created": False,
        "notification_delivery_created": False,
    }
    notification_side_effect_flags = notification_side_effect_flags or {
        "notification_plan_created": False,
        "notification_render_created": False,
        "notification_delivery_created": False,
    }
    event = runner.validator_runner.JudgeOutputReadyEvent(
        event_id=READY_EVENT_ID,
        judge_run_id=RUN_ID,
        judge_output_id=OUTPUT_ID,
        finish_reason="stop",
        refusal_detected=False,
    )
    judge_run = runner.validator_runner.JudgeRunRecord(
        judge_run_id=RUN_ID,
        bundle_id=BUNDLE_ID,
        status="succeeded",
        finish_reason="stop",
        refusal_detected=False,
    )
    judge_output = runner.validator_runner.JudgeOutputRecord(
        judge_output_id=OUTPUT_ID,
        judge_run_id=RUN_ID,
        candidate_group_id=GROUP_ID,
        judge_schema_version="judge_output_v1",
        payload_json=payload,
        model_proposed_verdict="later",
        model_confidence_band="medium",
    )
    bundle = runner.validator_runner.BundleContext(
        bundle_id=BUNDLE_ID,
        candidate_group_id=GROUP_ID,
        current_bundle_id=BUNDLE_ID,
        ready_for_analysis=True,
    )

    def record_state_transition(*args, **kwargs) -> None:
        calls["state_transition"] += 1

    def record_policy_event(*args, **kwargs) -> None:
        calls["policy_event"] += 1

    def execute_policy(*args, **kwargs) -> runner.policy_runner.ReplayExecutionResult:
        calls["policy_engine"] += 1
        return runner.policy_runner.ReplayExecutionResult(
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
            notification_plan_created=policy_notification_flags["notification_plan_created"],
            notification_render_created=policy_notification_flags["notification_render_created"],
            notification_delivery_created=policy_notification_flags["notification_delivery_created"],
            checks_failed=policy_failures,
        )

    monkeypatch.setattr(runner, "_load_judge_output_ready_event_by_id", lambda connection, event_id: event)
    monkeypatch.setattr(runner.validator_runner, "_load_judge_run", lambda connection, judge_run_id: judge_run)
    monkeypatch.setattr(runner.validator_runner, "_load_judge_output", lambda connection, judge_output_id: judge_output)
    monkeypatch.setattr(runner.validator_runner, "_load_bundle_context", lambda connection, bundle_id: bundle)
    monkeypatch.setattr(runner.validator_runner, "_insert_or_reuse_state_transition", record_state_transition)
    monkeypatch.setattr(runner.validator_runner, "_insert_or_reuse_analysis_policy_apply_event", record_policy_event)
    monkeypatch.setattr(runner, "_analysis_policy_apply_event_count", lambda *args, **kwargs: 1)
    monkeypatch.setattr(runner.policy_runner, "_execute_policy_engine_replay", execute_policy)
    monkeypatch.setattr(runner, "_load_notification_side_effect_flags", lambda *args, **kwargs: notification_side_effect_flags)
    monkeypatch.setattr(runner, "_load_analysis_id", lambda *args, **kwargs: ANALYSIS_ID)
    monkeypatch.setattr(runner, "_analysis_row_count", lambda *args, **kwargs: 1)
    monkeypatch.setattr(runner, "_notification_plan_created_event_count", lambda *args, **kwargs: 1)
    return calls


def _expected_pass_report() -> dict[str, object]:
    return {
        "schema_version": "local_db_judge_output_ready_policy_apply_fixture_runner_v1",
        "status": "pass",
        "database_url_guard_passed": True,
        "judge_output_ready_event_found": True,
        "judge_run_loaded": True,
        "judge_output_loaded": True,
        "evidence_bundle_loaded": True,
        "candidate_group_loaded": True,
        "analysis_validator_passed": True,
        "refusal_stopped_at_validator": False,
        "analysis_policy_apply_event_created": True,
        "policy_engine_applied": True,
        "analysis_created": True,
        "notification_plan_created_event_created": True,
        "openai_called": False,
        "telegram_called": False,
        "live_github_called": False,
        "workers_started": False,
        "redis_mutation": False,
        "production_db_write": False,
        "alembic_or_ddl_ran": False,
        "notification_created": False,
        "checks_failed": [],
    }
