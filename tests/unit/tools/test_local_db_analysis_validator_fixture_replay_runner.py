from __future__ import annotations

import ast
import json
from pathlib import Path
from uuid import UUID, uuid4

from tools import local_db_analysis_validator_fixture_replay_runner as runner
from tools import local_db_fake_judge_output_fixture_replay_runner as fake_judge_runner


ROOT = Path(__file__).resolve().parents[3]
SOURCE_FIXTURE = "tests/fixtures/upstream/source_message_github_repo_signal.json"
GITHUB_FIXTURE = "tests/fixtures/upstream/github_repo_snapshot_example_tool.json"
PG_SCHEME = "postgresql" + "+psycopg"
SAFE_DATABASE_NAME = "github_ai_catchbot_test"
SOCKET_HOST = "/var/run/postgresql"
SAFE_SOCKET_URL = f"{PG_SCHEME}:///{SAFE_DATABASE_NAME}?host={SOCKET_HOST}"
PASSWORD_VALUE = "local" + "_" + "password"
PASSWORD_URL = f"{PG_SCHEME}://local_user:{PASSWORD_VALUE}@127.0.0.1:5432/{SAFE_DATABASE_NAME}"
GROUP_ID = UUID("11111111-1111-4111-8111-111111111111")
BUNDLE_ID = UUID("22222222-2222-4222-8222-222222222222")
RUN_ID = UUID("33333333-3333-4333-8333-333333333333")
OUTPUT_ID = UUID("44444444-4444-4444-8444-444444444444")


class FakeFakeJudgeOutputRunner:
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
            "schema_version": "local_db_fake_judge_output_fixture_replay_v1",
            "status": "pass",
            "source_candidate_replay_confirmed": True,
            "artifact_snapshot_replay_confirmed": True,
            "evidence_bundle_replay_confirmed": True,
            "analysis_router_replay_confirmed": True,
            "judge_call_requested_event_found": True,
            "judge_run_pending_confirmed": True,
            "bundle_context_loaded": True,
            "fake_judge_output_created_or_reused": True,
            "judge_run_succeeded": True,
            "judge_output_ready_event_created": True,
            "structured_output_schema_valid": True,
            "usage_telemetry_recorded": True,
            "checks_failed": [],
        }
        report.update(self.report_overrides)
        return fake_judge_runner.RunnerResult(exit_code=self.exit_code, report=report)


class FakeAnalysisValidatorExecutor:
    def __init__(
        self,
        *,
        checks_failed: tuple[str, ...] = (),
        policy_event_insert_attempted: bool = True,
        judge_output_mutation_attempted: bool = False,
        refusal: bool = False,
    ) -> None:
        self.calls = []
        self.policy_event_insert_attempted = policy_event_insert_attempted
        self.judge_output_mutation_attempted = judge_output_mutation_attempted
        self.checks_failed = checks_failed
        self.refusal = refusal

    def execute(
        self,
        *,
        database_url: str,
        replay_namespace: str,
    ) -> runner.ReplayExecutionResult:
        self.calls.append((database_url, replay_namespace))
        return runner.ReplayExecutionResult(
            judge_output_ready_event_found=True,
            judge_run_succeeded_confirmed=True,
            judge_output_loaded=True,
            bundle_context_confirmed=True,
            structured_output_schema_valid=True,
            semantic_validation_passed=not self.refusal,
            refusal_not_detected=not self.refusal,
            validation_state_transition_recorded=True,
            analysis_policy_apply_event_created=not self.refusal,
            judge_output_mutated=self.judge_output_mutation_attempted,
            analysis_created=False,
            notification_created=False,
            checks_failed=self.checks_failed or (("model_refusal",) if self.refusal else ()),
        )


def _socket_url(database_name: str, *, host: str = SOCKET_HOST) -> str:
    return f"{PG_SCHEME}:///{database_name}?host={host}"


def _network_url(host: str, database_name: str, *, password: str = "secret") -> str:
    return f"{PG_SCHEME}://user:{password}@{host}/{database_name}"


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
    predecessor = FakeFakeJudgeOutputRunner()
    executor = FakeAnalysisValidatorExecutor()

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
        predecessor_runner=FakeFakeJudgeOutputRunner(),
        executor=FakeAnalysisValidatorExecutor(),
    )

    text = runner.render_json(result.report)
    parsed = json.loads(text)
    assert list(parsed) == list(_expected_pass_report())
    assert PASSWORD_URL not in text
    assert PASSWORD_VALUE not in text


def test_rejects_missing_confirmation_before_predecessor_runs() -> None:
    predecessor = FakeFakeJudgeOutputRunner()
    executor = FakeAnalysisValidatorExecutor()

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
    predecessor = FakeFakeJudgeOutputRunner()
    executor = FakeAnalysisValidatorExecutor()

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
    unsafe_url = PG_SCHEME + ":///unsafe"

    def fake_validate(database_url):
        calls.append(database_url)
        return False, ["delegated_failure"], None

    monkeypatch.setattr(runner.fake_judge_runner, "validate_database_url", fake_validate)

    ok, failures, parsed = runner.validate_database_url(unsafe_url)

    assert ok is False
    assert failures == ["delegated_failure"]
    assert parsed is None
    assert calls == [unsafe_url]


def test_database_url_guard_rejects_remote_prod_and_default_targets() -> None:
    cases = [
        ("mysql" + "://localhost/" + SAFE_DATABASE_NAME, "database_url_unsupported_scheme"),
        (_network_url("db.example.com", SAFE_DATABASE_NAME), "database_url_remote_host_rejected"),
        (_socket_url(SAFE_DATABASE_NAME, host="db.example.com"), "database_url_remote_query_host_rejected"),
        (_socket_url("postgres"), "database_url_forbidden_database_name"),
        (_socket_url("github_ai_catchbot"), "database_url_forbidden_database_name"),
        (_socket_url("github_ai_catchbot_prod_test"), "database_url_production_name_rejected"),
        (_socket_url("github_ai_catchbot_stage"), "database_url_missing_local_test_marker"),
    ]
    for database_url, expected_failure in cases:
        ok, failures, parsed = runner.validate_database_url(database_url)
        assert ok is False
        assert expected_failure in failures
        assert parsed is not None


def test_runner_source_has_no_forbidden_runtime_or_network_imports() -> None:
    source = (ROOT / "tools/local_db_analysis_validator_fixture_replay_runner.py").read_text(encoding="utf-8")
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


def test_judge_output_v1_schema_helper_accepts_prior_fake_payload() -> None:
    payload = _fake_payload()

    ok, failures = runner.validate_judge_output_v1_payload(payload)

    assert ok is True
    assert failures == ()
    assert "implementation_signal" not in payload["scores"]
    assert "urgency" not in payload["scores"]


def test_schema_helper_accepts_optional_legacy_scores_without_requiring_them() -> None:
    payload = _fake_payload()
    payload["scores"]["implementation_signal"] = 58
    payload["scores"]["urgency"] = 32

    ok, failures = runner.validate_judge_output_v1_payload(payload)

    assert ok is True
    assert failures == ()


def test_schema_helper_accepts_skip_model_proposed_verdict() -> None:
    payload = _fake_payload()
    payload["model_proposed_verdict"] = "skip"

    ok, failures = runner.validate_judge_output_v1_payload(payload)

    assert ok is True
    assert failures == ()


def test_schema_helper_rejects_archive_model_proposed_verdict() -> None:
    payload = _fake_payload()
    payload["model_proposed_verdict"] = "archive"

    ok, failures = runner.validate_judge_output_v1_payload(payload)

    assert ok is False
    assert "model_proposed_verdict_invalid" in failures


def test_schema_helper_rejects_drop_model_proposed_verdict() -> None:
    payload = _fake_payload()
    payload["model_proposed_verdict"] = "drop"

    ok, failures = runner.validate_judge_output_v1_payload(payload)

    assert ok is False
    assert "model_proposed_verdict_invalid" in failures


def test_schema_helper_rejects_missing_required_fields() -> None:
    payload = _fake_payload()
    del payload["headline"]

    ok, failures = runner.validate_judge_output_v1_payload(payload)

    assert ok is False
    assert "missing:headline" in failures


def test_schema_helper_rejects_missing_locked_required_score() -> None:
    payload = _fake_payload()
    del payload["scores"]["practical_usefulness"]

    ok, failures = runner.validate_judge_output_v1_payload(payload)

    assert ok is False
    assert "scores.practical_usefulness:missing" in failures


def test_schema_helper_rejects_score_outside_zero_to_hundred() -> None:
    payload = _fake_payload()
    payload["scores"]["hype_penalty"] = 101

    ok, failures = runner.validate_judge_output_v1_payload(payload)

    assert ok is False
    assert "scores.hype_penalty:out_of_range" in failures


def test_semantic_validator_rejects_candidate_group_mismatch() -> None:
    payload = _fake_payload()
    output = _output(candidate_group_id=uuid4())

    ok, failures = runner.validate_semantic_consistency(payload, output)

    assert ok is False
    assert "payload_candidate_group_mismatch" in failures


def test_semantic_validator_accepts_conservative_no_comparables_with_gap_marker() -> None:
    payload = _fake_payload()

    ok, failures = runner.validate_semantic_consistency(payload, _output(payload=payload))

    assert ok is True
    assert failures == ()


def test_semantic_validator_rejects_no_comparables_without_gap_marker() -> None:
    payload = _fake_payload()
    payload["reason_codes"] = ["github_repo_fixture_evidence"]
    payload["evidence_limitations_ko"] = ["synthetic local fixture only"]

    ok, failures = runner.validate_semantic_consistency(payload, _output(payload=payload))

    assert ok is False
    assert "github_comparables_missing_comparison_gap" in failures


def test_semantic_validator_rejects_no_comparables_high_confidence() -> None:
    payload = _fake_payload()
    payload["model_confidence_band"] = "high"
    output = _output(payload=payload)
    output = runner.JudgeOutputRecord(
        judge_output_id=output.judge_output_id,
        judge_run_id=output.judge_run_id,
        candidate_group_id=output.candidate_group_id,
        judge_schema_version=output.judge_schema_version,
        payload_json=output.payload_json,
        model_proposed_verdict=output.model_proposed_verdict,
        model_confidence_band="high",
    )

    ok, failures = runner.validate_semantic_consistency(payload, output)

    assert ok is False
    assert "github_comparables_required_for_high_action" in failures


def test_refusal_branch_does_not_emit_policy_handoff_in_fake_executor_path() -> None:
    executor = FakeAnalysisValidatorExecutor(refusal=True)

    result = _run(
        "--database-url",
        SAFE_SOCKET_URL,
        "--source-fixture",
        SOURCE_FIXTURE,
        "--github-snapshot-fixture",
        GITHUB_FIXTURE,
        "--replay-namespace",
        "unit-refusal",
        "--confirm-local-test-db",
        predecessor_runner=FakeFakeJudgeOutputRunner(),
        executor=executor,
    )

    assert result.exit_code == 1
    assert result.report["analysis_policy_apply_event_created"] is False
    assert result.report["checks_failed"] == [
        "model_refusal",
        "semantic_validation_passed:missing",
        "refusal_not_detected:missing",
        "analysis_policy_apply_event_created:missing",
    ]


def test_existing_policy_apply_event_reuse_does_not_require_duplicate_event_in_executor_fake_path() -> None:
    executor = FakeAnalysisValidatorExecutor(policy_event_insert_attempted=False)

    result = _run(
        "--database-url",
        SAFE_SOCKET_URL,
        "--source-fixture",
        SOURCE_FIXTURE,
        "--github-snapshot-fixture",
        GITHUB_FIXTURE,
        "--replay-namespace",
        "unit-existing-policy-event",
        "--confirm-local-test-db",
        predecessor_runner=FakeFakeJudgeOutputRunner(),
        executor=executor,
    )

    assert result.exit_code == 0
    assert result.report == _expected_pass_report()
    assert executor.policy_event_insert_attempted is False


def test_judge_outputs_mutation_is_never_attempted_in_fake_path() -> None:
    executor = FakeAnalysisValidatorExecutor(judge_output_mutation_attempted=False)

    result = _run(
        "--database-url",
        SAFE_SOCKET_URL,
        "--source-fixture",
        SOURCE_FIXTURE,
        "--github-snapshot-fixture",
        GITHUB_FIXTURE,
        "--replay-namespace",
        "unit-no-output-mutation",
        "--confirm-local-test-db",
        predecessor_runner=FakeFakeJudgeOutputRunner(),
        executor=executor,
    )

    assert result.exit_code == 0
    assert result.report["judge_output_mutated"] is False
    assert executor.judge_output_mutation_attempted is False


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


def _output(*, candidate_group_id: UUID = GROUP_ID, payload=None) -> runner.JudgeOutputRecord:
    return runner.JudgeOutputRecord(
        judge_output_id=OUTPUT_ID,
        judge_run_id=RUN_ID,
        candidate_group_id=candidate_group_id,
        judge_schema_version="judge_output_v1",
        payload_json=payload or _fake_payload(),
        model_proposed_verdict="later",
        model_confidence_band="medium",
    )


def _expected_pass_report() -> dict[str, object]:
    return {
        "schema_version": "local_db_analysis_validator_fixture_replay_v1",
        "status": "pass",
        "database_url_guard_passed": True,
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
        "judge_output_mutated": False,
        "analysis_created": False,
        "notification_created": False,
        "production_db_write": False,
        "live_github_called": False,
        "live_telegram_called": False,
        "openai_called": False,
        "workers_started": False,
        "redis_mutation": False,
        "checks_failed": [],
    }
