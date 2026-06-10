from __future__ import annotations

import ast
import json
from pathlib import Path
from uuid import UUID

from tools import local_db_notification_plan_created_render_dry_run_fixture_runner as runner


ROOT = Path(__file__).resolve().parents[3]
SOURCE_FIXTURE = "tests/fixtures/upstream/source_message_github_repo_signal.json"
GITHUB_FIXTURE = "tests/fixtures/upstream/github_repo_snapshot_example_tool.json"
PG_SCHEME = "postgresql+psycopg"
SAFE_DATABASE_NAME = "github_ai_catchbot_test"
SOCKET_HOST = "/var/run/postgresql"
SAFE_SOCKET_URL = f"{PG_SCHEME}:///{SAFE_DATABASE_NAME}?host={SOCKET_HOST}"
SECRET_VALUE = "local" + "_" + "secret"
PASSWORD_URL = f"{PG_SCHEME}://local_user:{SECRET_VALUE}@127.0.0.1:5432/{SAFE_DATABASE_NAME}"
EVENT_ID = UUID("10000000-0000-4000-8000-000000000001")
PLAN_ID = UUID("11111111-1111-4111-8111-111111111111")
ANALYSIS_ID = UUID("22222222-2222-4222-8222-222222222222")
GROUP_ID = UUID("33333333-3333-4333-8333-333333333333")
OUTPUT_ID = UUID("44444444-4444-4444-8444-444444444444")
DELIVERY_ID = UUID("55555555-5555-4555-8555-555555555555")
ARTIFACT_ID = UUID("66666666-6666-4666-8666-666666666666")


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
    ) -> runner.PlanEventResolutionResult:
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
        return runner.PlanEventResolutionResult(
            notification_plan_created_event_id=self.event_id,
            notification_plan_created_event_found=self.event_id is not None,
            delivery_dedupe_namespace=replay_namespace,
            checks_failed=self.checks_failed,
        )


class FakeExecutor:
    def __init__(self, *, checks_failed: tuple[str, ...] = (), suppress: bool = False) -> None:
        self.calls = []
        self.checks_failed = checks_failed
        self.suppress = suppress

    def execute(
        self,
        *,
        database_url: str,
        notification_plan_created_event_id: UUID,
        delivery_dedupe_namespace: str,
    ) -> runner.RenderDryRunExecutionResult:
        self.calls.append((database_url, notification_plan_created_event_id, delivery_dedupe_namespace))
        if self.suppress:
            return runner.RenderDryRunExecutionResult(
                notification_plan_created_event_found=True,
                analysis_loaded=False,
                judge_output_loaded=False,
                candidate_group_loaded=False,
                primary_artifact_loaded=False,
                notification_plan_concretized=False,
                notification_render_created=False,
                render_length_within_limit=False,
                render_hash_stable=False,
                dry_run_delivery_record_created=False,
                notification_state_transition_recorded=False,
                notification_delivery_result_event_created=False,
                checks_failed=("suppress_delivery_decision_refused",),
            )
        return runner.RenderDryRunExecutionResult(
            notification_plan_created_event_found=True,
            analysis_loaded=True,
            judge_output_loaded=True,
            candidate_group_loaded=True,
            primary_artifact_loaded=True,
            notification_plan_concretized=True,
            notification_render_created=True,
            render_length_within_limit=True,
            render_hash_stable=True,
            dry_run_delivery_record_created=True,
            notification_state_transition_recorded=True,
            notification_delivery_result_event_created=True,
            verdict_recomputed=False,
            delivery_decision_overridden=False,
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


def test_fixture_namespace_mode_returns_expected_pass_report() -> None:
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
        "unit-notification-render",
        "--confirm-local-test-db",
        resolver=resolver,
        executor=executor,
    )

    assert result.exit_code == 0
    assert result.report == _expected_pass_report()
    assert len(resolver.calls) == 1
    assert executor.calls == [(SAFE_SOCKET_URL, EVENT_ID, "unit-notification-render")]


def test_explicit_event_id_mode_skips_fixture_resolver_and_uses_stable_namespace() -> None:
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
    assert executor.calls == [(SAFE_SOCKET_URL, EVENT_ID, f"event-{EVENT_ID}")]


def test_explicit_event_id_can_use_caller_namespace_for_event_dedupe_context() -> None:
    executor = FakeExecutor()

    result = _run(
        "--database-url",
        SAFE_SOCKET_URL,
        "--notification-plan-created-event-id",
        str(EVENT_ID),
        "--replay-namespace",
        "unit-explicit-event",
        "--confirm-local-test-db",
        resolver=FakeResolver(),
        executor=executor,
    )

    assert result.exit_code == 0
    assert executor.calls == [(SAFE_SOCKET_URL, EVENT_ID, "unit-explicit-event")]


def test_rejects_missing_confirmation_before_resolver_or_executor() -> None:
    resolver = FakeResolver()
    executor = FakeExecutor()

    result = _run(
        "--database-url",
        SAFE_SOCKET_URL,
        "--notification-plan-created-event-id",
        str(EVENT_ID),
        resolver=resolver,
        executor=executor,
    )

    assert result.exit_code == 1
    assert result.report["database_url_guard_passed"] is True
    assert result.report["checks_failed"] == ["confirm_local_test_db_required"]
    assert resolver.calls == []
    assert executor.calls == []


def test_requires_app_env_test() -> None:
    result = _run(
        "--database-url",
        SAFE_SOCKET_URL,
        "--notification-plan-created-event-id",
        str(EVENT_ID),
        "--confirm-local-test-db",
        env={"APP_ENV": "prod"},
        resolver=FakeResolver(),
        executor=FakeExecutor(),
    )

    assert result.exit_code == 1
    assert result.report["checks_failed"] == ["app_env_test_required"]


def test_rejects_missing_selector_and_partial_fixture_pair() -> None:
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
    assert "fixture_path_pair_required" in result.report["checks_failed"]


def test_rejects_ambiguous_selector_mode() -> None:
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
        "unit-ambiguous",
        "--confirm-local-test-db",
        resolver=FakeResolver(),
        executor=FakeExecutor(),
    )

    assert result.exit_code == 1
    assert result.report["checks_failed"] == ["selector_mode_ambiguous"]


def test_database_url_guard_delegates_to_existing_guard(monkeypatch) -> None:
    calls = []

    def fake_validate(database_url):
        calls.append(database_url)
        return False, ["delegated_failure"], None

    monkeypatch.setattr(runner.upstream_runner, "validate_database_url", fake_validate)

    ok, failures, parsed = runner.validate_database_url("unsafe")

    assert ok is False
    assert failures == ["delegated_failure"]
    assert parsed is None
    assert calls == ["unsafe"]


def test_required_output_shape_is_stable_json_without_raw_url_or_password() -> None:
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


def test_render_uses_analysis_verdict_and_delivery_without_recomputing() -> None:
    analysis = _analysis(verdict="later", delivery_decision="send_now")
    render = runner.build_notification_render(
        intent=_intent(delivery_decision="send_now", urgency_profile="normal_silent"),
        analysis=analysis,
        judge_output=_judge_output(headline="example/example-tool"),
        candidate=_candidate(primary_canonical_id="github:example/example-tool"),
    )

    assert "Final verdict: later" in render.message_text
    assert "Delivery decision: send_now" in render.message_text
    assert "Headline: example/example-tool" in render.message_text
    assert analysis.verdict == "later"
    assert analysis.delivery_decision == "send_now"


def test_render_uses_judge_output_only_as_supporting_text_source() -> None:
    render = runner.build_notification_render(
        intent=_intent(),
        analysis=_analysis(),
        judge_output=_judge_output(
            headline="supporting headline",
            summary="한 줄 요약",
            skeptical="차갑게 보면 아직 검증 필요",
            red_flags=["maintainer signal weak"],
        ),
        candidate=_candidate(),
    )

    assert "supporting headline" in render.message_text
    assert "한 줄 요약" in render.message_text
    assert "차갑게 보면 아직 검증 필요" in render.message_text
    assert "maintainer signal weak" in render.message_text
    assert "model_proposed_verdict" not in render.message_text


def test_later_send_now_normal_silent_maps_to_disable_notification_true() -> None:
    render = runner.build_notification_render(
        intent=_intent(delivery_decision="send_now", urgency_profile="normal_silent"),
        analysis=_analysis(verdict="later", delivery_decision="send_now"),
        judge_output=_judge_output(),
        candidate=_candidate(),
    )

    assert render.disable_notification is True


def test_inspect_now_send_now_high_maps_to_disable_notification_false() -> None:
    render = runner.build_notification_render(
        intent=_intent(delivery_decision="send_now", urgency_profile="high"),
        analysis=_analysis(verdict="inspect_now", delivery_decision="send_now"),
        judge_output=_judge_output(),
        candidate=_candidate(),
    )

    assert render.disable_notification is False


def test_link_preview_disabled_and_keyboard_created_only_for_safe_primary_url() -> None:
    safe = runner.build_notification_render(
        intent=_intent(),
        analysis=_analysis(),
        judge_output=_judge_output(),
        candidate=_candidate(primary_canonical_url="https://example.com/repo"),
    )
    unsafe = runner.build_notification_render(
        intent=_intent(),
        analysis=_analysis(),
        judge_output=_judge_output(),
        candidate=_candidate(primary_canonical_url="https://user:secret@example.com/repo?token=x"),
    )

    assert safe.link_preview_options_json == {"is_disabled": True}
    assert safe.reply_markup_json == {
        "inline_keyboard": [[{"text": "Primary Link", "url": "https://example.com/repo"}]]
    }
    assert unsafe.link_preview_options_json == {"is_disabled": True}
    assert unsafe.reply_markup_json is None


def test_message_text_stays_under_telegram_limit_with_priority_fields_preserved() -> None:
    long_text = "x" * 10000
    render = runner.build_notification_render(
        intent=_intent(),
        analysis=_analysis(evidence_limitations_ko=long_text, recommended_action_ko=long_text),
        judge_output=_judge_output(summary=long_text, skeptical=long_text, red_flags=[long_text]),
        candidate=_candidate(),
    )

    assert len(render.message_text) <= runner.MAX_TELEGRAM_TEXT_CHARS
    assert render.message_text.startswith("Urgency: normal_silent")
    assert "Final verdict: later" in render.message_text
    assert render.render_hash == runner.build_notification_render(
        intent=_intent(),
        analysis=_analysis(evidence_limitations_ko=long_text, recommended_action_ko=long_text),
        judge_output=_judge_output(summary=long_text, skeptical=long_text, red_flags=[long_text]),
        candidate=_candidate(),
    ).render_hash


def test_context_mismatch_returns_stable_reason_code() -> None:
    failures = runner.context_failure_codes(
        intent=_intent(),
        analysis=_analysis(candidate_group_id=UUID("99999999-9999-4999-8999-999999999999")),
        judge_output=_judge_output(),
    )

    assert failures == ("analysis_candidate_mismatch",)


def test_suppress_event_result_does_not_render_or_emit_delivery_result() -> None:
    result = _run(
        "--database-url",
        SAFE_SOCKET_URL,
        "--notification-plan-created-event-id",
        str(EVENT_ID),
        "--confirm-local-test-db",
        executor=FakeExecutor(suppress=True),
    )

    assert result.exit_code == 1
    assert result.report["notification_plan_created_event_found"] is True
    assert result.report["notification_render_created"] is False
    assert result.report["dry_run_delivery_record_created"] is False
    assert result.report["notification_delivery_result_event_created"] is False
    assert "suppress_delivery_decision_refused" in result.report["checks_failed"]


def test_dry_run_delivery_payload_and_result_event_dedupe_key_are_stable() -> None:
    payload = runner.build_delivery_result_payload(
        notification_plan_id=PLAN_ID,
        notification_delivery_record_id=DELIVERY_ID,
        telegram_chat_id=424242001,
    )
    left = runner.build_delivery_result_event_dedupe_key(
        delivery_dedupe_namespace="unit-idempotent",
        notification_plan_id=PLAN_ID,
        notification_delivery_record_id=DELIVERY_ID,
    )
    right = runner.build_delivery_result_event_dedupe_key(
        delivery_dedupe_namespace="unit-idempotent",
        notification_plan_id=PLAN_ID,
        notification_delivery_record_id=DELIVERY_ID,
    )

    assert payload["delivery_status"] == "suppressed"
    assert payload["telegram_message_id"] is None
    assert payload["attempt_count"] == 0
    assert payload["noop"] is True
    assert payload["dry_run"] is True
    assert payload["reason_code"] == "dry_run_skip_transport"
    assert left == right
    assert left.startswith("local-db-notification-render-dry-run:unit-idempotent:")


def test_success_report_keeps_runtime_network_and_mutation_boundaries_false() -> None:
    result = _run(
        "--database-url",
        SAFE_SOCKET_URL,
        "--notification-plan-created-event-id",
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
        "verdict_recomputed",
        "delivery_decision_overridden",
    ):
        assert result.report[key] is False


def test_runner_source_has_no_forbidden_runtime_or_network_imports() -> None:
    source = (ROOT / "tools/local_db_notification_plan_created_render_dry_run_fixture_runner.py").read_text(
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
    assert "CREATE TABLE" not in source
    assert "ALTER TABLE" not in source
    assert "DROP TABLE" not in source


def _intent(
    *,
    delivery_decision: str = "send_now",
    urgency_profile: str = "normal_silent",
    primary_chat: int = 424242001,
) -> runner.notifier_base.NotificationPlanIntent:
    return runner.notifier_base.NotificationPlanIntent(
        notification_plan_id=PLAN_ID,
        analysis_id=ANALYSIS_ID,
        candidate_group_id=GROUP_ID,
        delivery_decision=delivery_decision,
        urgency_profile=urgency_profile,
        target_chat_id=primary_chat,
        target_thread_id=None,
        render_profile="telegram_single_alert_normal_v1",
        dedupe_subject_key=str(GROUP_ID),
        material_change_hash="material-hash",
        send_after=None,
        suppress_reason_code=None,
    )


def _analysis(
    *,
    verdict: str = "later",
    delivery_decision: str = "send_now",
    candidate_group_id: UUID = GROUP_ID,
    evidence_limitations_ko: str | None = "synthetic local fixture; no GitHub API call",
    recommended_action_ko: str | None = "inspect later",
) -> runner.notifier_base.AnalysisRecord:
    return runner.notifier_base.AnalysisRecord(
        analysis_id=ANALYSIS_ID,
        candidate_group_id=candidate_group_id,
        judge_output_id=OUTPUT_ID,
        verdict=verdict,
        delivery_decision=delivery_decision,
        reason_codes_json=["github_repo_fixture_evidence", "policy_threshold_later"],
        evidence_limitations_ko=evidence_limitations_ko,
        recommended_action_ko=recommended_action_ko,
        freshness_note_ko="local fixture",
        policy_reconciled_flag=True,
    )


def _judge_output(
    *,
    headline: str = "example/example-tool",
    summary: str = "fixture summary",
    skeptical: str = "fixture skeptical take",
    red_flags: list[str] | None = None,
) -> runner.notifier_base.JudgeOutputRecord:
    return runner.notifier_base.JudgeOutputRecord(
        judge_output_id=OUTPUT_ID,
        candidate_group_id=GROUP_ID,
        payload_json={
            "headline": headline,
            "summary_one_line_ko": summary,
            "skeptical_take_ko": skeptical,
            "why_it_might_matter_ko": "fixture why",
            "red_flags_ko": red_flags or [],
            "model_proposed_verdict": "later",
        },
        model_proposed_verdict="later",
        model_confidence_band="medium",
    )


def _candidate(
    *,
    primary_canonical_id: str = "github:example/example-tool",
    primary_canonical_url: str | None = "https://example.invalid/example/example-tool",
) -> runner.notifier_base.CandidateContext:
    return runner.notifier_base.CandidateContext(
        candidate_group_id=GROUP_ID,
        current_primary_artifact_id=ARTIFACT_ID,
        primary_artifact_type="github_repo",
        primary_canonical_id=primary_canonical_id,
        primary_canonical_url=primary_canonical_url,
    )


def _expected_pass_report() -> dict[str, object]:
    return {
        "schema_version": "local_db_notification_plan_created_render_dry_run_fixture_runner_v1",
        "status": "pass",
        "database_url_guard_passed": True,
        "notification_plan_created_event_found": True,
        "analysis_loaded": True,
        "judge_output_loaded": True,
        "candidate_group_loaded": True,
        "primary_artifact_loaded": True,
        "notification_plan_concretized": True,
        "notification_render_created": True,
        "render_length_within_limit": True,
        "render_hash_stable": True,
        "dry_run_delivery_record_created": True,
        "notification_state_transition_recorded": True,
        "notification_delivery_result_event_created": True,
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
