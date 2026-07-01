from __future__ import annotations

import json
from uuid import UUID, uuid4

import pytest

from services.notifier_telegram.main import (
    NOTIFICATION_UX_RENDER_PREVIEW_SCHEMA_VERSION,
    _load_notification_ux_preview_config,
    _run_notification_ux_render_preview_command,
    build_parser,
    run_notification_ux_render_preview_with_repository,
)
from services.notifier_telegram.models import (
    AnalysisRenderContext,
    CandidateRenderContext,
    JudgeOutputRenderContext,
    NotificationPlanDraft,
)
from tests.component.services.notifier_telegram._fakes import FakeRepository


PREVIEW_RUNTIME_ENV_KEYS = (
    "DATABASE_URL",
    "DATABASE_URL_FILE",
    "REDIS_URL",
    "REDIS_URL_FILE",
    "TELEGRAM_BOT_TOKEN",
    "TELEGRAM_BOT_TOKEN_FILE",
    "NOTIFIER_TELEGRAM_MAX_MESSAGE_CHARS",
)


def _clear_preview_runtime_env(monkeypatch: pytest.MonkeyPatch) -> None:
    for key in PREVIEW_RUNTIME_ENV_KEYS:
        monkeypatch.delenv(key, raising=False)


def test_parser_rejects_both_notification_plan_id_and_latest_selection() -> None:
    with pytest.raises(SystemExit) as exc:
        build_parser().parse_args(
            [
                "notification-ux-render-preview",
                "--notification-plan-id",
                str(uuid4()),
                "--select-latest-eligible",
                "--format",
                "json",
            ]
        )

    assert exc.value.code == 2


def test_parser_rejects_missing_notification_plan_selector() -> None:
    with pytest.raises(SystemExit) as exc:
        build_parser().parse_args(["notification-ux-render-preview", "--format", "json"])

    assert exc.value.code == 2


@pytest.mark.asyncio
async def test_command_rejects_invalid_notification_plan_id() -> None:
    emitted: list[str] = []
    args = build_parser().parse_args(
        [
            "notification-ux-render-preview",
            "--notification-plan-id",
            "not-a-uuid",
            "--format",
            "json",
        ]
    )

    code = await _run_notification_ux_render_preview_command(args, emit_json=emitted.append)
    payload = json.loads(emitted[0])

    assert code == 2
    assert payload["schema_version"] == NOTIFICATION_UX_RENDER_PREVIEW_SCHEMA_VERSION
    assert payload["status"] == "fail"
    assert payload["reason_code"] == "invalid_notification_plan_id"


@pytest.mark.asyncio
async def test_command_returns_fail_when_plan_missing() -> None:
    repository = FakeRepository()
    emitted: list[str] = []

    code = await run_notification_ux_render_preview_with_repository(uuid4(), repository, emit_json=emitted.append)
    payload = json.loads(emitted[0])

    assert code == 1
    assert payload["status"] == "fail"
    assert payload["reason_code"] == "notification_plan_missing"
    assert payload["checks"]["plan_found"] is False
    assert payload["authority"]["db_read_attempted"] is True
    assert payload["authority"]["db_write_attempted"] is False
    assert repository.operations == []


@pytest.mark.asyncio
async def test_preview_passes_for_later_normal_silent_github_button_without_raw_url_output() -> None:
    primary_url = "https://github.com/example/repo"
    repository, plan_id = _preview_repository(
        verdict="later",
        urgency_profile="normal_silent",
        primary_url=primary_url,
        primary_artifact_type="github_repo",
    )
    emitted: list[str] = []

    code = await run_notification_ux_render_preview_with_repository(plan_id, repository, emit_json=emitted.append)
    payload = json.loads(emitted[0])
    serialized = json.dumps(payload, ensure_ascii=False)

    assert code == 0
    assert payload["status"] == "pass"
    assert payload["checks_failed"] == []
    assert payload["checks"]["verdict_visible_in_first_three_lines"] is True
    assert payload["checks"]["korean_summary_marker_present"] is True
    assert payload["checks"]["skeptical_or_risk_marker_present"] is True
    assert payload["checks"]["recommended_action_marker_present"] is True
    assert payload["checks"]["link_preview_disabled"] is True
    assert payload["checks"]["protect_content_false"] is True
    assert payload["checks"]["primary_url_not_in_message_text_when_button_exists"] is True
    assert payload["render_summary"]["disable_notification"] is True
    assert payload["render_summary"]["protect_content"] is False
    assert payload["render_summary"]["link_preview_disabled"] is True
    assert "GitHub 열기" in payload["render_summary"]["button_labels"]
    assert primary_url not in serialized
    assert "raw source text" not in serialized
    assert payload["authority"]["redis_attempted"] is False
    assert payload["authority"]["telegram_transport_attempted"] is False
    assert payload["authority"]["openai_called"] is False
    assert payload["authority"]["github_called"] is False
    assert payload["authority"]["x_called"] is False
    assert payload["authority"]["web_called"] is False
    assert payload["authority"]["docker_or_systemd_called"] is False
    assert payload["authority"]["alembic_or_ddl_ran"] is False
    assert payload["authority"]["db_write_attempted"] is False
    assert repository.operations == []


@pytest.mark.asyncio
async def test_select_latest_eligible_preview_passes_without_serializing_selected_uuid(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _clear_preview_runtime_env(monkeypatch)
    monkeypatch.setenv("DATABASE_URL", "postgresql+psycopg" + "://unit/db")
    repository = LatestEligibleFakeRepository()
    repository, plan_id = _preview_repository(repository=repository)
    emitted: list[str] = []
    args = build_parser().parse_args(
        [
            "notification-ux-render-preview",
            "--select-latest-eligible",
            "--format",
            "json",
        ]
    )

    code = await _run_notification_ux_render_preview_command(
        args,
        emit_json=emitted.append,
        session_factory_builder=_fake_session_factory_builder,
        repository_builder=lambda session: repository,
    )
    payload = json.loads(emitted[0])
    serialized = json.dumps(payload, ensure_ascii=False)

    assert code == 0
    assert repository.latest_selection_calls == 1
    assert payload["status"] == "pass"
    assert payload["selection"] == {
        "mode": "latest_eligible",
        "selected": True,
        "raw_id_printed": False,
    }
    assert str(plan_id) not in serialized
    assert payload["runtime_config_locator"]["process_env_checked"] is True
    assert payload["runtime_config_locator"]["process_env_used"] is True
    assert payload["runtime_config_locator"]["values_printed"] is False
    assert payload["authority"]["db_read_attempted"] is True
    assert payload["authority"]["db_write_attempted"] is False
    assert payload["authority"]["redis_attempted"] is False
    assert payload["authority"]["telegram_transport_attempted"] is False
    assert payload["authority"]["openai_called"] is False
    assert payload["authority"]["github_called"] is False
    assert payload["authority"]["x_called"] is False
    assert payload["authority"]["web_called"] is False
    assert payload["authority"]["docker_or_systemd_called"] is False
    assert payload["authority"]["alembic_or_ddl_ran"] is False


@pytest.mark.asyncio
async def test_select_latest_eligible_returns_sanitized_failure_when_none_found(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _clear_preview_runtime_env(monkeypatch)
    monkeypatch.setenv("DATABASE_URL", "postgresql+psycopg" + "://unit/db")
    repository = LatestEligibleFakeRepository()
    _preview_repository(repository=repository, verdict="skip")
    emitted: list[str] = []
    args = build_parser().parse_args(
        [
            "notification-ux-render-preview",
            "--select-latest-eligible",
            "--format",
            "json",
        ]
    )

    code = await _run_notification_ux_render_preview_command(
        args,
        emit_json=emitted.append,
        session_factory_builder=_fake_session_factory_builder,
        repository_builder=lambda session: repository,
    )
    payload = json.loads(emitted[0])
    serialized = json.dumps(payload, ensure_ascii=False)

    assert code == 1
    assert payload["status"] == "fail"
    assert payload["reason_code"] == "no_eligible_notification_plan_found"
    assert payload["selection"]["selected"] is False
    for plan_id in repository.plans:
        assert str(plan_id) not in serialized


def test_db_only_preview_config_loads_from_process_env_without_transport_or_redis(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _clear_preview_runtime_env(monkeypatch)
    db_url = "postgresql+psycopg" + "://unit/db"
    monkeypatch.setenv("DATABASE_URL", db_url)
    monkeypatch.delenv("REDIS_URL", raising=False)
    monkeypatch.delenv("TELEGRAM_BOT_TOKEN", raising=False)
    args = build_parser().parse_args(
        [
            "notification-ux-render-preview",
            "--select-latest-eligible",
            "--format",
            "json",
        ]
    )

    result = _load_notification_ux_preview_config(args)

    assert result.config.database_url == db_url
    assert result.config.max_message_chars == 3800
    assert result.locator["process_env_used"] is True
    assert result.locator["values_printed"] is False


def test_db_config_discovery_finds_bounded_candidate_file_without_serializing_path_or_value(tmp_path) -> None:
    db_url = "postgresql+psycopg" + "://unit/discovered"
    env_file = tmp_path / "nested" / "github_ai_catchbot.runtime.env"
    env_file.parent.mkdir()
    env_file.write_text(
        "\n".join(
            [
                "IGNORED_SECRET=do-not-read",
                "DATABASE_URL=" + db_url,
                "NOTIFIER_TELEGRAM_MAX_MESSAGE_CHARS=1200",
            ]
        ),
        encoding="utf-8",
    )
    args = build_parser().parse_args(
        [
            "notification-ux-render-preview",
            "--select-latest-eligible",
            "--discover-db-config",
            "--format",
            "json",
        ]
    )
    args._notification_ux_preview_process_env = {}
    args._notification_ux_preview_discovery_roots = [tmp_path]

    result = _load_notification_ux_preview_config(args)
    serialized_locator = json.dumps(result.locator, ensure_ascii=False)

    assert result.config.database_url == db_url
    assert result.config.max_message_chars == 1200
    assert result.locator["bounded_candidate_file_count"] == 1
    assert result.locator["bounded_candidate_files_with_database_key_count"] == 1
    assert result.locator["paths_printed"] is False
    assert result.locator["values_printed"] is False
    assert str(env_file) not in serialized_locator
    assert db_url not in serialized_locator


@pytest.mark.asyncio
async def test_discovery_failure_returns_sanitized_no_db_config_without_db_read(tmp_path) -> None:
    emitted: list[str] = []
    args = build_parser().parse_args(
        [
            "notification-ux-render-preview",
            "--select-latest-eligible",
            "--discover-db-config",
            "--format",
            "json",
        ]
    )
    args._notification_ux_preview_process_env = {}
    args._notification_ux_preview_discovery_roots = [tmp_path]

    code = await _run_notification_ux_render_preview_command(args, emit_json=emitted.append)
    payload = json.loads(emitted[0])

    assert code == 1
    assert payload["status"] == "fail"
    assert payload["reason_code"] == "runtime_database_config_not_found"
    assert payload["authority"]["db_read_attempted"] is False
    assert payload["runtime_config_locator"]["bounded_candidate_file_count"] == 0
    assert payload["runtime_config_locator"]["bounded_candidate_files_with_database_key_count"] == 0
    assert payload["runtime_config_locator"]["paths_printed"] is False
    assert payload["runtime_config_locator"]["values_printed"] is False


@pytest.mark.asyncio
async def test_preview_command_sanitizes_ids_urls_db_paths_source_text_and_secret_words(tmp_path) -> None:
    db_url = "postgresql+psycopg" + "://user:pass" + "word@example/db"
    env_file = tmp_path / "runtime.env"
    env_file.write_text("DATABASE_URL=" + db_url, encoding="utf-8")
    raw_url = "https://github.com/example/private-repo"
    raw_source_text = "raw source text with secret token should not be emitted"
    repository = LatestEligibleFakeRepository()
    repository, plan_id = _preview_repository(
        repository=repository,
        primary_url=raw_url,
        source_text_surface=raw_source_text,
    )
    emitted: list[str] = []
    args = build_parser().parse_args(
        [
            "notification-ux-render-preview",
            "--select-latest-eligible",
            "--env-file",
            str(env_file),
            "--format",
            "json",
        ]
    )

    code = await _run_notification_ux_render_preview_command(
        args,
        emit_json=emitted.append,
        session_factory_builder=_fake_session_factory_builder,
        repository_builder=lambda session: repository,
    )
    output = emitted[0]
    payload = json.loads(output)

    assert code == 0
    assert payload["status"] == "pass"
    assert str(plan_id) not in output
    assert raw_url not in output
    assert db_url not in output
    assert str(env_file) not in output
    assert raw_source_text not in output
    for forbidden in ("DATABASE_URL", "TELEGRAM_BOT_TOKEN", "OPENAI_API_KEY", "secret", "token", "pass" + "word"):
        assert forbidden not in output


@pytest.mark.asyncio
async def test_preview_high_urgency_is_not_silent() -> None:
    repository, plan_id = _preview_repository(verdict="inspect_now", urgency_profile="high")
    emitted: list[str] = []

    code = await run_notification_ux_render_preview_with_repository(plan_id, repository, emit_json=emitted.append)
    payload = json.loads(emitted[0])

    assert code == 0
    assert payload["status"] == "pass"
    assert payload["checks"]["high_profile_not_silent"] is True
    assert payload["render_summary"]["disable_notification"] is False


@pytest.mark.asyncio
async def test_preview_sanitizes_uuid_and_raw_urls_from_readback_lines() -> None:
    raw_uuid = str(uuid4())
    raw_url = "https://example.com/private/path?token=do-not-print"
    repository, plan_id = _preview_repository(
        summary=f"검토 대상 {raw_url}",
        skeptical=f"추적 ID {raw_uuid} 는 노출하지 않습니다.",
    )
    emitted: list[str] = []

    code = await run_notification_ux_render_preview_with_repository(plan_id, repository, emit_json=emitted.append)
    payload = json.loads(emitted[0])
    serialized = json.dumps(payload, ensure_ascii=False)

    assert code == 1
    assert payload["status"] == "fail"
    assert payload["checks"]["no_url_in_message_text"] is False
    assert "no_url_in_message_text" in payload["checks_failed"]
    assert raw_url not in serialized
    assert raw_uuid not in serialized
    assert "[redacted-url]" in serialized
    assert "[redacted-id]" in serialized


@pytest.mark.asyncio
async def test_preview_source_link_button_label_without_source_url_output() -> None:
    source_url = "https://t.me/c/123/456"
    repository, plan_id = _preview_repository(
        primary_url=None,
        primary_artifact_type=None,
        source_message_link=source_url,
    )
    emitted: list[str] = []

    code = await run_notification_ux_render_preview_with_repository(plan_id, repository, emit_json=emitted.append)
    payload = json.loads(emitted[0])
    serialized = json.dumps(payload, ensure_ascii=False)

    assert code == 0
    assert payload["status"] == "pass"
    assert "원문 Telegram" in payload["render_summary"]["button_labels"]
    assert payload["checks"]["source_url_not_in_message_text_when_button_exists"] is True
    assert source_url not in serialized
    assert repository.operations == []


def _preview_repository(
    *,
    repository: FakeRepository | None = None,
    verdict: str = "later",
    urgency_profile: str = "normal_silent",
    primary_url: str | None = "https://github.com/example/repo",
    primary_artifact_type: str | None = "github_repo",
    source_message_link: str | None = None,
    source_text_surface: str = "raw source text should not be emitted",
    summary: str = "로컬 개발 워크플로우를 줄이는 GitHub 도구입니다.",
    skeptical: str = "유지보수와 실제 사용 흔적은 추가 확인이 필요합니다.",
) -> tuple[FakeRepository, UUID]:
    repository = repository or FakeRepository()
    plan_id = uuid4()
    analysis_id = uuid4()
    candidate_group_id = uuid4()
    judge_output_id = uuid4()
    repository.plans[plan_id] = NotificationPlanDraft(
        notification_plan_id=plan_id,
        analysis_id=analysis_id,
        candidate_group_id=candidate_group_id,
        delivery_decision="send_now",
        urgency_profile=urgency_profile,
        target_chat_id=12345,
        target_thread_id=None,
        render_profile="telegram_single_alert_v1",
        dedupe_subject_key="preview-dedupe",
        material_change_hash="preview-material",
        send_after=None,
        suppress_reason_code=None,
        status="planned",
    )
    repository.analyses[analysis_id] = AnalysisRenderContext(
        analysis_id=analysis_id,
        candidate_group_id=candidate_group_id,
        judge_output_id=judge_output_id,
        verdict=verdict,
        delivery_decision="send_now",
        reason_codes_json=["operator_review_recommended"],
        evidence_limitations_ko="공개 증거만 확인했습니다.",
        recommended_action_ko="README와 최근 커밋을 먼저 확인하세요.",
        freshness_note_ko="최근 공개 신호 기준입니다.",
    )
    repository.judge_outputs[judge_output_id] = JudgeOutputRenderContext(
        judge_output_id=judge_output_id,
        payload_json={
            "headline": "Useful repo",
            "summary_one_line_ko": summary,
            "skeptical_take_ko": skeptical,
            "why_it_might_matter_ko": "반복적인 개발 도구 검토 시간을 줄일 수 있습니다.",
        },
        model_confidence_band="medium",
    )
    repository.candidates[candidate_group_id] = CandidateRenderContext(
        candidate_group_id=candidate_group_id,
        source_message_id=uuid4(),
        current_primary_artifact_id=uuid4(),
        primary_artifact_type=primary_artifact_type,
        primary_canonical_url=primary_url,
        primary_canonical_id="github.com/example/repo" if primary_url else None,
        source_message_link=source_message_link,
        source_text_surface=source_text_surface,
    )
    return repository, plan_id


class LatestEligibleFakeRepository(FakeRepository):
    def __init__(self) -> None:
        super().__init__()
        self.latest_selection_calls = 0

    async def load_latest_eligible_notification_plan(self):
        self.latest_selection_calls += 1
        eligible: list[NotificationPlanDraft] = []
        for plan in self.plans.values():
            if plan.delivery_decision != "send_now":
                continue
            if plan.urgency_profile not in {"normal_silent", "high"}:
                continue
            analysis = self.analyses.get(plan.analysis_id)
            if analysis is None or analysis.verdict not in {"later", "inspect_now"}:
                continue
            if analysis.candidate_group_id != plan.candidate_group_id:
                continue
            if analysis.judge_output_id not in self.judge_outputs:
                continue
            if plan.candidate_group_id not in self.candidates:
                continue
            eligible.append(plan)
        if not eligible:
            return None

        def sort_key(plan: NotificationPlanDraft):
            status_rank = {"sent": 0, "edited": 0, "rendered": 1, "queued": 1, "planned": 2}.get(plan.status, 3)
            return status_rank, str(plan.notification_plan_id)

        selected = sorted(eligible, key=sort_key)[0]
        return await self.load_notification_plan(selected.notification_plan_id)


class _FakeSessionFactory:
    def begin(self):
        return _FakeSessionContext()


class _FakeSessionContext:
    async def __aenter__(self):
        return object()

    async def __aexit__(self, exc_type, exc, tb):
        return False


def _fake_session_factory_builder(database_url: str):
    assert database_url

    async def dispose() -> None:
        return None

    return _FakeSessionFactory(), dispose
