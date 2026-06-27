from __future__ import annotations

import json
import inspect
import re
from dataclasses import asdict
from uuid import UUID, uuid4

import pytest

from services.analysis_validator.config import AnalysisValidatorConfig
from services.maintenance.post_judge_notification_pipeline_inventory import (
    JUDGE_OUTPUT_SELECTION_CONFIRM_TOKEN,
    MACRO_CONFIRM_TOKEN,
    NOTIFICATION_INTENT_SELECTION_CONFIRM_TOKEN,
    NOTIFIER_CONFIRM_TOKEN,
    POLICY_APPLY_SELECTION_CONFIRM_TOKEN,
    POLICY_CONFIRM_TOKEN,
    STRICT_UUID_TEXT_SQL_RE,
    VALIDATOR_CONFIRM_TOKEN,
    InventoryCounts,
    NotificationProofReadback,
    PolicyReadback,
    PostJudgeNotificationPipelineInventoryComponents,
    PostJudgeNotificationPipelineInventoryRequest,
    RuntimeConfigBundle,
    SelectedNotificationIntentTarget,
    SelectedTarget,
    SqlPostJudgeNotificationPipelineInventoryRepository,
    ValidatorReadback,
    _ELIGIBLE_POLICY_SQL,
    _ELIGIBLE_READY_SQL,
    _SEND_WORTHY_NOTIFICATION_INTENT_SQL,
    _fingerprint,
    run_cli,
    run_post_judge_notification_pipeline_inventory,
)
from services.notifier_telegram.config import NotifierTelegramConfig
from services.policy_engine.config import PolicyEngineConfig


SENSITIVE_TEXT_SENTINEL = "sensitive text sentinel must stay hidden"
SENSITIVE_URL_SENTINEL = "sensitive url sentinel must stay hidden"
SENSITIVE_CHAT_LOCATOR_SENTINEL = "sensitive chat locator sentinel must stay hidden"
SENSITIVE_MESSAGE_LOCATOR_SENTINEL = "sensitive message locator sentinel must stay hidden"
SENSITIVE_DB_LOCATOR_SENTINEL = "sensitive database locator sentinel must stay hidden"
SENSITIVE_REDIS_LOCATOR_SENTINEL = "sensitive redis locator sentinel must stay hidden"
SENSITIVE_EXCEPTION_SENTINEL = "sensitive exception sentinel must stay hidden"
SENSITIVE_STDERR_SENTINEL = "sensitive stderr sentinel must stay hidden"
SENSITIVE_DEDUPE_SENTINEL = "sensitive dedupe sentinel must stay hidden"


class FakeRepository:
    def __init__(
        self,
        *,
        counts: InventoryCounts | None = None,
        selected_ready: SelectedTarget | None = None,
        selected_policy: SelectedTarget | None = None,
        selected_notification: SelectedNotificationIntentTarget | None = None,
        validator_readback: ValidatorReadback | None = None,
        policy_readback: PolicyReadback | None = None,
        notification_readbacks: list[NotificationProofReadback] | None = None,
    ) -> None:
        self.counts = counts or InventoryCounts()
        self.selected_ready = selected_ready
        self.selected_policy = selected_policy
        self.selected_notification = selected_notification
        self.validator_readback = validator_readback or ValidatorReadback()
        self.policy_readback = policy_readback or PolicyReadback()
        self.notification_readbacks = list(notification_readbacks or [NotificationProofReadback()])
        self.inventory_calls = 0
        self.ready_select_calls = 0
        self.policy_select_calls = 0
        self.notification_select_calls = 0
        self.validator_readback_calls = 0
        self.policy_readback_calls = 0
        self.notification_readback_calls = 0
        self.raw_values = [
            SENSITIVE_TEXT_SENTINEL,
            SENSITIVE_URL_SENTINEL,
            SENSITIVE_CHAT_LOCATOR_SENTINEL,
            SENSITIVE_MESSAGE_LOCATOR_SENTINEL,
            SENSITIVE_DB_LOCATOR_SENTINEL,
            SENSITIVE_REDIS_LOCATOR_SENTINEL,
            SENSITIVE_EXCEPTION_SENTINEL,
            SENSITIVE_STDERR_SENTINEL,
            SENSITIVE_DEDUPE_SENTINEL,
        ]

    async def load_inventory_counts(
        self,
        *,
        lookback_hours: int,
        sample_limit: int,
        policy_version: str,
        delivery_policy_version: str,
    ) -> InventoryCounts:
        assert 1 <= lookback_hours <= 720
        assert 1 <= sample_limit <= 500
        assert policy_version == "verdict_policy_v1"
        assert delivery_policy_version == "delivery_policy_v1"
        self.inventory_calls += 1
        return self.counts

    async def select_latest_eligible_judge_output_ready(
        self,
        *,
        lookback_hours: int,
        sample_limit: int,
        policy_version: str,
        delivery_policy_version: str,
    ) -> SelectedTarget | None:
        assert 1 <= lookback_hours <= 720
        assert 1 <= sample_limit <= 500
        assert policy_version == "verdict_policy_v1"
        assert delivery_policy_version == "delivery_policy_v1"
        self.ready_select_calls += 1
        return self.selected_ready

    async def select_latest_eligible_policy_apply(
        self,
        *,
        lookback_hours: int,
        sample_limit: int,
        policy_version: str,
        delivery_policy_version: str,
    ) -> SelectedTarget | None:
        assert 1 <= lookback_hours <= 720
        assert 1 <= sample_limit <= 500
        assert policy_version == "verdict_policy_v1"
        assert delivery_policy_version == "delivery_policy_v1"
        self.policy_select_calls += 1
        return self.selected_policy

    async def select_latest_send_worthy_notification_intent(
        self,
        *,
        lookback_hours: int,
        sample_limit: int,
        policy_version: str,
        delivery_policy_version: str,
    ) -> SelectedNotificationIntentTarget | None:
        assert 1 <= lookback_hours <= 720
        assert 1 <= sample_limit <= 500
        assert policy_version == "verdict_policy_v1"
        assert delivery_policy_version == "delivery_policy_v1"
        self.notification_select_calls += 1
        return self.selected_notification

    async def load_validator_readback(
        self,
        *,
        judge_run_id: UUID,
        judge_output_id: UUID,
        policy_version: str,
        delivery_policy_version: str,
    ) -> ValidatorReadback:
        assert judge_run_id
        assert judge_output_id
        assert policy_version == "verdict_policy_v1"
        assert delivery_policy_version == "delivery_policy_v1"
        self.validator_readback_calls += 1
        return self.validator_readback

    async def load_policy_readback(
        self,
        *,
        judge_output_id: UUID,
        policy_version: str,
        delivery_policy_version: str,
    ) -> PolicyReadback:
        assert judge_output_id
        assert policy_version == "verdict_policy_v1"
        assert delivery_policy_version == "delivery_policy_v1"
        self.policy_readback_calls += 1
        return self.policy_readback

    async def load_notification_proof_readback(
        self,
        *,
        notification_intent_event_id: UUID,
        analysis_id: UUID,
        notification_plan_id: UUID,
    ) -> NotificationProofReadback:
        assert notification_intent_event_id
        assert analysis_id
        assert notification_plan_id
        self.notification_readback_calls += 1
        if len(self.notification_readbacks) > 1:
            return self.notification_readbacks.pop(0)
        return self.notification_readbacks[0]


class FakeTriggerService:
    def __init__(self) -> None:
        self.calls: list[UUID] = []

    async def handle_trigger_event(self, trigger_event_id: str | UUID) -> None:
        self.calls.append(UUID(str(trigger_event_id)))


class FakeNotifierService(FakeTriggerService):
    pass


class CommitRecorder:
    def __init__(self) -> None:
        self.calls = 0

    async def __call__(self) -> None:
        self.calls += 1


class _SingleMappingResult:
    def __init__(self, row: dict[str, object]) -> None:
        self._row = row

    def mappings(self) -> "_SingleMappingResult":
        return self

    def one(self) -> dict[str, object]:
        return self._row


class _CapturingSqlSession:
    def __init__(self) -> None:
        self.queries: list[str] = []

    async def execute(self, statement: object, params: dict[str, object]) -> _SingleMappingResult:
        assert params["lookback_hours"] == 72
        assert params["policy_version"] == "verdict_policy_v1"
        assert params["delivery_policy_version"] == "delivery_policy_v1"
        self.queries.append(str(statement))
        return _SingleMappingResult({"materialized_count": 0})


def _validator_config() -> AnalysisValidatorConfig:
    return AnalysisValidatorConfig(
        app_env="test",
        database_url=SENSITIVE_DB_LOCATOR_SENTINEL,
        redis_url="redis_locator_not_attempted",
        queue_name="q.analysis.validate",
        consumer_group="analysis-validator",
        consumer_name="test",
        batch_size=1,
        block_ms=1,
        max_headline_chars=200,
        max_summary_chars=1200,
        max_text_items=10,
        log_level="INFO",
    )


def _policy_config() -> PolicyEngineConfig:
    return PolicyEngineConfig(
        app_env="test",
        database_url=SENSITIVE_DB_LOCATOR_SENTINEL,
        redis_url="redis_locator_not_attempted",
        queue_name="q.analysis.policy",
        consumer_group="policy-engine",
        consumer_name="test",
        batch_size=1,
        block_ms=1,
        policy_version="verdict_policy_v1",
        delivery_policy_version="delivery_policy_v1",
        operator_chat_id=0,
        enable_later_delivery=True,
        enable_silent_later=True,
        enable_notification_send=False,
        render_profile_high="telegram_single_alert_high_v1",
        render_profile_normal="telegram_single_alert_normal_v1",
        log_level="INFO",
    )


def _notifier_config() -> NotifierTelegramConfig:
    return NotifierTelegramConfig(
        app_env="test",
        database_url=SENSITIVE_DB_LOCATOR_SENTINEL,
        redis_url="redis_locator_not_attempted",
        telegram_bot_token="",
        queue_name="q.notification.send",
        consumer_group="notifier-telegram",
        consumer_name="test",
        batch_size=1,
        block_ms=1,
        dry_run=True,
        allow_edits=False,
        enable_notification_send=False,
        enable_digest_runtime=False,
        max_message_chars=3800,
        edit_window_minutes=180,
        telegram_api_base_url="https://api.telegram.org",
        request_timeout_sec=1.0,
        log_level="INFO",
    )


def _runtime_bundle() -> RuntimeConfigBundle:
    return RuntimeConfigBundle(
        database_url=SENSITIVE_DB_LOCATOR_SENTINEL,
        values={},
        validator_config=_validator_config(),
        policy_config=_policy_config(),
        notifier_config=_notifier_config(),
    )


def _components(
    repository: FakeRepository,
    *,
    validator: FakeTriggerService | None = None,
    policy: FakeTriggerService | None = None,
    notifier: FakeNotifierService | None = None,
    commit: CommitRecorder | None = None,
) -> tuple[
    PostJudgeNotificationPipelineInventoryComponents,
    FakeTriggerService,
    FakeTriggerService,
    FakeNotifierService,
    CommitRecorder,
]:
    validator_service = validator or FakeTriggerService()
    policy_service = policy or FakeTriggerService()
    notifier_service = notifier or FakeNotifierService()
    commit_recorder = commit or CommitRecorder()
    return (
        PostJudgeNotificationPipelineInventoryComponents(
            inventory_repository=repository,
            validator_service=validator_service,
            policy_service=policy_service,
            notifier_service=notifier_service,
            commit_active_transaction=commit_recorder,
        ),
        validator_service,
        policy_service,
        notifier_service,
        commit_recorder,
    )


def _target() -> SelectedTarget:
    return SelectedTarget(
        event_id=uuid4(),
        judge_run_id=uuid4(),
        judge_output_id=uuid4(),
        candidate_group_id=uuid4(),
        bundle_id=uuid4(),
    )


def _notification_target(
    *,
    delivery_decision: str = "send_now",
    judge_output_id: UUID | None = None,
    candidate_group_id: UUID | None = None,
) -> SelectedNotificationIntentTarget:
    return SelectedNotificationIntentTarget(
        event_id=uuid4(),
        analysis_id=uuid4(),
        notification_plan_id=uuid4(),
        judge_output_id=judge_output_id or uuid4(),
        candidate_group_id=candidate_group_id or uuid4(),
        verdict="inspect_now",
        delivery_decision=delivery_decision,
    )


def _counts(**overrides: object) -> InventoryCounts:
    values = {
        "judge_call_requested_v1_count": 1,
        "judge_run_status_counts": {
            "pending": 0,
            "running": 0,
            "succeeded": 1,
            "failed_retryable": 0,
            "failed_terminal": 0,
            "other": 0,
        },
        "retryable_finish_reason_counts": [],
        "judge_outputs_count": 1,
        "judge_output_ready_v1_count": 1,
        "analysis_policy_apply_v1_count": 0,
        "analyses_count": 0,
        "analyses_by_verdict_count": {},
        "analyses_by_delivery_decision_count": {},
        "notification_plan_created_v1_count": 0,
        "notification_plans_by_status_count": {},
        "notification_renders_count": 0,
        "notification_delivery_records_by_delivery_status_count": {},
        "eligible_judge_output_ready_count": 1,
        "eligible_policy_apply_count": 0,
        "policy_apply_already_materialized_count": 0,
        "send_worthy_notification_intent_count": 0,
        "send_worthy_notification_intent_already_materialized_count": 0,
    }
    values.update(overrides)
    return InventoryCounts(**values)  # type: ignore[arg-type]


def test_strict_uuid_payload_predicate_rejects_malformed_36_character_uuid_like_text() -> None:
    malformed_payload_uuid = "------------------------------------"

    assert len(malformed_payload_uuid) == 36
    assert re.fullmatch(STRICT_UUID_TEXT_SQL_RE, malformed_payload_uuid) is None
    assert re.fullmatch(STRICT_UUID_TEXT_SQL_RE, "01234567-89ab-cdef-ABCD-0123456789ab")


def test_old_loose_uuid_predicate_is_absent_from_payload_sql_guards() -> None:
    policy_materialized_sql_source = inspect.getsource(
        SqlPostJudgeNotificationPipelineInventoryRepository._policy_apply_already_materialized_count
    )
    sql_surfaces = [
        _ELIGIBLE_READY_SQL,
        _ELIGIBLE_POLICY_SQL,
        _SEND_WORTHY_NOTIFICATION_INTENT_SQL,
        policy_materialized_sql_source,
    ]

    for sql in sql_surfaces:
        assert "^[0-9a-f-]{36}$" not in sql
        assert "~* '^[0-9a-f-" not in sql


@pytest.mark.asyncio
async def test_policy_materialized_count_runtime_sql_uses_strict_uuid_payload_guards() -> None:
    session = _CapturingSqlSession()
    repository = SqlPostJudgeNotificationPipelineInventoryRepository(session)

    count = await repository._policy_apply_already_materialized_count(
        lookback_hours=72,
        policy_version="verdict_policy_v1",
        delivery_policy_version="delivery_policy_v1",
    )

    assert count == 0
    assert len(session.queries) == 1
    query = session.queries[0]
    assert query.count(STRICT_UUID_TEXT_SQL_RE) == 2
    assert "^[0-9a-f-]{36}$" not in query
    assert "~* '^[0-9a-f-" not in query


def test_send_worthy_notification_intent_sql_stays_on_existing_intent_and_non_suppress_analysis() -> None:
    assert "event_type = 'notification.plan.created.v1'" in _SEND_WORTHY_NOTIFICATION_INTENT_SQL
    assert "aggregate_type = 'analysis'" in _SEND_WORTHY_NOTIFICATION_INTENT_SQL
    assert "a.delivery_decision::text <> 'suppress'" in _SEND_WORTHY_NOTIFICATION_INTENT_SQL
    assert "notification.delivery.result.v1" in _SEND_WORTHY_NOTIFICATION_INTENT_SQL
    assert "telegram_response_json->>'send_disabled' = 'true'" in _SEND_WORTHY_NOTIFICATION_INTENT_SQL


async def _run(
    request: PostJudgeNotificationPipelineInventoryRequest,
    repository: FakeRepository,
) -> tuple[object, FakeTriggerService, FakeTriggerService, CommitRecorder]:
    components, validator, policy, _notifier, commit = _components(repository)
    report = await run_post_judge_notification_pipeline_inventory(
        request,
        validator_config=_validator_config(),
        policy_config=_policy_config(),
        components=components,
    )
    return report, validator, policy, commit


async def _run_with_notifier(
    request: PostJudgeNotificationPipelineInventoryRequest,
    repository: FakeRepository,
    *,
    notifier: FakeNotifierService | None = None,
    telegram_attempted: bool = False,
) -> tuple[object, FakeTriggerService, FakeTriggerService, FakeNotifierService, CommitRecorder]:
    components, validator, policy, notifier_service, commit = _components(
        repository,
        notifier=notifier,
    )
    components = PostJudgeNotificationPipelineInventoryComponents(
        inventory_repository=components.inventory_repository,
        validator_service=components.validator_service,
        policy_service=components.policy_service,
        commit_active_transaction=components.commit_active_transaction,
        notifier_service=components.notifier_service,
        telegram_transport_attempted=lambda: telegram_attempted,
    )
    report = await run_post_judge_notification_pipeline_inventory(
        request,
        validator_config=_validator_config(),
        policy_config=_policy_config(),
        components=components,
    )
    return report, validator, policy, notifier_service, commit


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("argv", "reason_code"),
    [
        (["--mode", "bad", "--env-file", "/tmp/runtime.env"], "invalid_mode"),
        (["--mode", "plan"], "env_file_required"),
        (["--mode", "plan", "--env-file", "/tmp/runtime.env", "--confirm", "exact-validator-apply"], "confirm_not_allowed_for_plan"),
        (["--mode", "plan", "--env-file", "/tmp/runtime.env", "--lookback-hours", "0"], "lookback_hours_out_of_range"),
        (["--mode", "plan", "--env-file", "/tmp/runtime.env", "--sample-limit", "501"], "sample_limit_out_of_range"),
        (["--mode", "execute-validator", "--env-file", "/tmp/runtime.env"], "exact_validator_apply_confirm_missing"),
        (
            [
                "--mode",
                "execute-validator",
                "--env-file",
                "/tmp/runtime.env",
                "--confirm",
                VALIDATOR_CONFIRM_TOKEN,
                "--select-latest-eligible-judge-output-ready",
            ],
            "judge_output_selection_confirm_missing",
        ),
        (
            [
                "--mode",
                "execute-validator",
                "--env-file",
                "/tmp/runtime.env",
                "--confirm",
                VALIDATOR_CONFIRM_TOKEN,
                "--select-latest-eligible-policy-apply",
            ],
            "policy_selector_not_allowed_for_validator_execute",
        ),
        (
            [
                "--mode",
                "execute-policy",
                "--env-file",
                "/tmp/runtime.env",
                "--confirm",
                POLICY_CONFIRM_TOKEN,
                "--select-latest-eligible-policy-apply",
                "--policy-apply-selection-confirm",
                POLICY_APPLY_SELECTION_CONFIRM_TOKEN,
                "--expected-policy-apply-fingerprint",
                "bad/value",
            ],
            "expected_policy_apply_fingerprint_invalid",
        ),
        (["--mode", "execute-notifier", "--env-file", "/tmp/runtime.env"], "exact_notifier_send_disabled_confirm_missing"),
        (["--mode", "execute-macro", "--env-file", "/tmp/runtime.env"], "macro_send_worthy_confirm_missing"),
        (
            [
                "--mode",
                "execute-macro",
                "--env-file",
                "/tmp/runtime.env",
                "--confirm",
                MACRO_CONFIRM_TOKEN,
            ],
            "macro_exactly_one_selector_required",
        ),
        (
            [
                "--mode",
                "execute-macro",
                "--env-file",
                "/tmp/runtime.env",
                "--confirm",
                MACRO_CONFIRM_TOKEN,
                "--select-latest-eligible-judge-output-ready",
                "--judge-output-selection-confirm",
                JUDGE_OUTPUT_SELECTION_CONFIRM_TOKEN,
            ],
            "expected_judge_output_ready_fingerprint_missing",
        ),
        (
            [
                "--mode",
                "execute-notifier",
                "--env-file",
                "/tmp/runtime.env",
                "--confirm",
                NOTIFIER_CONFIRM_TOKEN,
                "--select-latest-send-worthy-notification-intent",
            ],
            "notification_intent_selection_confirm_missing",
        ),
        (
            [
                "--mode",
                "execute-notifier",
                "--env-file",
                "/tmp/runtime.env",
                "--confirm",
                NOTIFIER_CONFIRM_TOKEN,
                "--select-latest-send-worthy-notification-intent",
                "--notification-intent-selection-confirm",
                NOTIFICATION_INTENT_SELECTION_CONFIRM_TOKEN,
                "--expected-notification-intent-fingerprint",
                "bad/value",
            ],
            "expected_notification_intent_fingerprint_invalid",
        ),
    ],
)
async def test_cli_validation_blocks_before_env_or_db(argv: list[str], reason_code: str) -> None:
    emitted: list[str] = []

    def runtime_loader(_env_file: str) -> RuntimeConfigBundle:
        raise AssertionError("runtime loader must not be called")

    exit_code = await run_cli(argv, emit_json=emitted.append, runtime_config_loader=runtime_loader)

    assert exit_code == 2
    assert json.loads(emitted[0])["reason_code"] == reason_code


@pytest.mark.asyncio
async def test_plan_mode_inventory_is_read_only_and_does_not_call_services_or_commit() -> None:
    repository = FakeRepository(counts=_counts(), selected_ready=None, selected_policy=None)

    report, validator, policy, commit = await _run(
        PostJudgeNotificationPipelineInventoryRequest(mode="plan", lookback_hours=72, sample_limit=100),
        repository,
    )

    assert report.status == "pass"
    assert report.reason_code == "inventory_plan_complete"
    assert report.counts["judge_call_requested_v1_count"] == 1
    assert report.validator_attempted is False
    assert report.policy_attempted is False
    assert validator.calls == []
    assert policy.calls == []
    assert commit.calls == 0
    assert repository.validator_readback_calls == 0
    assert repository.policy_readback_calls == 0
    assert report.redis_attempted is False
    assert report.telegram_attempted is False
    assert report.openai_attempted is False
    assert report.external_network_attempted is False


@pytest.mark.asyncio
async def test_plan_selects_latest_eligible_judge_output_ready_target() -> None:
    selected = _target()
    repository = FakeRepository(counts=_counts(), selected_ready=selected)

    report, validator, policy, commit = await _run(
        PostJudgeNotificationPipelineInventoryRequest(mode="plan", lookback_hours=72, sample_limit=100),
        repository,
    )

    assert report.status == "pass"
    assert report.selected_judge_output_ready_fingerprint == _fingerprint(selected.event_id)
    assert report.selected_judge_run_fingerprint == _fingerprint(selected.judge_run_id)
    assert report.selected_judge_output_fingerprint == _fingerprint(selected.judge_output_id)
    assert report.selected_bundle_fingerprint == _fingerprint(selected.bundle_id)
    assert report.selected_candidate_group_fingerprint == _fingerprint(selected.candidate_group_id)
    assert validator.calls == []
    assert policy.calls == []
    assert commit.calls == 0


@pytest.mark.asyncio
async def test_plan_does_not_select_judge_output_ready_when_policy_event_already_exists() -> None:
    repository = FakeRepository(
        counts=_counts(
            analysis_policy_apply_v1_count=1,
            eligible_judge_output_ready_count=0,
        ),
        selected_ready=None,
    )

    report, validator, policy, commit = await _run(
        PostJudgeNotificationPipelineInventoryRequest(mode="plan", lookback_hours=72, sample_limit=100),
        repository,
    )

    assert report.status == "pass"
    assert report.selected_judge_output_ready_fingerprint is None
    assert report.counts["analysis_policy_apply_v1_count"] == 1
    assert validator.calls == []
    assert policy.calls == []
    assert commit.calls == 0


@pytest.mark.asyncio
async def test_execute_validator_fingerprint_mismatch_blocks_before_writes() -> None:
    selected = _target()
    repository = FakeRepository(counts=_counts(), selected_ready=selected)

    report, validator, policy, commit = await _run(
        PostJudgeNotificationPipelineInventoryRequest(
            mode="execute-validator",
            lookback_hours=72,
            sample_limit=100,
            select_latest_eligible_judge_output_ready=True,
            expected_judge_output_ready_fingerprint="mismatch",
        ),
        repository,
    )

    assert report.status == "blocked"
    assert report.reason_code == "judge_output_ready_fingerprint_mismatch"
    assert report.validator_attempted is False
    assert validator.calls == []
    assert policy.calls == []
    assert commit.calls == 0
    assert repository.validator_readback_calls == 0


@pytest.mark.asyncio
async def test_execute_validator_calls_service_once_and_readback_proves_policy_event() -> None:
    selected = _target()
    policy_event_id = uuid4()
    repository = FakeRepository(
        counts=_counts(),
        selected_ready=selected,
        validator_readback=ValidatorReadback(
            policy_event_count=1,
            policy_event_id=policy_event_id,
            validator_passed_transition_count=1,
            active_analysis_count=0,
        ),
    )

    report, validator, policy, commit = await _run(
        PostJudgeNotificationPipelineInventoryRequest(
            mode="execute-validator",
            lookback_hours=72,
            sample_limit=100,
            select_latest_eligible_judge_output_ready=True,
            expected_judge_output_ready_fingerprint=_fingerprint(selected.event_id),
        ),
        repository,
    )

    assert report.status == "pass"
    assert report.reason_code == "validator_policy_apply_materialized"
    assert report.validator_attempted is True
    assert report.policy_event_created_or_present is True
    assert report.analysis_created_or_present is False
    assert report.selected_policy_apply_fingerprint == _fingerprint(policy_event_id)
    assert validator.calls == [selected.event_id]
    assert policy.calls == []
    assert commit.calls == 1
    assert repository.validator_readback_calls == 1
    assert repository.ready_select_calls == 2


@pytest.mark.asyncio
async def test_plan_selects_latest_eligible_policy_apply_target() -> None:
    selected = _target()
    repository = FakeRepository(
        counts=_counts(
            analysis_policy_apply_v1_count=1,
            eligible_judge_output_ready_count=0,
            eligible_policy_apply_count=1,
        ),
        selected_policy=selected,
    )

    report, validator, policy, commit = await _run(
        PostJudgeNotificationPipelineInventoryRequest(mode="plan", lookback_hours=72, sample_limit=100),
        repository,
    )

    assert report.status == "pass"
    assert report.selected_policy_apply_fingerprint == _fingerprint(selected.event_id)
    assert report.selected_judge_run_fingerprint == _fingerprint(selected.judge_run_id)
    assert report.selected_judge_output_fingerprint == _fingerprint(selected.judge_output_id)
    assert validator.calls == []
    assert policy.calls == []
    assert commit.calls == 0


@pytest.mark.asyncio
async def test_plan_does_not_select_policy_apply_when_analysis_already_exists() -> None:
    repository = FakeRepository(
        counts=_counts(
            analysis_policy_apply_v1_count=1,
            analyses_count=1,
            eligible_policy_apply_count=0,
            policy_apply_already_materialized_count=1,
        ),
        selected_policy=None,
    )

    report, validator, policy, commit = await _run(
        PostJudgeNotificationPipelineInventoryRequest(mode="plan", lookback_hours=72, sample_limit=100),
        repository,
    )

    assert report.status == "pass"
    assert report.selected_policy_apply_fingerprint is None
    assert report.counts["policy_apply_already_materialized_count"] == 1
    assert validator.calls == []
    assert policy.calls == []
    assert commit.calls == 0


@pytest.mark.asyncio
async def test_execute_policy_fingerprint_mismatch_blocks_before_writes() -> None:
    selected = _target()
    repository = FakeRepository(
        counts=_counts(analysis_policy_apply_v1_count=1, eligible_policy_apply_count=1),
        selected_policy=selected,
    )

    report, validator, policy, commit = await _run(
        PostJudgeNotificationPipelineInventoryRequest(
            mode="execute-policy",
            lookback_hours=72,
            sample_limit=100,
            select_latest_eligible_policy_apply=True,
            expected_policy_apply_fingerprint="mismatch",
        ),
        repository,
    )

    assert report.status == "blocked"
    assert report.reason_code == "policy_apply_fingerprint_mismatch"
    assert report.policy_attempted is False
    assert validator.calls == []
    assert policy.calls == []
    assert commit.calls == 0
    assert repository.policy_readback_calls == 0


@pytest.mark.asyncio
async def test_execute_policy_calls_service_once_and_readback_proves_analysis_and_notification_intent() -> None:
    selected = _target()
    repository = FakeRepository(
        counts=_counts(analysis_policy_apply_v1_count=1, eligible_policy_apply_count=1),
        selected_policy=selected,
        policy_readback=PolicyReadback(
            analysis_count=1,
            analysis_id=uuid4(),
            verdict="inspect_now",
            delivery_decision="send_now",
            notification_intent_event_count=1,
            notification_plan_count=0,
        ),
    )

    report, validator, policy, commit = await _run(
        PostJudgeNotificationPipelineInventoryRequest(
            mode="execute-policy",
            lookback_hours=72,
            sample_limit=100,
            select_latest_eligible_policy_apply=True,
            expected_policy_apply_fingerprint=_fingerprint(selected.event_id),
        ),
        repository,
    )

    assert report.status == "pass"
    assert report.reason_code == "policy_analysis_notification_intent_materialized"
    assert report.policy_attempted is True
    assert report.analysis_created_or_present is True
    assert report.notification_intent_created_or_present is True
    assert report.notification_plan_created_or_present is False
    assert validator.calls == []
    assert policy.calls == [selected.event_id]
    assert commit.calls == 1
    assert repository.policy_readback_calls == 1
    assert repository.policy_select_calls == 2


@pytest.mark.asyncio
async def test_execute_policy_suppress_requires_no_notification_intent() -> None:
    selected = _target()
    repository = FakeRepository(
        counts=_counts(analysis_policy_apply_v1_count=1, eligible_policy_apply_count=1),
        selected_policy=selected,
        policy_readback=PolicyReadback(
            analysis_count=1,
            analysis_id=uuid4(),
            verdict="skip",
            delivery_decision="suppress",
            notification_intent_event_count=0,
            notification_plan_count=0,
        ),
    )

    report, validator, policy, commit = await _run(
        PostJudgeNotificationPipelineInventoryRequest(
            mode="execute-policy",
            lookback_hours=72,
            sample_limit=100,
            select_latest_eligible_policy_apply=True,
            expected_policy_apply_fingerprint=_fingerprint(selected.event_id),
        ),
        repository,
    )

    assert report.status == "pass"
    assert report.reason_code == "policy_suppressed_no_notification_intent_required"
    assert report.analysis_created_or_present is True
    assert report.notification_intent_created_or_present is False
    assert policy.calls == [selected.event_id]
    assert commit.calls == 1
    assert validator.calls == []


@pytest.mark.asyncio
async def test_plan_selects_latest_send_worthy_notification_intent_target() -> None:
    selected = _notification_target()
    repository = FakeRepository(
        counts=_counts(
            notification_plan_created_v1_count=1,
            send_worthy_notification_intent_count=1,
        ),
        selected_notification=selected,
    )

    report, validator, policy, commit = await _run(
        PostJudgeNotificationPipelineInventoryRequest(mode="plan", lookback_hours=72, sample_limit=100),
        repository,
    )

    assert report.status == "pass"
    assert report.selected_notification_intent_fingerprint == _fingerprint(selected.event_id)
    assert report.selected_analysis_fingerprint == _fingerprint(selected.analysis_id)
    assert report.selected_notification_plan_fingerprint == _fingerprint(selected.notification_plan_id)
    assert report.selected_judge_output_fingerprint == _fingerprint(selected.judge_output_id)
    assert report.selected_candidate_group_fingerprint == _fingerprint(selected.candidate_group_id)
    assert report.final_verdict == "inspect_now"
    assert report.delivery_decision == "send_now"
    assert validator.calls == []
    assert policy.calls == []
    assert commit.calls == 0


@pytest.mark.asyncio
async def test_execute_notifier_calls_existing_service_once_and_proves_send_disabled_readback() -> None:
    selected = _notification_target()
    repository = FakeRepository(
        counts=_counts(
            notification_plan_created_v1_count=1,
            send_worthy_notification_intent_count=1,
        ),
        selected_notification=selected,
        notification_readbacks=[
            NotificationProofReadback(notification_intent_event_count=1),
            NotificationProofReadback(
                notification_intent_event_count=1,
                notification_plan_count=1,
                notification_render_count=1,
                send_disabled_delivery_record_count=1,
                notification_delivery_result_event_count=1,
            ),
        ],
    )

    report, validator, policy, notifier, commit = await _run_with_notifier(
        PostJudgeNotificationPipelineInventoryRequest(
            mode="execute-notifier",
            lookback_hours=72,
            sample_limit=100,
            select_latest_send_worthy_notification_intent=True,
            expected_notification_intent_fingerprint=_fingerprint(selected.event_id),
        ),
        repository,
    )

    assert report.status == "pass"
    assert report.reason_code == "notification_send_disabled_suppressed"
    assert report.notifier_attempted is True
    assert report.db_write_attempted is True
    assert report.notification_intent_created_or_present is True
    assert report.notification_plan_created_or_present is True
    assert report.notification_render_created_or_present is True
    assert report.send_disabled_delivery_record_created_or_present is True
    assert report.notification_delivery_result_event_created_or_present is True
    assert report.telegram_transport_attempted is False
    assert report.redis_attempted is False
    assert report.openai_attempted is False
    assert report.external_network_attempted is False
    assert validator.calls == []
    assert policy.calls == []
    assert notifier.calls == [selected.event_id]
    assert commit.calls == 1
    assert repository.notification_select_calls == 2
    assert repository.notification_readback_calls == 2


@pytest.mark.asyncio
async def test_execute_notifier_already_materialized_short_circuits_without_duplicate_writes() -> None:
    selected = _notification_target()
    repository = FakeRepository(
        counts=_counts(
            notification_plan_created_v1_count=1,
            send_worthy_notification_intent_count=1,
            send_worthy_notification_intent_already_materialized_count=1,
        ),
        selected_notification=selected,
        notification_readbacks=[
            NotificationProofReadback(
                notification_intent_event_count=1,
                notification_plan_count=1,
                notification_render_count=1,
                send_disabled_delivery_record_count=1,
                notification_delivery_result_event_count=1,
            ),
        ],
    )

    report, _validator, _policy, notifier, commit = await _run_with_notifier(
        PostJudgeNotificationPipelineInventoryRequest(
            mode="execute-notifier",
            lookback_hours=72,
            sample_limit=100,
            select_latest_send_worthy_notification_intent=True,
            expected_notification_intent_fingerprint=_fingerprint(selected.event_id),
        ),
        repository,
    )

    assert report.status == "pass"
    assert report.reason_code == "already_materialized"
    assert report.notifier_attempted is False
    assert report.db_write_attempted is False
    assert report.notification_plan_created_or_present is True
    assert report.notification_render_created_or_present is True
    assert report.send_disabled_delivery_record_created_or_present is True
    assert report.notification_delivery_result_event_created_or_present is True
    assert notifier.calls == []
    assert commit.calls == 0
    assert repository.notification_readback_calls == 1


@pytest.mark.asyncio
async def test_execute_notifier_blocks_when_send_worthy_target_missing() -> None:
    repository = FakeRepository(
        counts=_counts(notification_plan_created_v1_count=1, send_worthy_notification_intent_count=0),
        selected_notification=None,
    )

    report, _validator, _policy, notifier, commit = await _run_with_notifier(
        PostJudgeNotificationPipelineInventoryRequest(
            mode="execute-notifier",
            lookback_hours=72,
            sample_limit=100,
            select_latest_send_worthy_notification_intent=True,
            expected_notification_intent_fingerprint="abc",
        ),
        repository,
    )

    assert report.status == "blocked"
    assert report.reason_code == "send_worthy_target_missing"
    assert report.notifier_attempted is False
    assert notifier.calls == []
    assert commit.calls == 0


@pytest.mark.asyncio
async def test_execute_notifier_transport_attempt_fails_closed() -> None:
    selected = _notification_target()
    repository = FakeRepository(
        counts=_counts(
            notification_plan_created_v1_count=1,
            send_worthy_notification_intent_count=1,
        ),
        selected_notification=selected,
        notification_readbacks=[NotificationProofReadback(notification_intent_event_count=1)],
    )

    report, _validator, _policy, notifier, commit = await _run_with_notifier(
        PostJudgeNotificationPipelineInventoryRequest(
            mode="execute-notifier",
            lookback_hours=72,
            sample_limit=100,
            select_latest_send_worthy_notification_intent=True,
            expected_notification_intent_fingerprint=_fingerprint(selected.event_id),
        ),
        repository,
        telegram_attempted=True,
    )

    assert report.status == "failed"
    assert report.reason_code == "telegram_transport_attempted"
    assert report.telegram_transport_attempted is True
    assert notifier.calls == [selected.event_id]
    assert commit.calls == 1


@pytest.mark.asyncio
async def test_execute_macro_from_ready_target_reuses_existing_services_to_send_disabled_proof() -> None:
    selected_ready = _target()
    selected_policy = SelectedTarget(
        event_id=uuid4(),
        judge_run_id=selected_ready.judge_run_id,
        judge_output_id=selected_ready.judge_output_id,
        candidate_group_id=selected_ready.candidate_group_id,
        bundle_id=selected_ready.bundle_id,
    )
    selected_notification = _notification_target(
        judge_output_id=selected_ready.judge_output_id,
        candidate_group_id=selected_ready.candidate_group_id,
    )
    repository = FakeRepository(
        counts=_counts(
            judge_output_ready_v1_count=1,
            eligible_judge_output_ready_count=1,
            analysis_policy_apply_v1_count=0,
            eligible_policy_apply_count=0,
            notification_plan_created_v1_count=0,
            send_worthy_notification_intent_count=0,
        ),
        selected_ready=selected_ready,
        selected_policy=selected_policy,
        selected_notification=selected_notification,
        validator_readback=ValidatorReadback(
            policy_event_count=1,
            policy_event_id=selected_policy.event_id,
            validator_passed_transition_count=1,
            active_analysis_count=0,
        ),
        policy_readback=PolicyReadback(
            analysis_count=1,
            analysis_id=selected_notification.analysis_id,
            verdict="inspect_now",
            delivery_decision="send_now",
            notification_intent_event_count=1,
            notification_plan_count=0,
        ),
        notification_readbacks=[
            NotificationProofReadback(notification_intent_event_count=1),
            NotificationProofReadback(
                notification_intent_event_count=1,
                notification_plan_count=1,
                notification_render_count=1,
                send_disabled_delivery_record_count=1,
                notification_delivery_result_event_count=1,
            ),
        ],
    )

    report, validator, policy, notifier, commit = await _run_with_notifier(
        PostJudgeNotificationPipelineInventoryRequest(
            mode="execute-macro",
            lookback_hours=72,
            sample_limit=100,
            select_latest_eligible_judge_output_ready=True,
            expected_judge_output_ready_fingerprint=_fingerprint(selected_ready.event_id),
        ),
        repository,
    )

    assert report.status == "pass"
    assert report.reason_code == "notification_send_disabled_suppressed"
    assert report.validator_attempted is True
    assert report.policy_attempted is True
    assert report.notifier_attempted is True
    assert report.policy_event_created_or_present is True
    assert report.analysis_created_or_present is True
    assert report.notification_intent_created_or_present is True
    assert report.notification_plan_created_or_present is True
    assert report.notification_render_created_or_present is True
    assert report.send_disabled_delivery_record_created_or_present is True
    assert report.notification_delivery_result_event_created_or_present is True
    assert report.redis_attempted is False
    assert report.telegram_transport_attempted is False
    assert report.openai_attempted is False
    assert validator.calls == [selected_ready.event_id]
    assert policy.calls == [selected_policy.event_id]
    assert notifier.calls == [selected_notification.event_id]
    assert commit.calls == 3


@pytest.mark.asyncio
async def test_execute_macro_blocks_with_precise_missing_upstream_stage_when_no_target_exists() -> None:
    repository = FakeRepository(
        counts=_counts(
            judge_call_requested_v1_count=0,
            judge_run_status_counts={},
            judge_outputs_count=0,
            judge_output_ready_v1_count=0,
            analysis_policy_apply_v1_count=0,
            eligible_judge_output_ready_count=0,
            eligible_policy_apply_count=0,
            notification_plan_created_v1_count=0,
            send_worthy_notification_intent_count=0,
        ),
        selected_ready=None,
    )

    report, validator, policy, notifier, commit = await _run_with_notifier(
        PostJudgeNotificationPipelineInventoryRequest(
            mode="execute-macro",
            lookback_hours=72,
            sample_limit=100,
            select_latest_eligible_judge_output_ready=True,
            expected_judge_output_ready_fingerprint="abc",
        ),
        repository,
    )

    assert report.status == "blocked"
    assert report.reason_code == "send_worthy_target_requires_exact_source_or_live_openai"
    assert report.nearest_send_worthy_missing_stage == "source_or_live_openai"
    assert validator.calls == []
    assert policy.calls == []
    assert notifier.calls == []
    assert commit.calls == 0


@pytest.mark.asyncio
async def test_redaction_omits_raw_uuid_source_text_urls_chat_message_locators_exception_stderr_and_dedupe() -> None:
    selected = _target()
    repository = FakeRepository(
        counts=_counts(
            retryable_finish_reason_counts=[
                {"reason_code": "openai_retryable_rate_limited", "count": 2},
                {"reason_code": SENSITIVE_URL_SENTINEL + "/", "count": 1},
            ],
        ),
        selected_ready=selected,
    )

    report, validator, policy, commit = await _run(
        PostJudgeNotificationPipelineInventoryRequest(mode="plan", lookback_hours=72, sample_limit=100),
        repository,
    )

    payload = json.dumps(asdict(report), sort_keys=True)
    assert report.status == "pass"
    assert "openai_retryable_rate_limited" in payload
    assert '"reason_code": "other"' in payload
    forbidden = [
        str(selected.event_id),
        str(selected.judge_run_id),
        str(selected.judge_output_id),
        str(selected.candidate_group_id),
        str(selected.bundle_id),
        SENSITIVE_TEXT_SENTINEL,
        SENSITIVE_URL_SENTINEL,
        SENSITIVE_CHAT_LOCATOR_SENTINEL,
        SENSITIVE_MESSAGE_LOCATOR_SENTINEL,
        SENSITIVE_DB_LOCATOR_SENTINEL,
        SENSITIVE_REDIS_LOCATOR_SENTINEL,
        SENSITIVE_EXCEPTION_SENTINEL,
        SENSITIVE_STDERR_SENTINEL,
        SENSITIVE_DEDUPE_SENTINEL,
    ]
    for value in forbidden:
        assert value not in payload
    assert report.redactions_applied is True
    assert validator.calls == []
    assert policy.calls == []
    assert commit.calls == 0


@pytest.mark.asyncio
async def test_retryable_judge_run_rate_limit_counts_are_bucketed_and_safe() -> None:
    repository = FakeRepository(
        counts=_counts(
            judge_run_status_counts={
                "pending": 0,
                "running": 0,
                "succeeded": 0,
                "failed_retryable": 3,
                "failed_terminal": 0,
                "other": 0,
            },
            retryable_finish_reason_counts=[
                {"reason_code": "openai_retryable_rate_limited", "count": 2},
                {"reason_code": "openai_retryable_server_error", "count": 1},
                {"reason_code": SENSITIVE_EXCEPTION_SENTINEL + " with spaces", "count": 1},
            ],
        )
    )

    report, validator, policy, commit = await _run(
        PostJudgeNotificationPipelineInventoryRequest(mode="plan", lookback_hours=72, sample_limit=100),
        repository,
    )

    assert report.counts["judge_run_status_counts"]["failed_retryable"] == 3
    assert report.counts["retryable_finish_reason_counts"] == [
        {"reason_code": "openai_retryable_rate_limited", "count": 2},
        {"reason_code": "openai_retryable_server_error", "count": 1},
        {"reason_code": "other", "count": 1},
    ]
    payload = json.dumps(asdict(report), sort_keys=True)
    assert SENSITIVE_EXCEPTION_SENTINEL not in payload
    assert validator.calls == []
    assert policy.calls == []
    assert commit.calls == 0
