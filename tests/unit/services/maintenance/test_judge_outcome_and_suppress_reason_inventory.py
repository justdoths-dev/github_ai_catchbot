from __future__ import annotations

import json
import re
from dataclasses import asdict, replace
from uuid import uuid4

import pytest

from services.maintenance.judge_outcome_and_suppress_reason_inventory import (
    RETRYABLE_SELECTION_CONFIRM_TOKEN,
    STRICT_UUID_TEXT_SQL_RE,
    InventoryCounts,
    JudgeOutcomeAndSuppressReasonInventoryComponents,
    JudgeOutcomeAndSuppressReasonInventoryRequest,
    RuntimeConfigBundle,
    RetryableJudgeRunCandidate,
    SqlJudgeOutcomeAndSuppressReasonInventoryRepository,
    _FINISH_REASON_COUNTS_SQL,
    _INVENTORY_COUNTS_SQL,
    _RETRYABLE_SELECTION_SQL,
    _fingerprint,
    run_cli,
    run_judge_outcome_and_suppress_reason_inventory,
)


SENSITIVE_TEXT_SENTINEL = "sensitive source text sentinel must stay hidden"
SENSITIVE_URL_SENTINEL = "sensitive url sentinel must stay hidden"
SENSITIVE_DB_LOCATOR_SENTINEL = "sensitive database locator sentinel must stay hidden"
SENSITIVE_REDIS_LOCATOR_SENTINEL = "sensitive redis locator sentinel must stay hidden"
SENSITIVE_CHAT_LOCATOR_SENTINEL = "chat id sentinel must stay hidden"
SENSITIVE_MESSAGE_LOCATOR_SENTINEL = "message id sentinel must stay hidden"
SENSITIVE_DEDUPE_SENTINEL = "dedupe key sentinel must stay hidden"
SENSITIVE_EXCEPTION_SENTINEL = "exception body sentinel must stay hidden"
SENSITIVE_STDERR_SENTINEL = "stderr sentinel must stay hidden"


class FakeRepository:
    def __init__(
        self,
        *,
        counts: InventoryCounts | None = None,
        selected_retryable: RetryableJudgeRunCandidate | None = None,
    ) -> None:
        self.counts = counts or InventoryCounts()
        self.selected_retryable = selected_retryable
        self.inventory_calls = 0
        self.retryable_select_calls = 0
        self.write_calls = 0

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

    async def select_latest_retryable_judge_run(
        self,
        *,
        lookback_hours: int,
        sample_limit: int,
    ) -> RetryableJudgeRunCandidate | None:
        assert 1 <= lookback_hours <= 720
        assert 1 <= sample_limit <= 500
        self.retryable_select_calls += 1
        return self.selected_retryable


def _runtime() -> RuntimeConfigBundle:
    return RuntimeConfigBundle(
        database_url=SENSITIVE_DB_LOCATOR_SENTINEL,
        values={},
        policy_version="verdict_policy_v1",
        delivery_policy_version="delivery_policy_v1",
    )


def _counts(**overrides: object) -> InventoryCounts:
    return replace(InventoryCounts(), **overrides)


def _candidate(
    *,
    finish_reason: str | None = "openai_retryable_rate_limited",
    current_bundle_matches: bool = True,
    judge_output_count: int = 0,
    ready_event_count: int = 0,
    policy_event_count: int = 0,
    analysis_count: int = 0,
    notification_intent_count: int = 0,
    notification_plan_count: int = 0,
) -> RetryableJudgeRunCandidate:
    bundle_id = uuid4()
    return RetryableJudgeRunCandidate(
        judge_run_id=uuid4(),
        judge_call_event_id=uuid4(),
        bundle_id=bundle_id,
        candidate_group_id=uuid4(),
        current_bundle_id=bundle_id if current_bundle_matches else uuid4(),
        finish_reason=finish_reason,
        judge_output_count=judge_output_count,
        ready_event_count=ready_event_count,
        policy_event_count=policy_event_count,
        analysis_count=analysis_count,
        notification_intent_count=notification_intent_count,
        notification_plan_count=notification_plan_count,
    )


async def _run(
    request: JudgeOutcomeAndSuppressReasonInventoryRequest,
    repository: FakeRepository,
):
    return await run_judge_outcome_and_suppress_reason_inventory(
        request,
        runtime=_runtime(),
        components=JudgeOutcomeAndSuppressReasonInventoryComponents(
            inventory_repository=repository,
        ),
    )


def _fail_loader(_env_file: str) -> RuntimeConfigBundle:
    raise AssertionError("runtime config loader must not be called")


@pytest.mark.asyncio
async def test_cli_blocks_invalid_mode_before_env_or_db() -> None:
    emitted: list[str] = []

    exit_code = await run_cli(
        ["--mode", "invalid"],
        emit_json=emitted.append,
        runtime_config_loader=_fail_loader,
    )

    assert exit_code == 2
    assert len(emitted) == 1
    payload = json.loads(emitted[0])
    assert payload["status"] == "blocked"
    assert payload["reason_code"] == "invalid_mode"


@pytest.mark.asyncio
async def test_cli_blocks_confirm_and_execute_like_args_because_runner_is_plan_only() -> None:
    emitted_confirm: list[str] = []
    emitted_execute: list[str] = []

    confirm_exit = await run_cli(
        ["--mode", "plan", "--env-file", "ignored.env", "--confirm", "retry"],
        emit_json=emitted_confirm.append,
        runtime_config_loader=_fail_loader,
    )
    execute_exit = await run_cli(
        ["--mode", "execute", "--env-file", "ignored.env"],
        emit_json=emitted_execute.append,
        runtime_config_loader=_fail_loader,
    )

    assert confirm_exit == 2
    assert json.loads(emitted_confirm[0])["reason_code"] == "execute_argument_not_allowed"
    assert execute_exit == 2
    assert json.loads(emitted_execute[0])["reason_code"] == "execute_mode_not_supported"


@pytest.mark.asyncio
async def test_plan_mode_is_read_only_and_does_not_attempt_services_or_db_write() -> None:
    repository = FakeRepository(counts=_counts(judge_call_requested_v1_count=7))

    report = await _run(
        JudgeOutcomeAndSuppressReasonInventoryRequest(mode="plan", lookback_hours=72, sample_limit=100),
        repository,
    )

    assert report.status == "pass"
    assert report.reason_code == "inventory_plan_complete"
    assert report.counts["judge_call_requested_v1_count"] == 7
    assert repository.inventory_calls == 1
    assert repository.retryable_select_calls == 0
    assert repository.write_calls == 0
    assert report.db_write_attempted is False
    assert report.redis_attempted is False
    assert report.openai_attempted is False
    assert report.telegram_attempted is False
    assert report.external_network_attempted is False


@pytest.mark.asyncio
async def test_inventory_report_includes_judge_analysis_policy_and_suppress_counts() -> None:
    repository = FakeRepository(
        counts=_counts(
            judge_call_requested_v1_count=7,
            judge_run_status_counts={
                "succeeded": 4,
                "failed_retryable": 1,
                "failed_terminal": 2,
            },
            judge_run_finish_reason_counts={
                "failed_retryable": [
                    {"reason_code": "openai_retryable_rate_limited", "count": 1},
                ],
                "failed_terminal": [
                    {"reason_code": "schema_invalid_after_retry", "count": 2},
                ],
                "succeeded": [
                    {"reason_code": "completed", "count": 4},
                ],
            },
            judge_output_count=6,
            judge_outputs_count=6,
            analysis_policy_apply_v1_count=4,
            analysis_policy_apply_already_materialized_count=4,
            validator_passed_transition_count=4,
            validator_terminal_transition_reason_counts=[
                {"reason_code": "validator_missing_github_comparables", "count": 2},
            ],
            analyses_count=4,
            analyses_by_verdict_count={"skip": 4},
            analyses_by_delivery_decision_count={"suppress": 4},
            policy_reason_code_counts=[
                {"reason_code": "policy_threshold_skip", "count": 4},
                {"reason_code": "policy_verdict_skip", "count": 4},
            ],
            suppress_reason_code_counts=[
                {"reason_code": "delivery_decision=suppress", "count": 4},
                {"reason_code": "policy_verdict_skip", "count": 4},
            ],
            notification_plan_created_v1_count=0,
            eligible_judge_output_ready_count=0,
            eligible_policy_apply_count=0,
        )
    )

    report = await _run(
        JudgeOutcomeAndSuppressReasonInventoryRequest(mode="plan", lookback_hours=72, sample_limit=100),
        repository,
    )

    assert report.counts["judge_run_status_counts"]["succeeded"] == 4
    assert report.counts["retryable_finish_reason_counts"] == [
        {"reason_code": "openai_retryable_rate_limited", "count": 1}
    ]
    assert report.counts["terminal_finish_reason_counts"] == [
        {"reason_code": "schema_invalid_after_retry", "count": 2}
    ]
    assert report.counts["analyses_by_verdict_count"] == {"skip": 4}
    assert report.counts["analyses_by_delivery_decision_count"] == {"suppress": 4}
    assert report.counts["policy_reason_code_counts"] == [
        {"reason_code": "policy_threshold_skip", "count": 4},
        {"reason_code": "policy_verdict_skip", "count": 4},
    ]
    assert report.counts["suppress_reason_code_counts"] == [
        {"reason_code": "delivery_decision=suppress", "count": 4},
        {"reason_code": "policy_verdict_skip", "count": 4},
    ]


@pytest.mark.asyncio
async def test_suppress_logic_counts_expected_and_unexpected_notification_absence() -> None:
    repository = FakeRepository(
        counts=_counts(
            analyses_count=5,
            analyses_by_verdict_count={"skip": 4, "inspect_now": 1},
            analyses_by_delivery_decision_count={"suppress": 4, "send_now": 1},
            skip_suppress_analysis_count=4,
            non_suppress_analysis_count=1,
            notification_intent_absence_expected_count=4,
            notification_intent_absence_unexpected_count=1,
            suppress_reason_code_counts=[
                {"reason_code": "delivery_decision=suppress", "count": 4},
                {"reason_code": "policy_verdict_skip", "count": 4},
            ],
        )
    )

    report = await _run(
        JudgeOutcomeAndSuppressReasonInventoryRequest(mode="plan", lookback_hours=72, sample_limit=100),
        repository,
    )

    assert report.counts["skip_suppress_analysis_count"] == 4
    assert report.counts["non_suppress_analysis_count"] == 1
    assert report.counts["notification_intent_absence_expected_count"] == 4
    assert report.counts["notification_intent_absence_unexpected_count"] == 1


@pytest.mark.asyncio
async def test_retryable_selection_selects_latest_candidate_with_fingerprints_only() -> None:
    selected = _candidate()
    repository = FakeRepository(selected_retryable=selected)

    report = await _run(
        JudgeOutcomeAndSuppressReasonInventoryRequest(
            mode="plan",
            lookback_hours=72,
            sample_limit=100,
            select_latest_retryable_judge_run=True,
            retryable_selection_confirm=RETRYABLE_SELECTION_CONFIRM_TOKEN,
        ),
        repository,
    )

    assert report.status == "pass"
    assert repository.retryable_select_calls == 1
    assert report.selected_retryable_judge_run_fingerprint == _fingerprint(selected.judge_run_id)
    assert report.selected_retryable_judge_call_event_fingerprint == _fingerprint(selected.judge_call_event_id)
    assert report.selected_bundle_fingerprint == _fingerprint(selected.bundle_id)
    assert report.selected_candidate_group_fingerprint == _fingerprint(selected.candidate_group_id)
    assert report.selected_retryable_reason_code == "openai_retryable_rate_limited"
    assert report.selected_retry_readiness == "ready_for_operator_openai_retry"
    payload = json.dumps(asdict(report), sort_keys=True)
    for raw_id in (
        selected.judge_run_id,
        selected.judge_call_event_id,
        selected.bundle_id,
        selected.candidate_group_id,
        selected.current_bundle_id,
    ):
        assert str(raw_id) not in payload


@pytest.mark.asyncio
async def test_retryable_selection_blocks_stale_bundle() -> None:
    selected = _candidate(current_bundle_matches=False)
    repository = FakeRepository(selected_retryable=selected)

    report = await _run(
        JudgeOutcomeAndSuppressReasonInventoryRequest(
            mode="plan",
            lookback_hours=72,
            sample_limit=100,
            select_latest_retryable_judge_run=True,
            retryable_selection_confirm=RETRYABLE_SELECTION_CONFIRM_TOKEN,
        ),
        repository,
    )

    assert report.selected_retry_readiness == "blocked_stale_bundle"
    assert report.selected_retryable_judge_run_fingerprint == _fingerprint(selected.judge_run_id)


@pytest.mark.asyncio
async def test_retryable_selection_blocks_downstream_already_exists() -> None:
    selected = _candidate(analysis_count=1, notification_plan_count=1)
    repository = FakeRepository(selected_retryable=selected)

    report = await _run(
        JudgeOutcomeAndSuppressReasonInventoryRequest(
            mode="plan",
            lookback_hours=72,
            sample_limit=100,
            select_latest_retryable_judge_run=True,
            retryable_selection_confirm=RETRYABLE_SELECTION_CONFIRM_TOKEN,
        ),
        repository,
    )

    assert report.selected_retry_readiness == "blocked_downstream_exists"


class _FakeSqlResult:
    def __init__(self, rows):
        self._rows = rows

    def mappings(self):
        return self

    def one(self):
        assert len(self._rows) == 1
        return self._rows[0]

    def all(self):
        return self._rows


class _BindParamCheckingSession:
    def __init__(self):
        self.calls = []

    async def execute(self, query, params):
        required = set(getattr(query, "_bindparams", {}))
        provided = set(params)
        missing = required - provided
        assert not missing, f"missing bind params: {sorted(missing)}"
        self.calls.append((str(query), dict(params)))

        if len(self.calls) == 1:
            return _FakeSqlResult([_empty_inventory_sql_row()])
        return _FakeSqlResult([])


def _empty_inventory_sql_row():
    return {
        "judge_call_requested_count": 0,
        "judge_pending_count": 0,
        "judge_running_count": 0,
        "judge_succeeded_count": 0,
        "judge_failed_retryable_count": 0,
        "judge_failed_terminal_count": 0,
        "judge_other_count": 0,
        "judge_outputs_count": 0,
        "judge_output_ready_event_count": 0,
        "judge_output_missing_ready_event_count": 0,
        "ready_event_missing_output_count": 0,
        "judge_run_succeeded_without_output_count": 0,
        "judge_run_retryable_without_output_count": 0,
        "judge_run_terminal_without_output_count": 0,
        "analysis_policy_apply_count": 0,
        "analysis_policy_apply_already_materialized_count": 0,
        "validator_passed_transition_count": 0,
        "analyses_count": 0,
        "skip_suppress_analysis_count": 0,
        "non_suppress_analysis_count": 0,
        "notification_plan_created_count": 0,
        "notification_intent_absence_expected_count": 0,
        "notification_intent_absence_unexpected_count": 0,
        "notification_intent_unexpected_present_count": 0,
        "notification_plans_count": 0,
        "notification_renders_count": 0,
        "notification_delivery_records_count": 0,
        "eligible_judge_output_ready_count": 0,
        "eligible_policy_apply_count": 0,
    }


@pytest.mark.asyncio
async def test_sql_repository_supplies_all_required_bind_params() -> None:
    session = _BindParamCheckingSession()
    repo = SqlJudgeOutcomeAndSuppressReasonInventoryRepository(session)

    counts = await repo.load_inventory_counts(
        lookback_hours=72,
        sample_limit=100,
        policy_version="verdict_policy_v1",
        delivery_policy_version="delivery_policy_v1",
    )

    assert counts.judge_call_requested_v1_count == 0
    assert len(session.calls) >= 8
    assert set(session.calls[1][1]) >= {
        "lookback_hours",
        "policy_version",
        "delivery_policy_version",
    }


def test_malformed_uuid_payload_is_rejected_by_strict_regex_and_loose_regex_is_absent() -> None:
    malformed_payload_uuid = "------------------------------------"
    assert re.fullmatch(STRICT_UUID_TEXT_SQL_RE, malformed_payload_uuid) is None
    assert re.fullmatch(STRICT_UUID_TEXT_SQL_RE, "01234567-89ab-cdef-ABCD-0123456789ab")
    sql = _INVENTORY_COUNTS_SQL + _RETRYABLE_SELECTION_SQL
    assert STRICT_UUID_TEXT_SQL_RE in sql
    assert "[-0-9a-fA-F]{36}" not in sql


@pytest.mark.asyncio
async def test_redaction_omits_raw_uuid_url_source_text_db_redis_chat_message_dedupe_exception_and_stderr() -> None:
    selected = _candidate(finish_reason=SENSITIVE_EXCEPTION_SENTINEL)
    repository = FakeRepository(
        counts=_counts(
            judge_run_finish_reason_counts={
                "failed_retryable": [
                    {"reason_code": SENSITIVE_URL_SENTINEL, "count": 1},
                    {"reason_code": "openai_retryable_rate_limited", "count": 1},
                ],
                "failed_terminal": [],
                "succeeded": [],
            },
            policy_reason_code_counts=[
                {"reason_code": SENSITIVE_TEXT_SENTINEL, "count": 1},
                {"reason_code": SENSITIVE_DEDUPE_SENTINEL, "count": 1},
            ],
            suppress_reason_code_counts=[
                {"reason_code": SENSITIVE_CHAT_LOCATOR_SENTINEL, "count": 1},
                {"reason_code": "delivery_decision=suppress", "count": 1},
            ],
        ),
        selected_retryable=selected,
    )

    report = await _run(
        JudgeOutcomeAndSuppressReasonInventoryRequest(
            mode="plan",
            lookback_hours=72,
            sample_limit=100,
            select_latest_retryable_judge_run=True,
            retryable_selection_confirm=RETRYABLE_SELECTION_CONFIRM_TOKEN,
        ),
        repository,
    )

    payload = json.dumps(asdict(report), sort_keys=True)
    forbidden = [
        str(selected.judge_run_id),
        str(selected.judge_call_event_id),
        str(selected.bundle_id),
        str(selected.candidate_group_id),
        SENSITIVE_TEXT_SENTINEL,
        SENSITIVE_URL_SENTINEL,
        SENSITIVE_DB_LOCATOR_SENTINEL,
        SENSITIVE_REDIS_LOCATOR_SENTINEL,
        SENSITIVE_CHAT_LOCATOR_SENTINEL,
        SENSITIVE_MESSAGE_LOCATOR_SENTINEL,
        SENSITIVE_DEDUPE_SENTINEL,
        SENSITIVE_EXCEPTION_SENTINEL,
        SENSITIVE_STDERR_SENTINEL,
    ]
    for value in forbidden:
        assert value not in payload
    assert '"reason_code": "other"' in payload
    assert report.redactions_applied is True


@pytest.mark.asyncio
async def test_reason_code_sanitization_maps_unsafe_reason_strings_to_other() -> None:
    repository = FakeRepository(
        counts=_counts(
            policy_reason_code_counts=[
                {"reason_code": "policy_threshold_skip", "count": 1},
                {"reason_code": "unsafe reason with raw text", "count": 2},
            ],
            suppress_reason_code_counts=[
                {"reason_code": "delivery_decision=suppress", "count": 1},
                {"reason_code": "unsafe/reason/raw", "count": 1},
            ],
        )
    )

    report = await _run(
        JudgeOutcomeAndSuppressReasonInventoryRequest(mode="plan", lookback_hours=72, sample_limit=100),
        repository,
    )

    assert report.counts["policy_reason_code_counts"] == [
        {"reason_code": "other", "count": 2},
        {"reason_code": "policy_threshold_skip", "count": 1},
    ]
    assert report.counts["suppress_reason_code_counts"] == [
        {"reason_code": "delivery_decision=suppress", "count": 1},
        {"reason_code": "other", "count": 1},
    ]
