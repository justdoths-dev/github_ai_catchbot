from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import re
from collections.abc import Callable, Mapping, Sequence
from contextlib import asynccontextmanager
from dataclasses import asdict, dataclass, field, replace
from pathlib import Path
from typing import Any, AsyncIterator, Protocol
from uuid import UUID

import sqlalchemy as sa


SCHEMA_VERSION = "judge_outcome_and_suppress_reason_inventory_report_v1"
JUDGE_CALL_REQUESTED_EVENT_TYPE = "judge.call.requested.v1"
JUDGE_OUTPUT_READY_EVENT_TYPE = "judge.output.ready.v1"
POLICY_APPLY_EVENT_TYPE = "analysis.policy.apply.v1"
NOTIFICATION_PLAN_CREATED_EVENT_TYPE = "notification.plan.created.v1"
RETRYABLE_SELECTION_CONFIRM_TOKEN = "latest-retryable-judge-run"
STRICT_UUID_TEXT_SQL_RE = (
    r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-"
    r"[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$"
)

RUNTIME_VALUE_KEYS = {
    "APP_ENV",
    "DATABASE_URL",
    "VERDICT_POLICY_VERSION",
    "DELIVERY_POLICY_VERSION",
    "LOG_LEVEL",
}
RUNTIME_FILE_KEYS = {"DATABASE_URL_FILE"}
RUNTIME_ENV_KEYS = RUNTIME_VALUE_KEYS | RUNTIME_FILE_KEYS
SAFE_REASON_RE = re.compile(r"^[A-Za-z0-9_.:=-]{1,96}$")
OPENAI_RETRYABLE_REASON_CODES = {
    "openai_retryable_rate_limited",
    "openai_retryable_timeout",
    "openai_retryable_connection",
    "openai_retryable_server_error",
    "openai_retryable_unknown",
}
DELIVERY_SUPPRESS_REASON_CODES = {
    "policy_verdict_skip",
    "later_delivery_disabled",
}


class JudgeOutcomeAndSuppressReasonInventoryConfigError(ValueError):
    pass


class SilentArgumentParser(argparse.ArgumentParser):
    def error(self, message: str) -> None:  # pragma: no cover - argparse calls this
        del message
        raise JudgeOutcomeAndSuppressReasonInventoryConfigError("invalid_cli_arguments")


@dataclass(slots=True, frozen=True)
class InventoryCounts:
    judge_call_requested_v1_count: int = 0
    judge_run_status_counts: dict[str, int] = field(default_factory=dict)
    judge_run_finish_reason_counts: dict[str, list[dict[str, int | str]]] = field(default_factory=dict)
    judge_output_count: int = 0
    judge_outputs_count: int = 0
    judge_output_ready_event_count: int = 0
    judge_output_missing_ready_event_count: int = 0
    ready_event_missing_output_count: int = 0
    judge_run_succeeded_without_output_count: int = 0
    judge_run_retryable_without_output_count: int = 0
    judge_run_terminal_without_output_count: int = 0
    analysis_policy_apply_v1_count: int = 0
    analysis_policy_apply_already_materialized_count: int = 0
    validator_passed_transition_count: int = 0
    validator_retryable_transition_reason_counts: list[dict[str, int | str]] = field(default_factory=list)
    validator_terminal_transition_reason_counts: list[dict[str, int | str]] = field(default_factory=list)
    analyses_count: int = 0
    analyses_by_verdict_count: dict[str, int] = field(default_factory=dict)
    analyses_by_delivery_decision_count: dict[str, int] = field(default_factory=dict)
    policy_reason_code_counts: list[dict[str, int | str]] = field(default_factory=list)
    suppress_reason_code_counts: list[dict[str, int | str]] = field(default_factory=list)
    skip_suppress_analysis_count: int = 0
    non_suppress_analysis_count: int = 0
    notification_plan_created_v1_count: int = 0
    notification_intent_absence_expected_count: int = 0
    notification_intent_absence_unexpected_count: int = 0
    notification_intent_unexpected_present_count: int = 0
    notification_plans_count: int = 0
    notification_renders_count: int = 0
    notification_delivery_records_count: int = 0
    eligible_judge_output_ready_count: int = 0
    eligible_policy_apply_count: int = 0


@dataclass(slots=True, frozen=True)
class RetryableJudgeRunCandidate:
    judge_run_id: UUID
    judge_call_event_id: UUID | None
    bundle_id: UUID | None
    candidate_group_id: UUID | None
    current_bundle_id: UUID | None
    finish_reason: str | None
    judge_output_count: int = 0
    ready_event_count: int = 0
    policy_event_count: int = 0
    analysis_count: int = 0
    notification_intent_count: int = 0
    notification_plan_count: int = 0
    notification_render_count: int = 0
    notification_delivery_record_count: int = 0

    @property
    def downstream_count(self) -> int:
        return (
            _int(self.judge_output_count)
            + _int(self.ready_event_count)
            + _int(self.policy_event_count)
            + _int(self.analysis_count)
            + _int(self.notification_intent_count)
            + _int(self.notification_plan_count)
            + _int(self.notification_render_count)
            + _int(self.notification_delivery_record_count)
        )


@dataclass(slots=True, frozen=True)
class RuntimeConfigBundle:
    database_url: str
    values: Mapping[str, str]
    policy_version: str
    delivery_policy_version: str


@dataclass(slots=True, frozen=True)
class JudgeOutcomeAndSuppressReasonInventoryRequest:
    mode: str
    lookback_hours: int
    sample_limit: int
    select_latest_retryable_judge_run: bool = False
    retryable_selection_confirm: str | None = None


@dataclass(slots=True, frozen=True)
class JudgeOutcomeAndSuppressReasonInventoryReport:
    schema_version: str
    mode: str
    status: str
    reason_code: str
    lookback_hours: int
    sample_limit: int
    counts: dict[str, Any]
    selected_retryable_judge_run_fingerprint: str | None
    selected_retryable_judge_call_event_fingerprint: str | None
    selected_bundle_fingerprint: str | None
    selected_candidate_group_fingerprint: str | None
    selected_retryable_reason_code: str | None
    selected_retry_readiness: str | None
    redis_attempted: bool
    telegram_attempted: bool
    openai_attempted: bool
    external_network_attempted: bool
    db_write_attempted: bool
    redactions_applied: bool
    cleanup_completed: bool


class InventoryRepositoryProtocol(Protocol):
    async def load_inventory_counts(
        self,
        *,
        lookback_hours: int,
        sample_limit: int,
        policy_version: str,
        delivery_policy_version: str,
    ) -> InventoryCounts: ...

    async def select_latest_retryable_judge_run(
        self,
        *,
        lookback_hours: int,
        sample_limit: int,
    ) -> RetryableJudgeRunCandidate | None: ...


@dataclass(slots=True, frozen=True)
class JudgeOutcomeAndSuppressReasonInventoryComponents:
    inventory_repository: InventoryRepositoryProtocol


class SqlJudgeOutcomeAndSuppressReasonInventoryRepository:
    def __init__(self, session: Any) -> None:
        self._session = session

    async def load_inventory_counts(
        self,
        *,
        lookback_hours: int,
        sample_limit: int,
        policy_version: str,
        delivery_policy_version: str,
    ) -> InventoryCounts:
        del sample_limit
        row = await self._one(
            _INVENTORY_COUNTS_SQL,
            {
                "lookback_hours": lookback_hours,
                "policy_version": policy_version,
                "delivery_policy_version": delivery_policy_version,
            },
        )
        finish_reason_rows = await self._rows(
            _FINISH_REASON_COUNTS_SQL,
            {
                "lookback_hours": lookback_hours,
                "policy_version": policy_version,
                "delivery_policy_version": delivery_policy_version,
            },
        )
        validator_retryable_rows = await self._rows(
            _VALIDATOR_RETRYABLE_TRANSITION_REASON_COUNTS_SQL,
            {"lookback_hours": lookback_hours},
        )
        validator_terminal_rows = await self._rows(
            _VALIDATOR_TERMINAL_TRANSITION_REASON_COUNTS_SQL,
            {"lookback_hours": lookback_hours},
        )
        policy_reason_rows = await self._rows(
            _POLICY_REASON_CODE_COUNTS_SQL,
            {"lookback_hours": lookback_hours},
        )
        suppress_reason_rows = await self._rows(
            _SUPPRESS_REASON_CODE_COUNTS_SQL,
            {"lookback_hours": lookback_hours},
        )
        return InventoryCounts(
            judge_call_requested_v1_count=_int(row["judge_call_requested_count"]),
            judge_run_status_counts={
                "pending": _int(row["judge_pending_count"]),
                "running": _int(row["judge_running_count"]),
                "succeeded": _int(row["judge_succeeded_count"]),
                "failed_retryable": _int(row["judge_failed_retryable_count"]),
                "failed_terminal": _int(row["judge_failed_terminal_count"]),
                "other": _int(row["judge_other_count"]),
            },
            judge_run_finish_reason_counts=_finish_reason_counts_by_status(finish_reason_rows),
            judge_output_count=_int(row["judge_outputs_count"]),
            judge_outputs_count=_int(row["judge_outputs_count"]),
            judge_output_ready_event_count=_int(row["judge_output_ready_event_count"]),
            judge_output_missing_ready_event_count=_int(row["judge_output_missing_ready_event_count"]),
            ready_event_missing_output_count=_int(row["ready_event_missing_output_count"]),
            judge_run_succeeded_without_output_count=_int(row["judge_run_succeeded_without_output_count"]),
            judge_run_retryable_without_output_count=_int(row["judge_run_retryable_without_output_count"]),
            judge_run_terminal_without_output_count=_int(row["judge_run_terminal_without_output_count"]),
            analysis_policy_apply_v1_count=_int(row["analysis_policy_apply_count"]),
            analysis_policy_apply_already_materialized_count=_int(
                row["analysis_policy_apply_already_materialized_count"]
            ),
            validator_passed_transition_count=_int(row["validator_passed_transition_count"]),
            validator_retryable_transition_reason_counts=_safe_reason_count_rows(validator_retryable_rows),
            validator_terminal_transition_reason_counts=_safe_reason_count_rows(validator_terminal_rows),
            analyses_count=_int(row["analyses_count"]),
            analyses_by_verdict_count=await self._count_by(
                _ANALYSES_BY_VERDICT_SQL,
                {"lookback_hours": lookback_hours},
            ),
            analyses_by_delivery_decision_count=await self._count_by(
                _ANALYSES_BY_DELIVERY_SQL,
                {"lookback_hours": lookback_hours},
            ),
            policy_reason_code_counts=_safe_reason_count_rows(policy_reason_rows),
            suppress_reason_code_counts=_safe_reason_count_rows(suppress_reason_rows),
            skip_suppress_analysis_count=_int(row["skip_suppress_analysis_count"]),
            non_suppress_analysis_count=_int(row["non_suppress_analysis_count"]),
            notification_plan_created_v1_count=_int(row["notification_plan_created_count"]),
            notification_intent_absence_expected_count=_int(row["notification_intent_absence_expected_count"]),
            notification_intent_absence_unexpected_count=_int(row["notification_intent_absence_unexpected_count"]),
            notification_intent_unexpected_present_count=_int(row["notification_intent_unexpected_present_count"]),
            notification_plans_count=_int(row["notification_plans_count"]),
            notification_renders_count=_int(row["notification_renders_count"]),
            notification_delivery_records_count=_int(row["notification_delivery_records_count"]),
            eligible_judge_output_ready_count=_int(row["eligible_judge_output_ready_count"]),
            eligible_policy_apply_count=_int(row["eligible_policy_apply_count"]),
        )

    async def select_latest_retryable_judge_run(
        self,
        *,
        lookback_hours: int,
        sample_limit: int,
    ) -> RetryableJudgeRunCandidate | None:
        rows = await self._rows(
            _RETRYABLE_SELECTION_SQL,
            {"lookback_hours": lookback_hours, "sample_limit": sample_limit},
        )
        if not rows:
            return None
        return _retryable_candidate_from_row(rows[0])

    async def _count_by(self, query: str, params: Mapping[str, Any]) -> dict[str, int]:
        rows = await self._rows(query, params)
        return {_safe_bucket_value(row["bucket"]): _int(row["bucket_count"]) for row in rows}

    async def _rows(self, query: str, params: Mapping[str, Any]) -> list[Mapping[str, Any]]:
        result = await self._session.execute(sa.text(query), dict(params))
        return list(result.mappings().all())

    async def _one(self, query: str, params: Mapping[str, Any]) -> Mapping[str, Any]:
        result = await self._session.execute(sa.text(query), dict(params))
        return result.mappings().one()


_JUDGE_RUNS_WINDOW_SQL = """
judge_runs_window AS (
    SELECT jr.judge_run_id, jr.bundle_id, jr.status, jr.finish_reason
    FROM judge_runs jr
    WHERE COALESCE(jr.finished_at, jr.started_at) >= now() - make_interval(hours => :lookback_hours)
       OR EXISTS (
            SELECT 1
            FROM event_outbox eo
            WHERE eo.event_type = 'judge.call.requested.v1'
              AND eo.aggregate_type = 'judge_run'
              AND eo.aggregate_id = jr.judge_run_id
              AND eo.created_at >= now() - make_interval(hours => :lookback_hours)
       )
),
judge_outputs_window AS (
    SELECT jo.judge_output_id, jo.judge_run_id, jo.candidate_group_id, jo.created_at
    FROM judge_outputs jo
    WHERE jo.created_at >= now() - make_interval(hours => :lookback_hours)
       OR EXISTS (
            SELECT 1
            FROM judge_runs_window jrw
            WHERE jrw.judge_run_id = jo.judge_run_id
       )
),
ready_events_window AS (
    SELECT
        eo.event_id,
        eo.aggregate_id,
        eo.payload_json->>'judge_run_id' AS judge_run_id_text,
        eo.payload_json->>'judge_output_id' AS judge_output_id_text
    FROM event_outbox eo
    WHERE eo.event_type = 'judge.output.ready.v1'
      AND eo.created_at >= now() - make_interval(hours => :lookback_hours)
),
valid_ready_events_window AS (
    SELECT *
    FROM ready_events_window
    WHERE judge_run_id_text ~ '""" + STRICT_UUID_TEXT_SQL_RE + """'
      AND judge_output_id_text ~ '""" + STRICT_UUID_TEXT_SQL_RE + """'
),
policy_events_window AS (
    SELECT
        eo.event_id,
        eo.aggregate_id,
        eo.payload_json->>'judge_run_id' AS judge_run_id_text,
        eo.payload_json->>'judge_output_id' AS judge_output_id_text,
        eo.payload_json->>'candidate_group_id' AS candidate_group_id_text,
        eo.payload_json->>'bundle_id' AS bundle_id_text
    FROM event_outbox eo
    WHERE eo.event_type = 'analysis.policy.apply.v1'
      AND eo.created_at >= now() - make_interval(hours => :lookback_hours)
),
valid_policy_events_window AS (
    SELECT *
    FROM policy_events_window
    WHERE judge_run_id_text ~ '""" + STRICT_UUID_TEXT_SQL_RE + """'
      AND judge_output_id_text ~ '""" + STRICT_UUID_TEXT_SQL_RE + """'
      AND candidate_group_id_text ~ '""" + STRICT_UUID_TEXT_SQL_RE + """'
      AND bundle_id_text ~ '""" + STRICT_UUID_TEXT_SQL_RE + """'
),
analyses_window AS (
    SELECT a.*
    FROM analyses a
    WHERE a.created_at >= now() - make_interval(hours => :lookback_hours)
),
notification_intents_window AS (
    SELECT eo.event_id, eo.aggregate_id, eo.payload_json->>'analysis_id' AS analysis_id_text
    FROM event_outbox eo
    WHERE eo.event_type = 'notification.plan.created.v1'
      AND eo.created_at >= now() - make_interval(hours => :lookback_hours)
),
eligible_ready_targets AS (
    SELECT vr.event_id
    FROM valid_ready_events_window vr
    JOIN judge_runs jr
      ON jr.judge_run_id = vr.aggregate_id
    JOIN judge_outputs jo
      ON jo.judge_output_id = CAST(vr.judge_output_id_text AS uuid)
     AND jo.judge_run_id = jr.judge_run_id
    JOIN candidate_evidence_bundles ceb
      ON ceb.bundle_id = jr.bundle_id
     AND ceb.candidate_group_id = jo.candidate_group_id
    JOIN candidate_group_proposals cgp
      ON cgp.candidate_group_id = jo.candidate_group_id
     AND cgp.current_bundle_id = jr.bundle_id
    WHERE vr.judge_run_id_text = jr.judge_run_id::text
      AND vr.judge_output_id_text = jo.judge_output_id::text
      AND jr.status = 'succeeded'
      AND NOT EXISTS (
            SELECT 1
            FROM event_outbox policy
            WHERE policy.event_type = 'analysis.policy.apply.v1'
              AND policy.aggregate_type = 'judge_run'
              AND policy.aggregate_id = jr.judge_run_id
              AND policy.payload_json->>'judge_run_id' = jr.judge_run_id::text
              AND policy.payload_json->>'judge_output_id' = jo.judge_output_id::text
      )
      AND NOT EXISTS (
            SELECT 1
            FROM analyses a
            WHERE a.judge_output_id = jo.judge_output_id
              AND a.policy_version = :policy_version
              AND a.delivery_policy_version = :delivery_policy_version
      )
),
eligible_policy_targets AS (
    SELECT vp.event_id
    FROM valid_policy_events_window vp
    JOIN judge_runs jr
      ON jr.judge_run_id = CAST(vp.judge_run_id_text AS uuid)
    JOIN judge_outputs jo
      ON jo.judge_output_id = CAST(vp.judge_output_id_text AS uuid)
     AND jo.judge_run_id = jr.judge_run_id
    JOIN candidate_group_proposals cgp
      ON cgp.candidate_group_id = CAST(vp.candidate_group_id_text AS uuid)
    JOIN candidate_evidence_bundles ceb
      ON ceb.bundle_id = CAST(vp.bundle_id_text AS uuid)
     AND ceb.candidate_group_id = cgp.candidate_group_id
    WHERE vp.aggregate_id = jr.judge_run_id
      AND vp.judge_run_id_text = jr.judge_run_id::text
      AND vp.judge_output_id_text = jo.judge_output_id::text
      AND vp.candidate_group_id_text = jo.candidate_group_id::text
      AND vp.bundle_id_text = ceb.bundle_id::text
      AND cgp.current_bundle_id = ceb.bundle_id
      AND jr.bundle_id = ceb.bundle_id
      AND jr.status = 'succeeded'
      AND NOT EXISTS (
            SELECT 1
            FROM analyses a
            WHERE a.judge_output_id = jo.judge_output_id
              AND a.policy_version = :policy_version
              AND a.delivery_policy_version = :delivery_policy_version
      )
)
"""


_INVENTORY_COUNTS_SQL = (
    "WITH "
    + _JUDGE_RUNS_WINDOW_SQL
    + """
SELECT
    (
        SELECT count(*)
        FROM event_outbox
        WHERE event_type = 'judge.call.requested.v1'
          AND created_at >= now() - make_interval(hours => :lookback_hours)
    ) AS judge_call_requested_count,
    (SELECT count(*) FROM judge_runs_window WHERE status = 'pending') AS judge_pending_count,
    (SELECT count(*) FROM judge_runs_window WHERE status = 'running') AS judge_running_count,
    (SELECT count(*) FROM judge_runs_window WHERE status = 'succeeded') AS judge_succeeded_count,
    (SELECT count(*) FROM judge_runs_window WHERE status = 'failed_retryable') AS judge_failed_retryable_count,
    (SELECT count(*) FROM judge_runs_window WHERE status = 'failed_terminal') AS judge_failed_terminal_count,
    (
        SELECT count(*)
        FROM judge_runs_window
        WHERE status NOT IN ('pending', 'running', 'succeeded', 'failed_retryable', 'failed_terminal')
    ) AS judge_other_count,
    (SELECT count(*) FROM judge_outputs_window) AS judge_outputs_count,
    (SELECT count(*) FROM ready_events_window) AS judge_output_ready_event_count,
    (
        SELECT count(*)
        FROM judge_outputs_window jo
        WHERE NOT EXISTS (
            SELECT 1
            FROM valid_ready_events_window vr
            WHERE vr.judge_output_id_text = jo.judge_output_id::text
              AND vr.judge_run_id_text = jo.judge_run_id::text
        )
    ) AS judge_output_missing_ready_event_count,
    (
        SELECT count(*)
        FROM valid_ready_events_window vr
        LEFT JOIN judge_outputs jo
          ON jo.judge_output_id = CAST(vr.judge_output_id_text AS uuid)
         AND jo.judge_run_id = CAST(vr.judge_run_id_text AS uuid)
        WHERE jo.judge_output_id IS NULL
    ) AS ready_event_missing_output_count,
    (
        SELECT count(*)
        FROM judge_runs_window jr
        WHERE jr.status = 'succeeded'
          AND NOT EXISTS (SELECT 1 FROM judge_outputs jo WHERE jo.judge_run_id = jr.judge_run_id)
    ) AS judge_run_succeeded_without_output_count,
    (
        SELECT count(*)
        FROM judge_runs_window jr
        WHERE jr.status = 'failed_retryable'
          AND NOT EXISTS (SELECT 1 FROM judge_outputs jo WHERE jo.judge_run_id = jr.judge_run_id)
    ) AS judge_run_retryable_without_output_count,
    (
        SELECT count(*)
        FROM judge_runs_window jr
        WHERE jr.status = 'failed_terminal'
          AND NOT EXISTS (SELECT 1 FROM judge_outputs jo WHERE jo.judge_run_id = jr.judge_run_id)
    ) AS judge_run_terminal_without_output_count,
    (SELECT count(*) FROM policy_events_window) AS analysis_policy_apply_count,
    (
        SELECT count(*)
        FROM valid_policy_events_window vp
        JOIN judge_outputs jo
          ON jo.judge_output_id = CAST(vp.judge_output_id_text AS uuid)
        JOIN analyses a
          ON a.judge_output_id = jo.judge_output_id
         AND a.policy_version = :policy_version
         AND a.delivery_policy_version = :delivery_policy_version
    ) AS analysis_policy_apply_already_materialized_count,
    (
        SELECT count(*)
        FROM state_transitions st
        WHERE st.object_type = 'judge_run'
          AND st.reason_code = 'validator_passed'
          AND st.created_at >= now() - make_interval(hours => :lookback_hours)
    ) AS validator_passed_transition_count,
    (SELECT count(*) FROM analyses_window) AS analyses_count,
    (
        SELECT count(*)
        FROM analyses_window
        WHERE verdict::text = 'skip'
          AND delivery_decision::text = 'suppress'
    ) AS skip_suppress_analysis_count,
    (
        SELECT count(*)
        FROM analyses_window
        WHERE delivery_decision::text <> 'suppress'
    ) AS non_suppress_analysis_count,
    (
        SELECT count(*)
        FROM event_outbox
        WHERE event_type = 'notification.plan.created.v1'
          AND created_at >= now() - make_interval(hours => :lookback_hours)
    ) AS notification_plan_created_count,
    (
        SELECT count(*)
        FROM analyses_window a
        WHERE a.delivery_decision::text = 'suppress'
          AND NOT EXISTS (
                SELECT 1
                FROM notification_intents_window ni
                WHERE ni.aggregate_id = a.analysis_id
                   OR ni.analysis_id_text = a.analysis_id::text
          )
          AND NOT EXISTS (
                SELECT 1
                FROM notification_plans np
                WHERE np.analysis_id = a.analysis_id
          )
    ) AS notification_intent_absence_expected_count,
    (
        SELECT count(*)
        FROM analyses_window a
        WHERE a.delivery_decision::text <> 'suppress'
          AND NOT EXISTS (
                SELECT 1
                FROM notification_intents_window ni
                WHERE ni.aggregate_id = a.analysis_id
                   OR ni.analysis_id_text = a.analysis_id::text
          )
          AND NOT EXISTS (
                SELECT 1
                FROM notification_plans np
                WHERE np.analysis_id = a.analysis_id
          )
    ) AS notification_intent_absence_unexpected_count,
    (
        SELECT count(*)
        FROM analyses_window a
        WHERE a.delivery_decision::text = 'suppress'
          AND (
                EXISTS (
                    SELECT 1
                    FROM notification_intents_window ni
                    WHERE ni.aggregate_id = a.analysis_id
                       OR ni.analysis_id_text = a.analysis_id::text
                )
                OR EXISTS (
                    SELECT 1
                    FROM notification_plans np
                    WHERE np.analysis_id = a.analysis_id
                )
          )
    ) AS notification_intent_unexpected_present_count,
    (
        SELECT count(*)
        FROM notification_plans
        WHERE created_at >= now() - make_interval(hours => :lookback_hours)
    ) AS notification_plans_count,
    (
        SELECT count(*)
        FROM notification_renders
        WHERE created_at >= now() - make_interval(hours => :lookback_hours)
    ) AS notification_renders_count,
    (
        SELECT count(*)
        FROM notification_delivery_records
        WHERE created_at >= now() - make_interval(hours => :lookback_hours)
    ) AS notification_delivery_records_count,
    (SELECT count(*) FROM eligible_ready_targets) AS eligible_judge_output_ready_count,
    (SELECT count(*) FROM eligible_policy_targets) AS eligible_policy_apply_count
"""
)


_FINISH_REASON_COUNTS_SQL = (
    "WITH "
    + _JUDGE_RUNS_WINDOW_SQL
    + """
SELECT
    status::text AS status_bucket,
    COALESCE(finish_reason, 'missing') AS reason_code,
    count(*) AS reason_count
FROM judge_runs_window
WHERE status IN ('failed_retryable', 'failed_terminal', 'succeeded')
GROUP BY status::text, COALESCE(finish_reason, 'missing')
ORDER BY status_bucket ASC, reason_count DESC, reason_code ASC
"""
)


_VALIDATOR_RETRYABLE_TRANSITION_REASON_COUNTS_SQL = """
SELECT COALESCE(reason_code, 'missing') AS reason_code, count(*) AS reason_count
FROM state_transitions
WHERE object_type = 'judge_run'
  AND created_at >= now() - make_interval(hours => :lookback_hours)
  AND (to_state = 'analysis_failed_truncation' OR reason_code = 'analysis_failed_truncation')
GROUP BY COALESCE(reason_code, 'missing')
ORDER BY reason_count DESC, reason_code ASC
"""


_VALIDATOR_TERMINAL_TRANSITION_REASON_COUNTS_SQL = """
SELECT COALESCE(reason_code, 'missing') AS reason_code, count(*) AS reason_count
FROM state_transitions
WHERE object_type = 'judge_run'
  AND created_at >= now() - make_interval(hours => :lookback_hours)
  AND COALESCE(reason_code, 'missing') <> 'validator_passed'
  AND to_state IN (
        'analysis_failed_missing_run',
        'analysis_failed_missing_output',
        'analysis_failed_identity_mismatch',
        'analysis_failed_schema',
        'analysis_failed_semantic',
        'analysis_refused'
  )
GROUP BY COALESCE(reason_code, 'missing')
ORDER BY reason_count DESC, reason_code ASC
"""


_ANALYSES_BY_VERDICT_SQL = """
SELECT verdict::text AS bucket, count(*) AS bucket_count
FROM analyses
WHERE created_at >= now() - make_interval(hours => :lookback_hours)
GROUP BY verdict::text
"""


_ANALYSES_BY_DELIVERY_SQL = """
SELECT delivery_decision::text AS bucket, count(*) AS bucket_count
FROM analyses
WHERE created_at >= now() - make_interval(hours => :lookback_hours)
GROUP BY delivery_decision::text
"""


_POLICY_REASON_CODE_COUNTS_SQL = """
SELECT reason_code, count(*) AS reason_count
FROM (
    SELECT reason_item.value #>> '{}' AS reason_code
    FROM analyses a
    CROSS JOIN LATERAL jsonb_array_elements(
        CASE
            WHEN jsonb_typeof(a.reason_codes_json) = 'array' THEN a.reason_codes_json
            ELSE '[]'::jsonb
        END
    ) AS reason_item(value)
    WHERE a.created_at >= now() - make_interval(hours => :lookback_hours)
) reasons
GROUP BY reason_code
ORDER BY reason_count DESC, reason_code ASC
"""


_SUPPRESS_REASON_CODE_COUNTS_SQL = """
WITH suppress_analyses AS (
    SELECT analysis_id, reason_codes_json
    FROM analyses
    WHERE created_at >= now() - make_interval(hours => :lookback_hours)
      AND delivery_decision::text = 'suppress'
),
explicit_reasons AS (
    SELECT reason_item.value #>> '{}' AS reason_code
    FROM suppress_analyses a
    CROSS JOIN LATERAL jsonb_array_elements(
        CASE
            WHEN jsonb_typeof(a.reason_codes_json) = 'array' THEN a.reason_codes_json
            ELSE '[]'::jsonb
        END
    ) AS reason_item(value)
    WHERE (reason_item.value #>> '{}') IN ('policy_verdict_skip', 'later_delivery_disabled')
),
decision_bucket AS (
    SELECT 'delivery_decision=suppress' AS reason_code
    FROM suppress_analyses
)
SELECT reason_code, count(*) AS reason_count
FROM (
    SELECT reason_code FROM explicit_reasons
    UNION ALL
    SELECT reason_code FROM decision_bucket
) reasons
GROUP BY reason_code
ORDER BY reason_count DESC, reason_code ASC
"""


_RETRYABLE_SELECTION_SQL = f"""
WITH retryable_runs AS (
    SELECT
        jr.judge_run_id,
        jr.bundle_id,
        jr.finish_reason,
        COALESCE(jr.finished_at, jr.started_at) AS run_sort_at
    FROM judge_runs jr
    WHERE jr.status = 'failed_retryable'
      AND (
            COALESCE(jr.finished_at, jr.started_at) >= now() - make_interval(hours => :lookback_hours)
            OR EXISTS (
                SELECT 1
                FROM event_outbox eo
                WHERE eo.event_type = 'judge.call.requested.v1'
                  AND eo.aggregate_type = 'judge_run'
                  AND eo.aggregate_id = jr.judge_run_id
                  AND eo.created_at >= now() - make_interval(hours => :lookback_hours)
            )
      )
),
call_events AS (
    SELECT DISTINCT ON (eo.payload_json->>'judge_run_id')
        eo.event_id,
        eo.created_at,
        eo.payload_json->>'judge_run_id' AS judge_run_id_text,
        eo.payload_json->>'bundle_id' AS bundle_id_text
    FROM event_outbox eo
    WHERE eo.event_type = 'judge.call.requested.v1'
      AND eo.payload_json->>'judge_run_id' ~ '{STRICT_UUID_TEXT_SQL_RE}'
      AND eo.payload_json->>'bundle_id' ~ '{STRICT_UUID_TEXT_SQL_RE}'
    ORDER BY eo.payload_json->>'judge_run_id', eo.created_at DESC, eo.event_id DESC
)
SELECT
    rr.judge_run_id,
    ce.event_id AS judge_call_event_id,
    rr.bundle_id,
    ceb.candidate_group_id,
    cgp.current_bundle_id,
    rr.finish_reason,
    COALESCE(downstream.judge_output_count, 0) AS judge_output_count,
    COALESCE(downstream.ready_event_count, 0) AS ready_event_count,
    COALESCE(downstream.policy_event_count, 0) AS policy_event_count,
    COALESCE(downstream.analysis_count, 0) AS analysis_count,
    COALESCE(downstream.notification_intent_count, 0) AS notification_intent_count,
    COALESCE(downstream.notification_plan_count, 0) AS notification_plan_count,
    COALESCE(downstream.notification_render_count, 0) AS notification_render_count,
    COALESCE(downstream.notification_delivery_record_count, 0) AS notification_delivery_record_count,
    COALESCE(rr.run_sort_at, ce.created_at) AS selected_sort_at
FROM retryable_runs rr
LEFT JOIN call_events ce
  ON ce.judge_run_id_text = rr.judge_run_id::text
 AND ce.bundle_id_text = rr.bundle_id::text
LEFT JOIN candidate_evidence_bundles ceb
  ON ceb.bundle_id = rr.bundle_id
LEFT JOIN candidate_group_proposals cgp
  ON cgp.candidate_group_id = ceb.candidate_group_id
LEFT JOIN LATERAL (
    WITH run_outputs AS (
        SELECT judge_output_id
        FROM judge_outputs
        WHERE judge_run_id = rr.judge_run_id
    ),
    run_analyses AS (
        SELECT a.analysis_id
        FROM analyses a
        JOIN run_outputs ro ON ro.judge_output_id = a.judge_output_id
    ),
    run_plans AS (
        SELECT np.notification_plan_id
        FROM notification_plans np
        JOIN run_analyses ra ON ra.analysis_id = np.analysis_id
    )
    SELECT
        (SELECT count(*) FROM run_outputs) AS judge_output_count,
        (
            SELECT count(*)
            FROM event_outbox ready
            WHERE ready.event_type = 'judge.output.ready.v1'
              AND ready.payload_json->>'judge_run_id' = rr.judge_run_id::text
        ) AS ready_event_count,
        (
            SELECT count(*)
            FROM event_outbox policy
            WHERE policy.event_type = 'analysis.policy.apply.v1'
              AND policy.payload_json->>'judge_run_id' = rr.judge_run_id::text
        ) AS policy_event_count,
        (SELECT count(*) FROM run_analyses) AS analysis_count,
        (
            SELECT count(*)
            FROM event_outbox ni
            JOIN run_analyses ra
              ON ni.aggregate_id = ra.analysis_id
              OR ni.payload_json->>'analysis_id' = ra.analysis_id::text
            WHERE ni.event_type = 'notification.plan.created.v1'
        ) AS notification_intent_count,
        (SELECT count(*) FROM run_plans) AS notification_plan_count,
        (
            SELECT count(*)
            FROM notification_renders nr
            JOIN run_plans rp ON rp.notification_plan_id = nr.notification_plan_id
        ) AS notification_render_count,
        (
            SELECT count(*)
            FROM notification_delivery_records ndr
            JOIN run_plans rp ON rp.notification_plan_id = ndr.notification_plan_id
        ) AS notification_delivery_record_count
) downstream ON true
ORDER BY COALESCE(rr.run_sort_at, ce.created_at) DESC NULLS LAST, rr.judge_run_id DESC
LIMIT :sample_limit
"""


async def run_judge_outcome_and_suppress_reason_inventory(
    request: JudgeOutcomeAndSuppressReasonInventoryRequest,
    *,
    runtime: RuntimeConfigBundle,
    components: JudgeOutcomeAndSuppressReasonInventoryComponents,
) -> JudgeOutcomeAndSuppressReasonInventoryReport:
    report = _report(
        mode=request.mode,
        status="failed",
        reason_code="unhandled_error",
        lookback_hours=request.lookback_hours,
        sample_limit=request.sample_limit,
    )
    try:
        if request.mode != "plan":
            return replace(report, status="blocked", reason_code="execute_mode_not_supported")

        counts = await components.inventory_repository.load_inventory_counts(
            lookback_hours=request.lookback_hours,
            sample_limit=request.sample_limit,
            policy_version=runtime.policy_version,
            delivery_policy_version=runtime.delivery_policy_version,
        )
        report = replace(report, counts=_counts_to_report(counts))

        if request.select_latest_retryable_judge_run:
            if request.retryable_selection_confirm != RETRYABLE_SELECTION_CONFIRM_TOKEN:
                return replace(report, status="blocked", reason_code="retryable_selection_confirm_missing")
            selected = await components.inventory_repository.select_latest_retryable_judge_run(
                lookback_hours=request.lookback_hours,
                sample_limit=request.sample_limit,
            )
            report = _apply_retryable_selection(report, selected)

        return replace(report, status="pass", reason_code="inventory_plan_complete")
    except JudgeOutcomeAndSuppressReasonInventoryConfigError as exc:
        return replace(report, status="blocked", reason_code=_safe_reason_code_value(exc))
    except Exception:
        return replace(report, status="failed", reason_code="unhandled_error")


def build_parser() -> argparse.ArgumentParser:
    parser = SilentArgumentParser(
        prog="judge-outcome-and-suppress-reason-inventory",
        allow_abbrev=False,
    )
    parser.add_argument("--mode")
    parser.add_argument("--env-file")
    parser.add_argument("--lookback-hours", type=int, default=72)
    parser.add_argument("--sample-limit", type=int, default=100)
    parser.add_argument("--select-latest-retryable-judge-run", action="store_true")
    parser.add_argument("--retryable-selection-confirm", default=None)
    parser.add_argument("--confirm", default=None)
    return parser


async def run_cli(
    argv: Sequence[str] | None = None,
    *,
    emit_json: Callable[[str], None] = print,
    runtime_config_loader: Callable[[str], RuntimeConfigBundle] | None = None,
    components_builder: Callable[[RuntimeConfigBundle], AsyncIterator[JudgeOutcomeAndSuppressReasonInventoryComponents]]
    | None = None,
) -> int:
    try:
        args, unknown = build_parser().parse_known_args(list(argv) if argv is not None else None)
    except JudgeOutcomeAndSuppressReasonInventoryConfigError as exc:
        emit_json(_compact_json(asdict(_argument_report(str(exc)))))
        return 2

    mode = str(args.mode) if args.mode is not None else "unknown"
    validation_error = _cli_request_error(args, unknown)
    if validation_error is not None:
        emit_json(
            _compact_json(
                asdict(
                    _report(
                        mode=mode,
                        status="blocked",
                        reason_code=validation_error,
                        lookback_hours=_bounded_cli_int(args.lookback_hours, default=72),
                        sample_limit=_bounded_cli_int(args.sample_limit, default=100),
                    )
                )
            )
        )
        return 2

    try:
        runtime = (runtime_config_loader or load_runtime_config)(str(args.env_file))
    except JudgeOutcomeAndSuppressReasonInventoryConfigError as exc:
        emit_json(
            _compact_json(
                asdict(
                    _report(
                        mode=mode,
                        status="blocked",
                        reason_code=_safe_reason_code_value(exc),
                        lookback_hours=int(args.lookback_hours),
                        sample_limit=int(args.sample_limit),
                    )
                )
            )
        )
        return 2

    request = JudgeOutcomeAndSuppressReasonInventoryRequest(
        mode=str(args.mode),
        lookback_hours=int(args.lookback_hours),
        sample_limit=int(args.sample_limit),
        select_latest_retryable_judge_run=bool(args.select_latest_retryable_judge_run),
        retryable_selection_confirm=_optional_str(args.retryable_selection_confirm),
    )
    builder = components_builder or sql_inventory_components
    async with builder(runtime) as components:
        report = await run_judge_outcome_and_suppress_reason_inventory(
            request,
            runtime=runtime,
            components=components,
        )
    emit_json(_compact_json(asdict(report)))
    return 0 if report.status == "pass" else 2


def load_runtime_config(env_file: str) -> RuntimeConfigBundle:
    values = _read_runtime_env_file(env_file)
    resolved_values = dict(values)
    database_url = _resolve_file_indirection(
        resolved_values,
        value_key="DATABASE_URL",
        file_key="DATABASE_URL_FILE",
        missing_reason_code="database_url_missing",
        file_missing_reason_code="database_url_file_missing",
        file_empty_reason_code="database_url_file_empty",
    )
    policy_version = _read(resolved_values, "VERDICT_POLICY_VERSION", "verdict_policy_v1")
    delivery_policy_version = _read(resolved_values, "DELIVERY_POLICY_VERSION", "delivery_policy_v1")
    if not policy_version:
        raise JudgeOutcomeAndSuppressReasonInventoryConfigError("policy_version_missing")
    if not delivery_policy_version:
        raise JudgeOutcomeAndSuppressReasonInventoryConfigError("delivery_policy_version_missing")
    return RuntimeConfigBundle(
        database_url=database_url,
        values=resolved_values,
        policy_version=policy_version,
        delivery_policy_version=delivery_policy_version,
    )


@asynccontextmanager
async def sql_inventory_components(
    runtime: RuntimeConfigBundle,
) -> AsyncIterator[JudgeOutcomeAndSuppressReasonInventoryComponents]:
    from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

    engine = create_async_engine(runtime.database_url, future=True)
    session_factory = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    try:
        async with session_factory() as session:
            yield JudgeOutcomeAndSuppressReasonInventoryComponents(
                inventory_repository=SqlJudgeOutcomeAndSuppressReasonInventoryRepository(session),
            )
    finally:
        await engine.dispose()


def _cli_request_error(args: argparse.Namespace, unknown: Sequence[str]) -> str | None:
    if any(_is_execute_like_arg(item) for item in unknown):
        return "execute_argument_not_allowed"
    if unknown:
        return "invalid_cli_arguments"
    if args.mode != "plan":
        return "execute_mode_not_supported" if _is_execute_like_arg(args.mode) else "invalid_mode"
    if args.confirm is not None:
        return "execute_argument_not_allowed"
    if not args.env_file:
        return "env_file_required"
    if args.lookback_hours < 1 or args.lookback_hours > 720:
        return "lookback_hours_out_of_range"
    if args.sample_limit < 1 or args.sample_limit > 500:
        return "sample_limit_out_of_range"
    if args.retryable_selection_confirm is not None and not args.select_latest_retryable_judge_run:
        return "retryable_selection_confirm_without_selector"
    if args.select_latest_retryable_judge_run and args.retryable_selection_confirm != RETRYABLE_SELECTION_CONFIRM_TOKEN:
        return "retryable_selection_confirm_missing"
    return None


def _is_execute_like_arg(value: Any) -> bool:
    text = str(value or "").strip().lower()
    return any(token in text for token in ("execute", "exec", "apply", "retry-now", "mutate", "commit", "confirm"))


def _read_runtime_env_file(env_file: str) -> dict[str, str]:
    path = Path(env_file)
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except FileNotFoundError:
        raise JudgeOutcomeAndSuppressReasonInventoryConfigError("env_file_missing") from None
    except OSError:
        raise JudgeOutcomeAndSuppressReasonInventoryConfigError("env_file_unreadable") from None

    values: dict[str, str] = {}
    for raw_line in lines:
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        if key not in RUNTIME_ENV_KEYS:
            continue
        value = value.strip()
        if len(value) >= 2 and value[0] == value[-1] and value[0] in {"'", '"'}:
            value = value[1:-1]
        values[key] = value
    if not values:
        raise JudgeOutcomeAndSuppressReasonInventoryConfigError("env_file_no_runtime_config")
    return values


def _resolve_file_indirection(
    values: dict[str, str],
    *,
    value_key: str,
    file_key: str,
    missing_reason_code: str,
    file_missing_reason_code: str,
    file_empty_reason_code: str,
) -> str:
    direct = values.get(value_key, "").strip()
    if direct:
        return direct
    file_path = values.get(file_key, "").strip()
    if not file_path:
        raise JudgeOutcomeAndSuppressReasonInventoryConfigError(missing_reason_code)
    path = Path(file_path)
    if not path.is_file():
        raise JudgeOutcomeAndSuppressReasonInventoryConfigError(file_missing_reason_code)
    try:
        value = path.read_text(encoding="utf-8").strip()
    except OSError:
        raise JudgeOutcomeAndSuppressReasonInventoryConfigError(file_missing_reason_code) from None
    if not value:
        raise JudgeOutcomeAndSuppressReasonInventoryConfigError(file_empty_reason_code)
    values[value_key] = value
    return value


def _apply_retryable_selection(
    report: JudgeOutcomeAndSuppressReasonInventoryReport,
    selected: RetryableJudgeRunCandidate | None,
) -> JudgeOutcomeAndSuppressReasonInventoryReport:
    if selected is None:
        return replace(report, selected_retry_readiness="blocked_missing_context")
    safe_reason = _safe_reason_code_value(selected.finish_reason)
    return replace(
        report,
        selected_retryable_judge_run_fingerprint=_fingerprint(selected.judge_run_id),
        selected_retryable_judge_call_event_fingerprint=_fingerprint(selected.judge_call_event_id),
        selected_bundle_fingerprint=_fingerprint(selected.bundle_id),
        selected_candidate_group_fingerprint=_fingerprint(selected.candidate_group_id),
        selected_retryable_reason_code=safe_reason,
        selected_retry_readiness=_retry_readiness(selected, safe_reason),
    )


def _retry_readiness(selected: RetryableJudgeRunCandidate, safe_reason: str) -> str:
    if (
        selected.judge_call_event_id is None
        or selected.bundle_id is None
        or selected.candidate_group_id is None
        or selected.current_bundle_id is None
    ):
        return "blocked_missing_context"
    if selected.current_bundle_id != selected.bundle_id:
        return "blocked_stale_bundle"
    if selected.downstream_count > 0:
        return "blocked_downstream_exists"
    if safe_reason in {"missing", "other"} or safe_reason not in OPENAI_RETRYABLE_REASON_CODES:
        return "blocked_cooldown_unknown"
    return "ready_for_operator_openai_retry"


def _counts_to_report(counts: InventoryCounts) -> dict[str, Any]:
    finish_counts = _safe_finish_reason_counts(counts.judge_run_finish_reason_counts)
    status_counts = {
        "pending": 0,
        "running": 0,
        "succeeded": 0,
        "failed_retryable": 0,
        "failed_terminal": 0,
        "other": 0,
    }
    status_counts.update(_safe_count_mapping(counts.judge_run_status_counts))
    return {
        "judge_call_requested_v1_count": _int(counts.judge_call_requested_v1_count),
        "judge_run_status_counts": status_counts,
        "judge_run_finish_reason_counts": finish_counts,
        "retryable_finish_reason_counts": finish_counts.get("failed_retryable", []),
        "terminal_finish_reason_counts": finish_counts.get("failed_terminal", []),
        "succeeded_finish_reason_counts": finish_counts.get("succeeded", []),
        "judge_output_count": _int(counts.judge_output_count or counts.judge_outputs_count),
        "judge_outputs_count": _int(counts.judge_outputs_count or counts.judge_output_count),
        "judge_output_ready_event_count": _int(counts.judge_output_ready_event_count),
        "judge_output_missing_ready_event_count": _int(counts.judge_output_missing_ready_event_count),
        "ready_event_missing_output_count": _int(counts.ready_event_missing_output_count),
        "judge_run_succeeded_without_output_count": _int(counts.judge_run_succeeded_without_output_count),
        "judge_run_retryable_without_output_count": _int(counts.judge_run_retryable_without_output_count),
        "judge_run_terminal_without_output_count": _int(counts.judge_run_terminal_without_output_count),
        "analysis_policy_apply_v1_count": _int(counts.analysis_policy_apply_v1_count),
        "analysis_policy_apply_already_materialized_count": _int(
            counts.analysis_policy_apply_already_materialized_count
        ),
        "validator_passed_transition_count": _int(counts.validator_passed_transition_count),
        "validator_retryable_transition_reason_counts": _safe_reason_count_rows(
            counts.validator_retryable_transition_reason_counts
        ),
        "validator_terminal_transition_reason_counts": _safe_reason_count_rows(
            counts.validator_terminal_transition_reason_counts
        ),
        "analyses_count": _int(counts.analyses_count),
        "analyses_by_verdict_count": _safe_count_mapping(counts.analyses_by_verdict_count),
        "analyses_by_delivery_decision_count": _safe_count_mapping(
            counts.analyses_by_delivery_decision_count
        ),
        "policy_reason_code_counts": _safe_reason_count_rows(counts.policy_reason_code_counts),
        "suppress_reason_code_counts": _safe_reason_count_rows(counts.suppress_reason_code_counts),
        "skip_suppress_analysis_count": _int(counts.skip_suppress_analysis_count),
        "non_suppress_analysis_count": _int(counts.non_suppress_analysis_count),
        "notification_plan_created_v1_count": _int(counts.notification_plan_created_v1_count),
        "notification_intent_absence_expected_count": _int(counts.notification_intent_absence_expected_count),
        "notification_intent_absence_unexpected_count": _int(
            counts.notification_intent_absence_unexpected_count
        ),
        "notification_intent_unexpected_present_count": _int(counts.notification_intent_unexpected_present_count),
        "notification_plans_count": _int(counts.notification_plans_count),
        "notification_renders_count": _int(counts.notification_renders_count),
        "notification_delivery_records_count": _int(counts.notification_delivery_records_count),
        "eligible_judge_output_ready_count": _int(counts.eligible_judge_output_ready_count),
        "eligible_policy_apply_count": _int(counts.eligible_policy_apply_count),
    }


def _report(
    *,
    mode: str,
    status: str,
    reason_code: str,
    lookback_hours: int,
    sample_limit: int,
) -> JudgeOutcomeAndSuppressReasonInventoryReport:
    return JudgeOutcomeAndSuppressReasonInventoryReport(
        schema_version=SCHEMA_VERSION,
        mode=mode,
        status=status,
        reason_code=reason_code,
        lookback_hours=lookback_hours,
        sample_limit=sample_limit,
        counts=_counts_to_report(InventoryCounts()),
        selected_retryable_judge_run_fingerprint=None,
        selected_retryable_judge_call_event_fingerprint=None,
        selected_bundle_fingerprint=None,
        selected_candidate_group_fingerprint=None,
        selected_retryable_reason_code=None,
        selected_retry_readiness=None,
        redis_attempted=False,
        telegram_attempted=False,
        openai_attempted=False,
        external_network_attempted=False,
        db_write_attempted=False,
        redactions_applied=True,
        cleanup_completed=True,
    )


def _argument_report(reason_code: str) -> JudgeOutcomeAndSuppressReasonInventoryReport:
    return _report(
        mode="unknown",
        status="blocked",
        reason_code=reason_code,
        lookback_hours=72,
        sample_limit=100,
    )


def _finish_reason_counts_by_status(
    rows: Sequence[Mapping[str, Any]],
) -> dict[str, list[dict[str, int | str]]]:
    grouped: dict[str, dict[str, int]] = {
        "failed_retryable": {},
        "failed_terminal": {},
        "succeeded": {},
    }
    for row in rows:
        status = _safe_bucket_value(row.get("status_bucket"))
        if status not in grouped:
            status = "other"
            grouped.setdefault(status, {})
        reason = _safe_reason_code_value(row.get("reason_code"))
        grouped[status][reason] = grouped[status].get(reason, 0) + _int(row.get("reason_count"))
    return {
        status: _count_items_from_mapping(reason_counts)
        for status, reason_counts in grouped.items()
    }


def _safe_finish_reason_counts(
    values: Mapping[str, Sequence[Mapping[str, Any]]],
) -> dict[str, list[dict[str, int | str]]]:
    result: dict[str, list[dict[str, int | str]]] = {
        "failed_retryable": [],
        "failed_terminal": [],
        "succeeded": [],
    }
    for status, rows in values.items():
        safe_status = _safe_bucket_value(status)
        result[safe_status] = _safe_reason_count_rows(rows)
    return result


def _safe_reason_count_rows(rows: Sequence[Mapping[str, Any]]) -> list[dict[str, int | str]]:
    merged: dict[str, int] = {}
    for row in rows:
        reason = _safe_reason_code_value(row.get("reason_code"))
        merged[reason] = merged.get(reason, 0) + _int(row.get("reason_count", row.get("count")))
    return _count_items_from_mapping(merged)


def _count_items_from_mapping(values: Mapping[str, int]) -> list[dict[str, int | str]]:
    return [
        {"reason_code": reason, "count": count}
        for reason, count in sorted(values.items(), key=lambda item: (-item[1], item[0]))
        if count > 0
    ][:50]


def _retryable_candidate_from_row(row: Mapping[str, Any]) -> RetryableJudgeRunCandidate:
    return RetryableJudgeRunCandidate(
        judge_run_id=UUID(str(row["judge_run_id"])),
        judge_call_event_id=_uuid_or_none(row.get("judge_call_event_id")),
        bundle_id=_uuid_or_none(row.get("bundle_id")),
        candidate_group_id=_uuid_or_none(row.get("candidate_group_id")),
        current_bundle_id=_uuid_or_none(row.get("current_bundle_id")),
        finish_reason=str(row["finish_reason"]) if row.get("finish_reason") is not None else None,
        judge_output_count=_int(row.get("judge_output_count")),
        ready_event_count=_int(row.get("ready_event_count")),
        policy_event_count=_int(row.get("policy_event_count")),
        analysis_count=_int(row.get("analysis_count")),
        notification_intent_count=_int(row.get("notification_intent_count")),
        notification_plan_count=_int(row.get("notification_plan_count")),
        notification_render_count=_int(row.get("notification_render_count")),
        notification_delivery_record_count=_int(row.get("notification_delivery_record_count")),
    )


def _read(values: Mapping[str, str], key: str, default: str = "") -> str:
    return str(values.get(key, default)).strip()


def _optional_str(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _safe_count_mapping(values: Mapping[str, int]) -> dict[str, int]:
    return {_safe_bucket_value(key): _int(value) for key, value in values.items()}


def _safe_bucket_value(value: Any) -> str:
    text = str(value if value is not None else "missing").strip()
    if "://" in text or text.lower().startswith(("http:", "https:")):
        return "other"
    return text if SAFE_REASON_RE.fullmatch(text) else "other"


def _safe_reason_code_value(value: Any) -> str:
    return _safe_bucket_value(value)


def _int(value: Any) -> int:
    try:
        return max(int(value), 0)
    except (TypeError, ValueError):
        return 0


def _bounded_cli_int(value: Any, *, default: int) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _uuid_or_none(value: Any) -> UUID | None:
    if value is None:
        return None
    try:
        return UUID(str(value))
    except (TypeError, ValueError, AttributeError):
        return None


def _fingerprint(value: UUID | str | None) -> str | None:
    if value is None:
        return None
    return hashlib.sha256(str(value).encode("utf-8")).hexdigest()[:16]


def _compact_json(payload: Mapping[str, Any]) -> str:
    return json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def main(argv: Sequence[str] | None = None) -> int:
    return asyncio.run(run_cli(argv))


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())


__all__ = [
    "InventoryCounts",
    "JudgeOutcomeAndSuppressReasonInventoryComponents",
    "JudgeOutcomeAndSuppressReasonInventoryConfigError",
    "JudgeOutcomeAndSuppressReasonInventoryReport",
    "JudgeOutcomeAndSuppressReasonInventoryRequest",
    "RETRYABLE_SELECTION_CONFIRM_TOKEN",
    "RuntimeConfigBundle",
    "SCHEMA_VERSION",
    "STRICT_UUID_TEXT_SQL_RE",
    "RetryableJudgeRunCandidate",
    "SqlJudgeOutcomeAndSuppressReasonInventoryRepository",
    "_INVENTORY_COUNTS_SQL",
    "_RETRYABLE_SELECTION_SQL",
    "_fingerprint",
    "build_parser",
    "load_runtime_config",
    "main",
    "run_cli",
    "run_judge_outcome_and_suppress_reason_inventory",
]
