from __future__ import annotations

from typing import Final


MAINTENANCE_STALE_OUTBOX_HYGIENE_STAGE: Final = "maintenance_stale_outbox_hygiene"
MAINTENANCE_QUEUE_NAME: Final = "q.maintenance"
EVENT_OUTBOX_ROOT_OBJECT_TYPE: Final = "event_outbox"

JUDGE_OUTPUT_READY_EVENT_TYPE: Final = "judge.output.ready.v1"
POLICY_APPLY_EVENT_TYPE: Final = "analysis.policy.apply.v1"

JUDGE_OUTPUT_READY_STALE_PROOF_ERROR_CODE: Final = (
    "stale_outbox_judge_output_ready_already_handed_off_logical_noop"
)
POLICY_APPLY_STALE_PROOF_ERROR_CODE: Final = "stale_outbox_policy_apply_already_analyzed_logical_noop"

STALE_OUTBOX_RELAY_EXCLUSION_ERROR_CODES: Final[dict[str, str]] = {
    JUDGE_OUTPUT_READY_EVENT_TYPE: JUDGE_OUTPUT_READY_STALE_PROOF_ERROR_CODE,
    POLICY_APPLY_EVENT_TYPE: POLICY_APPLY_STALE_PROOF_ERROR_CODE,
}

STALE_OUTBOX_CLASSIFICATION_ERROR_CODES: Final[dict[str, str]] = {
    "judge_output_ready_already_handed_off": JUDGE_OUTPUT_READY_STALE_PROOF_ERROR_CODE,
    "policy_apply_already_analyzed": POLICY_APPLY_STALE_PROOF_ERROR_CODE,
}


def stale_resolution_proof_error_code_for_classification(classification: str) -> str | None:
    return STALE_OUTBOX_CLASSIFICATION_ERROR_CODES.get(classification)


def stale_resolution_proof_error_code_for_event_type(event_type: str) -> str | None:
    return STALE_OUTBOX_RELAY_EXCLUSION_ERROR_CODES.get(event_type)


def stale_resolution_exclusion_exists_sql(outbox_alias: str = "eo") -> str:
    """Return the exact maintenance proof predicate that suppresses relay eligibility."""

    return f"""
EXISTS (
    SELECT 1
    FROM job_attempts stale_resolution_proof
    WHERE stale_resolution_proof.stage_name = '{MAINTENANCE_STALE_OUTBOX_HYGIENE_STAGE}'
      AND stale_resolution_proof.queue_name = '{MAINTENANCE_QUEUE_NAME}'
      AND stale_resolution_proof.root_object_type = '{EVENT_OUTBOX_ROOT_OBJECT_TYPE}'
      AND stale_resolution_proof.root_object_id = {outbox_alias}.event_id
      AND stale_resolution_proof.attempt_status = 'succeeded'::job_attempt_status_enum
      AND (
        (
          {outbox_alias}.event_type = '{JUDGE_OUTPUT_READY_EVENT_TYPE}'
          AND stale_resolution_proof.error_code = '{JUDGE_OUTPUT_READY_STALE_PROOF_ERROR_CODE}'
        )
        OR (
          {outbox_alias}.event_type = '{POLICY_APPLY_EVENT_TYPE}'
          AND stale_resolution_proof.error_code = '{POLICY_APPLY_STALE_PROOF_ERROR_CODE}'
        )
      )
)
""".strip()


def stale_resolution_exclusion_not_exists_sql(outbox_alias: str = "eo") -> str:
    return f"NOT {stale_resolution_exclusion_exists_sql(outbox_alias)}"


def canonical_relay_eligible_sql(outbox_alias: str = "eo") -> str:
    return (
        f"{outbox_alias}.status = 'pending'::outbox_status_enum "
        f"AND {stale_resolution_exclusion_not_exists_sql(outbox_alias)}"
    )
