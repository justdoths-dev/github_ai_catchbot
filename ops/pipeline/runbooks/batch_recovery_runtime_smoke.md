# Batch Recovery Runtime Smoke

## Purpose

This runbook covers the opt-in DB-backed acceptance smoke for:

```bash
maintenance batch-recovery retry-selected-due
```

The smoke verifies the existing `DeliveryBatchRecoveryTool.retry_selected_due()` control-plane path against a local PostgreSQL smoke database. It proves that one selected due `failed_retryable` notification plan emits one pending `notification.plan.created.v1` manual retry-intent outbox row, and that a duplicate rerun is skipped by the existing manual retry dedupe key.

## Scope

- Scenario: `retry_selected_due_minimal`
- Report type: `batch_recovery_runtime_smoke_v1`
- Script: `scripts/ops/batch_recovery_runtime_smoke.py`
- DB source: `--database-url` or `GITHUB_AI_CATCHBOT_DB_SMOKE_DATABASE_URL`
- Confirmation: `--confirm write`

The fixture inserts a marker-scoped minimal chain:

```text
SourceMessage -> Artifact -> CandidateGroup -> EvidenceBundle -> JudgeOutput -> Analysis -> Notification
```

The selected notification plan is seeded as `failed_retryable`, has `send_after <= now`, has one latest `failed_retryable` delivery record below the retry ceiling, has no open replay request, has no delivery DLQ, and does not carry a send-disabled marker.

## Non-Goals

- This is not a new batch recovery implementation.
- This does not exercise `replay-selected`.
- This does not directly mutate `notification_plans`.
- This does not create notification renders, delivery sends, replay requests, dead letters, or state transitions.
- This does not start live collector, notifier, maintenance, replay, or delivery workers.
- This does not call Telegram Bot API, OpenAI, GitHub, X, or any external network.
- This does not authorize production rollout.

## Preconditions

- Use a local dev/test/smoke PostgreSQL database only.
- Apply Alembic head to the smoke database before running.
- Export `GITHUB_AI_CATCHBOT_DB_SMOKE_DATABASE_URL` in the same shell, or pass `--database-url`.
- The database URL host must be `localhost`, `127.0.0.1`, or `::1`.
- The database name must include `smoke`, `test`, or `dev`.
- Do not use production DB or production-like hostnames.

## Smoke DB Setup Reference

Use the same local PostgreSQL smoke DB preparation used by the other post-Stage44 runtime smokes. Run migrations to Alembic head before invoking this script. The smoke intentionally leaves marker-scoped rows whose marker starts with:

```text
ops-smoke:batch-recovery:
```

Those rows are left for manual inspection. Do not add broad cleanup tooling for this smoke.

## Command Example

```bash
cd ~/workspace/bots/github_ai_catchbot
source venv/bin/activate

python scripts/ops/batch_recovery_runtime_smoke.py \
  --database-url "$GITHUB_AI_CATCHBOT_DB_SMOKE_DATABASE_URL" \
  --confirm write
```

## Expected JSON Success Shape

Successful output is one JSON object on stdout with:

```json
{
  "report_type": "batch_recovery_runtime_smoke_v1",
  "selected_scenario": "retry_selected_due_minimal",
  "checks_failed": [],
  "failures": [],
  "database_url_redacted": true,
  "mutation_safety_fields": {
    "redis_required": false,
    "runtime_workers_started": false,
    "telegram_bot_api_called": false,
    "openai_called": false,
    "github_or_x_api_called": false,
    "notification_renders_created": false,
    "extra_notification_delivery_records_created": false,
    "replay_requests_created": false,
    "dead_letter_entries_created": false,
    "state_transitions_created": false
  },
  "batch_recovery_result_summary": {
    "recovery_mode": "retry-selected-due",
    "selected_count": 1,
    "accepted_count": 1,
    "emitted_count": 1,
    "skipped_count": 0,
    "skipped_reason_codes": {}
  },
  "manual_retry_intent_summary": {
    "after_first_run_count": 1,
    "after_second_run_count": 1
  }
}
```

The observed outbox row must be:

- `event_type = notification.plan.created.v1`
- `aggregate_type = notification_plan`
- `aggregate_id = notification_plan_id`
- `status = pending`

The payload must include `notification_plan_id`, `analysis_id`, `candidate_group_id`, `delivery_decision`, `urgency_profile`, `target_chat_id`, `target_thread_id`, `render_profile`, `dedupe_subject_key`, `material_change_hash`, `send_after = null`, `retry_reason = manual_selected_due_retry`, `previous_attempt_count`, and `recovery_batch_id`.

The dedupe key must follow:

```text
notify:manual-retry-intent:{notification_plan_id}:{attempt_count}:{send_after_epoch}
```

`recovery_batch_id` must be present in the payload but must not be used as the dedupe key source.

## Safety Boundaries

- Redis is not required.
- No Redis messages are published.
- No notifier worker is started.
- No Telegram Bot API call is made.
- No OpenAI call is made.
- No GitHub or X API call is made.
- No feature flags or env files are mutated.
- The smoke writes only marker-scoped fixture rows plus the expected manual retry-intent outbox row.
- The smoke does not authorize production rollout.

## Failure Interpretation

- `safety.database_url_required`: set `GITHUB_AI_CATCHBOT_DB_SMOKE_DATABASE_URL` in the same shell or pass `--database-url`.
- `safety.local_smoke_database_url_guard`: the DB URL is missing the local smoke/test/dev boundary or looks production-like.
- `db.exactly_one_manual_retry_intent_outbox_row`: the existing recovery tool did not emit exactly one manual retry-intent outbox row.
- `batch_recovery.second_result_idempotent_duplicate_skip`: the duplicate rerun did not hit the expected existing manual retry-intent skip.
- Forbidden side-effect count failures mean the smoke observed writes outside the expected DB boundary and should block promotion review.

Passing this smoke is acceptance evidence for the local DB-backed control-plane path only. It is not a production rollout approval.
