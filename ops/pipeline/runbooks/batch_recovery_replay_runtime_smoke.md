# Batch Recovery Replay Runtime Smoke

## Purpose

This runbook verifies the existing Post-Stage44 maintenance control-plane path:

```text
maintenance batch-recovery replay-selected
-> DeliveryBatchRecoveryTool.replay_selected()
-> replay_requests row with replay_type = delivery
```

The smoke proves that a selected `failed_terminal` notification plan is accepted for replay and creates exactly one open replay request. It also proves a duplicate rerun does not create a second replay request.

## Scope

- Scenario: `replay_selected_minimal`
- Report type: `batch_recovery_replay_runtime_smoke_v1`
- Script: `scripts/ops/batch_recovery_replay_runtime_smoke.py`
- DB URL env var: `GITHUB_AI_CATCHBOT_DB_SMOKE_DATABASE_URL`
- Control-plane path under test: `DeliveryBatchRecoveryTool.replay_selected()`
- Expected recovery mode: `replay-selected`
- Expected replay request:
  - `replay_type = delivery`
  - `root_object_type = notification_plan`
  - `root_object_id = <seeded notification_plan_id>`
  - `requested_by = ops-smoke`
  - `status = requested`

## Non-Goals

- This is not a new batch recovery feature implementation.
- This does not exercise `retry-selected-due`.
- This does not test runtime worker consumption of `replay_requests`.
- This does not verify Telegram delivery.
- This does not authorize production rollout.
- This does not clean up marker-scoped fixture rows.

`failed_retryable` rows belong to `retry-selected-due`, not `replay-selected`.

## Preconditions

- Run from the repository root.
- Activate the repo virtual environment.
- PostgreSQL smoke database is local and disposable.
- Alembic migrations have been applied to the smoke DB.
- `GITHUB_AI_CATCHBOT_DB_SMOKE_DATABASE_URL` points to a local dev/test/smoke PostgreSQL database, or pass `--database-url`.
- The command must include `--confirm write`.

The smoke rejects production-like database URLs. Use a local host such as `localhost`, `127.0.0.1`, or `::1`, and a database name containing `smoke`, `test`, or `dev`.

## Smoke DB Setup Reference

```bash
cd ~/workspace/bots/github_ai_catchbot
source venv/bin/activate

python -m alembic upgrade head
python -m alembic current
```

## Command Example

```bash
python scripts/ops/batch_recovery_replay_runtime_smoke.py \
  --database-url "$GITHUB_AI_CATCHBOT_DB_SMOKE_DATABASE_URL" \
  --confirm write
```

## Expected JSON Success Shape

```json
{
  "report_type": "batch_recovery_replay_runtime_smoke_v1",
  "selected_scenario": "replay_selected_minimal",
  "checks_failed": [],
  "failures": [],
  "database_url_redacted": true,
  "batch_recovery_result_summary": {
    "recovery_mode": "replay-selected",
    "selected_count": 1,
    "accepted_count": 1,
    "skipped_count": 0,
    "emitted_count": 1,
    "skipped_reason_codes": {}
  },
  "replay_request_summary": {
    "after_first_run_count": 1,
    "after_second_run_count": 1,
    "status": "requested"
  }
}
```

Expected passing checks include:

- `batch_recovery.first_result_accepts_and_emits_one`
- `batch_recovery.second_result_idempotent_open_replay_skip`
- `db.exactly_one_replay_request_row`
- `db.replay_request_row_contract`
- `db.duplicate_rerun_inserted_no_second_replay_request_row`
- `db.notification_plan_row_unchanged_after_replay`
- `db.notification_delivery_record_row_unchanged_after_replay`
- `db.marker_notification_plan_created_intents`
- `db.marker_notification_renders`
- `db.marker_dead_letter_entries`
- `db.marker_state_transitions`

## Safety Boundaries

- Redis is not required.
- No Redis messages are published.
- No collector is started.
- No notifier worker is started.
- No runtime worker is started.
- No Telegram Bot API call is made.
- No OpenAI call is made.
- No GitHub or X API call is made.
- No feature flags or env files are mutated.
- No `notification.plan.created.v1` retry-intent is expected.
- No `notification_renders` rows are expected.
- No extra `notification_delivery_records` rows are expected beyond the seeded failed terminal record.
- No `dead_letter_entries` rows are expected.
- No `state_transitions` rows are expected.

The smoke leaves marker-scoped fixture rows in the local smoke DB for manual inspection.

## Failure Interpretation

- `safety.database_url_required`: pass `--database-url` or export `GITHUB_AI_CATCHBOT_DB_SMOKE_DATABASE_URL`.
- `safety.local_smoke_database_url_required`: the URL did not look like a local dev/test/smoke PostgreSQL database.
- `batch_recovery.first_result_accepts_and_emits_one`: the selected failed terminal row was not accepted by replay-selected or did not insert one replay request.
- `batch_recovery.second_result_idempotent_open_replay_skip`: duplicate replay did not skip because of the existing open replay request.
- `db.replay_request_row_contract`: the inserted `replay_requests` row did not match the expected delivery replay contract.
- `db.notification_plan_row_unchanged_after_replay`: maintenance mutated the notifier-owned notification plan row.
- `db.notification_delivery_record_row_unchanged_after_replay`: maintenance mutated the notifier-owned delivery record row.
- `db.marker_notification_plan_created_intents`: replay-selected emitted a retry-intent outbox row; that belongs to retry-selected-due, not this path.

Any failure is a local smoke failure only. It does not prove production state, and it does not authorize production rollout.
