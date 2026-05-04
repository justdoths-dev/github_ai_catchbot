# maintenance runtime smoke

This runbook covers Post-Stage44 Runtime Verification Slice 11 for the `maintenance` delivery-result boundary.
It is a controlled runtime verification harness, not notifier delivery, not live Telegram transport, and not a production rollout.

## Boundary

The harness verifies this runtime path:

```text
notification.delivery.result.v1
-> outbox-relay route/publish
-> q.maintenance
-> maintenance worker consumes a thin Redis message
-> worker rehydrates event_outbox by trigger_event_id
-> maintenance reads notification_plans / notification_delivery_records
-> retryable due delivery result is classified by existing maintenance code
-> pending notification.plan.created.v1 retry-intent outbox event is emitted
-> Redis message is acknowledged
```

The selected scenario is `retryable_due_promotion` because current maintenance code supports `failed_retryable` plans with `send_after <= now` and emits retry intents through `notification.plan.created.v1`.

## Safety

Required safeguards:

- Explicit smoke database URL via `--database-url` or `GITHUB_AI_CATCHBOT_DB_SMOKE_DATABASE_URL`.
- Explicit Redis URL via `--redis-url` or `REDIS_URL`.
- Redis DB 14 must be local: `redis://localhost:6379/14`.
- `APP_ENV=smoke`.
- `MAINTENANCE_ENABLE_NOTIFICATION_RETRY_PROMOTION=true`.
- No OpenAI key, Telegram token, GitHub credentials, or X credentials are required.
- The harness does not call OpenAI.
- The harness does not call GitHub, X, Telegram, or any external network.
- The harness must not call the Telegram Bot API.
- The harness does not start notifier-telegram, policy-engine, replay worker, or Telegram transport workers.
- Do not use production DB or production Redis.

## Command

Run after the smoke database is migrated to Alembic head:

```bash
cd ~/workspace/bots/github_ai_catchbot
source venv/bin/activate

python scripts/ops/maintenance_runtime_smoke.py \
  --database-url "$GITHUB_AI_CATCHBOT_DB_SMOKE_DATABASE_URL" \
  --redis-url "${REDIS_URL:-redis://localhost:6379/14}" \
  --confirm write
```

The script prints a single JSON report with `report_type=maintenance_runtime_smoke_v1`.
It exits non-zero if `checks_failed` or `failures` is non-empty.

## Expected Checks

The JSON report includes:

- `selected_scenario=retryable_due_promotion`
- seeded IDs for source, artifact, snapshot, candidate, bundle, judge output, analysis, notification plan, notification delivery record, and trigger event
- Redis stream message IDs
- DB postcondition counts for `notification_plans`, `notification_delivery_records`, `notification_renders`, `job_attempts`, `dead_letter_entries`, `replay_requests`, `pipeline_runs`, and retry-intent outbox rows
- mutation booleans for `notification_plans`, `notification_delivery_records`, `analyses`, `judge_outputs`, and `candidate_group_proposals`
- emitted maintenance output outbox IDs and payloads

Expected postconditions:

- The seeded `notification.delivery.result.v1` outbox row is `published`.
- The Redis payload contains only `job_id`, `stage_name`, `root_object_type`, `root_object_id`, `idempotency_key`, `pipeline_run_id`, `not_before`, and `trigger_event_id`.
- `payload_json` is not placed in Redis.
- For the delivery-result maintenance handoff, `root_object_type=notification_plan` and `root_object_id=<notification_plan_id>`.
- The maintenance worker processes and acknowledges exactly one `q.maintenance` message.
- Exactly one pending `notification.plan.created.v1` retry-intent outbox row exists.
- The retry-intent payload matches the existing maintenance retry contract, including `retry_reason=due_retry_promotion` and `retry_attempt=2`.
- The retry-intent dedupe key is stable and scoped to the seeded notification plan.
- No `notification_renders` row is created by maintenance.
- No second `notification_delivery_records` row is created by maintenance.
- No `dead_letter_entries` row is created for this retryable due path.
- No `replay_requests` row is created.
- The seeded `notification_plans`, `notification_delivery_records`, `analyses`, `judge_outputs`, and `candidate_group_proposals` rows are not mutated by maintenance processing.
