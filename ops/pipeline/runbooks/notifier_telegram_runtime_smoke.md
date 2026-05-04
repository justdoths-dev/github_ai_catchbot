# notifier-telegram runtime smoke

This runbook covers the Post-Stage44 `notifier-telegram` runtime verification slice.
It is a dry-run/replay-safe harness, not a production rollout and not live Telegram delivery.

## Boundary

The harness verifies this runtime path:

```text
notification.plan.created.v1
-> outbox-relay route/publish
-> q.notification.send
-> notifier-telegram worker consumes a thin Redis message
-> worker rehydrates event_outbox by trigger_event_id
-> notifier-telegram rehydrates analyses / judge_outputs / candidate_group_proposals / artifact_registry / source_messages
-> notification_plans row is concretized
-> notification_renders row is created
-> dry-run path records notification_delivery_records without Telegram transport
-> state_transitions are recorded
-> pending notification.delivery.result.v1 outbox event is emitted
-> Redis message is acknowledged
```

The downstream `notification.delivery.result.v1` route resolves to `q.maintenance`, but this smoke does not run maintenance.

## Safety

Required safeguards:

- Explicit smoke database URL via `--database-url` or `GITHUB_AI_CATCHBOT_DB_SMOKE_DATABASE_URL`.
- Explicit Redis URL via `--redis-url` or `REDIS_URL`.
- Redis DB 14 must be local: `redis://localhost:6379/14`.
- `APP_ENV=smoke`.
- `ENABLE_NOTIFICATION_SEND=false`.
- `NOTIFIER_TELEGRAM_DRY_RUN=true`.
- `NOTIFIER_TELEGRAM_ALLOW_EDITS=false`.
- No OpenAI key, Telegram token, GitHub credentials, or X credentials are required.
- The harness does not call OpenAI.
- The harness injects no Telegram client and must not call the Telegram Bot API.
- Do not use production DB or production Redis.

## Command

Run after the smoke database is migrated to Alembic head:

```bash
cd ~/workspace/bots/github_ai_catchbot
source venv/bin/activate

python scripts/ops/notifier_telegram_runtime_smoke.py \
  --database-url "$GITHUB_AI_CATCHBOT_DB_SMOKE_DATABASE_URL" \
  --redis-url "${REDIS_URL:-redis://localhost:6379/14}" \
  --confirm write
```

The script prints a single JSON report with `report_type=notifier_telegram_runtime_smoke_v1`.
It exits non-zero if `checks_failed` or `failures` is non-empty.

## Expected Checks

The JSON report includes:

- seeded IDs for source, artifact, snapshot, candidate, bundle, judge output, analysis, notification plan, and trigger event
- Redis stream message IDs
- DB postcondition counts for `notification_plans`, `notification_renders`, `notification_delivery_records`, `state_transitions`, and `notification.delivery.result.v1`
- transport safety fields proving dry-run/no-transport mode
- mutation booleans for `analyses`, `judge_outputs`, and `candidate_group_proposals`
- notification plan/render/delivery IDs
- pending `notification.delivery.result.v1` outbox IDs

Expected postconditions:

- The seeded `notification.plan.created.v1` outbox row is `published`.
- The Redis payload contains only `job_id`, `stage_name`, `root_object_type`, `root_object_id`, `idempotency_key`, `pipeline_run_id`, `not_before`, and `trigger_event_id`.
- For the initial policy-engine notification intent, `root_object_type=analysis` and `root_object_id=<analysis_id>`, matching the current `policy-engine` event_outbox writer.
- `payload_json` is not placed in Redis.
- The notifier worker processes and acknowledges exactly one `q.notification.send` message.
- Exactly one `notification_plans` row exists for the seeded plan.
- Exactly one `notification_renders` row exists for the seeded plan.
- Exactly one `notification_delivery_records` row exists for the seeded plan.
- Delivery status is `suppressed` with `dry_run_skip_transport` metadata.
- `telegram_response_json` identifies dry-run/no-transport behavior.
- `state_transitions` includes rendered and dry-run suppressed transitions.
- Exactly one pending `notification.delivery.result.v1` outbox row exists.
- The downstream result payload matches the notifier maintenance/observability handoff contract.
- The seeded `analyses`, `judge_outputs`, and `candidate_group_proposals` rows are not mutated by notifier processing.
