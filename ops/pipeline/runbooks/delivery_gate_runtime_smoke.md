# Delivery Gate Runtime Smoke

This runbook covers Post-Stage44 Control-Plane Verification Slice 12 for the existing delivery gate runner.

This is a one-shot DB acceptance harness, not a runtime worker smoke, and does not authorize production rollout. Redis is not required. It seeds a marker-scoped `restricted_pass_minimal` delivery-line fixture into a local smoke PostgreSQL database, runs the existing `DeliveryGateRunner` once in restricted mode, and prints a deterministic JSON report.

## Command

```bash
python scripts/ops/delivery_gate_runtime_smoke.py \
  --database-url "$GITHUB_AI_CATCHBOT_DB_SMOKE_DATABASE_URL" \
  --confirm write
```

`--database-url` may be omitted only when `GITHUB_AI_CATCHBOT_DB_SMOKE_DATABASE_URL` is set in the same shell. The URL must point to local PostgreSQL on `localhost`, `127.0.0.1`, or `::1`, and the database name must contain `smoke`, `test`, or `dev`.

## Scenario

Selected scenario: `restricted_pass_minimal`.

The fixture inserts marker-scoped rows for source, artifact, candidate, bundle, judge, analysis, one `notification_plan`, and one `sent` `notification_delivery_records` row. Timestamps are deterministic relative to run time so the delivery gate can observe:

- `success_rate_1h` passing
- `high_source_to_delivery_p95_sec` passing
- `due_retry_oldest_lag_sec` passing
- `open_delivery_dlq_count = 0`
- `unexpected_send_disabled_count = 0`
- `gate_status = pass`
- empty blocking and warning reason codes
- `recommended_flag_patch` with exactly `ENABLE_NOTIFICATION_SEND`, `MAINTENANCE_ENABLE_NOTIFICATION_RETRY_PROMOTION`, and `NOTIFIER_TELEGRAM_DRY_RUN`

The current runner also emits the Stage 43 compatibility metric `plan_to_transport_p95_sec`; the smoke reports the full observed order and separately checks the locked restricted core order.

## Safety

The smoke does not require Redis and does not start collector, outbox-relay, router-normalizer, enrichers, evidence-assembler, analysis-router, judge-openai, validator, policy-engine, notifier, maintenance workers, or Telegram transport.

It does not call OpenAI, GitHub, X, Telegram Bot API, or external network. It does not read API credentials, does not mutate feature flags, does not write `.env` files, does not auto-apply `recommended_flag_patch`, create notification renders, publish `event_outbox`, create dead-letter rows, create replay requests, or create state transitions.

The local database gate window must be clean enough for deterministic restricted-pass scoring. If recent non-marker delivery rows, due retry plans, open delivery DLQ rows, or unexpected send-disabled rows are present, the harness exits non-zero with JSON rather than cleaning or modifying unrelated rows.

## Output

The script prints JSON with:

- `report_type = delivery_gate_runtime_smoke_v1`
- `selected_scenario = restricted_pass_minimal`
- `checks_failed` and `failures`
- marker and seeded IDs
- gate report summary
- observed and locked metric order
- `gate_status`
- blocking and warning reason codes
- `recommended_flag_patch`
- mutation safety fields
- DB precondition and postcondition counts

Exit code is non-zero whenever `checks_failed` or `failures` is non-empty.
