# Delivery Gate Full Runtime Smoke

## Purpose

`delivery_gate_full_runtime_smoke_v1` is a narrow DB-backed acceptance smoke for the existing `maintenance delivery-gate --mode full` control-plane path. It verifies that `DeliveryGateRunner.run(mode="full", operator_review_passed=...)` returns a clean pass when operator review is supplied and a warning when it is missing.

## Scope

- Seeds marker-scoped synthetic PostgreSQL fixture rows for the existing delivery gate snapshot queries.
- Runs the existing full-mode delivery gate runner against the smoke DB.
- Verifies the full metric set, operator-review pass branch, operator-review warning branch, and output-only recommended flag patch.
- Leaves marker-scoped fixture rows for manual inspection.

## Non-goals

- This is not a new delivery gate feature implementation.
- This is not a runtime worker smoke.
- This does not re-open restricted-gate, batch-recovery, retry, or replay slices.
- This does not authorize production rollout.
- This does not clean up fixture rows.

## Preconditions

- Use only a local dev/test/smoke PostgreSQL database.
- Apply Alembic head before running the smoke.
- Provide the database URL with `--database-url` or `GITHUB_AI_CATCHBOT_DB_SMOKE_DATABASE_URL`.
- The target DB gate window must be otherwise clean: no recent non-marker delivery records, due retry rows, open delivery DLQ rows, replay guard rejects, retry-ceiling DLQ rows, or unexpected send-disabled rows.
- Redis is not required.

## Smoke DB Setup Reference

```bash
cd ~/workspace/bots/github_ai_catchbot
source venv/bin/activate

python -m alembic upgrade head
python -m alembic current
```

Use a database name containing `smoke`, `test`, or `dev`, hosted on `localhost`, `127.0.0.1`, or `::1`.

## Command Example

```bash
python scripts/ops/delivery_gate_full_runtime_smoke.py \
  --database-url "$GITHUB_AI_CATCHBOT_DB_SMOKE_DATABASE_URL"
```

`--confirm write` is intentionally not required. The script performs controlled marker-scoped fixture seeding, then the delivery gate path is read/report-only.

## Expected JSON Success Shape

```json
{
  "report_type": "delivery_gate_full_runtime_smoke_v1",
  "selected_scenario": "full_pass_with_operator_review",
  "checks_failed": [],
  "failures": [],
  "gate_report_summary": {
    "full_pass": {
      "mode": "full",
      "gate_status": "pass",
      "blocking_reason_codes": [],
      "warning_reason_codes": [],
      "operator_review_required": true,
      "operator_review_passed": true
    },
    "full_warn_without_operator_review": {
      "mode": "full",
      "gate_status": "warn",
      "blocking_reason_codes": [],
      "warning_reason_codes": ["delivery_gate_operator_review_required"],
      "operator_review_required": true,
      "operator_review_passed": false
    }
  },
  "recommended_flag_patch": {
    "ENABLE_NOTIFICATION_SEND": true,
    "MAINTENANCE_ENABLE_NOTIFICATION_RETRY_PROMOTION": true,
    "NOTIFIER_TELEGRAM_DRY_RUN": false
  }
}
```

The observed full-mode metric names must include:

- `success_rate_1h`
- `high_source_to_delivery_p95_sec`
- `plan_to_transport_p95_sec`
- `due_retry_oldest_lag_sec`
- `open_delivery_dlq_count`
- `unexpected_send_disabled_count`
- `success_rate_24h`
- `replay_guard_reject_count_24h`
- `retry_ceiling_exceeded_count_24h`
- `oldest_delivery_dlq_age_sec`
- `duplicate_noop_ratio_1h`

## Safety Boundaries

- Redis is not required.
- No Redis messages are published.
- No collector, notifier, maintenance, replay, or delivery worker is started.
- No notifier worker is started.
- No Telegram Bot API call is made.
- No OpenAI call is made.
- No GitHub or X API call is made.
- No external network call is made.
- No feature flags or env files are mutated.
- `recommended_flag_patch` is output-only and is not applied.
- No `event_outbox` rows are emitted by the gate runner.
- No `notification_renders` rows are created by the gate runner.
- No extra `notification_delivery_records` rows are created by the gate runner beyond the seeded fixture row.
- No `replay_requests`, `dead_letter_entries`, or `state_transitions` rows are created by the gate runner.

## Failure Interpretation

- `safety.database_url_required`: provide `--database-url` or `GITHUB_AI_CATCHBOT_DB_SMOKE_DATABASE_URL`.
- `safety.local_smoke_database_url_guard`: use a local PostgreSQL smoke/test/dev database, not a production-like URL.
- `db.precondition_clean_full_gate_window`: the local smoke DB contains non-marker rows that can affect the global delivery gate snapshot; use a clean smoke DB.
- `gate.full_pass.*`: the full-mode pass branch no longer matches the accepted operator-review contract.
- `gate.full_warn_without_operator_review.*`: the missing-operator-review branch no longer emits the required warning contract.
- `db.marker_*`: the gate runner produced an unexpected side effect after fixture seeding.

## Rollout Boundary

Passing this smoke proves only local DB-backed full-mode gate behavior for the seeded marker scenario. It does not authorize production rollout, production feature flag changes, notifier startup, Telegram delivery, or any external API use.
