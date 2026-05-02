# DB-Backed Acceptance Smoke

## Purpose

`scripts/ops/delivery_db_acceptance_smoke.py` is an opt-in post-Stage44 runtime acceptance smoke for a migrated PostgreSQL database. It verifies repository SQL compatibility, maintenance CLI help/import compatibility, required delivery tables and columns, enum casts, delivery gate query shape, and batch recovery SELECT query shape.

This is not a new architecture stage and does not authorize a live rollout by itself.

## Safe Dev/Test Usage

Run it against a migrated dev or test database:

```bash
python scripts/ops/delivery_db_acceptance_smoke.py --database-url "$DATABASE_URL" --format json
python scripts/ops/delivery_db_acceptance_smoke.py --database-url "$DATABASE_URL" --check schema
python scripts/ops/delivery_db_acceptance_smoke.py --database-url "$DATABASE_URL" --check delivery-gate
python scripts/ops/delivery_db_acceptance_smoke.py --database-url "$DATABASE_URL" --check maintenance-cli
```

The smoke is SELECT-only. It must not write to `notification_plans`, `notification_renders`, `notification_delivery_records`, `state_transitions`, `replay_requests`, `event_outbox`, `dead_letter_entries`, `job_attempts`, or `pipeline_runs`.

Do not run this casually against production. A passing smoke means the schema and query surfaces compile against that database; it does not mean production rollout is approved.

## Expected Output

JSON mode emits:

```json
{
  "report_type": "delivery_db_acceptance_smoke_v1",
  "checks_run": [],
  "checks_passed": [],
  "checks_failed": [],
  "failures": [],
  "warnings": [],
  "database_url_redacted": true,
  "mutation_safety": "select_only"
}
```

Values can be null or zero on an empty migrated database. The smoke checks query shape and compatibility, not business-data success.

## Network And Credential Boundary

This smoke does not send Telegram, OpenAI, GitHub, X, or Web requests. It does not require live Telegram/OpenAI/GitHub/X credentials and it does not start collector, notifier, maintenance worker, or live transport loops.

## Next Manual Checks After Pass

- Review `ops/delivery/runbooks/delivery_gate_handoff.md`.
- Review delivery DLQ rows and retry backlog manually.
- Confirm operator review requirements before any full rollout.
- Run the normal delivery gate command separately when an operator intends to evaluate rollout readiness.
