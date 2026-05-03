# Analysis Validator Runtime Smoke

This post-Stage44 smoke verifies the DB/Redis analysis-validator boundary:

`judge.output.ready.v1 -> outbox-relay -> q.analysis.validate -> analysis-validator -> analysis.policy.apply.v1`

It is a runtime verification harness, not live rollout authorization.

## Command

```bash
export GITHUB_AI_CATCHBOT_DB_SMOKE_DATABASE_URL='postgresql+psycopg://...@localhost:5432/github_ai_catchbot_smoke'
export REDIS_URL='redis://localhost:6379/14'

python scripts/ops/analysis_validator_runtime_smoke.py --confirm write
```

The database and Redis URLs can also be passed explicitly:

```bash
python scripts/ops/analysis_validator_runtime_smoke.py \
  --database-url 'postgresql+psycopg://...@localhost:5432/github_ai_catchbot_smoke' \
  --redis-url 'redis://localhost:6379/14' \
  --confirm write
```

## Safety

- Requires explicit `--confirm write`.
- Requires an explicit database URL from `--database-url` or `GITHUB_AI_CATCHBOT_DB_SMOKE_DATABASE_URL`.
- Requires an explicit Redis URL from `--redis-url` or `REDIS_URL`.
- Requires a local PostgreSQL URL whose database name contains `smoke`, `test`, or `dev`.
- Requires local Redis DB 14.
- Redacts DB and Redis URL fragments in JSON failures.
- It does not call OpenAI and does not require `OPENAI_API_KEY`.
- Does not call external network.
- Does not start Telegram collector, judge-openai, policy-engine, notifier, or transport.
- Does not reset the database or Redis.
- Does not clean unrelated rows.
- Seeds rows with `ops-smoke:analysis-validator-runtime:<smoke_id>`.

## Boundary Verified

- A pending `judge.output.ready.v1` outbox row routes through outbox-relay to `q.analysis.validate`.
- Redis Stream payload remains thin and only contains `job_id`, `stage_name`, `root_object_type`, `root_object_id`, `idempotency_key`, `pipeline_run_id`, `not_before`, and `trigger_event_id`.
- Redis payload does not include `payload_json`, judge output details, bundle details, candidate details, or credentials.
- The analysis-validator worker consumes the thin message and rehydrates `event_outbox` by `trigger_event_id`.
- The service rehydrates `judge_runs`, `judge_outputs`, and `candidate_evidence_bundles`.
- The seeded `judge_outputs.payload_json` validates against `judge_output_v1` schema and existing business rules.
- A `state_transitions` row with `to_state = analysis_validated` is recorded.
- A pending `analysis.policy.apply.v1` outbox row is emitted with `judge_run_id`, `judge_output_id`, `candidate_group_id`, and `bundle_id`.
- The Redis message is acked.
- No `analyses`, `notification_plans`, `notification_renders`, or `notification_delivery_records` rows are created.
- The seeded `judge_outputs` row is not mutated by validator processing.

The final stdout is a JSON report. Success requires:

```json
{
  "checks_failed": [],
  "failures": []
}
```

## Expected JSON Shape

The report includes:

- `report_type: analysis_validator_runtime_smoke_v1`
- `smoke_id` and `marker`
- `database_url_redacted: true`
- `redis_url_redacted: true`
- `queue_name: q.analysis.validate`
- `seeded_ids`
- `redis_stream_message_id`
- `redis_message_ids`
- `downstream_outbox_ids`
- `db_postcondition_counts`
- `forbidden_side_effect_counts`
- `judge_output_mutated`
- `checks_passed`
- `checks_failed`
- `failures`

## Cleanup

The smoke intentionally leaves controlled marker-scoped rows for traceability. Cleanup, if needed, should be a separate operator action scoped only to exact `ops-smoke:analysis-validator-runtime:` markers.
