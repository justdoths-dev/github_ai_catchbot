# Analysis Router Runtime Smoke

This post-Stage44 smoke verifies the DB/Redis analysis-router boundary. It does not call OpenAI, external network, live Telegram collector, notifier transport, judge-openai, validator, policy-engine, enrichers, router-normalizer, collector, or evidence-assembler.

## Command

```bash
export GITHUB_AI_CATCHBOT_DB_SMOKE_DATABASE_URL='postgresql+psycopg://...@localhost:5432/github_ai_catchbot_smoke'
export REDIS_URL='redis://localhost:6379/14'

python scripts/ops/analysis_router_runtime_smoke.py --confirm write
```

`--redis-url redis://localhost:6379/14` can be used instead of `REDIS_URL`.

## Safety

- Requires explicit `--confirm write`.
- Reads the database URL only from `GITHUB_AI_CATCHBOT_DB_SMOKE_DATABASE_URL`.
- Requires a local PostgreSQL URL whose database name contains `smoke`, `test`, or `dev`.
- Requires local Redis DB 14.
- Redacts DB and Redis URL fragments in JSON failures.
- Does not reset the database or Redis.
- Does not clean unrelated rows.
- Seeds rows with `ops-smoke:analysis-router-runtime:<smoke_id>`.

## Boundary Verified

- A pending `analysis.requested.v1` outbox row routes through outbox-relay to `q.analysis.route`.
- Redis Stream payload remains thin and ID-only.
- Redis payload does not include the full `event_outbox.payload_json`.
- The analysis-router worker consumes the thin message and rehydrates `event_outbox` by `trigger_event_id`.
- The seeded `candidate_group_proposals.current_bundle_id` matches the requested `bundle_id`.
- The seeded `candidate_evidence_bundles.ready_for_analysis` is true.
- `judge_profile=github_primary` is accepted by the allowlist.
- `judge_runs` is created with the default model, default reasoning effort, locked prompt version, `judge_output_v1`, `verdict_policy_v1`, and the locked prompt cache key.
- A pending `judge.call.requested.v1` outbox row is emitted.
- The Redis message is acked.
- No OpenAI call, external network call, final analysis, judge output, delivery decision, notification plan, notification render, or notification delivery row is created.

The final stdout line is a JSON report. Success requires:

```json
{
  "checks_failed": [],
  "failures": []
}
```

## Expected JSON Shape

The report includes:

- `report_type: analysis_router_runtime_smoke_v1`
- `smoke_id` and `marker`
- `database_url_redacted: true`
- `redis_url_redacted: true`
- `checks_passed`
- `checks_failed`
- `failures`
- `queue_name: q.analysis.route`
- `seeded_ids`
- `resulting_judge_run_ids`
- `downstream_outbox_ids`
- `forbidden_side_effect_counts`

## Cleanup

The smoke intentionally leaves controlled smoke rows for traceability. Cleanup, if needed, should be a separate operator action scoped only to exact `ops-smoke:analysis-router-runtime:` markers.
