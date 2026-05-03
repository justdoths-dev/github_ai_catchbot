# Judge OpenAI Runtime Smoke

This runbook covers the post-Stage44 judge-openai boundary smoke:

`judge.call.requested.v1 -> outbox-relay -> q.analysis.judge -> judge-openai -> judge_outputs -> judge.output.ready.v1`

The smoke is an operator-triggered verification harness. It is not live rollout authorization and does not replace the validator, policy engine, or notifier checks.

## Safety Contract

- Requires explicit `--confirm write`.
- Reads the PostgreSQL URL only from `GITHUB_AI_CATCHBOT_DB_SMOKE_DATABASE_URL`.
- Uses `REDIS_URL` or `--redis-url`.
- Refuses production-like URLs.
- Requires local Redis DB 14, for example `redis://localhost:6379/14`.
- Redacts DB and Redis URLs from all final JSON output.
- It does not call the real OpenAI API.
- Does not require `OPENAI_API_KEY`.
- Does not call external network.
- Does not start Telegram collector or notifier transport.
- Does not reset PostgreSQL or Redis.
- Does not clean unrelated rows.

## Boundary Verified

The smoke seeds marker-scoped rows for:

- `source_messages` and `source_message_versions` only to satisfy source-side FK conventions.
- `artifact_registry` with canonical ID `github:repo:octocat/judge-openai-smoke-<marker>`.
- `artifact_snapshots` only as a seeded fixture.
- `candidate_group_proposals` and `candidate_group_members`.
- `candidate_evidence_bundles` with deterministic GitHub summary data.
- `candidate_evidence_members`.
- `judge_runs` in `pending` state.
- pending `event_outbox` row with `event_type = judge.call.requested.v1`.

The outbox relay route publishes a thin Redis Stream message to `q.analysis.judge`. The Redis payload must only include transport fields such as `job_id`, `stage_name`, `root_object_type`, `root_object_id`, `idempotency_key`, `pipeline_run_id`, `not_before`, and `trigger_event_id`; it must not include `payload_json`, `judge_run_id`, `bundle_id`, model parameters, evidence summaries, or credentials.

The judge-openai worker then consumes the thin message, rehydrates `event_outbox` by `trigger_event_id`, loads `judge_runs`, loads `candidate_evidence_bundles`, builds model context from that evidence bundle only, calls a deterministic fake client, appends one `judge_outputs` row, updates `judge_runs` telemetry, emits pending `judge.output.ready.v1`, and acks the Redis message.

The smoke also checks that no `analyses`, `notification_plans`, `notification_renders`, or `notification_delivery_records` rows are created.

## Command

```bash
export GITHUB_AI_CATCHBOT_DB_SMOKE_DATABASE_URL='postgresql+psycopg://user:password@localhost:5432/github_ai_catchbot_smoke'
export REDIS_URL='redis://localhost:6379/14'

python scripts/ops/judge_openai_runtime_smoke.py --confirm write
```

To pass Redis explicitly:

```bash
python scripts/ops/judge_openai_runtime_smoke.py --redis-url 'redis://localhost:6379/14' --confirm write
```

## Success Shape

The script prints one final JSON object. A successful run has:

- `report_type = judge_openai_runtime_smoke_v1`
- `database_url_redacted = true`
- `redis_url_redacted = true`
- `queue_name = q.analysis.judge`
- `checks_failed = []`
- `failures = []`
- `resulting_judge_output_ids` containing one ID
- `downstream_outbox_ids` containing the pending `judge.output.ready.v1` ID
- `fake_openai_calls` containing one deterministic fake call
- `forbidden_side_effect_counts` all zero

If `GITHUB_AI_CATCHBOT_DB_SMOKE_DATABASE_URL` is missing, the script reports a failed safety check instead of attempting a live smoke.
