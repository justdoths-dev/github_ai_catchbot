# Router Normalizer Runtime Smoke

## Purpose

`scripts/ops/router_normalizer_runtime_smoke.py` is an opt-in post-Stage44 runtime verification slice for the Redis consumer boundary:

```text
q.source.normalize thin Redis Stream message
-> router-normalizer consumer
-> event_outbox rehydration by trigger_event_id
-> source_messages / source_message_versions rehydration
-> deterministic normalizer outputs
-> artifact.enrich.requested.v1 outbox handoff
```

This is not Stage 45 and does not authorize a live rollout by itself.

## Safety Boundary

The smoke writes only controlled smoke data marked with `ops-smoke:router-normalizer-runtime:<uuid>`:

- one synthetic `source_messages` row
- one `source_message_versions` row for version 1
- one already-published `source_message.created.v1` `event_outbox` row
- one thin Redis Stream message on `q.source.normalize`
- deterministic router-normalizer output rows, including a pending downstream `artifact.enrich.requested.v1` row

It aborts before writing if any pending `event_outbox` row exists or if any known Redis queue in Redis DB 14 is non-empty. It does not start live workers and does not call Telegram, OpenAI, GitHub, X, or Web.

## Safe Dev/Test Usage

Use a migrated dev/test PostgreSQL database and disposable local Redis DB 14:

```bash
redis-cli ping
redis-cli -n 14 DBSIZE

python scripts/ops/router_normalizer_runtime_smoke.py \
  --database-url "$GITHUB_AI_CATCHBOT_DB_SMOKE_DATABASE_URL" \
  --redis-url "redis://localhost:6379/14" \
  --confirm write \
  --format json
```

Do not use production PostgreSQL or production Redis. The script refuses production-like URL markers and refuses Redis URLs other than `redis://localhost:6379/14`.

## Redis Payload Contract

The Redis payload is ID-only and must include exactly:

- `job_id`
- `stage_name`
- `root_object_type`
- `root_object_id`
- `idempotency_key`
- `pipeline_run_id`
- `not_before`
- `trigger_event_id`

It must not include `payload_json`, source text, `raw_message_json`, database URLs, Redis URLs, passwords, tokens, or secret-like fields. Router-normalizer must rehydrate business input from PostgreSQL using `trigger_event_id`.

## Expected JSON Shape

JSON mode emits a redacted report shaped like:

```json
{
  "report_type": "router_normalizer_runtime_smoke_v1",
  "checks_run": [],
  "checks_passed": [],
  "checks_failed": [],
  "failures": [],
  "warnings": [],
  "database_url_redacted": true,
  "redis_url_redacted": true,
  "mutation_safety": "controlled smoke write only: ...",
  "queue_name": "q.source.normalize",
  "stream_message_id": "1710000000000-0",
  "smoke_source_message_id": "00000000-0000-0000-0000-000000000000",
  "smoke_event_id": "00000000-0000-0000-0000-000000000000",
  "normalization_run_id": "00000000-0000-0000-0000-000000000000",
  "candidate_group_id": "00000000-0000-0000-0000-000000000000",
  "primary_artifact_id": "00000000-0000-0000-0000-000000000000",
  "downstream_event_id": "00000000-0000-0000-0000-000000000000"
}
```

Passing output has empty `checks_failed` and `failures`, `database_url_redacted: true`, `redis_url_redacted: true`, and `queue_name: q.source.normalize`.

## Expected Runtime Proof

A passing run proves:

- Redis DB 14 was used.
- The source Redis message was consumed and acknowledged by the smoke consumer group.
- `normalization_runs` exists for the smoke source message/version and `router-normalizer-v1`.
- `signal_detected` and `candidate_eligible` are true.
- `trigger_strength` is `strong` for the direct GitHub URL path.
- `artifact_registry` contains the primary GitHub repo artifact.
- `artifact_observations`, `candidate_group_proposals`, and `candidate_group_members` contain the smoke output rows.
- `event_outbox` contains a pending `artifact.enrich.requested.v1` row with `provider_route: github`.
- No positive-path suppression trace was written.

## Cleanup

The smoke intentionally does not silently clean up rows. A successful run may leave controlled smoke DB rows and a pending downstream enrich request for traceability. Any cleanup should be an explicit, separate operation scoped only to exact `ops-smoke:router-normalizer-runtime:` markers.
