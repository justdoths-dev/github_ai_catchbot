# Outbox Redis Routing Smoke

## Purpose

`scripts/ops/outbox_redis_routing_smoke.py` is an opt-in post-Stage44 runtime verification slice for the durable PostgreSQL `event_outbox` to `outbox-relay` to Redis Streams boundary.

This is not Stage 45 and does not authorize a live rollout by itself.

## Safety Boundary

The smoke writes only controlled smoke data:

- one `event_outbox` row with a dedupe key prefixed by `ops-smoke:outbox-redis-routing:`
- the expected status update for that row from `pending` to `published`
- one matching `job_attempts` row with `attempt_status = succeeded`
- one Redis Stream message on `q.source.normalize`

It aborts before writing if any pending `event_outbox` rows already exist, including stale smoke rows. It does not start live workers and does not call Telegram, OpenAI, GitHub, X, or Web.

## Safe Dev/Test Usage

Use a migrated dev/test PostgreSQL database and a disposable local Redis DB. The preferred local Redis URL is DB 15:

```bash
redis-cli ping

python scripts/ops/outbox_redis_routing_smoke.py \
  --database-url "$GITHUB_AI_CATCHBOT_DB_SMOKE_DATABASE_URL" \
  --redis-url "redis://localhost:6379/15" \
  --confirm write \
  --format json
```

Do not use production PostgreSQL or production Redis. The script refuses URLs containing production-like markers such as `prod` or `production`.

## Expected Contract

The inserted event is `source_message.created.v1`. The expected route is:

- queue: `q.source.normalize`
- stage: `normalize`

The Redis payload must stay ID-only and include:

- `job_id`
- `stage_name`
- `root_object_type`
- `root_object_id`
- `idempotency_key`
- `pipeline_run_id`
- `not_before`
- `trigger_event_id`

The Redis payload must not include `payload_json`, raw message text, database URLs, Redis URLs, passwords, tokens, or secret-like fields.

## Expected JSON Shape

JSON mode emits a redacted report shaped like:

```json
{
  "report_type": "outbox_redis_routing_smoke_v1",
  "checks_run": [],
  "checks_passed": [],
  "checks_failed": [],
  "failures": [],
  "warnings": [],
  "database_url_redacted": true,
  "redis_url_redacted": true,
  "mutation_safety": "controlled smoke write only: inserts one event_outbox smoke row, publishes that row through one bounded outbox-relay pass, writes one succeeded job_attempts row, and writes one Redis Stream message",
  "queue_name": "q.source.normalize",
  "stream_message_id": "1710000000000-0",
  "smoke_event_id": "00000000-0000-0000-0000-000000000000"
}
```

Passing output has empty `checks_failed` and `failures`, `database_url_redacted: true`, and `redis_url_redacted: true`.

## Local Redis Setup

If Redis is not installed in WSL, install and start it separately:

```bash
sudo apt update
sudo apt install -y redis-server redis-tools
sudo service redis-server start || sudo systemctl start redis-server
redis-cli ping
```

If Redis is unavailable, do not fake the runtime smoke result. Run the unit/import validations and treat the live Redis smoke as blocked until Redis is installed.
