# Dedicated VPS runtime environment consumer preflight result record

## Scope

Record the redacted result of the separately approved dedicated VPS runtime
environment consumer preflight that was already executed once and passed.

This is a docs/test-only result-record slice. This repository slice does not
read `/etc/github-ai-catchbot/runtime.env`, does not rerun the preflight, and
does not perform runtime, network, DB, Redis, Alembic, Docker, systemd, TDLib,
Telegram, notifier, or rollout operations.

`docs/project-source/README_replacement_consolidated_v0_20.md` remains
authoritative. The locked architecture remains unchanged:

```text
SourceMessage -> Artifact -> CandidateGroup -> EvidenceBundle -> JudgeOutput -> Analysis -> Notification
```

Layer boundaries remain unchanged:

- collector preserves raw Telegram source messages and revisions only.
- outbox-relay publishes thin ID-only Redis Stream messages only.
- router-normalizer is deterministic and non-LLM.
- enrichers gather evidence only.
- evidence-assembler assembles candidate-centered evidence bundles and may
  reroot only within its contract.
- LLM judge produces structured `judge_output_v1` only.
- deterministic policy-engine computes final `analysis_v1` verdict and
  `delivery_decision`.
- notifier is presentation and delivery only.
- PostgreSQL is durable system of record.
- Redis is queue, lock, and short-lived execution state only.
- replay creates new runs or versions and never overwrites historical truth.
- production has exactly one live Telegram collector instance.
- `recommended_flag_patch` is output-only and must not be auto-applied.
- production rollout remains unauthorized.

## Source result being recorded

The approved runtime environment consumer preflight was executed once on the
dedicated VPS with this approved command:

```bash
python scripts/ops/dedicated_vps_runtime_environment_consumer_preflight_runner.py --approved-runtime-env-consumer-preflight --format json
```

The approved Python runner read `/etc/github-ai-catchbot/runtime.env` inside the
operator execution. The approved runner did not print runtime env values, full
connection URLs, credential material, raw public VPS IPs, or operator IPs.

This result-record slice records only redacted pass facts and shape metadata.
It does not inspect process env vars and does not read the runtime env file.

## Result summary

```yaml
contract_status: passed
checks_failed: []
failures: []
warnings: []
app_env_seen: prod
runtime_env_read: true
runtime_env_values_printed: false
secret_values_printed: false
process_env_inspected: false
required_keys_missing: []
runtime_env_key_count: 8
```

`runtime_env_read: true` applies only to the approved operator runner execution
described above. This repository result-record slice did not read
`/etc/github-ai-catchbot/runtime.env` and did not rerun the preflight.

## Approved operator execution facts

The approved operator preflight validated only key presence, redacted URL shape
metadata, feature flag posture, safety profile posture, and downstream consumer
readiness facts.

It did not connect to PostgreSQL. It did not connect to Redis. It did not write
to PostgreSQL. It did not mutate Redis. It did not run Alembic. It did not start
the app runtime. It did not perform TDLib auth. It did not connect to Telegram.
It did not start the live collector. It did not enable notifier transport. It
did not use Docker. It did not modify systemd. It did not perform production
rollout.

## Redacted key and shape facts

Required keys present by name:

```text
APP_ENV
DATABASE_URL
REDIS_URL
ENABLE_NOTIFICATION_SEND
NOTIFIER_TELEGRAM_DRY_RUN
NOTIFIER_TELEGRAM_ALLOW_EDITS
ENABLE_REPLAY_TO_PROD_DB
MAINTENANCE_ENABLE_NOTIFICATION_RETRY_PROMOTION
```

DATABASE_URL shape metadata:

```yaml
database_url:
  present: true
  scheme: postgresql+psycopg
  username: github_ai_catchbot_app
  host: 127.0.0.1
  port: 5432
  database: github_ai_catchbot
  has_credentials: true
  loopback_only: true
  full_value_printed: false
```

REDIS_URL shape metadata:

```yaml
redis_url:
  present: true
  scheme: redis
  host: 127.0.0.1
  port: 6379
  database_index: 0
  loopback_only: true
  full_value_printed: false
```

Shape metadata is allowed. Full connection URL values are not recorded.

## Feature flag facts

```yaml
feature_flags:
  ENABLE_NOTIFICATION_SEND: false
  NOTIFIER_TELEGRAM_DRY_RUN: false
  NOTIFIER_TELEGRAM_ALLOW_EDITS: true
  ENABLE_REPLAY_TO_PROD_DB: false
  MAINTENANCE_ENABLE_NOTIFICATION_RETRY_PROMOTION: false
```

`NOTIFIER_TELEGRAM_DRY_RUN=false` is acceptable in the prod pre-runtime baseline
only because `ENABLE_NOTIFICATION_SEND=false` is the actual transport blocker.
This result does not infer notifier transport authorization from
`NOTIFIER_TELEGRAM_DRY_RUN=false`. Re-evaluate it only in a future
notifier/restricted-delivery slice.

## Safety profile facts

```yaml
safety_profile: prod_pre_runtime
safety_profile_passed: true
```

## Consumer readiness facts

```yaml
consumer_profile:
  database_consumers_ready: true
  redis_consumers_ready: true
  notification_transport_disabled: true
  replay_to_prod_disabled: true
  maintenance_retry_promotion_disabled: true
  runtime_start_authorized: false
  tdlib_authorized: false
  telegram_authorized: false
  live_collector_authorized: false
  notifier_transport_authorized: false
  production_rollout_authorized: false
```

## Explicit side-effect denials

```yaml
side_effects:
  database_connected: false
  redis_connected: false
  db_write_performed: false
  redis_mutation_performed: false
  alembic_run: false
  app_runtime_started: false
  tdlib_auth_performed: false
  telegram_connected: false
  live_collector_started: false
  notifier_transport_enabled: false
  production_rollout_performed: false
  docker_used: false
  systemd_modified: false
  migration_files_modified: false
```

No DB connection happened in this preflight. No Redis connection happened in
this preflight. No DB write happened. No Redis mutation happened. No Alembic
happened. No app runtime happened. No TDLib auth happened. No Telegram
connection happened. No live collector happened. No notifier transport
happened. No Docker or systemd change happened. No production rollout happened.

## Redaction guarantees

- Key names are recorded.
- Boolean pass/fail facts are recorded.
- Loopback shape `127.0.0.1`, ports `5432` and `6379`, database name
  `github_ai_catchbot`, username `github_ai_catchbot_app`, and scheme names are
  recorded.
- Full DB and Redis URL values are not recorded.
- DB credential material is not recorded.
- Redis credential material is not recorded.
- Telegram, OpenAI, GitHub, X, and TDLib credential material is not recorded.
- Raw public VPS IP values are not recorded.
- Raw operator IP values are not recorded.
- Runtime env file contents are not recorded.
- Runtime env values were not printed by the approved runner.
- Secret values were not printed by the approved runner.

## Non-authorizations

Passing this result does not authorize runtime start.
Passing this result does not authorize TDLib auth.
Passing this result does not authorize Telegram connection.
Passing this result does not authorize live collector startup.
Passing this result does not authorize notifier transport.
Passing this result does not authorize production rollout.

This record also does not authorize DB mutation, Redis mutation, Alembic,
Docker, systemd changes, migration file modification, runtime env mutation, or
credential printing.

## Next bounded slice

Next likely slice:

```text
dedicated_vps_app_runtime_import_config_preflight
```

That next slice is an app/runtime import/config preflight only. It must not
start runtime, perform TDLib auth, connect to Telegram, start the live
collector, enable notifier transport, connect DB/Redis, run Alembic, or
authorize production rollout.

Alternative later slice:

```text
separately reviewed TDLib auth package
```

Do not jump directly to live collector or production rollout. Do not proceed to
TDLib auth, Telegram connection, live collector startup, notifier transport, or
production rollout without a separately approved slice.

## Anti-overconservatism check

If this result record and focused contract tests pass their redaction and
boundary checks, do not add another diagnostic, checker, runtime probe, or
preflight for marginal certainty. Move to the next bounded implementation slice:
`dedicated_vps_app_runtime_import_config_preflight`.
