# Dedicated VPS runtime environment consumer preflight

## Purpose

Prepare the repo-local ops/control-plane package for a future separately
approved runtime environment consumer preflight on the dedicated VPS.

This package validates whether the deployed runtime environment can be consumed
safely before any app runtime or live service is started. It does not prove app
runtime readiness and does not authorize runtime startup.

## Scope

This package adds a safe-by-default Python runner, this runbook, and focused
tests. The runner validates key presence, non-secret URL shape metadata, and
the prod pre-runtime safety flag posture.

The locked architecture remains unchanged:

```text
SourceMessage -> Artifact -> CandidateGroup -> EvidenceBundle -> JudgeOutput -> Analysis -> Notification
```

PostgreSQL remains the durable source of record. Redis remains queue, lock, and
short-lived execution state only.

This package does not start app runtime. This package does not authorize TDLib, Telegram, live collector, notifier transport, or production rollout.
It does not authorize Docker, systemd changes, Alembic, DB writes, Redis
mutation, or service execution.

## Source-of-truth handling

`docs/project-source/README_replacement_consolidated_v0_20.md` remains
authoritative. The locked project-source bundles `00~10` remain the
source-of-truth set and are interpreted in filename order. The GitHub AI
application plan remains advisory only.

This package consumes the existing dedicated VPS operational sequence:

- DB/Redis provisioning passed.
- runtime secret placement passed.
- Alembic upgrade passed.
- post-migration DB acceptance smoke passed.

The runner reads no project `.env` file and does not inspect process env vars.
It only reads the explicitly approved runtime env file path when the future
operator approval flag is present.

## Preconditions

Before a future operator execution, all of the following must already be true:

- DB/Redis provisioning passed.
- runtime secret placement passed.
- Alembic upgrade passed.
- post-migration DB acceptance smoke passed.
- The reviewed package has been committed, pushed, and pulled on the VPS.
- The operator has separate approval to run the runtime environment consumer
  preflight.

## Runner safety model

The runner is safe-by-default and requires
`--approved-runtime-env-consumer-preflight`.

Without approval, the runner reads no runtime.env and connects nowhere. It does
not inspect process env vars. It returns redacted JSON with
`contract_status: approval_required`.

With approval, the runner reads runtime.env inside Python only and prints redacted JSON only. It parses key/value lines locally. It does not use `cat`, `source`, dot-source, or `export`. It does not mutate the process environment globally.

The runner validates key presence and value shapes only. It validates safety flag posture for the prod pre-runtime baseline. It does not connect DB/Redis. It does not run Alembic. It does not mutate runtime.env.

The runner does not print `DATABASE_URL`, `REDIS_URL`, DB password, Redis credentials, secret values, raw server IP, or raw operator IP.

## Expected runtime env keys

Required keys:

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

Optional keys:

```text
LOG_LEVEL
ENABLE_LATER_DELIVERY
ENABLE_SILENT_LATER
NOTIFICATION_RETRY_MAX_ATTEMPTS
MAINTENANCE_NOTIFICATION_RETRY_POLL_SEC
MAINTENANCE_BATCH_SIZE
MAINTENANCE_BLOCK_MS
```

Telegram, TDLib, OpenAI, GitHub, and X secrets are not required in this slice.
If such keys are present, the runner reports key names only as
`optional_sensitive_keys_present` and warns that those consumers remain
unauthorized.

## Expected prod pre-runtime baseline

Expected safety values:

```text
APP_ENV=prod
ENABLE_NOTIFICATION_SEND=false
NOTIFIER_TELEGRAM_DRY_RUN=false
NOTIFIER_TELEGRAM_ALLOW_EDITS=true
ENABLE_REPLAY_TO_PROD_DB=false
MAINTENANCE_ENABLE_NOTIFICATION_RETRY_PROMOTION=false
```

`NOTIFIER_TELEGRAM_DRY_RUN=false` is acceptable in this prod pre-runtime
baseline because `ENABLE_NOTIFICATION_SEND=false` blocks actual transport.

`NOTIFIER_TELEGRAM_ALLOW_EDITS=true` is acceptable because actual transport
remains disabled.

Production delivery remains disabled. Replay-to-prod remains disabled.
Maintenance retry promotion remains disabled. App/runtime, TDLib, Telegram,
live collector, and notifier transport remain not started.

## URL shape checks

`DATABASE_URL` must be present, must not be printed, and must use the
SQLAlchemy-compatible `postgresql+psycopg` scheme.

Expected non-secret database URL shape:

- credentials are present, but never printed.
- username is `github_ai_catchbot_app`.
- host is loopback only: `127.0.0.1` or `localhost`.
- port is `5432`.
- database name is `github_ai_catchbot`.

The runner does not connect to PostgreSQL.

`REDIS_URL` must be present, must not be printed, and must use the `redis`
scheme.

Expected non-secret Redis URL shape:

- host is loopback only: `127.0.0.1` or `localhost`.
- port is `6379`.
- Redis DB index may be present and is recorded only if safely parseable and
  non-secret.

The runner does not connect to Redis.

## Future approved command

Future/separately approved only:

```bash
python scripts/ops/dedicated_vps_runtime_environment_consumer_preflight_runner.py \
  --approved-runtime-env-consumer-preflight \
  --format json
```

Codex must not execute this runner against the real VPS runtime.env during
implementation or validation.

## Failure handling

If preflight fails, stop and bring the redacted JSON back to ChatGPT.

Do not edit runtime.env based only on this runbook. Do not suggest changing
runtime.env unless the future approved runner actually reports a failure and
the redacted JSON has been reviewed.

Do not proceed to app runtime, TDLib, Telegram, live collector, notifier
transport, production rollout, Alembic, DB mutation, Redis mutation, Docker, or
systemd changes.

## Passing result boundary

Passing this preflight does not authorize runtime start.

A passed preflight only means:

- required key names are present.
- `DATABASE_URL` and `REDIS_URL` shapes match the expected dedicated VPS local
  topology.
- prod pre-runtime safety flags are in the expected posture.
- downstream runtime consumers have enough redacted shape metadata to proceed
  to the next reviewed slice.

The next slice after a passed preflight result record should be a separately reviewed app/runtime import/config preflight or TDLib auth package, depending on review.

## Completed redacted result record

The approved runtime environment consumer preflight PASS is recorded in
`ops/pipeline/runbooks/dedicated_vps_runtime_environment_consumer_preflight_result_record.md`.

The next bounded slice is
`dedicated_vps_app_runtime_import_config_preflight`. This result record does not
authorize TDLib auth, Telegram connection, live collector startup, notifier
transport, or production rollout.

## What remains unauthorized

This package does not authorize:

- SSH
- VPS command execution by Codex
- PostgreSQL connection
- Redis connection
- Alembic
- app runtime startup
- TDLib auth
- Telegram connection
- live collector startup
- notifier transport
- Docker or Docker Compose
- systemd changes
- production rollout
- migration file modification
- runtime.env mutation
- secret printing
