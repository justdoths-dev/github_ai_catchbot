# Dedicated VPS post-migration DB acceptance smoke

## Purpose

Verify the upgraded PostgreSQL database is structurally usable after Alembic
upgrade before app runtime, TDLib, live collector, notifier, or production
rollout.

This is a repo-local operator runbook for a future separately approved VPS
execution. Codex and reviewers must not run VPS commands from this repository
slice.

## Scope

The scope is a read-only DB metadata/queryability smoke after the recorded
manual Alembic upgrade reached head `0004_judge_delivery_obs`.

The future smoke is limited to:

- loading runtime configuration inside a redacted Python helper without
  printing values.
- opening a read-only PostgreSQL connection only after separate approval.
- verifying schema metadata and queryability using facts derived from
  `migrations/versions/*.py`.
- printing only a redacted JSON result.

Checked runbook path:

```text
ops/pipeline/runbooks/dedicated_vps_post_migration_db_acceptance_smoke.md
```

## Source-of-truth handling

`docs/project-source/README_replacement_consolidated_v0_20.md` remains
authoritative. The locked project-source bundles `00~10` remain the
source-of-truth set and are interpreted in filename order. The GitHub AI
application plan remains advisory only.

The canonical architecture invariant remains unchanged:

```text
SourceMessage -> Artifact -> CandidateGroup -> EvidenceBundle -> JudgeOutput -> Analysis -> Notification
```

Layer boundaries remain unchanged:

- PostgreSQL is durable source of record.
- Redis is queue, lock, and short-lived execution state only.
- production rollout remains unauthorized.
- this package does not re-diagnose or reimplement DB/Redis provisioning,
  runtime secret placement, Alembic current preflight, Alembic upgrade gate,
  Alembic upgrade execution result recording, package-vs-Docker decisions, or
  Desktop-only VPS access policy.

## Preconditions

- VPS already fast-forwarded to current repo HEAD.
- Runtime secret file exists outside repo at
  `/etc/github-ai-catchbot/runtime.env`.
- Alembic upgrade already completed and recorded.
- Alembic upgrade execution result reached
  `0004_judge_delivery_obs (head)`.
- Operator approval is required before future DB acceptance execution.
- This package does not authorize execution now.

## Non-goals

- No app runtime.
- No TDLib.
- No Telegram.
- No live collector.
- No notifier transport.
- No Redis mutation.
- No Alembic mutation.
- No Alembic upgrade.
- No Alembic downgrade.
- No Alembic stamp.
- No Alembic revision.
- No production rollout.
- No Docker or Docker Compose.
- No systemd modification.
- No migration edits.
- No DB mutation.

## Secret handling

- Do not `cat /etc/github-ai-catchbot/runtime.env`.
- Do not `source /etc/github-ai-catchbot/runtime.env`.
- Do not dot-source `/etc/github-ai-catchbot/runtime.env`.
- Do not `export DATABASE_URL`.
- Do not `export REDIS_URL`.
- Do not print `DATABASE_URL`.
- Do not print DB password.
- Do not print any secret value.
- Use a redacted Python helper only.

## Expected future smoke behavior

Future execution of the smoke on VPS is separately approved later. This package
does not authorize execution now.

After separate approval only, the future smoke should:

- Load runtime env from `/etc/github-ai-catchbot/runtime.env` inside Python
  without printing values.
- Connect to PostgreSQL read-only using SQLAlchemy/psycopg if available in the
  venv.
- Verify Alembic current/version state is `0004_judge_delivery_obs`.
- Verify `alembic_version` exists.
- Derive expected table, index, and simple constraint facts from
  `migrations/versions/*.py`, not from memory.
- Verify expected tables exist.
- Verify key tables are queryable via metadata-only reads or `SELECT COUNT(*)`.
- Perform no writes.
- Perform no Redis mutation.
- Start no services.
- Print only redacted JSON result.

## Future separately approved runner command

The future operator runner exists at:

```text
scripts/ops/dedicated_vps_post_migration_db_acceptance_smoke_runner.py
```

Codex must not execute this runner against the VPS DB. The runner must not be
executed without separate operator approval, and the explicit approval flag is
required for future read-only DB smoke execution.

Future/separately approved only:

```bash
python scripts/ops/dedicated_vps_post_migration_db_acceptance_smoke_runner.py \
  --approved-read-only-db-smoke \
  --format json
```

Runner constraints:

- Without `--approved-read-only-db-smoke`, the runner must not read
  `/etc/github-ai-catchbot/runtime.env`, must not connect to PostgreSQL, must
  not connect to Redis, must not run Alembic, and must return redacted JSON
  indicating approval is required.
- With `--approved-read-only-db-smoke`, the runner reads runtime.env inside
  Python only. It must not `cat`, `source`, dot-source, or `export` values from
  runtime.env.
- The runner must not print `DATABASE_URL`, DB password, or secret values.
- The runner is read-only and performs no writes.
- The runner must not connect to Redis.
- The runner must not run Alembic.
- The runner must not start app runtime, TDLib, Telegram, live collector,
  notifier transport, Docker, systemd, or production rollout.
- Expected operator output is redacted JSON only.
- If the runner fails, stop and bring the redacted JSON back to ChatGPT.

## Completed redacted result record

The separately approved dedicated VPS read-only DB acceptance smoke has since
been executed and passed. The redacted result is recorded at:

```text
ops/pipeline/runbooks/dedicated_vps_post_migration_db_acceptance_smoke_result_record.md
```

That result record does not authorize app runtime, TDLib, Telegram, live
collector, notifier transport, production rollout, Alembic mutation, DB
mutation, Redis mutation, Docker, systemd changes, migration edits, or secret
printing.

Expected future JSON safety flags must include:

```json
{
  "secret_values_printed": false,
  "database_url_printed": false,
  "db_write_performed": false,
  "redis_connected": false,
  "redis_mutation_performed": false,
  "alembic_upgrade_run": false,
  "alembic_downgrade_run": false,
  "alembic_stamp_run": false,
  "alembic_revision_run": false,
  "app_runtime_started": false,
  "tdlib_auth_performed": false,
  "telegram_connected": false,
  "live_collector_started": false,
  "notifier_transport_enabled": false,
  "production_rollout_performed": false
}
```

## Explicitly forbidden actions

- Do not run SSH from this package.
- Do not execute VPS commands from Codex.
- Do not connect to PostgreSQL during repo-local package validation.
- Do not connect to Redis during repo-local package validation.
- Do not run Alembic during repo-local package validation.
- Do not `cat /etc/github-ai-catchbot/runtime.env`.
- Do not `source /etc/github-ai-catchbot/runtime.env`.
- Do not dot-source `/etc/github-ai-catchbot/runtime.env`.
- Do not `export DATABASE_URL`.
- Do not `export REDIS_URL`.
- Do not print `DATABASE_URL`.
- Do not print DB password.
- Do not print secret values.
- Do not run `alembic upgrade` in the future smoke.
- Do not run `alembic downgrade` in the future smoke.
- Do not run `alembic stamp` in the future smoke.
- Do not run `alembic revision` in the future smoke.
- Do not run app runtime.
- Do not run TDLib auth.
- Do not connect Telegram.
- Do not start live collector.
- Do not enable notifier transport.
- Do not perform production rollout.
- Do not use Docker or Docker Compose.
- Do not modify systemd units.
- Do not edit migration files.
- Do not mutate the database.
- Do not mutate Redis.

## Repo-local checker

The repo-local checker for this runbook is:

```text
scripts/ops/dedicated_vps_post_migration_db_acceptance_smoke_check.py
```

The checker is repo-text-only. It may read this runbook and
`migrations/versions/*.py`. It must not read environment variables, must not
read `/etc/github-ai-catchbot/runtime.env`, must not connect to PostgreSQL or
Redis, must not run Alembic, must not start runtime services, must not execute
shell commands, and must not inspect host state.

## Failure handling

If the future separately approved smoke reports any unexpected table, revision,
queryability, redaction, or safety flag failure, stop. Do not start app runtime,
TDLib, Telegram, live collector, notifier transport, Docker, systemd changes,
or production rollout as part of this package.

## What output to bring back to ChatGPT

Bring back only redacted JSON and a concise operator summary. Do not include
runtime.env contents, `DATABASE_URL`, DB password, secret values, raw VPS
network identifiers, or local shell history.

## What remains unauthorized

This package does not authorize app runtime, TDLib, Telegram, live collector,
notifier transport, production rollout, Docker, Docker Compose, systemd changes,
migration edits, Alembic mutation, DB mutation, Redis mutation, or secret
printing.

## Next step

The current next recommended slice is
`dedicated_vps_runtime_environment_consumer_preflight`.
