# Dedicated VPS Alembic current preflight result record

## Scope

This is a repo-local operational result record for the dedicated VPS redacted
Alembic current preflight that was manually run on the dedicated VPS.

Codex did not execute VPS commands for this record. Codex did not SSH, inspect
host files, read secret files, inspect environment variables, connect to
PostgreSQL or Redis, run Alembic, start app runtime, perform TDLib auth, connect
Telegram, start the live collector, enable notifier transport, perform
production rollout, use Docker or Docker Compose, or modify systemd units.

This record is docs-only. It records the already completed manual preflight
result and does not authorize Alembic upgrade, Alembic stamp, Alembic revision,
app runtime startup, TDLib/Telegram, live collector startup, notifier
transport, production rollout, Docker, Docker Compose, or systemd unit
modification by itself.

## Source-of-truth handling

`docs/project-source/README_replacement_consolidated_v0_20.md` remains
authoritative. The locked project-source bundles `00~10` remain the
source-of-truth set and are interpreted in filename order. The GitHub AI
application plan remains advisory only.

This result record preserves the canonical architecture invariant:

```text
SourceMessage -> Artifact -> CandidateGroup -> EvidenceBundle -> JudgeOutput -> Analysis -> Notification
```

The service boundaries remain unchanged:

- collector preserves raw Telegram source messages and revisions only.
- outbox-relay publishes thin ID-only Redis Stream messages only.
- router-normalizer is deterministic and non-LLM.
- enrichers gather evidence only.
- evidence-assembler assembles candidate-centered EvidenceBundles and may
  reroot only within its contract.
- analysis-router is the deterministic judge-pipeline entry gate.
- LLM judge produces structured `judge_output_v1` only.
- deterministic policy-engine computes final `analysis_v1` verdict and
  `delivery_decision`.
- notifier is presentation and delivery only.
- maintenance is retry/replay orchestration plus explicitly requested one-shot
  delivery control-plane tools only.
- delivery gate is ops/control-plane reporting, not a runtime worker.
- PostgreSQL is durable system of record.
- Redis is queue, lock, and short-lived execution state only.
- replay creates new runs or versions and never overwrites historical truth.
- `recommended_flag_patch` is output-only and must not be auto-applied.
- production rollout remains unauthorized.

No source-document conflict was found that requires changing the Alembic
preflight package in this slice. The prior DB/Redis records select host
apt/systemd PostgreSQL and Redis for the immediate dedicated VPS state; Docker
Compose remains a future full app stack candidate and is not discarded.

## Execution summary

This record was recorded after 2026-05-09 Alembic current preflight execution.

The manual redacted Alembic current preflight was run on the dedicated VPS.
Block 0 context passed. Block 1 repo-local Alembic asset checks passed. Block 2
redacted runtime environment shape and safe-gate validation passed without
printing secrets or connecting to DB/Redis. Block 3 read-only Alembic current
preflight passed with exit code 0.

## Final result

PASS: dedicated VPS redacted Alembic current preflight completed with
`alembic_current_exit_code=0`, safe redacted Alembic output, no secret leakage,
and no upgrade/stamp/revision.

The duplicated Alembic log lines are recorded as a non-blocking observation.
This result record does not authorize Alembic upgrade/stamp/revision or runtime
rollout by itself.

## Environment summary

- Host label: `github-ai-catchbot-prod-1`.
- User: `deploy`.
- Repo path: `/home/deploy/workspace/bots/github_ai_catchbot`.
- Repo HEAD/origin at execution: `b32f5f0 test(ops): add Alembic preflight check`.
- Previous commit: `a38754d docs(ops): record runtime secret placement result`.
- Previous commit: `a660e52 test(ops): add runtime secret placement check`.
- Previous commit: `35cc7fa test(ops): stabilize collector help assertions`.
- Previous commit: `66c3bcc docs(ops): record DB Redis provisioning result`.
- Raw public IP and operator IP values are intentionally omitted.

## Commands actually run by block

- Block 0: context confirmation was run manually and passed.
- Block 1: repo-local Alembic asset checks were run manually and passed:
  - `alembic.ini` present.
  - `migrations` directory present.
  - `migrations/env.py` present.
  - `migrations/versions` directory present.
  - `migration_file_count=4`.
  - migration filenames:
    - `0001_ingest_core.py`
    - `0002_normalization_candidates.py`
    - `0003_enrichment_bundles.py`
    - `0004_judge_delivery_observability.py`
- Block 2: redacted runtime env shape and gate validation was run manually and
  passed:
  - required keys present by name:
    - `APP_ENV`
    - `DATABASE_URL`
    - `REDIS_URL`
    - `ENABLE_NOTIFICATION_SEND`
    - `NOTIFIER_TELEGRAM_DRY_RUN`
    - `NOTIFIER_TELEGRAM_ALLOW_EDITS`
    - `ENABLE_REPLAY_TO_PROD_DB`
    - `MAINTENANCE_ENABLE_NOTIFICATION_RETRY_PROMOTION`
  - optional keys present: none.
  - database_url_shape_valid=true.
  - `DATABASE_URL` exists and shape is valid, but the value was not printed.
  - `REDIS_URL=redis://127.0.0.1:6379/0`.
  - `ENABLE_NOTIFICATION_SEND=false`.
  - `NOTIFIER_TELEGRAM_DRY_RUN=true`.
  - `NOTIFIER_TELEGRAM_ALLOW_EDITS=false`.
  - `ENABLE_REPLAY_TO_PROD_DB=false`.
  - `MAINTENANCE_ENABLE_NOTIFICATION_RETRY_PROMOTION=false`.
  - `secret_values_printed=false`.
  - `database_url_printed=false`.
  - `db_connection_performed=false`.
  - `redis_connection_performed=false`.
  - `alembic_current_performed=false`.
  - `alembic_upgrade_run=false`.
  - `alembic_stamp_run=false`.
  - `alembic_revision_run=false`.
  - `app_runtime_started=false`.
  - `tdlib_auth_performed=false`.
  - `telegram_connected=false`.
  - `live_collector_started=false`.
  - `notifier_transport_enabled=false`.
  - `production_rollout_performed=false`.
- Block 3: read-only Alembic current preflight was run manually and passed:
  - `alembic_current_exit_code=0`.
  - redacted Alembic current output contained only safe Alembic log lines:
    - `INFO  [alembic.runtime.migration] Context impl PostgresqlImpl.`
    - `INFO  [alembic.runtime.migration] Will assume transactional DDL.`
  - `database_url_printed=false`.
  - `alembic_upgrade_run=false`.
  - `alembic_stamp_run=false`.
  - `alembic_revision_run=false`.

## Final verification evidence

- Block 0 context passed.
- Block 1 repo-local Alembic asset check passed.
- `alembic.ini` was present.
- `migrations` directory was present.
- `migrations/env.py` was present.
- `migrations/versions` directory was present.
- `migration_file_count=4`.
- Migration filenames were:
  - `0001_ingest_core.py`
  - `0002_normalization_candidates.py`
  - `0003_enrichment_bundles.py`
  - `0004_judge_delivery_observability.py`
- Block 2 redacted runtime env shape/gate validation passed.
- Required runtime keys were present by name only.
- Optional keys present: none.
- `DATABASE_URL` exists and shape is valid, but its value was not printed.
- `REDIS_URL=redis://127.0.0.1:6379/0`.
- Safe gates were set to notification-send disabled, dry-run enabled,
  notification edits disabled, replay-to-prod disabled, and maintenance retry
  promotion disabled.
- `secret_values_printed=false`.
- `database_url_printed=false`.
- Block 2 did not perform DB connection, Redis connection, or Alembic current.
- Block 3 read-only Alembic current preflight passed.
- `alembic_current_exit_code=0`.
- Redacted Alembic output was safe and did not include credentials.
- `alembic_upgrade_run=false`.
- `alembic_stamp_run=false`.
- `alembic_revision_run=false`.

## Non-blocking observations

- Duplicate Alembic log lines observed.
- No secret leakage was observed.
- Exit code remained 0.
- No upgrade, stamp, or revision ran.
- Do not fix logging in this slice.

## Security/redaction notes

This record contains no raw public IP, no operator IP, no actual `DATABASE_URL`,
no DB password, no credential-bearing `REDIS_URL`, no OpenAI secrets, no
Telegram secrets, no GitHub secrets, no X secrets, no TDLib secrets, no SSH
private key paths, no `.env` contents, and no runtime.env contents.

No actual `DATABASE_URL` was printed. No DB password was printed. No
runtime.env content was printed. No credential-bearing `REDIS_URL` was printed.
No raw server/operator IPs were printed in this result record. No SSH key path
was printed. No `.env` content was printed.

No `cat /etc/github-ai-catchbot/runtime.env` was performed. No
`source /etc/github-ai-catchbot/runtime.env` was performed. No
`. /etc/github-ai-catchbot/runtime.env` was performed. No `export DATABASE_URL`
was performed. No `export REDIS_URL` was performed.

Loopback literals such as `127.0.0.1` and credential-free loopback values such
as `redis://127.0.0.1:6379/0` are allowed redacted evidence.

## Unauthorized actions not performed

- No repo `.env` was created.
- No repo `env/*.env` was created.
- No `cat`, `source`, dot-source, or export command was run against
  runtime.env or DB/Redis URL variables.
- No Alembic upgrade was run.
- No Alembic stamp was run.
- No Alembic revision was run.
- No app runtime was started.
- No TDLib auth was performed.
- No Telegram connection was performed.
- No live collector was started.
- No notifier transport was enabled.
- No production rollout was performed.
- No Docker or Docker Compose was used.
- No systemd unit was modified.

## Follow-up actions

- Review this redacted result record and focused contract test.
- Do not patch the Alembic preflight runbook/checker in this slice unless a
  concrete redaction contradiction is found.
- Keep duplicate Alembic log-line cleanup outside this slice unless separately
  requested.

## Next step

Review and commit this result record and focused contract test. The next
operational action should be a separately approved post-preflight migration or
runtime rollout decision; this result record does not authorize Alembic
upgrade/stamp/revision or runtime rollout by itself.
