# Dedicated VPS Alembic upgrade execution result record

## Scope

This is a repo-local operational result record for the dedicated VPS redacted
Alembic `upgrade head` execution that was manually run on the dedicated VPS.

Codex did not execute VPS commands for this record. Codex did not SSH, inspect
host files, read secret files, inspect environment variables, connect to
PostgreSQL or Redis, run Alembic, start app runtime, perform TDLib auth, connect
Telegram, start the live collector, enable notifier transport, perform
production rollout, use Docker or Docker Compose, modify systemd units, edit
migration files, or read `/etc/github-ai-catchbot/runtime.env`.

This record is docs-only. It records the already completed manual Alembic
upgrade execution result and does not authorize app runtime, TDLib/Telegram,
live collector startup, notifier transport, or production rollout by itself.

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
upgrade gate package in this slice. The prior DB/Redis records select host
apt/systemd PostgreSQL and Redis for the immediate dedicated VPS state; Docker
Compose remains a future full app stack candidate and is not discarded.

## Execution summary

This record was recorded after 2026-05-09 Alembic upgrade execution.

The manual redacted Alembic upgrade execution was run on the dedicated VPS.
Block 0 context passed. Block 1 repo-local Alembic asset checks passed. Block 2
redacted runtime environment shape and safe-gate validation passed without
printing secrets or connecting to DB/Redis. Block 3 pre-upgrade read-only
Alembic current passed. Block 4 explicit approval checkpoint and DB schema
mutation acknowledgement were recorded. Block 5 Alembic `upgrade head` passed.
Block 6 post-upgrade read-only Alembic current passed and reached head.

## Final result

PASS: dedicated VPS redacted Alembic `upgrade head` execution completed with
`alembic_upgrade_exit_code=0`, no secret leakage, and post-upgrade current at
`0004_judge_delivery_obs (head)`.

The duplicated Alembic log lines are recorded as a non-blocking observation.
This result record does not authorize app runtime, TDLib/Telegram, live
collector, notifier transport, or production rollout by itself.

## Environment summary

- Host label: `github-ai-catchbot-prod-1`.
- User: `deploy`.
- Repo path: `/home/deploy/workspace/bots/github_ai_catchbot`.
- Repo HEAD/origin at execution:
  - `11ec9ab test(ops): add Alembic upgrade gate check`
  - `1daa7ed docs(ops): record Alembic current preflight result`
  - `b32f5f0 test(ops): add Alembic preflight check`
  - `a38754d docs(ops): record runtime secret placement result`
  - `a660e52 test(ops): add runtime secret placement check`
  - `35cc7fa test(ops): stabilize collector help assertions`
  - `66c3bcc docs(ops): record DB Redis provisioning result`
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
  - `runtime_env_required_keys_present=PASS`.
  - required keys present by name only:
    - `APP_ENV`
    - `DATABASE_URL`
    - `REDIS_URL`
    - `ENABLE_NOTIFICATION_SEND`
    - `NOTIFIER_TELEGRAM_DRY_RUN`
    - `NOTIFIER_TELEGRAM_ALLOW_EDITS`
    - `ENABLE_REPLAY_TO_PROD_DB`
    - `MAINTENANCE_ENABLE_NOTIFICATION_RETRY_PROMOTION`
  - `runtime_env_optional_keys_policy=PASS`.
  - `runtime_env_unauthorized_keys_absent=PASS`.
  - `runtime_env_placeholders_absent=PASS`.
  - `database_url_shape_valid=PASS`.
  - `DATABASE_URL` exists and shape is valid, but the value was not printed.
  - `redis_url_shape_valid=PASS`.
  - Credential-free loopback `REDIS_URL=redis://127.0.0.1:6379/0` may be
    recorded.
  - `safe_gates_disabled=PASS`.
  - `secret_values_printed=false`.
  - `database_url_printed=false`.
  - `db_connection_performed=false`.
  - `redis_connection_performed=false`.
  - `alembic_execution_performed=false`.
- Block 3: pre-upgrade read-only Alembic current was run manually and passed:
  - `pre_upgrade_alembic_current_exit_code=0`.
  - `database_url_printed=false`.
  - `alembic_upgrade_run=false`.
  - `alembic_stamp_run=false`.
  - `alembic_revision_run=false`.
  - redacted output contained safe duplicate Alembic log lines:
    - `INFO  [alembic.runtime.migration] Context impl PostgresqlImpl.`
    - `INFO  [alembic.runtime.migration] Will assume transactional DDL.`
- Block 4: explicit approval checkpoint was recorded:
  - `APPROVAL_CHECKPOINT: user explicitly approved Alembic upgrade execution now`
  - `DB_SCHEMA_MUTATION_ACKNOWLEDGED=true`
  - `BACKUP_ROLLBACK_NOTE=No app runtime/live collector/notifier production workload has been started from this repo state; continue only if this matches operator understanding.`
  - `NEXT_BLOCK=Block 5 Alembic upgrade head`
- Block 5: Alembic `upgrade head` execution was run manually and passed:
  - `alembic_upgrade_exit_code=0`.
  - `database_url_printed=false`.
  - `alembic_upgrade_run=true`.
  - `alembic_stamp_run=false`.
  - `alembic_revision_run=false`.
  - `alembic_downgrade_run=false`.
  - `app_runtime_started=false`.
  - `live_collector_started=false`.
  - `notifier_transport_enabled=false`.
  - `production_rollout_performed=false`.
- Block 6: post-upgrade read-only Alembic current was run manually and passed:
  - `post_upgrade_alembic_current_exit_code=0`.
  - `post_upgrade_alembic_current_output_redacted` includes
    `0004_judge_delivery_obs (head)`.
  - `database_url_printed=false`.
  - `alembic_stamp_run=false`.
  - `alembic_revision_run=false`.
  - `alembic_downgrade_run=false`.
  - `app_runtime_started=false`.

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
- `runtime_env_required_keys_present=PASS`.
- Required runtime keys were present by name only.
- `runtime_env_optional_keys_policy=PASS`.
- `runtime_env_unauthorized_keys_absent=PASS`.
- `runtime_env_placeholders_absent=PASS`.
- `DATABASE_URL` exists and shape is valid, but its value was not printed.
- `database_url_shape_valid=PASS`.
- `redis_url_shape_valid=PASS`.
- `REDIS_URL=redis://127.0.0.1:6379/0`.
- `safe_gates_disabled=PASS`.
- `secret_values_printed=false`.
- `database_url_printed=false`.
- Block 2 did not perform DB connection, Redis connection, or Alembic
  execution.
- Block 3 pre-upgrade read-only Alembic current passed.
- `pre_upgrade_alembic_current_exit_code=0`.
- Block 4 approval checkpoint and schema mutation acknowledgement were
  recorded.
- `DB_SCHEMA_MUTATION_ACKNOWLEDGED=true`.
- Block 5 Alembic upgrade passed.
- `alembic_upgrade_exit_code=0`.
- `alembic_upgrade_run=true`.
- `alembic_stamp_run=false`.
- `alembic_revision_run=false`.
- `alembic_downgrade_run=false`.
- Block 6 post-upgrade read-only Alembic current passed.
- `post_upgrade_alembic_current_exit_code=0`.
- Final current head: `0004_judge_delivery_obs (head)`.

## Applied migration evidence

Redacted Alembic output showed this migration chain:

- `Running upgrade  -> 0001_ingest_core, 0001_ingest_core`
- `Running upgrade 0001_ingest_core -> 0002_normalization_candidates, 0002_normalization_candidates`
- `Running upgrade 0002_normalization_candidates -> 0003_enrichment_bundles, 0003_enrichment_bundles`
- `Running upgrade 0003_enrichment_bundles -> 0004_judge_delivery_obs, 0004_judge_delivery_obs`

## Non-blocking observations

- Duplicate Alembic log lines observed.
- No secret leakage was observed.
- Exit codes remained 0.
- Post-upgrade current reached head.
- No logging correction is part of this slice.

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

Safe Alembic log lines are allowed when they contain no secrets.

## Unauthorized actions not performed

- No repo `.env` was created.
- No repo `env/*.env` was created.
- No `cat`, `source`, dot-source, or export command was run against
  runtime.env or DB/Redis URL variables.
- No Alembic downgrade was run.
- No Alembic stamp was run.
- No Alembic revision was run.
- No manual schema editing was performed.
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
- Do not patch the Alembic upgrade gate runbook/checker in this slice unless a
  concrete redaction contradiction is found.
- Keep duplicate Alembic log-line cleanup outside this slice unless separately
  requested.
- Keep app runtime, TDLib/Telegram, live collector, notifier transport, and
  production rollout behind separate explicit approval.
- Run post-migration DB acceptance/readiness smoke before any app runtime,
  live collector, notifier, delivery-control, or production rollout step.

## Next step

Review and commit this result record and focused contract test. The next
operational action should be a separately approved post-migration DB
acceptance/readiness smoke. This result record does not authorize app runtime,
TDLib/Telegram, live collector, notifier transport, delivery-control execution,
or production rollout.
