# Dedicated VPS post-migration DB acceptance smoke result record

## Purpose

Record the redacted result of the separately approved dedicated VPS
post-migration DB acceptance smoke that was already executed and passed.

This record is docs/checker/test evidence only. Codex did not rerun the DB
smoke for this slice.

## Scope

This document records the redacted facts from the completed read-only
post-migration PostgreSQL acceptance smoke. It does not add runtime behavior,
does not modify migrations, and does not authorize any new VPS operation.

The result confirms structural DB acceptance after the manual Alembic upgrade
reached `0004_judge_delivery_obs`.

## Source-of-truth handling

`docs/project-source/README_replacement_consolidated_v0_20.md` remains
authoritative. The locked project-source bundles `00~10` remain the
source-of-truth set and are interpreted in filename order. The GitHub AI
application plan remains advisory only.

Authoritative source set:

- `docs/project-source/README_replacement_consolidated_v0_20.md`
- `docs/project-source/00_foundations_stage0_stage1_bundle_v0_1.md`
- `docs/project-source/01_runtime_collector_design_stage2_stage3_bundle_v0_1.md`
- `docs/project-source/02_normalization_enrichment_design_stage4_stage5_bundle_v0_1.md`
- `docs/project-source/03_judge_delivery_operations_stage6_stage10_bundle_v0_1.md`
- `docs/project-source/04_execution_contracts_migrations_stage11_stage12_bundle_v0_1.md`
- `docs/project-source/05_migration_code_drafts_stage13_stage16_bundle_v0_1.md`
- `docs/project-source/06_collector_implementation_stage17_stage25_bundle_v0_1.md`
- `docs/project-source/07_outbox_normalizer_stage26_stage28_bundle_v0_1.md`
- `docs/project-source/08_enrichers_assembler_stage29_stage32_bundle_v0_1.md`
- `docs/project-source/09_analysis_pipeline_stage33_stage38_bundle_v0_1.md`
- `docs/project-source/10_delivery_hardening_stage39_plus_v0_1.md`
- `docs/project-source/03_GitHub_AI_application_plan.md` advisory only

This result record preserves the canonical architecture invariant:

```text
SourceMessage -> Artifact -> CandidateGroup -> EvidenceBundle -> JudgeOutput -> Analysis -> Notification
```

Layer boundaries remain unchanged:

- PostgreSQL is durable source of record.
- Redis is queue, lock, and short-lived execution state only.
- Production rollout remains unauthorized.
- The live collector remains unauthorized.
- TDLib auth remains unauthorized.
- Telegram connection remains unauthorized.
- Notifier transport remains unauthorized.
- App runtime remains unauthorized.

## Preconditions already satisfied

- The dedicated VPS repo had already been migrated to the expected post-upgrade
  state before this result record was written.
- The separately approved Python runner had already executed the read-only DB
  acceptance smoke on the dedicated VPS.
- The runner used the approved runtime env read path inside Python only.
- The runner emitted redacted result facts only.
- This repository slice did not run the smoke again.

## Redacted execution result summary

Exact pass/fail status:

```text
post-migration DB acceptance smoke passed
```

Result facts:

```yaml
contract_status: passed
checks_failed: []
failures: []
warnings: []
database_connected: true
read_only_transaction_requested: true
read_only_transaction_confirmed: true
expected_terminal_revision: 0004_judge_delivery_obs
observed_alembic_versions:
  - 0004_judge_delivery_obs
expected_table_count: 33
present_table_count: 33
missing_tables: []
index_check_summary:
  expected: 55
  present: 55
  missing: []
constraint_check_summary:
  expected: 58
  present: 58
  missing: []
key_tables_queried:
  - source_messages
  - event_outbox
  - artifact_registry
  - candidate_group_proposals
  - candidate_evidence_bundles
  - judge_runs
  - judge_outputs
  - analyses
  - notification_plans
  - notification_delivery_records
  - job_attempts
  - state_transitions
key_table_query_failures: []
derived_revision_ids:
  - 0001_ingest_core
  - 0002_normalization_candidates
  - 0003_enrichment_bundles
  - 0004_judge_delivery_obs
migration_files_inspected:
  - migrations/versions/0001_ingest_core.py
  - migrations/versions/0002_normalization_candidates.py
  - migrations/versions/0003_enrichment_bundles.py
  - migrations/versions/0004_judge_delivery_observability.py
```

Runtime env metadata was recorded as key-name-only metadata, not secret values:

```yaml
runtime_env_read: true
runtime_env_metadata:
  path: /etc/github-ai-catchbot/runtime.env
  database_url_present: true
  database_url_scheme: postgresql+psycopg
  database_url_has_credentials: true
  keys_present:
    - APP_ENV
    - DATABASE_URL
    - ENABLE_NOTIFICATION_SEND
    - ENABLE_REPLAY_TO_PROD_DB
    - MAINTENANCE_ENABLE_NOTIFICATION_RETRY_PROMOTION
    - NOTIFIER_TELEGRAM_ALLOW_EDITS
    - NOTIFIER_TELEGRAM_DRY_RUN
    - REDIS_URL
```

`runtime_env_read: true` means the approved Python runner read
`/etc/github-ai-catchbot/runtime.env` during the already completed operator
smoke. Values were not printed. This repository result-record slice did not
read `/etc/github-ai-catchbot/runtime.env`.

## Explicit side-effect confirmation

Side-effect flags:

```yaml
database_url_printed: false
db_write_performed: false
redis_connected: false
redis_mutation_performed: false
alembic_run: false
alembic_upgrade_run: false
alembic_downgrade_run: false
alembic_stamp_run: false
alembic_revision_run: false
app_runtime_started: false
tdlib_auth_performed: false
telegram_connected: false
live_collector_started: false
notifier_transport_enabled: false
production_rollout_performed: false
docker_used: false
systemd_modified: false
migration_files_modified: false
runtime_env_values_printed: false
secret_values_printed: false
```

The completed smoke performed no DB writes. It made no Redis connection and no
Redis mutation. It ran no Alembic command. It started no app runtime, performed
no TDLib auth, made no Telegram connection, started no live collector, enabled
no notifier transport, used no Docker or systemd changes, performed no
production rollout, modified no migration files, and printed no secret values.

## Redaction and secret handling

- `runtime_env_metadata.keys_present` is key-name-only metadata and not secret
  values.
- `/etc/github-ai-catchbot/runtime.env` path is allowed to be recorded.
- Runtime env contents are not recorded.
- `DATABASE_URL` was not printed.
- DB password was not printed.
- `REDIS_URL` value was not printed.
- Raw server IP was not recorded.
- Raw operator IP was not recorded.
- Raw URL credentials were not recorded.

## Current next recommended slice

`dedicated_vps_runtime_environment_consumer_preflight`

This next slice should remain separately reviewed and should not be treated as
authorized by this result record.

## What remains unauthorized

This result record does not authorize:

- app runtime
- TDLib
- Telegram
- live collector
- notifier transport
- production rollout

It also does not authorize DB mutation, Redis mutation, Alembic mutation,
Docker, systemd changes, migration file modification, or secret printing.
