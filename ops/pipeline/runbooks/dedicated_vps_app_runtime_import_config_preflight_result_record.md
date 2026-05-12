# Dedicated VPS app runtime import/config preflight result record

## Scope

Record the redacted result of the separately approved dedicated VPS app/runtime
import-config preflight that was already executed once and passed.

This is a docs/test-only result-record slice. This repository slice does not
read `/etc/github-ai-catchbot/runtime.env`, does not rerun the approved
preflight, and does not perform runtime, network, DB, Redis, Alembic, Docker,
systemd, TDLib, Telegram, notifier, or rollout operations.

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

## Source execution being recorded

The approved app/runtime import-config preflight was executed once on the
dedicated VPS with this approved command:

```bash
python scripts/ops/dedicated_vps_app_runtime_import_config_preflight_runner.py --approved-app-runtime-import-config-preflight --format json
```

The approved execution output file was:

```text
/tmp/github_ai_app_runtime_import_config_preflight_20260511T154352Z.json
```

The approved Python runner read `/etc/github-ai-catchbot/runtime.env` only
inside that approved operator execution. The approved runner did not print
runtime env values, secret values, full connection URLs, raw public VPS IPs, or
operator IPs. The approved runner did not inspect process env vars.

This result-record slice records only the operator-provided redacted summary.
It does not inspect process env vars, does not read the runtime env file, and
does not read the `/tmp` JSON output file.

## Result summary

```yaml
process_status: 0
contract_status: passed
checks_failed: []
failures: []
warnings: ['Secret-bound config loaders were deferred by design.', 'Runtime-bound config loaders were deferred by design.']
schema_version: dedicated_vps_app_runtime_import_config_preflight_v1
runtime_env_read: true
runtime_env_values_printed: false
secret_values_printed: false
process_env_inspected: false
runtime_env_key_count: 8
app_env_seen: prod
```

`runtime_env_read: true` applies only to the approved operator runner execution
described above. This repository result-record slice did not read
`/etc/github-ai-catchbot/runtime.env` and did not rerun the preflight.

Passing this result means the import/config preflight passed only. It does not
prove service readiness and does not authorize runtime start.

## Import/config surface facts

```yaml
import_surface_attempted: true
app_imports_attempted: true
config_surface_attempted: true
import_surface_passed: true
safe_config_surface_passed: true
import_result_count: 152
import_statuses: {'import_ok': 13, 'skipped_forbidden_runtime_surface': 139}
config_result_count: 13
config_statuses: {'config_loader_deferred_runtime_bound': 6, 'config_loader_deferred_secret_bound': 5, 'config_loader_ok': 2}
```

The approved result recorded 13 config modules imported successfully.

The approved result recorded 139 forbidden runtime, client, service, worker,
repository, Redis-stream, and `main.py` surfaces skipped.

The approved result recorded 2 safe config loaders succeeded.

## Deferred loader facts

```yaml
secret_bound_config_loaders_deferred: true
runtime_bound_config_loaders_deferred: true
```

Secret-bound config loaders were deferred by design. Runtime-bound config
loaders were deferred by design.

The approved result recorded 5 secret-bound config loaders deferred and 6 runtime-bound config loaders deferred.

## Side-effect denials

```yaml
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

No PostgreSQL connection occurred. No Redis connection occurred. No DB write
occurred. No Redis mutation occurred. No Alembic run occurred. No app runtime
started. No TDLib auth occurred. No Telegram connection occurred. No live
collector started. No notifier transport was enabled. No Docker was used. No
systemd was modified. No migration files were modified. No production rollout
occurred.

## Redaction guarantees

- Boolean pass/fail facts are recorded.
- Module/config status counts are recorded.
- Status names are recorded.
- Warning strings are recorded.
- Schema version is recorded.
- The approved output file path under `/tmp` is recorded.
- Runtime env key count is recorded.
- Runtime env values are not recorded.
- Runtime env values were not printed by the approved runner.
- Secret values are not recorded.
- Secret values were not printed by the approved runner.
- Full DB and Redis URL values are not recorded.
- DB credential material is not recorded.
- Redis credential material is not recorded.
- Telegram, OpenAI, GitHub, X, and TDLib credential material is not recorded.
- Raw public VPS IP values are not recorded.
- Raw operator IP values are not recorded.
- Runtime env file contents are not recorded.
- Secret file contents are not recorded.

## Non-authorizations

Passing this result does not authorize runtime start.
Passing this result does not authorize service readiness.
Passing this result does not authorize TDLib auth.
Passing this result does not authorize Telegram connection.
Passing this result does not authorize live collector startup.
Passing this result does not authorize notifier transport.
Passing this result does not authorize production rollout.
Passing this result does not authorize DB or Redis mutation.
Passing this result does not authorize Alembic.
Passing this result does not authorize Docker or systemd.

## Next bounded slice

After this result record is committed, pushed, pulled, and repo-locally
validated on the dedicated VPS, proceed to Telegram preparation only as a
separately reviewed slice:

```text
dedicated_vps_telegram_credentials_acquisition_plan
```

Do not jump directly to TDLib auth, Telegram connection, live collector
startup, notifier transport, or production rollout.

## Anti-overconservatism check

If this result record and focused contract tests pass with no boundary
violation, do not add another diagnostic, checker, runtime probe, or preflight
for marginal certainty. Move next to
`dedicated_vps_telegram_credentials_acquisition_plan` only after this result
record is committed, pushed, pulled, and validated on the dedicated VPS.
