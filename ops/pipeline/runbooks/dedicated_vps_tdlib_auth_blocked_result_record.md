# Dedicated VPS TDLib auth blocked result record

## Scope

Record the redacted result of the separately approved dedicated VPS TDLib
auth-only operator execution attempt.

This is a result-record slice. It documents an already completed wrapper-level
operator attempt and the fail-closed result. It does not add TDLib dependency
installation, a TDLib dependency preflight, a new auth feature, live collector
startup, notifier transport, Docker/systemd changes, Alembic, DB/Redis
mutation, channel registry execution docs, or production rollout docs.

Codex did not rerun auth for this record. This repository slice did not read or
print `/etc/github-ai-catchbot/runtime.env`.

## Source-of-truth boundary

`docs/project-source/README_replacement_consolidated_v0_20.md` remains
authoritative. The locked project-source bundles `00` through `10` remain the
source-of-truth set and are interpreted in filename order. The GitHub AI
application plan remains advisory only.

The canonical architecture remains unchanged:

```text
SourceMessage -> Artifact -> CandidateGroup -> EvidenceBundle -> JudgeOutput -> Analysis -> Notification
```

Production rollout remains unauthorized.

## Result summary

The approved auth-only operator execution was attempted at the wrapper level.
The wrapper selected the auth-only entrypoint
`src.services.collector_telegram.auth_entrypoint`.

No TDLib auth attempt occurred because real tdjson transport was missing. The
result is blocked, not successful, and must not be interpreted as Telegram auth
success.

```yaml
contract_status: blocked_real_transport_missing
blocked_reason: blocked_real_transport_missing
approved_execution_requested: true
auth_only_entrypoint_status: available
selected_entrypoint: src.services.collector_telegram.auth_entrypoint
tdlib_auth_attempted: false
tdlib_auth_completed: false
manual_intervention_required: false
telegram_connected: false
session_state_created_or_reused: false
runtime_env_path: /etc/github-ai-catchbot/runtime.env
runtime_env_read: false
runtime_env_values_printed: false
secret_values_printed: false
collector_main_used: false
collector_service_used: false
collector_runtime_used: false
live_collector_started: false
app_runtime_started: false
notifier_transport_enabled: false
production_rollout_performed: false
database_connected: false
redis_connected: false
alembic_run: false
systemd_or_docker_changed: false
boundary_check: pass
```

Focused VPS tests before the operator execution reported `25 passed`.

Required result markers:

```text
tdlib_auth_attempted=false
runtime_env_read=false
secret_values_printed=false
live_collector_started=false
app_runtime_started=false
notifier_transport_enabled=false
production_rollout_performed=false
```

## Runtime env and redaction facts

`runtime_env_read=false`.

No runtime.env values were printed. Runtime env values were not printed by the
approved wrapper-level attempt, and this result-record slice did not inspect
the runtime env file.

`secret_values_printed=false`.

No secret values, Telegram login code values, 2FA prompt values, TDLib session
contents, DB URLs, Redis URLs, private invite links, raw runtime env values, or
credential-bearing command output are recorded.

## TDLib and Telegram facts

`tdlib_auth_attempted=false`.

`tdlib_auth_completed=false`.

`manual_intervention_required=false`.

`telegram_connected=false`.

No TDLib auth attempt occurred. Real tdjson transport was unavailable before
the auth-only entrypoint could perform TDLib authorization work.

No Telegram auth success occurred. No Telegram connection occurred. No TDLib
session state was created or reused.

## Runtime and rollout facts

The live collector, app runtime, notifier transport, and production rollout
remained false:

```yaml
live_collector_started: false
app_runtime_started: false
notifier_transport_enabled: false
production_rollout_performed: false
```

The DB/Redis/Alembic/systemd/Docker boundary remained false:

```yaml
database_connected: false
redis_connected: false
alembic_run: false
systemd_or_docker_changed: false
```

Collector runtime surfaces were not used:

```yaml
collector_main_used: false
collector_service_used: false
collector_runtime_used: false
```

## Non-authorizations

This result must not authorize rerun, live collector, notifier, or rollout.

This result does not authorize another auth execution attempt.
This result does not authorize TDLib auth success claims.
This result does not authorize Telegram connection claims.
This result does not authorize live collector startup.
This result does not authorize app runtime startup.
This result does not authorize notifier transport.
This result does not authorize DB/Redis connections.
This result does not authorize Alembic.
This result does not authorize Docker/systemd changes.
This result does not authorize production rollout.

## Next bounded slice

The next slice is tdjson runtime dependency preflight.

The next slice is not result success, not a rerun authorization, not live
collector startup, not notifier enablement, and not production rollout.
