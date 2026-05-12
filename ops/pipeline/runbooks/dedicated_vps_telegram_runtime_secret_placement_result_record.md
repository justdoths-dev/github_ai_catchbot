# Dedicated VPS Telegram runtime secret placement result record

## Purpose

This is a repo-local result record for an already-approved operator execution
that placed Telegram runtime keys into the dedicated VPS runtime secret
boundary.

This record does not execute secret placement. It does not mutate or inspect
the current runtime secret file. It records only redacted operator-provided
facts needed for audit continuity after an external VPS file changed outside
Git.

## Source-of-truth / architecture boundary

`docs/project-source/README_replacement_consolidated_v0_20.md` remains
authoritative. The locked project-source bundles `00` through `10` remain the
source-of-truth set and are interpreted in filename order. The GitHub AI
application plan remains advisory only.

This result record preserves the canonical architecture invariant:

```text
SourceMessage -> Artifact -> CandidateGroup -> EvidenceBundle -> JudgeOutput -> Analysis -> Notification
```

Layer boundaries remain unchanged:

- collector preserves raw Telegram source messages and revisions only.
- outbox-relay publishes thin ID-only Redis Stream messages only.
- router-normalizer is deterministic and non-LLM.
- enrichers gather evidence only.
- evidence-assembler assembles candidate-centered EvidenceBundles and may
  reroot only within its contract.
- LLM judge produces structured `judge_output_v1` only.
- deterministic policy-engine computes final `analysis_v1` verdict and
  `delivery_decision`.
- notifier is presentation and delivery only.
- PostgreSQL is durable system of record.
- Redis is queue, lock, and short-lived execution state only.
- replay creates new runs or versions and never overwrites historical truth.
- production has exactly one live Telegram collector instance.
- production rollout remains unauthorized.

## Execution summary

- Dedicated VPS: `github-ai-catchbot-prod-1`.
- Repo path: `~/workspace/bots/github_ai_catchbot`.
- Approved package commit:
  `eec297d test(ops): add telegram runtime secret placement package`.
- Operator edited the runtime env using the approved editor procedure.
- The runtime env was updated by the operator during the already-approved
  external execution.
- The redacted validator read the runtime env during approved operator
  execution only.
- Runtime env values were not printed.

An initial redacted validation attempt showed required Telegram and TDLib keys
as missing because the corresponding lines were commented out. The operator
corrected the file by uncommenting or properly adding the key lines. No values
were printed in that diagnostic path. Final redacted validation passed.

## Runtime secret file target

The runtime secret target was recorded as a path label only:

```text
/etc/github-ai-catchbot/runtime.env
```

This result record does not include raw file content, examples of runtime env
assignments, shell append snippets, or actual secret values.

## Redacted validation result

```yaml
contract_status: passed
runtime_env_read: true
runtime_env_values_printed: false
```

`runtime_env_read: true` applies only to the approved operator validation on
the dedicated VPS. This repository result-record slice did not read the runtime
secret file and did not rerun the validator.

## Key status summary

```yaml
TELEGRAM_API_ID: present_redacted
TELEGRAM_API_HASH: present_redacted
TELEGRAM_PHONE_NUMBER: present_redacted
TELEGRAM_2FA_PASSWORD: present_redacted
TDLIB_DB_ENCRYPTION_KEY: present_redacted
TDLIB_STATE_DIR: present_redacted
TDLIB_FILES_DIR: present_redacted
TELEGRAM_BOT_TOKEN: present_redacted
```

All required Telegram runtime keys returned `present_redacted`. The
`TELEGRAM_2FA_PASSWORD` key also returned `present_redacted`.

## Side-effect boundary result

```yaml
database_connected: false
redis_connected: false
alembic_run: false
app_runtime_started: false
tdlib_auth_performed: false
telegram_connected: false
live_collector_started: false
notifier_transport_enabled: false
production_rollout_performed: false
```

The approved execution remained a runtime secret placement and redacted
validation action only. It did not connect to DB or Redis, run Alembic, start
the app runtime, perform TDLib auth, connect Telegram, start the live
collector, enable notifier transport, or perform production rollout.

## Secret / redaction confirmation

- No actual Telegram API ID is recorded.
- No actual Telegram API hash is recorded.
- No actual phone number is recorded.
- No actual 2FA password is recorded.
- No actual TDLib DB encryption key is recorded.
- No actual Telegram bot token is recorded.
- No chat ID or delivery target ID is recorded.
- No invite link is recorded.
- No raw runtime env content is recorded.
- No DB or Redis URL is recorded.
- No public VPS IP is recorded.
- No operator IP is recorded.
- Runtime env values were not printed by the approved validator.
- Runtime env values are not included in this repository record.

## Non-authorizations preserved

This result record does not authorize secret placement reruns.
This result record does not authorize runtime env inspection.
This result record does not authorize TDLib auth.
This result record does not authorize Telegram connection.
This result record does not authorize live collector startup.
This result record does not authorize notifier transport.
This result record does not authorize app runtime startup.
This result record does not authorize DB or Redis connections.
This result record does not authorize Alembic.
This result record does not authorize Docker or systemd changes.
This result record does not authorize production rollout.

## Known limitations

- Actual TDLib auth is not performed yet.
- Telegram connection remains untested.
- Live collector remains unstarted.
- Notifier transport remains disabled.
- Production rollout remains unauthorized.
- This result record depends on the user-provided redacted validator output and
  must not contain raw secrets.

## Acceptance criteria

- Result record contains no actual secret values.
- Final redacted validation is recorded as passed.
- Runtime env read is recorded as limited to the approved redacted validator.
- Runtime env values printed is recorded as false.
- All Telegram runtime key statuses are recorded as `present_redacted`.
- `TELEGRAM_2FA_PASSWORD` is recorded as `present_redacted`.
- DB, Redis, Alembic, app runtime, TDLib auth, Telegram connection, live
  collector, notifier transport, and production rollout side-effect booleans
  are all recorded as false.
- Unsafe raw runtime env display, shell-source, export-from-file, or direct
  append command patterns are not included.
- Service code, dependencies, migrations, Docker, and systemd remain unchanged.

## Next bounded action

This result record closes the Telegram runtime secret placement result.

The next safe slice is a separately reviewed TDLib auth package/preflight:

```text
dedicated_vps_tdlib_auth_package
```

TDLib auth is not executed by this record. Telegram connection, live collector
startup, notifier transport, and production rollout remain unauthorized.
