# Dedicated VPS TDLib auth package

## Purpose

Create a repo-local, redaction-safe package for a later dedicated VPS TDLib
auth operator execution/preflight.

This package defines the reviewed boundary, required key/path names, operator
pre-checklist, future execution shape, and redacted result shape for a later
explicitly approved TDLib auth action. It contains no actual credential values
and does not create a TDLib session.

This package does not execute TDLib auth. This package does not connect
Telegram. This package does not create or validate a live Telegram session.

## Source-of-truth / architecture boundary

`docs/project-source/README_replacement_consolidated_v0_20.md` remains
authoritative. The locked project-source bundles `00` through `10` remain the
source-of-truth set and are interpreted in filename order. The GitHub AI
application plan remains advisory only.

This package preserves the canonical architecture invariant:

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

## Current closed prerequisites

- Latest expected repo HEAD after result record:
  `fc3ac3d docs(ops): record telegram runtime secret placement result`.
- Previous package:
  `eec297d test(ops): add telegram runtime secret placement package`.
- Previous dependency correction:
  `bd3f483 test(ops): declare pytest asyncio dependency`.
- Previous acquisition plan:
  `8aa6f73 docs(ops): add telegram credentials acquisition plan`.
- Telegram credentials acquisition plan is PASS.
- pytest async dependency contract is PASS.
- VPS test parity is restored.
- Telegram runtime secret placement package is PASS.
- Telegram runtime secret placement operator execution is PASS.
- Telegram runtime secret placement result record is PASS.
- Runtime env has required Telegram keys present according to redacted
  validator.
- Runtime env values were not printed.
- TDLib auth has NOT been executed.
- Telegram connection has NOT been established.
- Live collector has NOT been started.
- Notifier transport has NOT been enabled.
- Production rollout remains unauthorized.

These conclusions are not reopened by this package.

## Scope

This package/checker/test slice covers only the repo-local TDLib auth package
contract for later operator execution.

Allowed repo-local content is limited to key names, path labels, operator
checklists, non-secret command shape constraints, redacted expected output
shape, rollback/recovery notes, and checker validation.

This package does not add a TDLib auth execution script. No service code,
config loader, migration, Docker, systemd, dependency, runtime env, DB, Redis,
notifier, live collector, or rollout behavior is changed by this package.

Current inspected implementation files show the TDLib auth-related collector
config keys below. The config also supports `_FILE` variants for
`TELEGRAM_API_HASH`, `TELEGRAM_2FA_PASSWORD`, and
`TDLIB_DB_ENCRYPTION_KEY` through the existing secret reader. This package uses
the direct runtime key names as the minimal-change contract and does not change
the config loader.

## Non-authorizations

This package does not execute TDLib auth.
This package does not connect Telegram.
This package does not create or validate a live Telegram session.
This package does not start live collector.
This package does not start the app runtime.
This package does not enable notifier transport.
This package does not perform production rollout.
This package does not mutate runtime.env.
This package does not mutate `/etc/github-ai-catchbot/runtime.env`.
This package does not print runtime.env values.
This package does not read or print runtime env values.
This package does not connect DB/Redis.
This package does not connect to DB or Redis.
This package does not run Alembic.
This package does not modify Docker/systemd.
This package does not modify Docker or systemd.
This package does not add secrets.

Passing this package does not authorize TDLib auth, Telegram connection, live
collector startup, notifier transport, app runtime startup, DB/Redis
connection, Alembic, Docker/systemd changes, runtime env mutation, or
production rollout.

## TDLib auth credential boundary

Collector reader account and notifier bot are separate credentials.

TDLib auth uses collector reader account / TDLib / MTProto credentials only.
Notifier bot token does not authorize channel collection. Reader account
credentials do not authorize notifier transport.

Required for TDLib auth/preflight:

- `TELEGRAM_API_ID`
- `TELEGRAM_API_HASH`
- `TELEGRAM_PHONE_NUMBER`
- `TDLIB_DB_ENCRYPTION_KEY`
- `TDLIB_STATE_DIR`
- `TDLIB_FILES_DIR`

Conditional for TDLib auth/preflight:

- `TELEGRAM_2FA_PASSWORD` only if the reader account has Telegram 2FA enabled.

Not used by TDLib auth:

- `TELEGRAM_BOT_TOKEN`

`TELEGRAM_BOT_TOKEN` is not a TDLib auth credential.
`TELEGRAM_BOT_TOKEN` is not required for TDLib auth.

The notifier bot token belongs to Telegram Bot API delivery only. It must not
be used as a collection credential, and it does not establish a reader-account
TDLib session.

## TDLib state/files directory boundary

TDLib state and files directories are collector-side local state labels:

- `TDLIB_STATE_DIR`
- `TDLIB_FILES_DIR`

The TDLib state directory may contain local authorization/session state after a
later approved TDLib auth operator execution. This package does not create,
inspect, validate, delete, or back up that session state.

The TDLib files directory may contain TDLib-managed file cache material after a
later approved execution. This package does not create, inspect, validate,
delete, or back up that directory.

Before the later approved operator execution, the operator should verify in a
redacted/status-only way that the configured path labels point to intended
dedicated VPS locations, are not repository paths, and are writable or
creatable by the approved runtime user. No actual path contents or secret
values should be pasted into ChatGPT, Codex, GitHub, repository files,
markdown, or shell history.

## Operator pre-checklist

Before any later TDLib auth execution, the operator must confirm:

- ChatGPT review approved this package.
- this package was committed and pushed after approval.
- dedicated VPS repository checkout was pulled to the approved commit.
- repo-local validation for this package passed on the VPS.
- the redacted runtime secret placement result remains the prerequisite source
  for required key presence.
- separate explicit approval was granted for
  `dedicated_vps_tdlib_auth_operator_execution`.
- the execution session is an approved VPS operator session.
- required collector-side key names are present according to redacted
  validation.
- `TELEGRAM_2FA_PASSWORD` handling matches the reader account 2FA state.
- `TELEGRAM_BOT_TOKEN` is not used for TDLib auth.
- collector reader account and notifier bot remain separate credentials.
- the future command is one-shot/operator-supervised and is not a daemon,
  systemd service, live collector start, notifier transport start, or rollout.
- no login code, 2FA prompt value, or secret value is pasted into ChatGPT,
  Codex, GitHub, repository files, markdown, or shell history.
- production still has exactly one live Telegram collector instance, but this
  package does not start it.

## Future approved TDLib auth execution shape

The future execution action name is
`dedicated_vps_tdlib_auth_operator_execution`.

Only after separate explicit approval, a VPS operator may run a one-shot TDLib
auth command or collector auth entrypoint selected from the current reviewed
repository state. The command must be operator-supervised, must emit redacted
status only, and must not start the live collector as a daemon/service.

No TDLib-auth-only entrypoint was added by this package. During inspection,
`src/services/collector_telegram/main.py` was identified as the collector
runtime entrypoint, so it must not be treated as approved by this package for
background runtime startup, notifier transport, production rollout, or a
daemonized live collector start.

The future command shape must preserve these constraints:

```text
<approved-one-shot-tdlib-auth-or-preflight-command> --redacted-status-json
```

Required execution properties:

- operator may need to enter Telegram login code interactively.
- any login code prompt is handled only in the approved VPS operator session.
- any 2FA prompt is handled only in the approved VPS operator session.
- login code and 2FA prompt values are never recorded.
- login code and 2FA prompt values are never pasted into ChatGPT, Codex,
  GitHub, repository files, markdown, or shell history.
- command output is redacted JSON/status only.
- no runtime env value is printed.
- no DB/Redis connection is performed.
- no Alembic command is run.
- no Docker/systemd command is run.
- no app runtime, live collector, notifier transport, or rollout is started.

A later approved TDLib auth execution may necessarily contact Telegram and may
create or reuse local TDLib session state. This package itself does not contact
Telegram and does not create or reuse session state.

## Redacted validation output shape

For this package slice, all execution booleans remain false because this
package does not execute auth:

```yaml
tdlib_auth_attempted: false
tdlib_auth_completed: false
telegram_connected: false
session_state_created_or_reused: false
runtime_env_values_printed: false
database_connected: false
redis_connected: false
alembic_run: false
app_runtime_started: false
live_collector_started: false
notifier_transport_enabled: false
production_rollout_performed: false
manual_intervention_required: false
result_record_required: false
```

For the future approved execution result shape, status fields remain
booleans/status only. After separate explicit approval and execution,
`tdlib_auth_attempted` and `tdlib_auth_completed` may be true only in the later
`dedicated_vps_tdlib_auth_result_record`.

Future result records must not include actual Telegram API ID, Telegram API
hash, phone number, 2FA password, TDLib DB encryption key, Telegram bot token,
login code, chat ID, delivery target ID, invite link, raw runtime env content,
DB/Redis URL, public VPS IP, operator IP, or any other secret value.

## Rollback / recovery notes

If the later approved TDLib auth operator execution fails before auth
completion:

- stop and record redacted status only in the future result record.
- do not retry with ad hoc commands that print secrets or start runtime.
- do not paste login code, 2FA prompt values, or secrets into ChatGPT, Codex,
  GitHub, repository files, markdown, or shell history.
- do not start the live collector to "test" the session.
- do not enable notifier transport.
- do not perform production rollout.
- preserve local TDLib state for operator review unless a separately approved
  rollback plan says otherwise.
- keep rollback/recovery facts redacted and source-statement-only.

If local TDLib state must be removed, backed up, or regenerated, that is a
separate explicitly approved operator action and not part of this package.

## Acceptance criteria

- Package contains no actual secret values.
- Package does not execute TDLib auth.
- Package does not connect Telegram.
- Package does not create or validate a live Telegram session.
- Package does not start live collector.
- Package does not start app runtime.
- Package does not enable notifier transport.
- Package does not perform production rollout.
- Package does not mutate runtime.env.
- Package does not read or print runtime env values.
- Package does not connect DB/Redis.
- Package does not run Alembic.
- Package does not modify Docker/systemd.
- Package identifies required TDLib auth key/path names aligned to current
  config.
- Package distinguishes required, conditional, and not-used TDLib auth keys.
- Package clearly separates collector reader account credentials from notifier
  bot token.
- Package states `TELEGRAM_BOT_TOKEN` is not a TDLib auth credential.
- Checker/test prove no unsafe command or secret-leak patterns.
- Checker reports all side-effect booleans false.
- No service code changes.
- No dependency changes.
- No migrations.
- No DB/Redis/Alembic/Docker/systemd/runtime/TDLib/Telegram/notifier/rollout
  side effects.
- Next bounded action is separate explicit operator execution and then
  `dedicated_vps_tdlib_auth_result_record`.

## Next bounded action

1. ChatGPT review.
2. Commit/push if approved.
3. VPS pull and repo-local validation.
4. Separate explicit approval to execute TDLib auth operator action:
   `dedicated_vps_tdlib_auth_operator_execution`.
5. Then create `dedicated_vps_tdlib_auth_result_record`.
6. Only after that consider channel registry / one-channel collector boundary
   smoke package.

Do not make Telegram connection, live collector startup, notifier transport,
or production rollout the immediate next action.
