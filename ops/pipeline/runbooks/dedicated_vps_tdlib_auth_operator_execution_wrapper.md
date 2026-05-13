# Dedicated VPS TDLib auth operator execution wrapper

## Purpose

Add a repo-local operator wrapper for a later, separately approved TDLib auth
execution decision.

This slice does not execute TDLib auth. This wrapper is not approval to run TDLib auth. No Telegram connection occurs in this slice. No live collector start occurs. No notifier transport or rollout occurs.

The wrapper resolves the current execution-entrypoint ambiguity by reporting
whether a standalone, auth-only TDLib entrypoint exists in the current
repository. The current auth-only entrypoint exists, so the wrapper reports it
as available while still requiring separate explicit approval before any TDLib
auth execution.

## Source-of-truth / architecture boundary

`docs/project-source/README_replacement_consolidated_v0_20.md` remains
authoritative. The locked project-source bundles `00` through `10` remain the
source-of-truth set and are interpreted in filename order. The GitHub AI
application plan remains advisory only.

This wrapper preserves the canonical architecture invariant:

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

- Latest expected repo HEAD before this implementation slice:
  `e205bf1 test(ops): add TDLib auth operator wrapper`.
- Previous TDLib package:
  `90b2d9e test(ops): add TDLib auth package`.
- Previous result record:
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
- TDLib auth package is PASS.
- TDLib auth operator wrapper is PASS.
- Wrapper default/no-approval mode has no side effects.
- Wrapper previously found no safe standalone TDLib-auth-only entrypoint.
- TDLib auth has NOT been executed.
- Telegram connection has NOT been established.
- Live collector has NOT been started.
- Notifier transport has NOT been enabled.
- Production rollout remains unauthorized.

These conclusions are not reopened by this wrapper.

## Scope

This slice updates the repo-local wrapper to detect the new standalone
auth-only entrypoint, adds the auth-only entrypoint implementation, and adds
focused fake-transport unit tests.

The wrapper uses repository source text only. It does not import or start the
collector runtime. It does not import collector `main.py`. It does not
instantiate TDLib clients or transports. It does not read the runtime env file,
process secret values, connect to DB/Redis, run Alembic, mutate files, run
Docker/systemd, enable notifier delivery, or perform rollout.

## Non-authorizations

This wrapper does not execute TDLib auth.
This wrapper does not connect Telegram.
This wrapper does not create or validate a real TDLib session.
This wrapper does not start live collector.
This wrapper does not start the app runtime.
This wrapper does not enable notifier transport.
This wrapper does not perform production rollout.
This wrapper does not mutate runtime.env.
This wrapper does not mutate `/etc/github-ai-catchbot/runtime.env`.
This wrapper does not read `/etc/github-ai-catchbot/runtime.env`.
This wrapper does not print runtime.env values.
This wrapper does not read or print runtime env values.
This wrapper does not connect DB/Redis.
This wrapper does not connect to DB or Redis.
This wrapper does not run Alembic.
This wrapper does not modify Docker/systemd.
This wrapper does not modify Docker or systemd.
This wrapper does not add secrets.

Passing this wrapper does not authorize TDLib auth, Telegram connection, live
collector startup, notifier transport, app runtime startup, DB/Redis
connection, Alembic, Docker/systemd changes, runtime env mutation, or
production rollout.

## Wrapper behavior

Default mode prints JSON only and remains no-side-effect:

- no runtime env read;
- no secret read;
- no TDLib auth;
- no Telegram connection;
- no app runtime start;
- no live collector;
- no notifier transport;
- no DB/Redis/Alembic;
- no Docker/systemd;
- no file mutation;
- no network call.

The wrapper may accept the explicit future approval flag
`--approved-tdlib-auth-operator-execution`, but this slice does not run that
mode. If a safe auth-only entrypoint exists, the wrapper still remains approval
required in default mode and does not call the entrypoint.

## Auth-only entrypoint decision

Current source inspection finds the standalone TDLib-auth-only entrypoint:

```text
src.services.collector_telegram.auth_entrypoint
```

The entrypoint is separate from collector runtime/service/main and is designed
to pump TDLib authorization states through injected auth-only boundaries.

`src/services/collector_telegram/main.py` is the collector runtime entrypoint.
It loads collector config, builds `CollectorRuntime` and
`CollectorTelegramService`, installs signal handling, and runs the service.
It is not an approved auth-only command for this wrapper.

The current auth-only entrypoint is `src.services.collector_telegram.auth_entrypoint`.
Actual operator execution still requires separate explicit approval.

## Approved execution guard

The approval flag name is:

```text
--approved-tdlib-auth-operator-execution
```

The default report must set `approved_execution_requested` to false. The
wrapper must not call any auth entrypoint unless this flag is present and a
safe standalone auth-only entrypoint has been identified.

This repository state has such an entrypoint, so the wrapper reports
`auth_only_entrypoint_status: available` and
`contract_status: approval_required` in default/no-approval mode.

## Redacted output shape

The wrapper prints a JSON object containing at least:

```yaml
report_type: dedicated_vps_tdlib_auth_operator_execution_wrapper_v1
contract_status: approval_required
approval_required: true
approved_execution_requested: false
auth_only_entrypoint_status: available
selected_entrypoint: src.services.collector_telegram.auth_entrypoint
runtime_env_path: /etc/github-ai-catchbot/runtime.env
runtime_env_read: false
runtime_env_values_printed: false
tdlib_auth_attempted: false
tdlib_auth_completed: false
telegram_connected: false
session_state_created_or_reused: false
database_connected: false
redis_connected: false
alembic_run: false
app_runtime_started: false
live_collector_started: false
notifier_transport_enabled: false
production_rollout_performed: false
files_mutated: false
network_called: false
checks_failed:
  - approval.required
failures:
  - check: approval.required
    message: TDLib auth operator execution requires separate explicit approval.
likely_next_slice: dedicated_vps_tdlib_auth_operator_execution
```

No secret values, login codes, phone numbers, API hashes, runtime env contents,
DB URLs, Redis URLs, invite links, VPS addresses, or operator addresses are
included.

## Operator safety rules

`TELEGRAM_BOT_TOKEN` is not used for TDLib auth.
`TELEGRAM_BOT_TOKEN` is not a TDLib auth credential.

TDLib auth belongs to the collector reader account / TDLib / MTProto credential
surface. The notifier bot token belongs to Telegram Bot API delivery only.

Telegram login code and 2FA prompt values must never be pasted into ChatGPT,
Codex, GitHub, repo files, markdown, terminal history, or review bundles.

The runtime env path may be referenced as a path label only. Do not display the
runtime secret file. Do not load it into the shell. Do not append secret
assignments from shell commands.

## Acceptance criteria

- Wrapper exists and runs in default/no-approval mode with no side effects.
- Wrapper does not execute TDLib auth.
- Wrapper does not connect Telegram.
- Wrapper does not start live collector.
- Wrapper does not start app runtime.
- Wrapper does not enable notifier transport.
- Wrapper does not mutate, read, or print runtime.env.
- Wrapper does not connect DB/Redis.
- Wrapper does not run Alembic.
- Wrapper does not modify Docker/systemd.
- Wrapper clearly reports whether a safe auth-only entrypoint is available.
- Safe auth-only entrypoint is available as
  `src.services.collector_telegram.auth_entrypoint`.
- Wrapper recommends `dedicated_vps_tdlib_auth_operator_execution`.
- `TELEGRAM_BOT_TOKEN` is not treated as a TDLib auth credential.
- Login code and 2FA prompt values are never recorded.
- No service code, dependencies, migrations, DB/Redis/Alembic/Docker/systemd,
  runtime, TDLib, Telegram, notifier, or rollout behavior is changed.
- `tests/unit/ops/pipeline` passes.

## Next bounded action

Because the current repository now has a safe standalone TDLib-auth-only
entrypoint, the next bounded action is:

```text
dedicated_vps_tdlib_auth_operator_execution
```

The review sequence is:

1. ChatGPT review bundle.
2. Commit/push if approved.
3. VPS pull and repo-local validation.
4. Create/approve `dedicated_vps_tdlib_auth_operator_execution`.
5. After execution, create `dedicated_vps_tdlib_auth_result_record`.

Actual TDLib auth execution remains a later, separately approved operator
action after a safe auth-only entrypoint exists.
