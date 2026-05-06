# Restricted Environment Preflight Package

## 1. Purpose

`restricted_environment_preflight_package_v1` is a repo-local plus optional environment-presence-only package for separately approved restricted rollout planning.

This does not authorize production rollout.

It preserves the canonical pipeline boundary:

```text
SourceMessage -> Artifact -> CandidateGroup -> EvidenceBundle -> JudgeOutput -> Analysis -> Notification
```

The package verifies that required environment variable names and operational plan markers are documented before an operator performs an environment inventory review.

Passing this package means only `ready_for_operator_environment_inventory_review`.

## 2. Non-goals

- This does not authorize production rollout.
- This does not apply recommended flag patches.
- This does not mutate env files.
- This does not mutate feature flags.
- This does not connect to DB or Redis.
- This does not call external APIs.
- This does not start runtime workers.
- This does not start the Telegram collector.
- This does not start Telegram notifier transport.
- This does not run Docker Compose or systemd.
- This does not read secret file contents.
- This does not print secret values.
- This does not print secret file contents.
- This is not a feature flag applier.
- This is not a real secret validator.
- This is not a DB/Redis smoke.
- This is not a runtime worker.
- This is not Docker Compose or systemd execution.
- This is not real Telegram, OpenAI, GitHub, or X transport.

## 3. Relationship to Slice 16 and Slice 17

Slice 16 added repo-local restricted rollout readiness smoke:

- `scripts/ops/restricted_rollout_readiness_smoke.py`
- `ops/pipeline/runbooks/restricted_rollout_readiness_smoke.md`

Slice 17 added restricted rollout planning package readiness:

- `ops/delivery/runbooks/restricted_rollout_planning_package.md`
- `scripts/ops/restricted_rollout_planning_readiness_check.py`

Slice 18 does not replace those checks. It adds an environment-preflight planning layer that checks this runbook, confirms Slice 16 and Slice 17 assets still exist, and optionally checks environment variable-name presence only.

## 4. Required env-name inventory

Environment checks are presence-only.

Common:

- `APP_ENV`
- `DATABASE_URL`
- `REDIS_URL`

Collector:

- `TELEGRAM_API_ID`
- `TELEGRAM_API_HASH_FILE`
- `TELEGRAM_PHONE_NUMBER`
- `TDLIB_STATE_DIR`

Notifier:

- `TELEGRAM_BOT_TOKEN_FILE`
- `TELEGRAM_OPERATOR_CHAT_ID`
- `ENABLE_NOTIFICATION_SEND`
- `NOTIFIER_TELEGRAM_DRY_RUN`

Maintenance/delivery:

- `MAINTENANCE_ENABLE_NOTIFICATION_RETRY_PROMOTION`

Judge/OpenAI:

- `OPENAI_API_KEY_FILE`

GitHub:

- `GITHUB_APP_ID`
- `GITHUB_INSTALLATION_ID`
- `GITHUB_PRIVATE_KEY_FILE`

Optional/future:

- `X_BEARER_TOKEN_FILE`

## 5. Secret redaction rules

Real secret values must never be pasted into reports or logs.

This does not read secret file contents.

This does not print secret values.

This does not print secret file contents.

Secret file variables such as `TELEGRAM_API_HASH_FILE`, `TELEGRAM_BOT_TOKEN_FILE`, `OPENAI_API_KEY_FILE`, `GITHUB_PRIVATE_KEY_FILE`, and `X_BEARER_TOKEN_FILE` must be reported by variable name and present/missing status only. Their values may be host paths and must not be printed.

Generic secret inventory wording is not enough for this boundary. Operator evidence must explicitly state that secret values and secret file contents were not read or printed.

## 6. Environment presence-only check procedure

Default check:

```bash
python scripts/ops/restricted_environment_preflight_check.py --format json
```

Optional environment-name presence check:

```bash
python scripts/ops/restricted_environment_preflight_check.py --format json --check-env-presence
```

Optional repository root:

```bash
python scripts/ops/restricted_environment_preflight_check.py --format json --repo-root /path/to/repo
```

The default mode is repo-local only. It must not require production environment variables.

When `--check-env-presence` is used, the check may report that a name such as `DATABASE_URL` or `TELEGRAM_BOT_TOKEN_FILE` is present or missing, but it must not connect to infrastructure, read variable values, print variable values, print secret file paths, or read secret file contents.

## 7. One-live-collector environment plan

Production must have exactly one live Telegram collector instance.

Before any separately approved live ingest or restricted transport smoke:

- Record the intended collector host, service name, or process ownership.
- Record how duplicate collector instances are detected.
- Record the operator command or procedure used to stop a stale collector.
- Confirm this package does not start the Telegram collector.
- Confirm this package does not run Docker Compose or systemd.

## 8. Manual feature-flag application boundary

Actual flag changes require explicit operator approval.

`recommended_flag_patch` is output-only.

This does not apply recommended flag patches.

This does not mutate feature flags.

This does not mutate env files.

The delivery gate remains ops/control-plane reporting, not runtime worker behavior.

## 9. DB/Redis non-connection boundary

This does not connect to DB or Redis.

This does not mutate DB rows.

This does not mutate Redis streams.

PostgreSQL remains durable truth.

Redis remains queue, lock, and short-lived execution state only.

Generic database wording is not enough for this boundary. Operator evidence must explicitly state that the package did not connect to DB or Redis.

## 10. Restricted transport prerequisites

Restricted transport smoke requires separate explicit approval.

Before separate approval, the operator must have reviewed:

- environment variable-name presence inventory
- secret ownership and storage locations without reading secret values
- one-live-collector plan
- rollback plan
- delivery gate report
- expected transport scope, expected message count, start time, stop time, observer, and rollback trigger

This does not call external APIs.

This does not start Telegram notifier transport.

It does not call Telegram, OpenAI, GitHub, X, DB, or Redis.

## 11. Operator evidence template

```text
Evidence timestamp:
Operator:
Target environment:
Repo HEAD:
Preflight command:
Env presence mode:
Required env-name groups reviewed:
Missing required env names:
Optional/future env names reviewed:
Secret values read or printed: no
Secret file contents read or printed: no
DB/Redis connected: no
External APIs called: no
Runtime workers started: no
Telegram collector started: no
Telegram notifier transport started: no
Docker Compose/systemd invoked: no
Env files mutated: no
Feature flags mutated: no
recommended_flag_patch applied: no
One-live-collector plan reviewed:
Restricted transport smoke separately approved:
No-Go reasons:
Follow-up actions:
```

## 12. Go / No-Go criteria for environment inventory review

Go for environment inventory review only:

- The preflight package is present.
- Slice 16 readiness smoke assets are present.
- Slice 17 planning package assets are present.
- Required env-name inventory is documented.
- Environment checks are presence-only.
- Secret redaction rules are explicit.
- DB/Redis non-connection boundary is explicit.
- External API non-call boundary is explicit.
- Runtime worker, collector, notifier transport, Docker Compose, and systemd non-start boundaries are explicit.
- One-live-collector invariant is explicit.
- Manual feature-flag application boundary is explicit.
- Restricted transport smoke is recorded as a separate approval.

No-Go for environment inventory review:

- Any required marker is missing.
- Any required Slice 16 or Slice 17 asset is missing.
- A report includes secret values, secret file contents, or secret file paths from env values.
- A command connects to DB/Redis or external APIs.
- A command mutates env files, feature flags, DB rows, Redis streams, deployment files, or production config.
- A command starts runtime workers, the Telegram collector, Telegram notifier transport, Docker Compose, or systemd.
- A report claims production rollout authorization.

## 13. Explicit next-state boundaries

Passing this package means only `ready_for_operator_environment_inventory_review`.

It does not authorize production rollout. It does not apply recommended flag patches. It does not mutate env files. It does not mutate feature flags. It does not connect to DB or Redis. It does not call external APIs. It does not start runtime workers. It does not start the Telegram collector. It does not start Telegram notifier transport. It does not run Docker Compose or systemd. It does not read secret file contents. It does not print secret values. It does not print secret file contents.

Production rollout remains unauthorized until a later, explicit, operator-approved step.
