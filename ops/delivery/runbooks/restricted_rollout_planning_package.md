# Restricted Rollout Planning Package

## Purpose

`restricted_rollout_planning_package_v1` is an operator-controlled planning package for separately approved restricted rollout planning after the repo-local Slice 16 readiness smoke.

This package keeps the canonical pipeline boundary intact:

```text
SourceMessage -> Artifact -> CandidateGroup -> EvidenceBundle -> JudgeOutput -> Analysis -> Notification
```

It references existing control-plane assets by path and does not modify them:

- `scripts/ops/restricted_rollout_readiness_smoke.py`
- `ops/pipeline/runbooks/restricted_rollout_readiness_smoke.md`
- `ops/delivery/runbooks/delivery_gate_handoff.md`
- `ops/runbooks/notifier_rollback.md`
- `ops/runbooks/notifier_acceptance_checklist.md`
- `ops/delivery/runbooks/db_backed_acceptance_smoke.md`
- `ops/delivery/sql/delivery_rollout_gate_queries.sql`
- `ops/delivery/dashboards/delivery_minimal_dashboard.md`
- `ops/delivery/alerts/delivery_minimal_alerts.yaml`

## Non-goals

- This does not authorize production rollout.
- This does not apply recommended flag patches.
- This does not mutate env files.
- This does not mutate feature flags.
- This does not read or print secret values.
- This does not require DB or Redis.
- This does not call external APIs.
- This does not start runtime workers.
- This does not start the Telegram collector.
- This does not start Telegram notifier transport.
- This does not run Docker Compose or systemd.
- This is not a feature flag applier.
- This is not a real environment validator.
- This is not a DB/Redis smoke.
- This is not a runtime worker.

## Required manual approvals

All of these approval domains must be reviewed before any restricted production rollout action:

1. `operator_approval`
2. `secrets_inventory_review`
3. `environment_plan_review`
4. `one_live_collector_plan`
5. `rollback_plan_review`
6. `restricted_transport_smoke_plan`

Actual flag changes require explicit operator approval. `recommended_flag_patch` is output-only and must remain an advisory field until an operator separately approves and applies a change.

## Rollout phase order

The rollout order is offline validation -> live ingest -> shadow analysis -> silent delivery -> restricted rollout -> full go-live.

The phases are:

1. `offline_validation`
2. `live_ingest`
3. `shadow_analysis`
4. `silent_delivery`
5. `restricted_rollout`
6. `full_go_live`

Do not skip phases. Full go-live cannot be considered until restricted rollout has passed the separately approved observation window.

## Environment readiness checklist

- Confirm the target environment and host ownership are documented.
- Confirm PostgreSQL remains the durable truth.
- Confirm Redis remains queue, lock, and short-lived execution state only.
- Confirm delivery gate output remains ops/control-plane reporting, not runtime worker behavior.
- Confirm no collector, normalizer, enricher, judge, validator, policy, notifier, or maintenance boundary is collapsed.
- Confirm `.env` files are not mutated by this package.
- Confirm feature flags are not mutated by this package.
- Confirm DB and Redis access are not required for this package.

## Secrets inventory checklist

- Identify required secret names and owners.
- Confirm each required secret has an owner and storage location.
- Confirm real secrets must be reviewed by presence/ownership only, never printed.
- Confirm this package does not read or print secret values.
- Confirm no command in this package requires a real secret value.

## One-live-collector plan

Production must have exactly one live Telegram collector instance.

Before any live ingest approval:

- Document the intended collector host or service unit.
- Document how duplicate collector instances are detected.
- Document how an operator stops a stale collector before starting the approved one.
- Confirm this package does not start the Telegram collector.

## Delivery gate prerequisite checks

Before a separately approved restricted rollout decision, review:

- `scripts/ops/restricted_rollout_readiness_smoke.py`
- `ops/pipeline/runbooks/restricted_rollout_readiness_smoke.md`
- `ops/delivery/runbooks/delivery_gate_handoff.md`
- `ops/delivery/sql/delivery_rollout_gate_queries.sql`
- `ops/delivery/dashboards/delivery_minimal_dashboard.md`
- `ops/delivery/alerts/delivery_minimal_alerts.yaml`

The delivery gate remains an operator scorecard. It does not apply flags automatically and does not start runtime workers.

## Rollback plan

Rollback must disable `ENABLE_NOTIFICATION_SEND=false`.

Rollback must disable `MAINTENANCE_ENABLE_NOTIFICATION_RETRY_PROMOTION=false`.

Review these assets before approval:

- `ops/runbooks/notifier_rollback.md`
- `ops/runbooks/notifier_acceptance_checklist.md`

Rollback must preserve durable PostgreSQL state and keep recovery focused on delivery replay or maintenance retry promotion only after explicit operator approval.

## Restricted production transport smoke plan

Restricted transport smoke requires separate explicit approval.

This package does not start Telegram notifier transport. It does not call Telegram, OpenAI, GitHub, X, or any external API.

Before approval, document:

- target chat or transport scope
- expected message count
- rollback trigger
- observer
- start time and stop time
- confirmation that `recommended_flag_patch` remains output-only until manually approved

## Go / No-Go decision record template

```text
Decision:
Operator:
Timestamp:
Target environment:
Requested phase:
Prior phase evidence:
Secrets inventory reviewed by presence/ownership only:
One-live-collector plan reviewed:
Delivery gate prerequisite checks reviewed:
Rollback plan reviewed:
Restricted transport smoke separately approved:
Flag changes explicitly approved:
No-Go reasons:
Follow-up actions:
```

## Explicit next-state boundaries

Passing the repo-local planning readiness check means only:

```text
ready_for_operator_reviewed_restricted_rollout_plan
```

It does not authorize production rollout. It does not apply recommended flag patches. It does not mutate env files, feature flags, DB rows, Redis streams, deployment files, or production config. It does not read or print secret values. It does not require DB or Redis. It does not call external APIs. It does not start runtime workers, the Telegram collector, Telegram notifier transport, Docker Compose, or systemd.
