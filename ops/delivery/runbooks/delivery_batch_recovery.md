# Delivery Batch Recovery

Stage 42 batch recovery is a control-plane procedure. It must never reset
`notification_plans`, must not mutate notifier-owned delivery rows, and must not
recalculate upstream analysis, judge, evidence bundle, candidate, artifact, or
source message state. Batch recovery must not recalculate upstream analysis.

All delivery recovery keeps the delivery root:

- `root_object_type = notification_plan`
- `root_object_id = notification_plan_id`

Allowed recovery bridges:

- `replay_requests` with `replay_type = delivery`
- `event_outbox` bridge using `notification.plan.created.v1` for retry intent

## Auto Retry Allowed

Auto retry is allowed only when all conditions are true:

- latest delivery status is `failed_retryable`
- `send_after <= now()`
- latest attempt count is below the retry ceiling
- the row is not a send-disabled suppress row

Send-disabled suppress backlog is explicit replay only, not auto retry.

## Explicit Replay Required

Explicit delivery replay is required for:

- send-disabled suppress backlog
- `failed_terminal`
- retry ceiling exceeded DLQ row
- `replay_requests.status = rejected_by_env_guard`
- `replay_requests.status = unsupported_in_stage41`

Explicit replay uses `delivery_replay_from_notification_plan`.
It does not start from analysis, judge, bundle, candidate, artifact, or source message roots.

## Recovery Priority

1. send-disabled suppress backlog
2. retry ceiling exceeded backlog
3. terminal chat access fixed backlog
4. render/template fix backlog
5. env-guard rejected replay backlog

## Prohibited Actions

- Do not reset `notification_plans.status`.
- Do not update `notification_plans.send_after` from batch recovery assets.
- Do not auto-close delivery DLQ rows.
- Do not auto-retry terminal failures.
- Do not change retry or replay dispatch semantics.
- Do not implement a Stage 43 batch recovery CLI in this stage.
