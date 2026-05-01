# Notifier Acceptance Checklist

Queue wiring:

- `notification.plan.created.v1` routes to `q.notification.send`.
- Notifier consumes thin Redis messages and rehydrates by `trigger_event_id`.
- `notification.delivery.result.v1` routes to `q.maintenance`.

Runtime behavior:

- `send_after > now()` skips transport.
- Same-material duplicate delivery is a no-op.
- Send/edit rules preserve Stage 39 behavior.
- `message is not modified` is treated as a logical no-op.
- `ENABLE_NOTIFICATION_SEND=false` keeps the worker alive and blocks only transport.

Environment safety:

- Dev/test defaults do not live-send or edit.
- Replay defaults do not live-send or edit.
- Prod baseline starts with transport disabled.
- Restricted/full delivery requires explicit `ENABLE_NOTIFICATION_SEND=true`.
- Maintenance retry promotion is disabled by default and opt-in for rollout.

Recovery boundary:

- Retryable failure leaves `notification_plans.status = failed_retryable` and `send_after = next_retry_at`.
- Maintenance treats notification plans as read-only and emits a retry-intent `notification.plan.created.v1` outbox event when due.
- Retry ceiling creates a dead-letter boundary; it does not mutate the plan.
- Send-disabled rollback recovery uses explicit delivery replay, not automatic retry promotion.
- Delivery replay root is `notification_plan` only.
- Production delivery replay requires explicit operator approval and `ENABLE_REPLAY_TO_PROD_DB=true`.
- Upstream source, artifact, candidate, bundle, judge, and analysis rows are not recomputed.
