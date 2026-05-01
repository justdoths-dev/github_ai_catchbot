# Delivery Retry Promotion

Maintenance handles due retry promotion only.

Rules:

- Consume `notification.delivery.result.v1` from `q.maintenance` and rehydrate through `event_outbox.trigger_event_id`.
- Periodically scan due `notification_plans.status = failed_retryable` rows using `send_after <= now()` so retry promotion does not depend on a fresh queue event at the due time.
- Treat `notification_plans` and `notification_delivery_records` as read-only.
- Promote only `failed_retryable` plans whose `send_after` is due.
- Emit a new `notification.plan.created.v1` retry-intent for the same `notification_plan_id`.
- Do not mutate `notification_plans`, `notification_renders`, or `notification_delivery_records`.
- Do not call Telegram, OpenAI, GitHub, X, or web fetchers.
- Do not recompute verdicts or delivery decisions.

Rollback notes:

- `notification_send_flag_disabled` suppressed rows are not automatic retry candidates.
- `dry_run_skip_transport` suppressed rows are not automatic retry candidates.
- Send-disabled recovery starts from an explicit delivery replay request rooted at `notification_plan`.

Ceiling:

- `DELIVERY_RETRY_MAX_ATTEMPTS` defaults to `3`.
- When delivery attempts reach the ceiling, maintenance appends a `dead_letter_entries` boundary record and does not emit retry intent.
