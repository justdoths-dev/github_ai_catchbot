# Notifier Rollback

Stage 40 rollback stops Telegram transport without stopping the notifier worker.

Set these flags first:

```env
ENABLE_NOTIFICATION_SEND=false
MAINTENANCE_ENABLE_NOTIFICATION_RETRY_PROMOTION=false
```

Expected behavior:

- `notifier-telegram` remains running and keeps consuming `q.notification.send`.
- Plan rehydrate and notification plan concretization continue.
- Renders may still be appended.
- Telegram send/edit transport is skipped.
- Durable delivery records may be written with `delivery_status = suppressed`.
- Reason metadata must distinguish rollback from dry-run with `notification_send_flag_disabled`.
- Queue recovery is explicit delivery replay or retry promotion after the issue is fixed.

Do not rerun collector, enrichment, judge, validator, or policy to recover delivery-only failures.
