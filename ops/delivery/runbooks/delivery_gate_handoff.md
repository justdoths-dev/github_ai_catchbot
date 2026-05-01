# Delivery Gate Handoff

Stage 44 defines rollout handoff as an operator-controlled process. It does not implement feature flag auto-apply.

## Report Metric Order

Restricted reports list metrics in this order: `success_rate_1h`, `high_source_to_delivery_p95_sec`, `plan_to_transport_p95_sec`, `due_retry_oldest_lag_sec`, `open_delivery_dlq_count`, `unexpected_send_disabled_count`.

Full reports append `success_rate_24h`, `replay_guard_reject_count_24h`, `retry_ceiling_exceeded_count_24h`, `oldest_delivery_dlq_age_sec`, and `duplicate_noop_ratio_1h`.

## Restricted Rollout Handoff

1. Run the gate runner in restricted mode and require `pass`.
2. Operator manually applies the recommended flag patch.
3. Verify notifier and maintenance health.
4. Observe for 15-30 minutes.
5. Verify no new open DLQ, no due retry lag breach, and no unexpected send-disabled suppress rows.

## Full Rollout Handoff

1. Confirm a stable restricted observation window.
2. Run the full gate.
3. Require hard metrics to pass.
4. Run or record full gate with `operator_review_passed=true`.
5. Review warning-only fields, including duplicate/no-op ratio and operator notes.
6. Approve full rollout manually.

## Fail Rules

Runtime or transport hard fail means set `ENABLE_NOTIFICATION_SEND=false` and `MAINTENANCE_ENABLE_NOTIFICATION_RETRY_PROMOTION=false`.

Rollout-only fail means transport may stay on, but full rollout remains blocked.

Gate output is advisory and does not apply flags automatically. Batch recovery remains a separate one-shot CLI path and requires `--confirm write`.
