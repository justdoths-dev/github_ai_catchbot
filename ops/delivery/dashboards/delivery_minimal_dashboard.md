# Delivery Minimal Dashboard

Stage 42 defines dashboard panels as documentation and query anchors only. Do not
add Grafana, external dashboard code, or runtime worker logic for this stage.

## Delivery Runtime

Purpose: show whether the delivery line is currently delayed.

Query anchors:
- `current_unsent_backlog`
- `due_retry_backlog`
- `restricted_oldest_due_retry_lag`

Fields:
- `q.notification.send` backlog count
- oldest unsent plan age
- due retry count
- oldest due retry lag
- future `send_after` backlog count when available

## Delivery Outcome Mix

Purpose: separate successful sends/edits from suppressions, retryable failures,
terminal failures, and no-op delivery outcomes.

Query anchors:
- `trailing_1h_delivery_outcome_mix`
- `trailing_1h_transport_error_class_mix`
- `full_duplicate_noop_ratio`

Expected outcome labels:
- `sent`
- `edited`
- `suppressed`
- `failed_retryable`
- `failed_terminal`
- `notification_duplicate_noop`
- `telegram_edit_not_modified_noop`

## Delivery DLQ / Recovery

Purpose: keep delivery dead-letter triage and recovery backlog visible without
mutating notifier-owned rows.

Query anchors:
- `delivery_dlq_triage_view`
- `send_disabled_suppress_backlog_selection`
- `full_retry_ceiling_exceeded_count`
- `full_replay_guard_reject_count`

Fields:
- delivery-related dead-letter count
- oldest DLQ age
- top `last_error_code`
- top `next_manual_action`
- replay request statuses: `requested`, `dispatched`, `completed`,
  `unsupported_in_stage41`, `rejected_by_env_guard`

## Rollout Gate Scorecard

Purpose: provide inputs for restricted rollout and full rollout decisions. This
panel does not implement the Stage 43 gate runner.

Restricted rollout gate inputs:
- trailing 1h delivery success rate
- HIGH source-to-delivery p95
- plan-to-transport p95
- oldest due retry lag
- open delivery DLQ count
- unexpected send-disabled suppress count

Full rollout gate inputs:
- trailing 24h delivery success rate
- retry ceiling exceeded count
- oldest delivery DLQ age
- prod replay guard reject count
- duplicate/no-op ratio

Restricted rollout can fail on any unexplained delivery DLQ row.
Full rollout requires zero unexpected send-disabled suppress rows.
