# Delivery Dead-Letter Triage

Stage 42 delivery DLQ triage is operator-driven. Do not auto-close delivery DLQ
rows, do not auto-retry terminal failures, and do not recalculate upstream
analysis, judge, bundle, candidate, or artifact state for delivery recovery.

Delivery recovery starts from:

- `root_object_type = notification_plan`
- `root_object_id = notification_plan_id`
- `replay_hint = delivery_replay_from_notification_plan`

## last_error_code

Locked delivery DLQ vocabulary:

- `max_notification_retry_attempts_exceeded`
- `notify_transport_terminal_chat_access`
- `notify_transport_terminal_edit_forbidden`
- `notify_render_invalid_payload`
- `delivery_replay_env_guard_rejected`
- `delivery_replay_unsupported_request`
- `maintenance_due_retry_emit_failed`

## next_manual_action

Locked operator action vocabulary:

- `request_explicit_delivery_replay`
- `fix_chat_access_then_delivery_replay`
- `disable_edits_then_delivery_replay`
- `fix_template_then_delivery_replay`
- `acknowledge_and_close_no_recovery`
- `fix_env_guard_then_retry_replay_request`

## replay_hint

Locked replay hint vocabulary:

- `delivery_replay_from_notification_plan`

## Triage Rules

- Retry ceiling rows require operator review before explicit delivery replay.
- Terminal chat access failures require access repair before delivery replay.
- Terminal edit failures should disable edit assumptions before delivery replay.
- Invalid render payload rows require template or rendering data repair before
  delivery replay.
- Env guard rejections require environment approval before retrying the replay
  request.
- Unsupported replay requests should be acknowledged and closed if no recovery is
  possible in the current stage.

Operator recovery must use explicit delivery replay or the retry-intent bridge as
appropriate. Delivery recovery must not reset `notification_plans.status` and
must not mutate `notification_plans` from maintenance or control-plane assets.
