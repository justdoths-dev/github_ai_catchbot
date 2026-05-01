# Delivery Replay

Delivery replay starts at the notification boundary only.

Replay root:

```text
root_object_type = notification_plan
root_object_id = notification_plan_id
replay_type = delivery
```

Allowed replay scope:

- Rehydrate the existing notification plan.
- Append a new `notification.plan.created.v1` replay-intent for the same `notification_plan_id`.
- Let `notifier-telegram` rehydrate the plan and decide render / transport according to current delivery flags.
- Update `replay_requests.status` only when the deployed schema supports it.

Forbidden replay scope:

- Do not recompute `analysis`.
- Do not call judge/OpenAI or rewrite `judge_output`.
- Do not rebuild evidence bundles, candidates, artifacts, or source messages.
- Do not recompute `verdict` or `delivery_decision`.
- Do not mutate `notification_plans`, `notification_renders`, or `notification_delivery_records` from maintenance.

Production replay dispatch requires explicit operator approval and `ENABLE_REPLAY_TO_PROD_DB=true`. The default remains `ENABLE_REPLAY_TO_PROD_DB=false`.
