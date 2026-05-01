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
- Re-render the Telegram message if needed.
- Retry notifier transport according to the current delivery flags.

Forbidden replay scope:

- Do not recompute `analysis`.
- Do not call judge/OpenAI or rewrite `judge_output`.
- Do not rebuild evidence bundles, candidates, artifacts, or source messages.
- Do not recompute `verdict` or `delivery_decision`.

When production replay dispatch is implemented later, it must require explicit operator approval and `ENABLE_REPLAY_TO_PROD_DB=true`. The Stage 40 default is `ENABLE_REPLAY_TO_PROD_DB=false`.
