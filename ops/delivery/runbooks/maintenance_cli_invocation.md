# Maintenance CLI Invocation

Stage 44 keeps maintenance worker runtime separate from one-shot delivery control-plane commands.

## Worker Mode

Run the long-lived maintenance workers with:

```bash
python -m src.services.maintenance.worker_bootstrap
```

This is the service startup mode. It owns retry promotion and replay orchestration workers only.

## One-Shot Gate

Run delivery gates as read-only operator checks:

```bash
python -m src.services.maintenance.main delivery-gate --mode restricted --format json
python -m src.services.maintenance.main delivery-gate --mode full --format json --operator-review-passed true
```

The gate does not apply flags, edit `.env`, or write compose override files. It only reports gate status and an output-only recommended flag patch.

## One-Shot Batch Recovery

Run write-capable batch recovery only with explicit confirmation:

```bash
python -m src.services.maintenance.main batch-recovery replay-selected --plan-id <uuid> --requested-by ops --confirm write
python -m src.services.maintenance.main batch-recovery retry-selected-due --plan-id <uuid> --requested-by ops --confirm write
```

Batch recovery writes only `replay_requests` or `event_outbox` manual retry-intent rows. It does not mutate `notification_plans`, `notification_renders`, `notification_delivery_records`, or `state_transitions`.

## Compose One-Shot Examples

Use `docker compose run --rm maintenance` for operator-invoked control-plane commands:

```bash
docker compose run --rm maintenance python -m src.services.maintenance.main delivery-gate --mode restricted --format json
docker compose run --rm maintenance python -m src.services.maintenance.main batch-recovery replay-selected --plan-id <uuid> --requested-by ops --confirm write
```

Never put batch-recovery as the compose service default command. Never run batch-recovery via `docker compose up`. Gate and recovery commands are operator-invoked one-shot commands, not service startup commands.
