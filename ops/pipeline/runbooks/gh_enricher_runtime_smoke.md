# gh-enricher Runtime Smoke

`scripts/ops/gh_enricher_runtime_smoke.py` is an opt-in post-Stage44 runtime verification slice for the gh-enricher boundary:

```text
artifact.enrich.requested.v1
-> outbox-relay route/publish
-> Redis Stream q.artifact.enrich.github
-> gh-enricher worker rehydrates event_outbox by trigger_event_id
-> gh-enricher writes GitHub snapshot tables with a deterministic fake GitHub client
-> artifact.snapshot.updated.v1 pending outbox row
-> Redis ack
```

## Boundaries

- Use only a dev/test PostgreSQL database from `GITHUB_AI_CATCHBOT_DB_SMOKE_DATABASE_URL`.
- Use local Redis DB 14 from `REDIS_URL` or pass `--redis-url`.
- Do not point this smoke at production DB/Redis.
- The script uses an in-process fake GitHub client; it does not call the real GitHub API and does not require GitHub App credentials.
- It does not start the live Telegram collector or notifier transport.
- It does not reset the database or Redis and does not clean unrelated rows.
- It leaves marker-scoped synthetic seed rows, gh-enricher output rows, an acked Redis Stream entry, and one pending `artifact.snapshot.updated.v1` outbox row.

## Run

```bash
export GITHUB_AI_CATCHBOT_DB_SMOKE_DATABASE_URL="postgresql+psycopg://user:pass@localhost:5432/github_ai_catchbot_smoke"
export REDIS_URL="redis://localhost:6379/14"

python scripts/ops/gh_enricher_runtime_smoke.py --confirm write
```

An explicit Redis URL can be supplied without changing the environment:

```bash
python scripts/ops/gh_enricher_runtime_smoke.py \
  --redis-url "redis://localhost:6379/14" \
  --confirm write
```

## Passing Output

The script prints one final JSON object. A passing run has empty `checks_failed` and `failures`, redacted URL indicators, the expected queue name, seeded IDs, resulting snapshot IDs, downstream outbox IDs, and fake GitHub client calls.

```json
{
  "report_type": "gh_enricher_runtime_smoke_v1",
  "checks_failed": [],
  "failures": [],
  "database_url_redacted": true,
  "redis_url_redacted": true,
  "queue_name": "q.artifact.enrich.github",
  "seeded_ids": {
    "artifact_id": "...",
    "candidate_group_id": "...",
    "enrich_event_id": "..."
  },
  "resulting_snapshot_ids": ["..."],
  "downstream_outbox_ids": ["..."]
}
```

## Required Checks

The smoke verifies:

- outbox relay routing publishes `artifact.enrich.requested.v1` with `provider_route=github` to `q.artifact.enrich.github`.
- Redis payload is ID-only and does not include `event_outbox.payload_json`.
- gh-enricher consumes `q.artifact.enrich.github` and rehydrates by `trigger_event_id`.
- `artifact_enrichment_runs`, `artifact_snapshots`, `artifact_snapshot_github_repo`, `artifact_snapshot_github_file_samples`, and `discovered_url_observations` are written.
- `artifact_registry.current_snapshot_id` is set and `current_status` is `ready` or `partial_ready`.
- a pending `artifact.snapshot.updated.v1` outbox row is emitted.
- the Redis message is acked.
- no real GitHub network call is made.
