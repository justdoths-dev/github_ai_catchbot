# Evidence Assembler Runtime Smoke

This post-Stage44 smoke verifies the DB/Redis evidence-assembler boundary. It does not call external network, OpenAI, live Telegram collector, or notifier transport.

## Command

```bash
export GITHUB_AI_CATCHBOT_DB_SMOKE_DATABASE_URL='postgresql+psycopg://...@localhost:5432/github_ai_catchbot_smoke'
export REDIS_URL='redis://localhost:6379/14'

python scripts/ops/evidence_assembler_runtime_smoke.py --confirm write
```

`--redis-url redis://localhost:6379/14` can be used instead of `REDIS_URL`.

## Safety

- Requires explicit `--confirm write`.
- Requires a local PostgreSQL URL whose database name contains `smoke`, `test`, or `dev`.
- Requires local Redis DB 14.
- Redacts DB and Redis URL fragments in JSON failures.
- Does not reset the database or Redis.
- Does not clean unrelated rows.
- Seeds rows with `ops-smoke:evidence-assembler-runtime:<smoke_id>`.

## Boundary Verified

- `artifact.snapshot.updated.v1` routes through outbox-relay to `q.candidate.bundle`.
- Redis Stream payload remains thin and ID-only.
- The evidence-assembler worker consumes the thin message and rehydrates `event_outbox` by `trigger_event_id`.
- GitHub snapshot updates without `candidate_group_id` fan out through `candidate_group_members`.
- Existing `artifact_registry.current_snapshot_id/current_status` and `artifact_snapshots` are used.
- `discovered_url_observations` are included only in `discovered_links_summary_json`; no artifacts are created from discovered URLs.
- `candidate_evidence_bundles` and `candidate_evidence_members` are written.
- `candidate_group_proposals.current_bundle_id` is updated.
- A ready GitHub bundle emits pending `analysis.requested.v1` with `judge_profile=github_primary`.
- The Redis message is acked.

The final stdout line is a JSON report. Success requires:

```json
{
  "checks_failed": [],
  "failures": []
}
```
