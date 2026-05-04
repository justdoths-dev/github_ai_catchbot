# Policy-engine runtime smoke

This runbook verifies the Post-Stage44 policy-engine DB/Redis runtime boundary:

`analysis.policy.apply.v1 -> outbox-relay -> q.analysis.policy -> policy-engine -> analyses + notification.plan.created.v1`

The smoke is opt-in and writes only marker-scoped synthetic rows into a local smoke database. It does not call OpenAI, external network, Telegram, notifier transport, or source enrichers. It does not start the notifier worker and must not create `notification_plans`, `notification_renders`, or `notification_delivery_records`.

## Preconditions

- Run only against a local dev/test/smoke PostgreSQL database.
- Run only against local Redis DB 14.
- Apply Alembic head to the smoke database before execution.
- Keep `.codex`, caches, logs, virtualenvs, and `/tmp` outputs untracked.

## Command

```bash
cd ~/workspace/bots/github_ai_catchbot
source venv/bin/activate

python scripts/ops/policy_engine_runtime_smoke.py \
  --database-url "$GITHUB_AI_CATCHBOT_DB_SMOKE_DATABASE_URL" \
  --redis-url "${REDIS_URL:-redis://localhost:6379/14}" \
  --confirm write
```

## Expected JSON

The script prints a single JSON report with `report_type = policy_engine_runtime_smoke_v1`.

Success requires:

- one pending `analysis.policy.apply.v1` row is seeded and then marked `published`;
- outbox-relay resolves the event to `q.analysis.policy` with `stage_name = analysis_policy`;
- Redis payload contains only `job_id`, `stage_name`, `root_object_type`, `root_object_id`, `idempotency_key`, `pipeline_run_id`, `not_before`, and `trigger_event_id`;
- policy-engine consumes and acknowledges exactly one stream message;
- one `analyses` row is created for the seeded judge output and policy versions;
- final verdict is recomputed from scores and differs from the seeded model proposal;
- one policy success `state_transitions` row is recorded;
- one pending `notification.plan.created.v1` outbox intent is emitted for the non-suppress result;
- `judge_outputs`, `candidate_evidence_bundles`, and `candidate_group_proposals.current_analysis_id` are not mutated;
- no notifier-owned rows are created.

Exit code is non-zero when `checks_failed` or `failures` is non-empty.
