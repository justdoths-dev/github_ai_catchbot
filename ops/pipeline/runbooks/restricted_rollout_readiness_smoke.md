# Restricted Rollout Readiness Smoke

## Purpose

`restricted_rollout_readiness_smoke_v1` is a repository-only readiness smoke for the local control-plane assets needed before separately approved restricted rollout planning.

It inspects files in the current repository and prints deterministic JSON. It does not connect to runtime systems, seed data, apply flags, or start services.

Passing result means only:

```text
ready for separately approved restricted rollout planning
```

## Non-Goals

- This does not authorize production rollout.
- This does not apply recommended flag patches.
- This does not mutate env files.
- This does not mutate feature flags.
- This does not require DB or Redis.
- This does not call external APIs.
- This does not start runtime workers.
- This is not a new runtime worker.
- This is not a delivery gate rewrite.
- This is not a feature flag applier.
- This is not a DB-backed runtime smoke.

## Command

Run from the repository root:

```bash
python scripts/ops/restricted_rollout_readiness_smoke.py --format json
```

Optional alternate root:

```bash
python scripts/ops/restricted_rollout_readiness_smoke.py --format json --repo-root /path/to/github_ai_catchbot
```

The script supports JSON output only.

## Assets Checked

The smoke verifies that the repository contains the expected local control-plane assets for these categories:

- Delivery gate smoke scripts, runbooks, maintenance runner source, and focused tests.
- Batch recovery retry/replay smoke scripts, runbooks, maintenance source, and focused tests.
- Maintenance and notifier safety smoke assets and config surfaces for the required flags.
- Delivery handoff, DB-backed acceptance, rollout gate query, minimal dashboard, minimal alerts, notifier rollback, and notifier acceptance runbooks.

The required flag names are exactly:

```text
ENABLE_NOTIFICATION_SEND
MAINTENANCE_ENABLE_NOTIFICATION_RETRY_PROMOTION
NOTIFIER_TELEGRAM_DRY_RUN
```

Marker checks are intentionally small substring checks. They verify that the relevant runbooks or config assets still state the local safety contract: no production rollout authorization, no env or feature flag mutation, output-only recommended flag patches, explicit retry/replay control-plane actions, operator-controlled handoff, and rollback by disabling notification send and maintenance retry promotion.

## Expected JSON Fields

Successful output includes:

```json
{
  "report_type": "restricted_rollout_readiness_smoke_v1",
  "selected_scenario": "repo_control_plane_readiness",
  "checks_failed": [],
  "failures": [],
  "production_authorization_status": "not_authorized",
  "rollout_stage": "pre_restricted_rollout_planning",
  "runtime_worker_started": false,
  "external_network_used": false,
  "database_required": false,
  "redis_required": false,
  "database_mutated": false,
  "redis_mutated": false,
  "env_mutated": false,
  "feature_flags_mutated": false,
  "recommended_flag_patch_applied": false,
  "production_db_or_redis_used": false,
  "live_collector_started": false,
  "live_notifier_transport_used": false,
  "required_flag_names": [
    "ENABLE_NOTIFICATION_SEND",
    "MAINTENANCE_ENABLE_NOTIFICATION_RETRY_PROMOTION",
    "NOTIFIER_TELEGRAM_DRY_RUN"
  ],
  "readiness_summary": {
    "delivery_gate_assets_present": true,
    "batch_recovery_assets_present": true,
    "maintenance_assets_present": true,
    "notifier_safety_assets_present": true,
    "rollback_assets_present": true,
    "handoff_assets_present": true
  },
  "recommended_next_state": "ready_for_separately_approved_restricted_rollout_planning"
}
```

`required_assets` contains one object per checked asset with:

- `category`
- `path`
- `exists`
- `markers_passed`
- `missing_markers`

## Safety Boundaries

- No DB or Redis is required.
- No external network is used.
- No env file is read or mutated.
- No feature flag is applied or mutated.
- No recommended flag patch is applied.
- No production DB or Redis is used.
- No live collector is started.
- No notifier transport is used.
- No collector, normalizer, enricher, judge, validator, policy, notifier, maintenance, replay, delivery, Docker Compose, or systemd worker is started.

## Failure Meanings

- `asset.exists:<path>` means a required local readiness asset is missing.
- `asset.markers:<path>` means the asset exists but no longer contains one or more required contract markers.

Failures are repository readiness failures only. They do not prove production state, do not inspect live infrastructure, and do not authorize any rollout action.

## Next Step

If the smoke passes, the next state is only separately approved restricted rollout planning. Operator approval, secrets review, environment plan, one-live-collector plan, rollback plan, and restricted production transport smoke remain separate future steps.
