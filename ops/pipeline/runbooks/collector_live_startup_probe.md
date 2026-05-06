# Collector Live Startup Probe

## Purpose

`collector_live_startup_probe_v1` is a repo-local fake-runtime startup probe for collector live-startup prerequisites.

It validates the collector config and singleton seams using synthetic temp runtime directories and synthetic fake secret files. It prints deterministic JSON and does not start real collector ingest.

Passing result means only:

```text
collector live-startup local prerequisites passed
```

Probe success does not authorize live ingest or production rollout.

## Non-Goals

- This is not live ingest.
- This is not TDLib auth.
- This is not a Telegram connection.
- This is not production rollout.
- This does not connect to PostgreSQL.
- This does not connect to Redis.
- This does not call Telegram, TDLib, OpenAI, GitHub, X, or external network.
- This does not invoke Docker, Docker Compose, systemd, shell service commands, or process supervisors.
- This does not mutate `.env`, feature flags, repository config, or production files.
- This does not read or print real secret values.
- This does not apply recommended flag patches.

## Command

Run from the repository root:

```bash
python scripts/ops/collector_live_startup_probe.py --format json
```

The script supports JSON output only.

## Checks

The probe verifies:

- repo-local execution only
- synthetic environment and temp runtime directories only
- default collector singleton lock-path computation
- `COLLECTOR_SINGLETON_LOCK_PATH` override computation
- singleton guard acquire/release on a temp lock path
- duplicate singleton acquisition is blocked by existing guard behavior
- replay-mode service startup skips live singleton acquisition
- fake runtime startup/stop path executes through the collector service seam

The fake runtime implements only in-memory `startup_acceptance_check`, `run_forever`, and `shutdown` methods. It does not instantiate TDLib, database, Redis, external API, Docker, or systemd clients.

## Expected JSON Fields

Successful output includes:

```json
{
  "report_type": "collector_live_startup_probe_v1",
  "contract_status": "passed",
  "checks_failed": [],
  "failures": [],
  "checks": {
    "repo_local_only": "passed",
    "uses_synthetic_environment_only": "passed",
    "default_lock_path_computed": "passed",
    "override_lock_path_computed": "passed",
    "singleton_guard_acquire_release": "passed",
    "replay_mode_skips_live_singleton": "passed",
    "fake_runtime_start_stop": "passed"
  },
  "side_effects": {
    "tdlib_started": false,
    "telegram_called": false,
    "db_connection_attempted": false,
    "redis_connection_attempted": false,
    "external_network_attempted": false,
    "docker_invoked": false,
    "systemd_invoked": false,
    "env_or_feature_flags_mutated": false,
    "secret_values_printed": false
  },
  "authorization": {
    "live_ingest_authorized": false,
    "production_rollout_authorized": false
  }
}
```

If a required check fails, `contract_status` is `failed` and `checks_failed` contains exact reason codes.

## Safety Boundaries

- No DB or Redis connection is attempted.
- No TDLib client is started.
- No Telegram/OpenAI/GitHub/X/Web network call is attempted.
- No Docker, Docker Compose, systemd, or process supervisor command is invoked.
- No live collector worker is started.
- No production file is mutated.
- No `.env` file or feature flag is mutated.
- No secret value or fake secret value is printed.
- No live ingest or production rollout is authorized.
