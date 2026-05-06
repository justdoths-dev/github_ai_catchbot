# Collector Restricted Environment Inventory Check

## Purpose

`collector_restricted_environment_inventory_v1` is a collector-specific restricted environment inventory check.

It reports required collector environment variable presence and safe path metadata for secret-file, TDLib directory, and singleton lock path settings. It prints JSON only.

Passing result means only:

```text
collector restricted-environment inventory metadata is acceptable for the selected mode
```

Inventory success does not authorize live ingest or production rollout.

## Non-Goals

- This is not live ingest.
- This is not TDLib auth.
- This is not a Telegram connection.
- This is not DB or Redis connectivity validation.
- This is not Docker or systemd validation.
- This is not production rollout.
- This does not start TDLib.
- This does not call Telegram, OpenAI, GitHub, X, Web, or external network.
- This does not connect to PostgreSQL.
- This does not connect to Redis.
- This does not create database engines or Redis clients.
- This does not invoke Docker, Docker Compose, systemd, systemctl, shell service commands, or process supervisors.
- This does not mutate `.env`, feature flags, repository config, or production files.
- This does not create TDLib directories, secret files, production lock files, or any other production path.
- This does not acquire the real configured singleton lock.
- This does not use Redis or PostgreSQL as a singleton lock.
- This does not read secret file contents.
- This does not print env values, secret values, secret file paths, raw TDLib paths, raw singleton lock paths, `DATABASE_URL`, `REDIS_URL`, or `TELEGRAM_PHONE_NUMBER`.
- This does not apply `recommended_flag_patch`.

## Commands

Run from the repository root:

```bash
python scripts/ops/collector_restricted_environment_inventory_check.py --format json
```

Default `schema` mode is safe for CI and local execution. It does not require production environment values.

Current-process environment inspection is explicit:

```bash
python scripts/ops/collector_restricted_environment_inventory_check.py --format json --mode current-env
```

Use `--mode current-env` only when the operator intentionally wants local process environment name presence and path metadata checked. Values are still never printed.

## Inventory

Required common names:

- `APP_ENV`
- `DATABASE_URL`
- `REDIS_URL`

Required collector names:

- `COLLECTOR_MODE`
- `TELEGRAM_API_ID`
- `TELEGRAM_API_HASH_FILE`
- `TELEGRAM_PHONE_NUMBER`
- `TELEGRAM_2FA_PASSWORD_FILE`
- `TDLIB_STATE_DIR`
- `TDLIB_FILES_DIR`
- `TDLIB_DB_ENCRYPTION_KEY_FILE`
- `COLLECTOR_SINGLETON_LOCK_PATH`

Optional collector tuning names:

- `RECONCILE_INTERVAL_SEC`
- `RECONCILE_BACKFILL_LIMIT`
- `WARM_BACKFILL_LIMIT`
- `HISTORY_PAGE_LIMIT`
- `STARTUP_PROBE_TIMEOUT_SEC`
- `STARTUP_WARM_BACKFILL_ENABLED`

Missing optional tuning names do not fail the check.

`COLLECTOR_SINGLETON_LOCK_PATH` is reported as `not_applicable` when absent but `TDLIB_STATE_DIR` is present, because the current collector config can derive its default lock path from the TDLib state directory. The check never computes or prints that raw path and never acquires the lock.

## Path Metadata

For `_FILE` variables, current-env mode reports only:

- `path_present`
- `exists`
- `is_file`
- `readable`
- `parent_exists`

For `TDLIB_STATE_DIR` and `TDLIB_FILES_DIR`, current-env mode reports only:

- `path_present`
- `exists`
- `is_dir`
- `parent_exists`
- `writable_or_creatable`

The check uses metadata only. If a TDLib directory is missing and its parent exists, writability is checked on the parent. The check does not create test files or directories.

For `COLLECTOR_SINGLETON_LOCK_PATH`, current-env mode reports only:

- `path_present`
- `parent_exists`
- `parent_writable`

The check does not create the lock file and does not acquire the singleton lock.

## Expected JSON Fields

Successful default output includes:

```json
{
  "report_type": "collector_restricted_environment_inventory_v1",
  "contract_status": "passed",
  "mode": "schema",
  "checks_failed": [],
  "failures": [],
  "redaction": {
    "env_values_printed": false,
    "secret_values_printed": false,
    "secret_file_contents_read": false,
    "raw_paths_printed": false,
    "database_url_printed": false,
    "redis_url_printed": false,
    "phone_number_printed": false
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
    "production_files_created": false,
    "singleton_lock_acquired": false
  },
  "authorization": {
    "live_ingest_authorized": false,
    "production_rollout_authorized": false
  }
}
```

If a current-env required name or required path metadata check fails, `contract_status` is `failed` and `checks_failed` contains reason codes keyed by environment variable name and metadata field.

## Safety Boundaries

- No TDLib client is started.
- No Telegram connection or auth is attempted.
- No DB connection is attempted.
- No Redis connection is attempted.
- No external network call is attempted.
- No Docker, Docker Compose, systemd, or process supervisor command is invoked.
- No production file is created.
- No real singleton lock is acquired.
- No env value, secret value, secret file path, TDLib path, singleton path, database URL, Redis URL, or phone number is printed.
- No live ingest is authorized.
- No production rollout is authorized.
