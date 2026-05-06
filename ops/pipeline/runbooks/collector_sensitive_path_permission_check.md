# Collector Sensitive Path Permission Check

## Purpose

`collector_sensitive_path_permission_v1` is a restricted collector sensitive-path permission metadata check.

It inspects only permission metadata for collector secret-file paths, TDLib state/files directories, and the singleton lock parent. It prints JSON only.

Passing result means only:

```text
collector sensitive path permission metadata is acceptable for the selected mode
```

Permission check success does not authorize live ingest or production rollout.

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
- This does not print env values, secret values, secret file paths, raw TDLib paths, raw singleton lock paths, uid/gid, mode bits, `DATABASE_URL`, `REDIS_URL`, or `TELEGRAM_PHONE_NUMBER`.
- This does not apply `recommended_flag_patch`.

## Commands

Run from the repository root:

```bash
python scripts/ops/collector_sensitive_path_permission_check.py --format json
```

Default `schema` mode is safe for CI and local execution. It does not require production environment values and does not inspect real paths.

Current-process environment inspection is explicit:

```bash
python scripts/ops/collector_sensitive_path_permission_check.py --format json --mode current-env
```

Use `--mode current-env` only when the operator intentionally wants local process sensitive path permission metadata checked. Values and raw paths are still never printed.

## Checked Names

Secret file path names:

- `TELEGRAM_API_HASH_FILE`
- `TELEGRAM_2FA_PASSWORD_FILE`
- `TDLIB_DB_ENCRYPTION_KEY_FILE`

TDLib directory names:

- `TDLIB_STATE_DIR`
- `TDLIB_FILES_DIR`

Singleton lock name:

- `COLLECTOR_SINGLETON_LOCK_PATH`

`COLLECTOR_SINGLETON_LOCK_PATH` is reported as `not_applicable` when absent but `TDLIB_STATE_DIR` is present, because the current collector config can derive its default lock path from the TDLib state directory. The check never computes or prints that raw path and never acquires the lock.

## Permission Metadata

For secret-file variables, current-env mode reports only booleans and a status:

- `checked`
- `path_present`
- `exists`
- `is_file`
- `is_symlink`
- `readable_by_process`
- `owner_readable`
- `group_readable`
- `world_readable`
- `owner_writable`
- `group_writable`
- `world_writable`
- `unsafe_world_readable`
- `unsafe_group_writable`
- `unsafe_world_writable`
- `permission_status`

Secret files fail when absent, missing, not regular files, symlinks, unreadable by the current process, world-readable, group-writable, or world-writable.

For `TDLIB_STATE_DIR` and `TDLIB_FILES_DIR`, current-env mode reports only:

- `checked`
- `path_present`
- `exists`
- `is_dir`
- `is_symlink`
- `parent_exists`
- `writable_by_process_or_parent`
- `world_writable`
- `parent_world_writable`
- `unsafe_world_writable`
- `permission_status`

Missing TDLib directories are not created. If a directory is missing, the check inspects only parent metadata. Existing non-directories, symlinks, unwritable directories or parents, world-writable directories, and world-writable parents fail.

For `COLLECTOR_SINGLETON_LOCK_PATH`, current-env mode reports only parent metadata:

- `checked`
- `path_present`
- `parent_exists`
- `parent_writable_by_process`
- `parent_world_writable`
- `unsafe_parent_world_writable`
- `permission_status`

The check does not create the lock file and does not acquire the singleton lock.

## Expected JSON Fields

Successful default output includes:

```json
{
  "report_type": "collector_sensitive_path_permission_v1",
  "contract_status": "passed",
  "mode": "schema",
  "checks_failed": [],
  "failures": [],
  "secret_file_permissions": {},
  "tdlib_path_permissions": {},
  "singleton_lock_parent_permissions": {},
  "redaction": {
    "env_values_printed": false,
    "secret_values_printed": false,
    "secret_file_contents_read": false,
    "raw_paths_printed": false,
    "uid_gid_printed": false,
    "mode_bits_printed": false,
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
    "singleton_lock_acquired": false,
    "secret_file_contents_read": false
  },
  "authorization": {
    "live_ingest_authorized": false,
    "production_rollout_authorized": false
  }
}
```

If a current-env sensitive path permission check fails, `contract_status` is `failed` and each failure contains only `env_name` and `reason_code`.

## Safety Boundaries

- No TDLib client is started.
- No Telegram connection or auth is attempted.
- No DB connection is attempted.
- No Redis connection is attempted.
- No external network call is attempted.
- No Docker, Docker Compose, systemd, or process supervisor command is invoked.
- No production file is created.
- No real singleton lock is acquired.
- No secret file content is read.
- No env value, secret value, secret file path, TDLib path, singleton path, database URL, Redis URL, phone number, uid/gid, or mode bit value is printed.
- No live ingest is authorized.
- No production rollout is authorized.
