# Dedicated VPS Baseline Preflight Check

## Purpose

`dedicated_vps_baseline_preflight_v1` is a metadata-only operator preflight for a newly purchased dedicated `github_ai_catchbot` VPS.

It answers whether the current host/repository/runtime shape is plausible before live provisioning work starts. It prints JSON only.

Passing result means only:

```text
dedicated VPS baseline metadata is acceptable for the selected mode
```

Dedicated VPS baseline preflight success does not authorize live ingest or production rollout.

## Dedicated VPS Decision Lock

- `github_ai_catchbot` is expected to run on a dedicated VPS.
- The dedicated VPS must not be shared with the existing trading-bot VPS.
- The trading-bot VPS and `github_ai_catchbot` VPS are separate operational failure domains.
- This check records `shared_with_trading_bot: false`.
- This check records `trading_bot_repo_inspected: false`.
- This check records `trading_bot_paths_touched: false`.
- This check does not inspect the trading-bot repository.
- This check does not add trading-bot integration.

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
- This does not run git commands through subprocess.
- This does not inspect the trading-bot repository.
- This does not touch trading-bot paths.
- This does not mutate `.env`, feature flags, repository config, or production files.
- This does not create TDLib directories, secret files, production lock files, or any other production path.
- This does not acquire the real configured singleton lock.
- This does not use Redis or PostgreSQL as a singleton lock.
- This does not read or print secret values.
- This does not print env values, raw filesystem paths, hostname, username, home path, IP address, or provider metadata.
- This does not apply `recommended_flag_patch`.

## Commands

Run from the repository root:

```bash
python scripts/ops/dedicated_vps_baseline_preflight_check.py --format json
```

Default `schema` mode is safe for CI and local execution. It does not require production environment values and does not inspect real host paths.

Current-host metadata inspection is explicit:

```bash
python scripts/ops/dedicated_vps_baseline_preflight_check.py --format json --mode current-host
```

Use `--mode current-host` only when the operator intentionally wants local host/repo/venv metadata checked. The report still never prints raw paths or host identifiers.

## Checked Metadata

Deployment topology fields are fixed:

- `expected_deployment_topology`
- `shared_with_trading_bot`
- `trading_bot_repo_inspected`
- `trading_bot_paths_touched`

Current-host mode reports only coarse host metadata:

- `system`
- `machine`
- `python_major_minor`
- `is_posix`

Current-host mode reports repo metadata as booleans only:

- `repo_root_detected`
- `pyproject_present`
- `docs_project_source_present`
- `scripts_ops_present`
- `ops_pipeline_runbooks_present`
- `tests_present`

Current-host mode reports venv metadata as booleans only:

- `venv_dir_present`
- `venv_python_present`
- `running_inside_venv`
- `python_version_supported`

Python `3.12` or newer is required for current-host pass status.

Runtime directory labels are reported without paths:

- `app_dir`
- `state_dir`
- `tdlib_state_parent`
- `blob_cache_parent`
- `logs_parent`
- `secrets_parent`
- `backups_parent`

Only `app_dir` maps to the detected repository root in current-host mode. Other future production path labels are `not_applicable` until a concrete repo-owned convention exists. The check does not create missing directories.

## Expected JSON Fields

Successful default output includes:

```json
{
  "report_type": "dedicated_vps_baseline_preflight_v1",
  "contract_status": "passed",
  "mode": "schema",
  "checks_failed": [],
  "failures": [],
  "deployment_topology": {
    "expected_deployment_topology": "dedicated_vps",
    "shared_with_trading_bot": false,
    "trading_bot_repo_inspected": false,
    "trading_bot_paths_touched": false
  },
  "host_metadata": {},
  "repo_metadata": {},
  "venv_metadata": {},
  "runtime_directory_metadata": {},
  "redaction": {
    "env_values_printed": false,
    "secret_values_printed": false,
    "raw_paths_printed": false,
    "hostname_printed": false,
    "username_printed": false,
    "home_path_printed": false,
    "ip_address_printed": false,
    "provider_metadata_printed": false
  },
  "side_effects": {
    "tdlib_started": false,
    "telegram_called": false,
    "db_connection_attempted": false,
    "redis_connection_attempted": false,
    "external_network_attempted": false,
    "docker_invoked": false,
    "systemd_invoked": false,
    "services_started": false,
    "collector_started": false,
    "env_or_feature_flags_mutated": false,
    "production_files_created": false,
    "trading_bot_repo_inspected": false,
    "trading_bot_paths_touched": false
  },
  "authorization": {
    "live_ingest_authorized": false,
    "production_rollout_authorized": false
  }
}
```

If a current-host check fails, `contract_status` is `failed` and each failure contains only `check` and `reason_code`.

## Safety Boundaries

- No TDLib client is started.
- No Telegram connection or auth is attempted.
- No DB connection is attempted.
- No Redis connection is attempted.
- No external network call is attempted.
- No Docker, Docker Compose, systemd, or process supervisor command is invoked.
- No git subprocess is invoked.
- No trading-bot repository is inspected.
- No trading-bot path is touched.
- No raw host, path, user, environment, secret, network, or provider metadata is printed.
- No production file is created.
- No real singleton lock is acquired.
- No live ingest is authorized.
- No production rollout is authorized.
