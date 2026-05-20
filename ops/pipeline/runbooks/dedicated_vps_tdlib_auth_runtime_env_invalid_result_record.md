# Dedicated VPS TDLib auth runtime env invalid result record

## Scope

This is a result record only.

It records an already completed approved auth-only VPS operator rerun on
`github-ai-catchbot-prod-1`. No new operation is performed by this record.

This record is not a TDLib auth rerun slice, runtime.env diagnostic or fix
slice, live collector slice, notifier slice, or rollout slice.

## Source-of-truth boundary

`docs/project-source/README_replacement_consolidated_v0_20.md` remains
authoritative. The locked project-source bundles `00` through `10` remain the
source-of-truth set and are interpreted in filename order. The GitHub AI
application plan remains advisory only.

The canonical architecture remains unchanged:

```text
SourceMessage -> Artifact -> CandidateGroup -> EvidenceBundle -> JudgeOutput -> Analysis -> Notification
```

## Recorded VPS context

- VPS: `github-ai-catchbot-prod-1`
- operator account: `deploy`
- repo: `~/workspace/bots/github_ai_catchbot`
- branch: `main`
- repo HEAD at execution:
  `3731134 docs(ops): record tdjson source build operator result`
- git status at execution: `## main...origin/main`

## Recorded tdjson preflight-before-auth

- selected libtdjson path:
  `/opt/github-ai-catchbot/tdlib/lib/libtdjson.so.1.8.64`
- `TDJSON_PREFLIGHT_BEFORE_AUTH_RERUN_PASS tdjson_available pass`

## Recorded auth wrapper invocation

- wrapper:
  `scripts/ops/dedicated_vps_tdlib_auth_operator_execution_wrapper.py`
- approved flag: `--approved-tdlib-auth-operator-execution`
- runtime env path flag:
  `--runtime-env-path /etc/github-ai-catchbot/runtime.env`
- output path:
  `/tmp/dedicated_vps_tdlib_auth_operator_execution_rerun.json`

## Recorded wrapper result

```yaml
contract_status: blocked_runtime_env_invalid
blocked_reason: runtime_env_invalid
checks_failed: ["runtime_env.invalid"]
failure check: runtime_env.invalid
failure message: Approved TDLib auth execution could not build collector config: ConfigurationError.
approved_execution_requested: true
approval_required: false
auth_only_entrypoint_status: available
selected_entrypoint: src.services.collector_telegram.auth_entrypoint
runtime_env_path: /etc/github-ai-catchbot/runtime.env
runtime_env_read: true
runtime_env_values_printed: false
secret_values_printed: false
tdlib_auth_attempted: false
tdlib_auth_completed: false
telegram_connected: false
session_state_created_or_reused: false
manual_intervention_required: false
network_called: false
files_mutated: false
collector_main_used: false
collector_service_used: false
collector_runtime_used: false
live_collector_started: false
app_runtime_started: false
notifier_transport_enabled: false
production_rollout_performed: false
database_connected: false
db_connected: false
redis_connected: false
alembic_run: false
docker_or_systemd_changed: false
systemd_or_docker_changed: false
telegram_bot_token_used_for_tdlib_auth: false
```

## Recorded final marker

```text
TDLIB_AUTH_OPERATOR_EXECUTION_RERUN_RESULT blocked_runtime_env_invalid tdlib_auth_attempted=False tdlib_auth_completed=False telegram_connected=False session_state_created_or_reused=False manual_intervention_required=False
```

## Explicit interpretation

This is a pre-auth runtime env / collector config construction block.

This is not TDLib auth success.

This is not TDLib auth failure after Telegram contact.

No TDLib auth attempt occurred.

No Telegram connection occurred.

No session state was created or reused.

No manual intervention was requested.

runtime.env was read only by the approved wrapper.

runtime.env values were not printed.

secrets were not printed.

## Explicit boundary statements

This result record does not run TDLib auth.
This result record does not run the auth wrapper.
This result record does not read runtime.env.
This result record does not write runtime.env.
This result record does not print runtime.env values.
This result record does not print secrets.
This result record does not request or handle Telegram login code/2FA.
This result record does not create TDLib client/session.
This result record does not contact Telegram.
This result record does not start collector/app runtime/notifier/rollout.
This result record does not connect to DB/Redis or run Alembic.
This result record does not change Docker/systemd.
This result record does not run source build/git clone/cmake/make/ninja.
This result record does not mutate packages.
This result record does not diagnose or fix runtime.env.

## Explicit non-authority statements

blocked_runtime_env_invalid does not authorize direct auth retry.

blocked_runtime_env_invalid does not authorize live collector startup.

blocked_runtime_env_invalid does not authorize notifier startup.

blocked_runtime_env_invalid does not authorize production rollout.

blocked_runtime_env_invalid must be handled by a separate bounded redacted
diagnostic/fix-plan slice.

## Next candidate slice

The next candidate slice is
`dedicated_vps_tdlib_auth_runtime_env_invalid_redacted_diagnostic_or_fix_plan`.

This result record does not perform that diagnostic or fix.
