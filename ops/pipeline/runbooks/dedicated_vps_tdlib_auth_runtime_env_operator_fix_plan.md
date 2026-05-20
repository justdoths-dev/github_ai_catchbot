# Dedicated VPS TDLib auth runtime env operator fix plan

## Purpose

This is an operator fix-planning slice only. It consumes the redacted
diagnostic JSON produced by
`dedicated_vps_tdlib_auth_runtime_env_invalid_redacted_diagnostic_or_fix_plan.py`
and emits a value-free future operator plan for runtime.env key-level fixes.

The plan tool does not read runtime.env. It does not edit runtime.env. It does
not collect, display, or store runtime.env values or secret values.

## Source-of-truth boundary

`docs/project-source/README_replacement_consolidated_v0_20.md` is the first
source of truth. The locked project-source bundles `00` through `10` remain
the source-of-truth set in filename order. `03_GitHub_AI_application_plan.md`
is advisory only.

The architecture invariant remains:

```text
SourceMessage -> Artifact -> CandidateGroup -> EvidenceBundle -> JudgeOutput -> Analysis -> Notification
```

## Inputs

Allowed inputs:

- explicit `--diagnostic-json` path to a redacted diagnostic JSON file
- optional `--runtime-env-path` path string for planning text only
- optional `--format json`

Forbidden inputs:

- raw runtime.env text
- secret values
- operator-provided replacement values
- Telegram login code or 2FA values

The default runtime env target path string is
`/etc/github-ai-catchbot/runtime.env`, but this tool treats it as text only.
The file is not opened.

## Non-authorizations

This slice does not execute a runtime.env fix.

This slice does not run TDLib auth, the auth execution wrapper, collector
main, `CollectorTelegramService`, `CollectorRuntime`, app runtime, notifier
transport, or production rollout.

This slice does not create or reuse TDLib session state and does not contact
Telegram.

This slice does not connect to DB or Redis and does not run Alembic.

This slice does not change Docker or systemd.

This slice does not run source build, package-manager mutation, git clone,
cmake, make, or ninja.

## Safety gate

Before producing a fix plan, the tool rejects unsafe diagnostic JSON.

Unsafe diagnostic JSON includes:

- `runtime_env_values_printed=true`
- `secret_values_printed=true`
- `raw_values_in_output=true`
- `boundary_check` not equal to `pass`
- schema or contract name mismatch
- a non-null `value_to_use` inside a diagnostic fix action
- serialized diagnostic content that looks like a Telegram bot token, Telegram
  API hash assignment, Telegram phone assignment, login code assignment,
  password assignment, PostgreSQL connection string, Redis connection string,
  or private Telegram invite link

If the diagnostic JSON is unsafe, stop and perform manual review. Do not use
the diagnostic as input to an operator fix.

## Safe invocation

Run the plan tool against the redirected redacted diagnostic JSON:

```bash
cd ~/workspace/bots/github_ai_catchbot
venv/bin/python scripts/ops/dedicated_vps_tdlib_auth_runtime_env_operator_fix_plan.py \
  --format json \
  --diagnostic-json /tmp/dedicated_vps_tdlib_auth_runtime_env_invalid_redacted_diagnostic_or_fix_plan.json \
  --runtime-env-path /etc/github-ai-catchbot/runtime.env \
  > /tmp/dedicated_vps_tdlib_auth_runtime_env_operator_fix_plan.json
```

This command reads the diagnostic JSON file only. It does not read or edit the
runtime env target path.

## Safe top-level inspection

Inspect safe top-level plan fields only:

```bash
cd ~/workspace/bots/github_ai_catchbot
venv/bin/python - <<'PY'
import json
from pathlib import Path

p = Path("/tmp/dedicated_vps_tdlib_auth_runtime_env_operator_fix_plan.json")
data = json.loads(p.read_text(encoding="utf-8"))

for key in (
    "schema_version",
    "contract_name",
    "contract_status",
    "recommended_next_slice",
    "boundary_check",
):
    print(key, data[key])

print("diagnostic_values_safe", data["diagnostic_input"]["diagnostic_values_safe"])
print("runtime_env_read", data["runtime_env_target"]["runtime_env_read"])
print("runtime_env_modified", data["runtime_env_target"]["runtime_env_modified"])
print("action_count", len(data["selected_plan"]["actions"]))
PY
```

Do not display runtime.env contents. Do not paste secrets into ChatGPT, Codex,
GitHub, markdown, shell history, logs, or review bundles.

## Output statuses

`diagnostic_json_missing` means no diagnostic path was provided or the provided
diagnostic file could not be read. The next slice is `defer_manual_review`.

`diagnostic_json_unsafe` means the diagnostic JSON failed the safety gate. The
next slice is `defer_manual_review`.

`runtime_env_operator_fix_plan_ready` means the redacted diagnostic was ready
and the tool produced key-level future actions. The next slice is
`tdlib_auth_runtime_env_operator_fix_execution`.

`runtime_env_operator_fix_plan_inconclusive` means the diagnostic said the
runtime env was invalid, but the redacted action set was empty or not usable.
The next slice is `defer_manual_review`.

`diagnostic_not_ready` means the diagnostic was still blocked, missing,
inconclusive, or not addressed to this fix-plan slice. The next slice is
`defer_manual_review`.

`runtime_env_shape_already_valid` means the redacted diagnostic shape check
already appeared valid. This does not rerun auth. The next slice is
`tdlib_auth_operator_execution_rerun_after_fix`, which still requires separate
approval.

## Future fix instructions

The selected plan may include only key-level instructions:

- `set_missing_key`: set the named key later with an operator-provided private
  value
- `replace_invalid_value`: replace the named key later with an
  operator-provided private value
- `remove_duplicate_key`: remove duplicate key entries later after the
  operator decides which entry is authoritative
- `manual_review`: review a redacted issue category only

All actual values must be supplied privately by the operator only during a
later approved fix execution slice.

Future manual editing may use `sudoedit /etc/github-ai-catchbot/runtime.env`,
but only in a later approved fix execution slice and only without displaying
values in shared tools, logs, markdown, review bundles, or shell history.

## Acceptance criteria

- The tool consumes redacted diagnostic JSON only.
- The tool does not read runtime.env.
- The tool does not edit runtime.env.
- The tool does not print runtime.env values.
- The tool does not print secret values.
- The tool does not collect operator replacement values.
- The tool does not generate commands containing actual values.
- The tool does not run TDLib auth or the auth execution wrapper.
- The tool does not start collector, app runtime, notifier, or rollout.
- The tool does not connect to DB or Redis and does not run Alembic.
- The tool does not change Docker or systemd.
- The tool does not run source build or package-manager mutation.
- `boundary_check` is `pass`.

## Next bounded action

If the status is `runtime_env_operator_fix_plan_ready`, the next bounded action
is a separate `tdlib_auth_runtime_env_operator_fix_execution` slice.

If the status is `runtime_env_shape_already_valid`, the next bounded action is
a separate `tdlib_auth_operator_execution_rerun_after_fix` slice.

Any future auth rerun remains explicitly out of scope for this planning slice.
