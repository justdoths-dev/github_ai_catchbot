# Dedicated VPS TDLib auth runtime env invalid redacted diagnostic/fix plan

## Purpose

This is a repo-local redacted diagnostic/fix-plan slice for the previously
recorded `blocked_runtime_env_invalid` TDLib auth wrapper result.

It identifies runtime.env shape problems that can prevent
`CollectorTelegramConfig` construction. It reports key names, presence,
empty-state, redacted value classes, format status, issue codes, and a future
fix-plan only. It never reports values.

## Source-of-truth boundary

`docs/project-source/README_replacement_consolidated_v0_20.md` is the first
source of truth. The locked project-source bundles `00` through `10` remain
the source-of-truth set in filename order. `03_GitHub_AI_application_plan.md`
is advisory only.

The architecture invariant remains:

```text
SourceMessage -> Artifact -> CandidateGroup -> EvidenceBundle -> JudgeOutput -> Analysis -> Notification
```

## Scope

Allowed behavior:

- inspect repository source text for the existing collector Telegram config
  contract
- read runtime.env only when the operator explicitly passes
  `--runtime-env-path`
- parse runtime.env internally into key/value pairs
- report key-level redacted presence and format categories only
- produce a redacted fix plan for a later operator fix slice

The tool does not edit runtime.env. The tool does not write result records.

## Non-authorizations

This slice does not execute the TDLib auth wrapper.

This slice does not rerun TDLib auth.

This slice does not start `collector main.py`, `CollectorTelegramService`,
`CollectorRuntime`, app runtime, notifier transport, or production rollout.

This slice does not request or handle Telegram login code or 2FA values.

This slice does not create a TDLib client or session and does not contact
Telegram.

This slice does not connect to DB or Redis and does not run Alembic.

This slice does not change Docker or systemd.

This slice does not run source build, package-manager mutation, git clone,
cmake, make, or ninja.

## Runtime env read rule

The default CLI path reads no runtime env file:

```bash
cd ~/workspace/bots/github_ai_catchbot
venv/bin/python scripts/ops/dedicated_vps_tdlib_auth_runtime_env_invalid_redacted_diagnostic_or_fix_plan.py \
  --format json
```

To inspect the dedicated VPS runtime env shape, the operator must pass the path
explicitly:

```bash
cd ~/workspace/bots/github_ai_catchbot
venv/bin/python scripts/ops/dedicated_vps_tdlib_auth_runtime_env_invalid_redacted_diagnostic_or_fix_plan.py \
  --format json \
  --runtime-env-path /etc/github-ai-catchbot/runtime.env \
  > /tmp/dedicated_vps_tdlib_auth_runtime_env_invalid_redacted_diagnostic_or_fix_plan.json
```

Values must never be displayed, pasted into ChatGPT, pasted into Codex, added
to GitHub, committed to the repo, written into markdown, or stored in shell
history.

## Safe output inspection

Inspect only safe top-level fields from the redirected JSON:

```bash
cd ~/workspace/bots/github_ai_catchbot
venv/bin/python - <<'PY'
import json
from pathlib import Path

p = Path("/tmp/dedicated_vps_tdlib_auth_runtime_env_invalid_redacted_diagnostic_or_fix_plan.json")
data = json.loads(p.read_text(encoding="utf-8"))

for key in (
    "schema_version",
    "contract_name",
    "contract_status",
    "recommended_next_slice",
    "boundary_check",
):
    print(key, data[key])

print("runtime_env_read", data["runtime_env_inspection"]["runtime_env_read"])
print("runtime_env_values_printed", data["runtime_env_inspection"]["runtime_env_values_printed"])
print("secret_values_printed", data["runtime_env_inspection"]["secret_values_printed"])
PY
```

Do not display runtime.env contents. Do not display the full redirected JSON if
an operator has not first reviewed that it contains only redacted fields.

## Output statuses

`runtime_env_read_blocked` means no runtime env path was provided. The next
slice is `defer_manual_review`.

`runtime_env_path_missing` means the explicitly provided path was missing or
unreadable. The next slice is `defer_manual_review`.

`runtime_env_invalid_diagnostic_ready` means required key, empty value, format,
duplicate key, malformed line, or redacted config-build evidence is enough to
prepare a future operator fix plan. The next slice is
`tdlib_auth_runtime_env_operator_fix_plan`.

`runtime_env_shape_appears_valid` means the redacted shape check did not find a
blocking runtime env shape issue. This does not rerun auth. The next slice is
`tdlib_auth_operator_execution_rerun_after_fix`.

`runtime_env_invalid_diagnostic_inconclusive` means the tool could not infer
the config contract or could not make a bounded redacted diagnosis. The next
slice is `defer_manual_review`.

## Redacted key checks

Each key check may include:

- key name
- required status
- present status
- empty status
- value class, such as `absent`, `empty`, `integer_like`,
  `boolean_like`, `path_like`, `url_like_redacted`,
  `secret_like_redacted`, `opaque_present_redacted`, or `invalid_format`
- format status
- issue code

No raw runtime env values are part of the contract.

## Future fix examples only

The following are NOT RUN / FUTURE SLICE ONLY examples. They are not commands
for this diagnostic slice.

- NOT RUN / FUTURE SLICE ONLY: Set missing key `TELEGRAM_API_HASH` with
  `<operator-provided-secret-value>` through an approved runtime.env fix
  process.
- NOT RUN / FUTURE SLICE ONLY: Replace invalid key `TELEGRAM_API_ID` with
  `<operator-provided-integer-value>` through an approved runtime.env fix
  process.
- NOT RUN / FUTURE SLICE ONLY: Remove duplicate key entries after an operator
  decides which entry is authoritative.

Do not perform any fix from this diagnostic/fix-plan slice.

## Acceptance criteria

- Default invocation reads no runtime env file.
- Explicit `--runtime-env-path` is required before runtime.env is read.
- The output is valid JSON.
- Runtime env values are not included in output.
- Secret values are not included in output.
- Raw exception messages are not included in output.
- The auth wrapper is not executed.
- TDLib auth is not attempted.
- Telegram is not contacted.
- Collector, notifier, rollout, DB, Redis, Alembic, Docker, systemd, source
  build, and package-manager mutation remain unused.
- `boundary_check` is `pass`.

## Next bounded action

If the output is `runtime_env_invalid_diagnostic_ready`, the next bounded
action is a separate `tdlib_auth_runtime_env_operator_fix_plan` slice.

If the output is `runtime_env_shape_appears_valid`, the next bounded action is
a separate `tdlib_auth_operator_execution_rerun_after_fix` slice. That later
slice still requires explicit approval and must not be conflated with this
diagnostic.
