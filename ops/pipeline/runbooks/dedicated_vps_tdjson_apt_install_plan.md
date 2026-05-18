# Dedicated VPS tdjson apt install plan

## Purpose

This is an apt install plan slice only for the dedicated VPS tdjson runtime
dependency. It produces a repo-local, read-only/report-only JSON plan for a
future reviewed installation slice.

Installation is not performed in this slice. `apt update`, `apt install`,
`apt upgrade`, `apt remove`, and `apt purge` are not performed. Package-manager
mutation commands are forbidden here.

This slice does not authorize a TDLib auth rerun. It does not create a TDLib
client/session, does not call tdjson client functions, does not contact
Telegram, does not read runtime.env, and does not print secrets.

## Boundary

The plan tool may inspect only local non-secret host metadata:

- `/etc/os-release`
- `platform.machine()` and `platform.platform()`
- read-only command discovery with `shutil.which`
- strict allowlisted local commands:
  - `dpkg-query -W -f=${binary:Package}\t${Version}\t${Status}\n <package>`
  - `apt-cache policy <package>`
  - `apt-cache show <package>`
  - `apt-cache depends <package>`
  - `apt-cache search <query>`
  - `ldconfig -p`
  - `uname -m`
- optional explicit prior preflight JSON passed with `--preflight-json`
- optional explicit prior package-decision JSON passed with
  `--package-decision-json`

No default `/tmp` prior-output path is read. If either prior JSON report is not
explicitly provided, the tool continues without it.

Forbidden in this slice:

- `sudo`
- `apt install`, `apt-get install`
- `apt update`, `apt-get update`
- `apt upgrade`, `apt-get upgrade`
- `apt remove`, `apt-get remove`
- `apt purge`, `apt-get purge`
- `dpkg -i`
- package download, `curl`, `wget`
- `git clone`
- source build commands such as `cmake`, `make`, or `ninja`
- binary placement or symlink creation
- runtime.env read or runtime.env value printing
- secret reads or secret printing
- TDLib auth rerun
- TDLib client/session creation
- tdjson client calls
- Telegram network contact
- DB, Redis, Alembic, Docker, systemd
- live collector, app runtime, notifier transport, production rollout

## Operator commands

Run from the repository checkout on the dedicated VPS:

```bash
cd ~/workspace/bots/github_ai_catchbot
venv/bin/python scripts/ops/dedicated_vps_tdjson_apt_install_plan.py --format json
```

If the operator has the previous preflight JSON and chooses to include it
explicitly:

```bash
cd ~/workspace/bots/github_ai_catchbot
venv/bin/python scripts/ops/dedicated_vps_tdjson_apt_install_plan.py \
  --format json \
  --preflight-json /tmp/dedicated_vps_tdjson_runtime_dependency_preflight.json
```

If the operator has the previous package-decision JSON and chooses to include it
explicitly:

```bash
cd ~/workspace/bots/github_ai_catchbot
venv/bin/python scripts/ops/dedicated_vps_tdjson_apt_install_plan.py \
  --format json \
  --package-decision-json /tmp/dedicated_vps_tdjson_runtime_dependency_package_decision.json
```

Both explicit prior reports may be provided together:

```bash
cd ~/workspace/bots/github_ai_catchbot
venv/bin/python scripts/ops/dedicated_vps_tdjson_apt_install_plan.py \
  --format json \
  --preflight-json /tmp/dedicated_vps_tdjson_runtime_dependency_preflight.json \
  --package-decision-json /tmp/dedicated_vps_tdjson_runtime_dependency_package_decision.json
```

Do not load runtime.env for this plan. Do not paste secret values into Codex,
ChatGPT, GitHub, repository files, markdown, or shell history.

## Output contract

The script returns redacted JSON with:

- `schema_version: dedicated_vps_tdjson_apt_install_plan_v1`
- `contract_name: dedicated_vps_tdjson_apt_install_plan`
- `contract_status`
- `recommended_next_slice`
- host OS and architecture summary
- optional explicit prior preflight summary
- optional explicit prior package-decision summary
- read-only apt, dpkg, ldconfig, and uname inspection summary
- `selected_plan`
- decision reasons
- risk notes
- stop conditions
- fixed false safety booleans
- `boundary_check: pass`

Future command examples appear only under fields named
`future_operator_commands_not_run`, `future_validation_commands_not_run`, and
`future_rollback_commands_not_run`. They are NOT RUN / FUTURE SLICE ONLY.

## Contract statuses

`apt_install_plan_ready` means read-only apt evidence found a package candidate
that can be reviewed in a later operator execution slice. It does not mean the
package was installed.

`apt_install_plan_inconclusive` means the host evidence is mixed, search-only,
or insufficient for a clean apt operator plan. Manual review or a different
planning slice is required.

`unsupported_host` means `/etc/os-release` did not identify Ubuntu or Debian, or
the host metadata was unreadable enough that no apt install plan should be
selected.

## Recommended next slices

`tdjson_apt_install_operator_execution` means a later explicitly approved
operator slice may review and run the selected apt install command, then run the
tdjson runtime dependency preflight. This slice does not execute it.

`tdjson_source_build_plan` means no usable apt policy candidate was visible on a
supported Ubuntu/Debian host. Any source-build work must be planned separately.

`defer_manual_review` means the evidence is unsupported, conflicting, or too weak
to recommend apt execution.

## Future examples only

The following commands are NOT RUN / FUTURE SLICE ONLY. They are examples of
commands a later reviewed slice might discuss if the JSON selected the matching
package:

```bash
# NOT RUN / FUTURE SLICE ONLY
sudo apt install libtdjson

# NOT RUN / FUTURE SLICE ONLY
venv/bin/python scripts/ops/dedicated_vps_tdjson_runtime_dependency_preflight.py --format json

# NOT RUN / FUTURE SLICE ONLY
sudo apt remove libtdjson
```

Do not execute these commands from this apt install plan slice.

## Next boundary

This plan does not claim installation success, auth readiness, Telegram
readiness, collector readiness, notifier readiness, app readiness, or rollout
readiness.

Any package mutation, rollback, auth rerun, runtime start, Telegram contact, DB
or Redis use, Alembic operation, Docker/systemd change, live collector, notifier
transport, or production rollout must be a separate explicitly approved slice.
