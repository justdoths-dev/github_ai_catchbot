# Dedicated VPS tdjson runtime dependency package decision

## Purpose

This is a repo-local, read-only/report-only package decision slice for the
dedicated VPS tdjson runtime dependency.

It does not install tdjson. It does not mutate packages. It does not rerun
TDLib auth. It does not start the collector, app runtime, notifier transport,
DB, Redis, Alembic, Docker, systemd, or production rollout.

The closed tdjson runtime dependency preflight result remains closed. A
`tdjson_missing` result is a runtime dependency finding, not a TDLib auth
failure and not an auth rerun trigger.

## Boundary

The script must not read runtime.env.

The script must not print secrets, login codes, 2FA values, phone numbers, API
hashes, DB URLs, Redis URLs, invite links, TDLib session contents, or operator
secret file values.

The script must not run package-manager mutation commands. The following are
forbidden in this slice: `sudo`, `apt install`, `apt-get install`, `apt
upgrade`, `apt update`, `apt remove`, `apt purge`, `dpkg -i`, package
downloads, `curl`, `wget`, `git clone`, source build commands, binary
placement, symlink creation, Docker changes, systemd changes, TDLib auth,
TDLib client/session creation, Telegram network contact, DB connection, Redis
connection, Alembic, live collector startup, notifier transport, or production
rollout.

Allowed read-only inspection is limited to:

- `/etc/os-release`
- `platform.machine()` and `platform.platform()`
- read-only command discovery with `shutil.which`
- strict allowlisted local commands:
  - `dpkg-query -W -f=${binary:Package}\t${Version}\t${Status}\n <package>`
  - `apt-cache policy <package>`
  - `apt-cache search <query>`
  - `ldconfig -p`
  - `uname -m`
- optional explicit preflight JSON passed by the operator with
  `--preflight-json`

No default `/tmp` preflight path is read. If a preflight report is not
explicitly provided, the decision continues without it.

## Operator commands

Run from the repository checkout on the dedicated VPS:

```bash
cd ~/workspace/bots/github_ai_catchbot
venv/bin/python scripts/ops/dedicated_vps_tdjson_runtime_dependency_package_decision.py --format json
```

If the operator has the previous preflight JSON and chooses to include it
explicitly:

```bash
cd ~/workspace/bots/github_ai_catchbot
venv/bin/python scripts/ops/dedicated_vps_tdjson_runtime_dependency_package_decision.py \
  --format json \
  --preflight-json /tmp/dedicated_vps_tdjson_runtime_dependency_preflight.json
```

Do not load runtime.env for this decision. Do not paste secret values into
ChatGPT, Codex, GitHub, repository files, markdown, or shell history.

## Output contract

The script returns redacted JSON with:

- `schema_version:
  dedicated_vps_tdjson_runtime_dependency_package_decision_v1`
- `contract_name:
  dedicated_vps_tdjson_runtime_dependency_package_decision`
- `contract_status`
- `recommended_next_slice`
- host OS and architecture summary
- optional explicit prior preflight summary
- read-only package/library inspection summary
- decision reasons
- risk notes
- descriptive candidate actions
- fixed false safety booleans
- `boundary_check: pass`

Descriptive candidate actions are not execution authorization. If command
examples appear under `future_operator_commands_not_run`, they are NOT RUN /
FUTURE SLICE ONLY.

## Recommended next slices

`tdjson_apt_install_plan` means read-only apt evidence showed a plausible
tdjson/TDLib package path. A future reviewed slice must choose exact package
names, mutation commands, rollback notes, and validation. This decision slice
does not install anything.

`tdjson_source_build_plan` means the host appears to be Ubuntu/Debian, but no
apt tdjson candidate was visible. A future reviewed slice may plan a source
build, including dependencies, build directory, artifact placement, rollback,
and validation. This decision slice does not run `git clone`, `cmake`, `make`,
or `ninja`.

`tdjson_prebuilt_library_path_plan` means explicit prior preflight evidence and
local linker hints suggest a prebuilt or existing library-path review may be
worth considering. A future reviewed slice must handle placement and
`TDJSON_LIBRARY_PATH` explicitly. This decision slice does not place binaries,
create symlinks, or edit runtime configuration.

`defer_manual_review` means no safe route is identifiable from the allowed
read-only evidence, the host is unsupported, or the evidence is mixed.

## Future examples only

The following examples are NOT RUN / FUTURE SLICE ONLY. They are included only
to show the kinds of commands a later reviewed slice might discuss:

```bash
# NOT RUN / FUTURE SLICE ONLY
sudo apt install libtdjson

# NOT RUN / FUTURE SLICE ONLY
git clone https://github.com/tdlib/td.git
cmake --build <reviewed-build-dir>

# NOT RUN / FUTURE SLICE ONLY
export TDJSON_LIBRARY_PATH=/reviewed/path/libtdjson.so
```

Do not execute these commands from this package decision slice.

## Next boundary

This decision does not claim installation success, auth readiness, Telegram
readiness, collector readiness, or rollout readiness.

Any next action must be a separate explicitly reviewed slice.
