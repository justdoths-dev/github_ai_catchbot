# Dedicated VPS tdjson runtime dependency preflight

## Purpose

This is a read-only/report-only operator preflight for the dedicated VPS
runtime host. It reports whether `tdjson` is available, loadable, and exposes
the required TDLib JSON client symbols.

This is not tdjson installation. This is not an install plan. This is not a
TDLib auth rerun. This is not Telegram connectivity validation.

The previously recorded TDLib auth blocked result remains closed and is not
reopened by this preflight.

## Boundary

The preflight must not read `/etc/github-ai-catchbot/runtime.env`.

The preflight must not print secrets, login code, 2FA values, phone number,
API hash, DB URL, Redis URL, invite links, or TDLib session contents.

The preflight must not install packages, use a package manager, change Docker
or systemd, run Alembic, connect to DB or Redis, start the app runtime, start
the live collector, enable notifier transport, or perform production rollout.

The preflight must not attempt TDLib auth, create a TDLib client, invoke
`td_json_client_create`, invoke `td_json_client_send`, invoke
`td_json_client_receive`, invoke `td_json_client_destroy`, or contact
Telegram.

The preflight may inspect only these non-secret runtime dependency signals:

- `ctypes.util.find_library("tdjson")`
- `TDJSON_LIBRARY_PATH` from the current operator shell environment only
- a fixed set of common tdjson library paths
- `pathlib` existence checks for candidate library paths
- `ctypes.CDLL(candidate)` loadability
- required symbol presence through attribute checks only

## Operator commands

Run from the repository checkout on the dedicated VPS:

```bash
cd ~/workspace/bots/github_ai_catchbot
venv/bin/python scripts/ops/dedicated_vps_tdjson_runtime_dependency_preflight.py --format json
```

If the operator shell intentionally exports `TDJSON_LIBRARY_PATH`, the same
command may be run from that shell:

```bash
cd ~/workspace/bots/github_ai_catchbot
venv/bin/python scripts/ops/dedicated_vps_tdjson_runtime_dependency_preflight.py --format json
```

Do not load runtime.env for this preflight. Do not paste secret values into
ChatGPT, Codex, GitHub, repository files, markdown, or shell history.

## Output contract

The script returns redacted JSON with:

- `schema_version:
  dedicated_vps_tdjson_runtime_dependency_preflight_v1`
- `contract_name: dedicated_vps_tdjson_runtime_dependency_preflight`
- `contract_status`
- tdjson availability booleans
- redacted `candidate_checks`
- fixed false safety booleans proving this preflight stayed read-only and
  non-authorizing
- `boundary_check: pass`

Candidate checks prefer source, basename, status, and error class. They must
not disclose runtime.env, secret files, or TDLib session contents.

## Status interpretation

`tdjson_available` means a candidate library loaded and all required symbols
were present. It does not mean TDLib auth succeeded. It does not mean Telegram
was contacted.

`tdjson_missing` means no candidate was found or loaded. Stop and bring the
redacted output to review. A missing result leads to a later package/install
decision slice, not immediate install.

`tdjson_load_failed` means at least one candidate was found, but every found
candidate failed to load. Stop and bring the redacted output to review. Do not
install or change packages in this slice.

`tdjson_missing_required_symbols` means a candidate loaded but one or more
required TDLib JSON client symbols were missing. Stop and bring the redacted
output to review. Do not attempt auth.

## Required symbols

The preflight checks for these symbol names only:

```text
td_json_client_create
td_json_client_send
td_json_client_receive
td_json_client_destroy
```

Presence checks are attribute checks only. The preflight must never call these
symbols.

## Next boundary

If the result is missing, load failed, or missing required symbols, the next
action is a separately reviewed package/install decision slice.

If the result is available, the next action is still separate review. Passing
this preflight does not authorize TDLib auth, Telegram network contact, live
collector startup, notifier transport, DB/Redis/Alembic activity,
Docker/systemd changes, or production rollout.
