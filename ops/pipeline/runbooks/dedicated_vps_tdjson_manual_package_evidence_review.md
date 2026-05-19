# Dedicated VPS tdjson manual package evidence review

This runbook covers only the `tdjson_manual_package_evidence_review` slice. It is a repo-local, read-only/report-only evidence review that explains why the prior apt install plan may have ended as inconclusive and records a more explicit package evidence matrix from local apt/dpkg/ldconfig metadata.

This slice does not install tdjson. It does not run apt update, apt install, apt upgrade, apt remove, or apt purge. It does not run source build commands. It does not authorize a TDLib auth rerun. It must not read `runtime.env`, and it must not print secrets.

Package-manager mutation commands are forbidden in this slice. The script uses stdlib only and only permits read-only local command inspection through a strict allowlist:

- `dpkg-query -W -f=${binary:Package}\t${Version}\t${Status}\n <package>`
- `apt-cache policy <package>`
- `apt-cache show <package>`
- `apt-cache depends <package>`
- `apt-cache madison <package>`
- `apt-cache search <query>`
- `ldconfig -p`
- `uname -m`

## Contract status values

- `manual_package_evidence_ready`: local apt evidence clearly identifies an installable runtime package that likely provides `libtdjson.so`; the next slice should recheck the apt install plan with stronger evidence.
- `manual_package_evidence_inconclusive`: package evidence exists but does not prove `libtdjson.so`, or no package evidence exists on a supported host.
- `unsupported_host`: `/etc/os-release` is unreadable or does not identify Ubuntu/Debian.

## Recommended next slice values

- `tdjson_source_build_plan`: plan a source-build path in a future slice when package evidence does not identify a suitable runtime provider.
- `tdjson_prebuilt_library_path_plan`: plan a reviewed explicit library path only when existing local library evidence points to a `libtdjson.so` path.
- `tdjson_apt_install_plan_recheck`: rerun the apt install plan in a future slice using the stronger package evidence matrix.
- `defer_manual_review`: stop and have an operator review the host/package evidence manually.

## Operator command examples

Run the review without prior JSON inputs:

```bash
cd ~/workspace/bots/github_ai_catchbot
source venv/bin/activate
venv/bin/python scripts/ops/dedicated_vps_tdjson_manual_package_evidence_review.py --format json
```

Run the review with explicit prior JSON paths:

```bash
cd ~/workspace/bots/github_ai_catchbot
source venv/bin/activate
venv/bin/python scripts/ops/dedicated_vps_tdjson_manual_package_evidence_review.py \
  --format json \
  --preflight-json /tmp/dedicated_vps_tdjson_runtime_dependency_preflight.json \
  --package-decision-json /tmp/dedicated_vps_tdjson_runtime_dependency_package_decision.json \
  --apt-plan-json /tmp/dedicated_vps_tdjson_apt_install_plan.json
```

Prior JSON paths are optional and must be provided explicitly. The script does not default to `/tmp`.

## NOT RUN / FUTURE SLICE ONLY examples

The JSON output may list future commands only under fields named `future_*_commands_not_run`. Those examples are descriptive only. They are not executed by this slice and do not authorize installation, source build, library placement, auth, collector startup, notifier transport, DB/Redis/Alembic, Docker/systemd, or production rollout.

Examples that remain future-slice-only:

- `sudo apt install <reviewed-package>`
- `sudo apt remove <reviewed-package>`
- `cmake -S td -B build -DTD_ENABLE_JNI=OFF`
- `cmake --build build`

## Boundary checklist

- No tdjson installation is performed.
- No apt update/install/upgrade/remove/purge is performed.
- No package download is performed.
- No source build is performed.
- No binary placement or symlink creation is performed.
- No `runtime.env` file is read.
- No secret values are printed.
- No TDLib auth rerun is authorized.
- No TDLib client/session is created.
- No `td_json_client_create`, `td_json_client_send`, `td_json_client_receive`, or `td_json_client_destroy` invocation is performed.
- No Telegram network contact is attempted.
- No DB, Redis, or Alembic connection/run is performed.
- No Docker/systemd change is performed.
- No live collector, notifier transport, app runtime, or production rollout is started.
