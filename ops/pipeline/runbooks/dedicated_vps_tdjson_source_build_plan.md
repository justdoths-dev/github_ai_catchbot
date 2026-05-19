# Dedicated VPS tdjson Source Build Plan

This runbook covers the `tdjson_source_build_plan` slice only. It prepares a
read-only/report-only plan for a future TDLib/tdjson source-build operator slice
after package and manual package evidence remained inconclusive.

It does not perform the source build. It does not clone, download, build,
install, place binaries, create symlinks, create build directories, edit
`runtime.env`, start TDLib auth, start the collector, or start application
runtime services.

## Hard Boundary

- `git clone` is not run.
- `cmake` is not run.
- `make` is not run.
- `ninja` is not run.
- `apt update`, `apt install`, `apt upgrade`, `apt remove`, and `apt purge` are not run.
- `apt-get update`, `apt-get install`, `apt-get upgrade`, `apt-get remove`, and `apt-get purge` are not run.
- `dpkg -i` is not run.
- Package downloads are not performed.
- Source build commands are not performed.
- TDLib auth rerun is not authorized.
- `runtime.env` must not be read.
- Secrets must not be printed.
- TDLib client/session creation is not allowed.
- Telegram network contact is not allowed.
- DB, Redis, Alembic, Docker, systemd, collector, notifier, and rollout work are not allowed.

Future command examples in the JSON report are examples only. They are emitted
only under fields named `future_*_commands_not_run` and are NOT RUN / FUTURE
SLICE ONLY.

## Operator Command

Run from the repository root:

```bash
venv/bin/python scripts/ops/dedicated_vps_tdjson_source_build_plan.py --format json \
  > /tmp/dedicated_vps_tdjson_source_build_plan.json
```

Optional explicit prior evidence inputs may be passed when those files already
exist:

```bash
venv/bin/python scripts/ops/dedicated_vps_tdjson_source_build_plan.py \
  --format json \
  --preflight-json /tmp/dedicated_vps_tdjson_runtime_dependency_preflight.json \
  --package-decision-json /tmp/dedicated_vps_tdjson_runtime_dependency_package_decision.json \
  --apt-plan-json /tmp/dedicated_vps_tdjson_apt_install_plan.json \
  --manual-evidence-json /tmp/dedicated_vps_tdjson_manual_package_evidence_review.json \
  > /tmp/dedicated_vps_tdjson_source_build_plan.json
```

The script has no default `/tmp` inputs. If a prior JSON path is not provided,
the script continues without that prior evidence.

## Contract Status

- `source_build_plan_ready`: Required build tools are available by
  `shutil.which`, required dependency packages appear installed, host evidence is
  supported, and a later explicitly approved source-build operator slice may be
  plausible.
- `source_build_plan_requires_dependency_plan`: Source build is plausible, but at
  least one required dependency package is only candidate-visible rather than
  installed. The next slice must be dependency install planning, not source build
  execution.
- `source_build_plan_inconclusive`: Evidence is too weak, required tools are
  missing, resources are insufficient, or prior evidence points to a prebuilt
  library path plan that should not be overridden automatically.
- `unsupported_host`: `/etc/os-release` does not identify Ubuntu/Debian or host
  evidence is unreadable for this contract.

## Recommended Next Slice

- `tdjson_source_build_operator_execution`: A future source-build operator slice
  may be reviewed, but only after explicit approval.
- `tdjson_source_build_dependency_install_plan`: Prepare a separate dependency
  installation plan before any source build is considered.
- `tdjson_prebuilt_library_path_plan`: Review an existing/prebuilt libtdjson path
  instead of automatically moving to source build.
- `defer_manual_review`: Stop for manual review.

## Read-Only Inspection

The tool uses stdlib only. It may inspect `/etc/os-release`, host architecture,
CPU count, and disk free space for the repository root. It checks build-tool
presence with `shutil.which` only and does not execute build tools, even with
`--version`.

Allowed subprocess commands are constrained to:

```bash
dpkg-query -W -f='${binary:Package}\t${Version}\t${Status}\n' <package>
apt-cache policy <package>
apt-cache show <package>
apt-cache depends <package>
apt-cache search <query>
uname -m
```

Build dependency package candidates inspected read-only:

```text
git
cmake
build-essential
g++
gcc
clang
make
ninja-build
pkg-config
zlib1g-dev
libssl-dev
gperf
libc++-dev
libc++abi-dev
```

Build tools checked with `shutil.which` only:

```text
git
cmake
g++
gcc
clang++
clang
make
ninja
pkg-config
python3
```

## Future Source Build Examples

The following examples are NOT RUN / FUTURE SLICE ONLY. They may appear in the
JSON report only under `future_operator_commands_not_run`:

```bash
git clone --depth 1 https://github.com/tdlib/td.git <build_workspace_candidate>/td
cmake -S <build_workspace_candidate>/td -B <build_workspace_candidate>/td/build -DCMAKE_BUILD_TYPE=Release -DTD_ENABLE_JNI=OFF -DCMAKE_INSTALL_PREFIX=<install_prefix_candidate>
cmake --build <build_workspace_candidate>/td/build --target install
```

Future rollback examples are also NOT RUN / FUTURE SLICE ONLY:

```bash
rm -rf <build_workspace_candidate>/td
rm -rf <install_prefix_candidate>
```

## Output Check

After producing `/tmp/dedicated_vps_tdjson_source_build_plan.json`, validate the
contract shape:

```bash
venv/bin/python - <<'PY'
import json
from pathlib import Path

data = json.loads(Path("/tmp/dedicated_vps_tdjson_source_build_plan.json").read_text(encoding="utf-8"))
assert data["schema_version"] == "dedicated_vps_tdjson_source_build_plan_v1"
assert data["contract_name"] == "dedicated_vps_tdjson_source_build_plan"
assert data["boundary_check"] == "pass"
print(
    "TDJSON_SOURCE_BUILD_PLAN_OUTPUT_CONTRACT_PASS",
    data["contract_status"],
    data["recommended_next_slice"],
)
PY
```
