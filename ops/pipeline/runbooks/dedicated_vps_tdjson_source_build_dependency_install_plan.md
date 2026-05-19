# Dedicated VPS tdjson Source Build Dependency Install Plan

This runbook covers the `tdjson_source_build_dependency_install_plan` slice only.
It prepares a read-only/report-only plan for a future operator-approved apt
installation of the minimum missing TDLib/tdjson source-build dependencies:
`cmake` and `gperf`.

It does not perform dependency installation. It does not run `apt update`. It
does not run a source build. It does not authorize TDLib auth rerun. It must not
read `runtime.env`, and it must not print secrets.

## Hard Boundary

- `apt install`, `apt-get install`, `apt update`, `apt-get update`, `apt upgrade`,
  `apt-get upgrade`, `apt remove`, `apt-get remove`, `apt purge`, and
  `apt-get purge` are not run.
- `dpkg -i` is not run.
- Package downloads are not performed.
- `git clone` is not run.
- Source build commands are not performed.
- `cmake`, `make`, and `ninja` are not run.
- Build directories are not created.
- Binaries are not placed, and symlinks are not created.
- TDLib auth rerun is not authorized.
- TDLib client/session creation is not allowed.
- Telegram network contact is not allowed.
- `runtime.env` must not be read.
- Secrets must not be printed.
- DB, Redis, Alembic, Docker, systemd, live collector, notifier transport, app
  runtime, and production rollout work are not allowed.

Future command examples in the JSON report are examples only. They are emitted
only under fields named `future_*_commands_not_run` and are NOT RUN / FUTURE
SLICE ONLY.

## Operator Command

Run from the repository root:

```bash
venv/bin/python scripts/ops/dedicated_vps_tdjson_source_build_dependency_install_plan.py \
  --format json \
  > /tmp/dedicated_vps_tdjson_source_build_dependency_install_plan.json
```

When the source-build-plan output already exists, pass it explicitly:

```bash
venv/bin/python scripts/ops/dedicated_vps_tdjson_source_build_dependency_install_plan.py \
  --format json \
  --source-build-plan-json /tmp/dedicated_vps_tdjson_source_build_plan.json \
  > /tmp/dedicated_vps_tdjson_source_build_dependency_install_plan.json
```

Optional prior chain JSONs may also be passed explicitly:

```bash
venv/bin/python scripts/ops/dedicated_vps_tdjson_source_build_dependency_install_plan.py \
  --format json \
  --source-build-plan-json /tmp/dedicated_vps_tdjson_source_build_plan.json \
  --preflight-json /tmp/dedicated_vps_tdjson_runtime_dependency_preflight.json \
  --package-decision-json /tmp/dedicated_vps_tdjson_runtime_dependency_package_decision.json \
  --apt-plan-json /tmp/dedicated_vps_tdjson_apt_install_plan.json \
  --manual-evidence-json /tmp/dedicated_vps_tdjson_manual_package_evidence_review.json \
  > /tmp/dedicated_vps_tdjson_source_build_dependency_install_plan.json
```

The script has no default `/tmp` inputs. If a prior JSON path is not provided,
the script continues without that prior evidence.

## Contract Status

- `dependency_install_plan_ready`: The host is supported, selected package
  candidates are visible, selected packages are not installed, and the future
  apt command is limited to the missing minimum dependency set.
- `dependency_install_plan_inconclusive`: Evidence is too weak, an apt candidate
  is missing, or explicit source-build-plan JSON lists missing required
  dependencies outside the conservative `cmake`/`gperf` path.
- `unsupported_host`: `/etc/os-release` does not identify Ubuntu/Debian or host
  evidence is unreadable for this contract.

## Recommended Next Slice

- `tdjson_source_build_dependency_install_operator_execution`: A future
  operator execution slice may review the not-run apt install example.
- `tdjson_source_build_plan_recheck`: No package remains to plan; rerun the
  source-build plan before considering any source-build operator slice.
- `defer_manual_review`: Stop for manual review.

## Read-Only Inspection

The tool uses stdlib only. It may inspect `/etc/os-release`, host architecture,
`platform.platform()`, and command availability through `shutil.which`.

Allowed subprocess commands are constrained to:

```bash
dpkg-query -W -f='${binary:Package}\t${Version}\t${Status}\n' <package>
apt-cache policy <package>
apt-cache show <package>
apt-cache depends <package>
uname -m
```

Dependency package candidates:

```text
cmake
gperf
```

Optional evidence-only packages:

```text
ninja-build
clang
libc++-dev
libc++abi-dev
```

Optional packages are not selected unless an explicit future source-build plan
contract widens the required dependency set in a later approved slice.

## Future Command Examples

The following examples are NOT RUN / FUTURE SLICE ONLY. They may appear in the
JSON report only under `future_operator_commands_not_run`:

```bash
sudo apt install cmake gperf
```

Future validation examples are also NOT RUN / FUTURE SLICE ONLY:

```bash
venv/bin/python scripts/ops/dedicated_vps_tdjson_source_build_plan.py --format json ...
```

The output contract should also be checked with a
`TDJSON_SOURCE_BUILD_PLAN_OUTPUT_CONTRACT_PASS` assertion before any later build
slice is considered.

Future rollback examples are NOT RUN / FUTURE SLICE ONLY:

```bash
sudo apt remove cmake gperf
```

## Output Check

After producing `/tmp/dedicated_vps_tdjson_source_build_dependency_install_plan.json`,
validate the contract shape:

```bash
venv/bin/python - <<'PY'
import json
from pathlib import Path

data = json.loads(Path("/tmp/dedicated_vps_tdjson_source_build_dependency_install_plan.json").read_text(encoding="utf-8"))
assert data["schema_version"] == "dedicated_vps_tdjson_source_build_dependency_install_plan_v1"
assert data["contract_name"] == "dedicated_vps_tdjson_source_build_dependency_install_plan"
assert data["boundary_check"] == "pass"
print(
    "TDJSON_SOURCE_BUILD_DEPENDENCY_INSTALL_PLAN_OUTPUT_CONTRACT_PASS",
    data["contract_status"],
    data["recommended_next_slice"],
    data["selected_plan"].get("packages_to_install"),
)
PY
```
