# Dedicated VPS tdjson source-build dependency install result record

## Scope

This is a result record only.

It records an already completed approved VPS operator execution on
`github-ai-catchbot-prod-1`. No new operation is performed by this record.

The completed operator execution installed only `cmake` and `gperf`.

This record is not a dependency installation execution slice, source build
execution slice, TDLib auth rerun slice, or live collector slice.

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
- closed commit before execution:
  `dbd67fe feat(ops): add tdjson source build dependency install plan`

## Recorded installed packages

- `cmake`
- `gperf`

## Recorded verification

```text
command -v cmake -> /usr/bin/cmake
command -v gperf -> /usr/bin/gperf
cmake --version | head -n 1 -> cmake version 3.28.3
gperf --version | head -n 1 -> GNU gperf 3.1
dpkg-query -> cmake 3.28.3-1build7 install ok installed
dpkg-query -> gperf 3.1-1build1 install ok installed
```

## Recorded post-install recheck

```text
TDJSON_SOURCE_BUILD_PLAN_AFTER_DEPENDENCY_INSTALL_PASS source_build_plan_ready tdjson_source_build_operator_execution [] []
```

## Recorded clean final state

```text
## main...origin/main
```

## Explicit boundary statements

This result record does not install tdjson/libtdjson.
This result record does not perform source build.
This result record does not run git clone.
This result record does not run cmake configure/build.
This result record does not run make/ninja.
This result record does not create build directories.
This result record does not place binaries or symlinks.
This result record does not read runtime.env.
This result record does not print secrets.
This result record does not create TDLib client/session.
This result record does not contact Telegram.
This result record does not run TDLib auth.
This result record does not connect to DB/Redis or run Alembic.
This result record does not change Docker/systemd.
This result record does not start live collector/app runtime/notifier/rollout.

## Next candidate slice

The next candidate slice is `tdjson_source_build_operator_execution`.

This result record does not authorize source build execution by itself. Source
build operator execution must be a separate approved bounded slice.
