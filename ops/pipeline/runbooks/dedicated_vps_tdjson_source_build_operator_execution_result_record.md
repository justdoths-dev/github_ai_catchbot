# Dedicated VPS tdjson source build operator execution result record

## Scope

This is a result record only.

It records an already completed approved VPS operator execution on
`github-ai-catchbot-prod-1`. No new operation is performed by this record.

This record is not a source build execution slice, TDLib auth rerun slice,
live collector slice, notifier slice, or rollout slice.

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
  `be68627d91a6f2a24fd6527f805975324ef43c91`
- git status at execution: `## main...origin/main`

## Recorded build paths

- TDLib source clone path: `/opt/github-ai-catchbot/build/tdlib-source/td`
- TDLib build dir: `/opt/github-ai-catchbot/build/tdlib-source/td/build`
- TDLib install prefix: `/opt/github-ai-catchbot/tdlib`
- selected libtdjson path:
  `/opt/github-ai-catchbot/tdlib/lib/libtdjson.so.1.8.64`
- TDLib source checkout commit: not captured in operator summary

## Recorded tool versions

- `cmake version 3.28.3`
- `GNU gperf 3.1`

## Recorded tdjson preflight result

```text
contract_status: tdjson_available
boundary_check: pass
selected libtdjson path: /opt/github-ai-catchbot/tdlib/lib/libtdjson.so.1.8.64
```

## Recorded final marker

```text
TDJSON_SOURCE_BUILD_OPERATOR_EXECUTION_RESULT source_build_completed_tdjson_available tdjson_available /opt/github-ai-catchbot/tdlib/lib/libtdjson.so.1.8.64
```

## Stream interruption note

Initial remote build command stream marker was interrupted.

Build logs continued.

After build completion only safe log/output reads and preflight were run.

No rebuild or overwrite was performed after the stream interruption.

## Explicit boundary statements

This result record does not perform source build.
This result record does not run git clone.
This result record does not run cmake configure/build.
This result record does not run make/ninja.
This result record does not run apt/package-manager mutation.
This result record does not rebuild tdjson/libtdjson.
This result record does not place binaries or symlinks.
This result record does not create build directories.
This result record does not read runtime.env.
This result record does not write runtime.env.
This result record does not print secrets.
This result record does not create TDLib client/session.
This result record does not contact Telegram.
This result record does not run TDLib auth.
This result record does not connect to DB/Redis or run Alembic.
This result record does not change Docker/systemd.
This result record does not start live collector/app runtime/notifier/rollout.

## Explicit non-authority statements

tdjson_available confirms only repo-local tdjson runtime dependency preflight
availability.

tdjson_available is not TDLib auth success.

tdjson_available is not Telegram login success.

tdjson_available is not collector readiness.

tdjson_available is not notifier readiness.

tdjson_available is not production readiness.

tdjson_available does not authorize TDLib auth rerun by itself.

TDLib auth rerun must be a separate approved bounded slice.

## Next candidate slice

The next candidate slice is `tdlib_auth_operator_execution_rerun`.

This result record does not authorize that execution by itself. TDLib auth
rerun must be a separate approved bounded slice.
