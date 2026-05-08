# Dedicated VPS runtime secret placement result record

## Scope

This is a repo-local operational result record for the dedicated VPS runtime
secret placement that was manually run on the dedicated VPS after DB/Redis
operator provisioning.

Codex did not execute VPS commands for this record. This document does not
authorize additional VPS execution, SSH, sudoedit, secret-file inspection,
PostgreSQL connections, Redis connections, Alembic, app runtime startup, TDLib
auth, Telegram connection, live collector startup, notifier transport,
production rollout, Docker, Docker Compose, or systemd unit modification.

## Source-of-truth handling

`docs/project-source/README_replacement_consolidated_v0_20.md` remains
authoritative. The locked project-source bundles `00~10` remain the
source-of-truth set and are interpreted in filename order. The GitHub AI
application plan remains advisory only.

This result record preserves the canonical architecture invariant:

```text
SourceMessage -> Artifact -> CandidateGroup -> EvidenceBundle -> JudgeOutput -> Analysis -> Notification
```

The service boundaries remain unchanged:

- collector preserves raw Telegram source messages and revisions only.
- outbox-relay publishes thin ID-only Redis Stream messages only.
- router-normalizer is deterministic and non-LLM.
- enrichers gather evidence only.
- evidence-assembler assembles candidate-centered EvidenceBundles and may
  reroot only within its contract.
- analysis-router is the deterministic judge-pipeline entry gate.
- LLM judge produces structured `judge_output_v1` only.
- deterministic policy-engine computes final `analysis_v1` verdict and
  `delivery_decision`.
- notifier is presentation and delivery only.
- maintenance is retry/replay orchestration plus explicitly requested one-shot
  delivery control-plane tools only.
- delivery gate is ops/control-plane reporting, not a runtime worker.
- PostgreSQL is durable system of record.
- Redis is queue, lock, and short-lived execution state only.
- replay creates new runs or versions and never overwrites historical truth.
- `recommended_flag_patch` is output-only and must not be auto-applied.
- production rollout remains unauthorized.

## Execution summary

This record was recorded after 2026-05-08 runtime secret placement execution.

Actual manual operator runtime secret placement was run on the dedicated VPS.
Block 0 context passed. Block 1 created the approved host-local runtime secret
directory and file. Block 2 used `sudoedit` so the PostgreSQL app-role password
was entered inside the editor only.

The first validation failed because the reviewed runbook validation script used
textual file-mode rendering instead of numeric permission extraction. The live
validation was corrected manually to use numeric mode extraction. A later
validation attempt failed because the `DATABASE_URL` placeholder remained.
The operator corrected the placeholder through `sudoedit` without disclosing the
password. Corrected redacted validation then passed.

## Final result

PASS: dedicated VPS runtime secret placement completed with redacted validation
passing and no runtime rollout.

The runtime secret values were not disclosed. The runtime file contents were not
printed. This result record does not authorize Alembic, app runtime, TDLib,
Telegram, notifier transport, live collector startup, or production rollout by
itself.

## Environment summary

- Host label: `github-ai-catchbot-prod-1`.
- User: `deploy`.
- Repo path: `/home/deploy/workspace/bots/github_ai_catchbot`.
- Repo HEAD/origin at execution: `a660e52 test(ops): add runtime secret placement check`.
- Previous commit: `35cc7fa test(ops): stabilize collector help assertions`.
- Previous commit: `66c3bcc docs(ops): record DB Redis provisioning result`.
- Runtime secret directory: `/etc/github-ai-catchbot`.
- Runtime secret file: `/etc/github-ai-catchbot/runtime.env`.
- Final parent directory ownership/mode: `root:deploy 0750`.
- Final runtime file ownership/mode: `root:deploy 0640`.
- Raw public IP and operator IP values are intentionally omitted.

## Commands actually run by block

- Block 0: context confirmation commands were run and passed.
- Block 1: approved host-local runtime secret directory and file creation
  commands were run and created `/etc/github-ai-catchbot` plus
  `/etc/github-ai-catchbot/runtime.env`.
- Block 2: `sudoedit /etc/github-ai-catchbot/runtime.env` was used. The
  PostgreSQL app-role password was entered inside the editor only.
- Block 3: redacted validation was run. The initial permission-mode check
  failed because of the runbook validation defect, then the manually corrected
  numeric-mode validation was used.
- Block 3 recovery validation: redacted validation caught a remaining
  `DATABASE_URL` placeholder.
- Block 2 recovery: `sudoedit /etc/github-ai-catchbot/runtime.env` was used
  again to replace the placeholder without disclosing the password.
- Final Block 3 validation: corrected redacted validation passed.

## Deviations from approved runbook

1. The reviewed runbook validation script used textual file-mode rendering for
   mode comparison. That does not produce numeric modes such as `750` or `640`,
   so permission validation failed despite correct permissions.
2. The live validation was manually corrected to use numeric permission
   extraction before comparing `750` and `640`.

The remaining `DATABASE_URL` placeholder found during one validation attempt was
an expected validation/recovery event, not a runbook bug. The placeholder guard
caught the incomplete edit before any later runtime step.

## Recovery actions performed

- The redacted validation permission check was corrected manually to use
  numeric mode extraction.
- The operator reran validation after the numeric-mode correction.
- When validation caught the remaining `DATABASE_URL` placeholder, the operator
  reopened the runtime file with `sudoedit`.
- The placeholder was replaced inside the editor only.
- The PostgreSQL app-role password was not disclosed in ChatGPT.
- The runtime file contents were not printed.
- Final corrected redacted validation passed.

## Final verification evidence

- Runtime secret directory exists at `/etc/github-ai-catchbot`.
- Runtime secret file exists at `/etc/github-ai-catchbot/runtime.env`.
- Parent directory final ownership/mode: `root:deploy 0750`.
- Runtime file final ownership/mode: `root:deploy 0640`.
- Required keys present by name:
  - `APP_ENV`
  - `DATABASE_URL`
  - `REDIS_URL`
  - `ENABLE_NOTIFICATION_SEND`
  - `NOTIFIER_TELEGRAM_DRY_RUN`
  - `NOTIFIER_TELEGRAM_ALLOW_EDITS`
  - `ENABLE_REPLAY_TO_PROD_DB`
  - `MAINTENANCE_ENABLE_NOTIFICATION_RETRY_PROMOTION`
- Optional keys present: none.
- Final redacted validation passed.
- `DATABASE_URL` exists, but its value was not printed.
- DATABASE_URL placeholder failure was caught and corrected.
- `secret_values_printed=false`.
- `database_connected=false`.
- `redis_connected=false`.
- `alembic_run=false`.
- `app_runtime_started=false`.
- `tdlib_auth_performed=false`.
- `telegram_connected=false`.
- `live_collector_started=false`.
- `notifier_transport_enabled=false`.

## Security/redaction notes

This record contains no raw public IP, no operator IP, no actual `DATABASE_URL`,
no DB password, no credential-bearing `REDIS_URL`, no OpenAI secrets, no
Telegram secrets, no GitHub secrets, no X secrets, no TDLib secrets, no SSH
private key paths, and no `.env` contents.

No `cat`, `source`, dot-source, or export command was run against the runtime
secret file or the DB/Redis URL variables. Runtime secret values were not
disclosed, printed, pasted into ChatGPT, or written into repo files.

## Unauthorized actions not performed

- No repo `.env` was created.
- No repo `env/*.env` was created.
- No Alembic was run.
- No app runtime was started.
- No TDLib auth was performed.
- No Telegram connection was performed.
- No live collector was started.
- No notifier transport was enabled.
- No production rollout was performed.
- No Docker or Docker Compose was used.
- No systemd unit was modified.

## Follow-up corrective actions

- Correct the runtime secret placement runbook validation script so permission
  validation uses numeric mode extraction and compares `750` and `640`.
- Extend the repo-local checker so future runbook revisions require numeric
  mode extraction.
- Extend focused contract tests so the textual file-mode extraction defect
  cannot silently reappear.
- Preserve all placeholder guards and secret exposure prohibitions.

## Next step

Review and commit this result record together with the corrective runbook,
checker, and test patch. The next operational step remains a separately
approved redacted Alembic preflight or runtime environment consumer preflight;
this result record does not authorize that step by itself.
