# Dedicated VPS DB/Redis operator provisioning result record

## Scope

This is a repo-local operational result record for the dedicated VPS DB/Redis
operator provisioning that was manually run on the dedicated VPS. It is a
redacted record of already completed operator work plus the defects found during
that run.

Codex did not execute VPS commands for this record. This document does not
authorize additional VPS execution, SSH, package installation, service changes,
database connections, Redis connections, Alembic, app runtime startup, TDLib
auth, Telegram connection, live collector startup, notifier transport, or
production rollout.

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
- evidence-assembler assembles candidate-centered evidence bundles and may
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

This record was recorded after 2026-05-08 operator execution.

Actual manual operator provisioning Blocks 0-6 were run on the dedicated VPS.
Block 0 context passed. Block 1 package install and enable completed without
fatal errors. Blocks 2 and 3 required manual recovery because the reviewed
runbook contained PostgreSQL and Redis command defects. Blocks 4, 5, and 6
passed after recovery.

## Final result

PASS: dedicated VPS host PostgreSQL and Redis package provisioning completed
with local-only DB/Redis exposure and no production rollout.

PostgreSQL 16/main reached final `online` cluster status. Redis reached active
status with the intended local/protected systemd configuration. The app role and
database exist, and the app role password was set interactively without
disclosure.

## Environment summary

- Host label: `github-ai-catchbot-prod-1`.
- User: `deploy`.
- OS: Ubuntu 24.04.4 LTS.
- Repo path: `/home/deploy/workspace/bots/github_ai_catchbot`.
- Repo HEAD/origin at execution: `a0d7e87 test(ops): add DB Redis operator command check`.
- PostgreSQL final cluster: `16/main`, online.
- Redis final service: active.
- Raw public IP and operator IP values are intentionally omitted.

## Commands actually run by block

- Block 0: context confirmation commands were run and passed.
- Block 1: PostgreSQL and Redis package install plus service enable commands
  were run and completed without fatal errors.
- Block 2: PostgreSQL cluster detection and configuration commands were run.
  The original PostgreSQL listen-address write produced a syntax error and was
  corrected manually.
- Block 3: Redis configuration commands were run. The original non-sudo config
  reads caused permission errors and were corrected manually.
- Block 4: PostgreSQL and Redis service restart/status checks were run after
  recovery and passed.
- Block 5: PostgreSQL app role, database, interactive password, and grant
  commands were run and passed.
- Block 6: local-only PostgreSQL, Redis, listen-socket, and UFW verification
  commands were run and passed.

## Deviations from approved runbook

1. PostgreSQL `listen_addresses` was initially written as an unquoted
   numeric-looking value, causing a PostgreSQL config syntax error near `.0`.
2. Redis config checks initially used non-sudo reads of `/etc/redis/redis.conf`,
   causing permission denied errors and creating duplicate-append risk if the
   read path falsely appeared absent.
3. The service status block relied on umbrella `systemctl is-active postgresql`
   status without proving that the selected PostgreSQL cluster itself was
   online.

## Recovery actions performed

- PostgreSQL config was manually corrected to:
  - `listen_addresses = '127.0.0.1'`
  - `password_encryption = 'scram-sha-256'`
- PostgreSQL 16/main was started successfully and reached `online` state.
- Redis config was manually recovered and cleaned with sudo-safe editing.
- Redis final config ended exactly as:
  - `bind 127.0.0.1 ::1`
  - `protected-mode yes`
  - `supervised systemd`
- Block 4 was rerun after recovery and passed.
- Block 5 was run after recovery and passed.
- Block 6 was run after recovery and passed.

## Final verification evidence

- Package installation completed.
- PostgreSQL final cluster status: `16/main` online.
- PostgreSQL final config:
  - `listen_addresses = '127.0.0.1'`
  - `password_encryption = 'scram-sha-256'`
- Redis final config:
  - `bind 127.0.0.1 ::1`
  - `protected-mode yes`
  - `supervised systemd`
- Services active/enabled:
  - postgresql active/enabled
  - redis-server active/enabled
- Role/database:
  - `github_ai_catchbot_app` exists.
  - `github_ai_catchbot` exists.
  - password was set interactively.
  - password was not disclosed.
- PostgreSQL local readiness: PASS.
- `SELECT 1`: PASS, returned `1`.
- Database existence check: PASS, returned `1`.
- Role existence check: PASS, returned `1`.
- Redis local PING: PASS, returned `PONG`.
- Listen sockets showed only loopback DB/Redis:
  - `127.0.0.1:5432`
  - `127.0.0.1:6379`
  - `[::1]:6379`
- Public exposure:
  - no public 5432.
  - no public 6379.
- UFW:
  - UFW status active.
  - no UFW public 5432/6379 allow rule.

## Security/redaction notes

The raw operator SSH source IP appeared in pasted chat output during the manual
review loop. It is intentionally omitted and redacted from repo documentation.

This record contains no raw public IP, no operator IP, no DB password, no
credential-bearing `DATABASE_URL`, no credential-bearing `REDIS_URL`, no
Telegram/OpenAI/GitHub/X secrets, no SSH private key paths, and no `.env`
contents.

Loopback literals in this record are local binding evidence only.

## Unauthorized actions not performed

- No `.env` was created.
- No Alembic migration was run.
- No app runtime was started.
- No TDLib auth was performed.
- No Telegram connection was performed.
- No live collector was started.
- No notifier transport was enabled.
- No production rollout was performed.

## Follow-up corrective actions

- Correct the PostgreSQL listen-address command so future operator runs write
  `listen_addresses = '127.0.0.1'` rather than an unquoted value.
- Correct Redis config reads so `/etc/redis/redis.conf` checks use sudo-safe
  reads before deciding whether to edit or append directives.
- Correct service verification so PostgreSQL cluster `online` status and local
  readiness are checked after restart, in addition to umbrella systemd status.
- Extend the repo-local checker and tests so these three defects cannot silently
  reappear in the operator provisioning runbook.

## Next step

Review and commit this result record together with the corrective runbook,
checker, and test patch. The next operational step remains a separately
approved migration or runtime-secret slice; this record does not authorize that
step by itself.
