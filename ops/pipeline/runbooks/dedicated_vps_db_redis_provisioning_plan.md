# Dedicated VPS DB/Redis Provisioning Plan

## 0. Status

This is a planning/runbook boundary only for future dedicated VPS PostgreSQL and
Redis provisioning.

It does not authorize PostgreSQL installation, Redis installation, Docker
installation, Docker Compose execution, systemd app service execution, Alembic
migration, database creation, user creation, DB/Redis connection checks, secret
placement, `.env` creation, TDLib auth, Telegram connection, app runtime
startup, live collector startup, notifier transport startup, or production
rollout.

The two dedicated VPS result records are operational history only. They do not
grant runtime authorization.

## 1. Source-of-truth alignment

README v20 remains authoritative. The locked project-source bundles `00~04`
define the architecture and responsibility contracts, implementation bundles
`05~10` provide implementation context, and the GitHub AI application plan is
advisory only.

The architecture invariant remains:

```text
SourceMessage -> Artifact -> CandidateGroup -> EvidenceBundle -> JudgeOutput -> Analysis -> Notification
```

The locked service boundaries remain unchanged:

- collector preserves raw Telegram source messages and revisions only.
- normalization and triggering are deterministic and non-LLM.
- enrichers gather evidence only.
- evidence-assembler assembles candidate-centered EvidenceBundles and may reroot
  only within its contract.
- analysis-router is the deterministic judge-pipeline entry gate.
- judge-openai calls OpenAI and stores structured `judge_output_v1` only.
- analysis-validator validates `judge_output_v1` and controls policy handoff
  only.
- policy-engine computes final `analysis_v1.verdict` and `delivery_decision`
  deterministically.
- notifier is presentation, delivery, and Telegram transport only.
- maintenance is retry/replay orchestration plus explicitly requested one-shot
  delivery control-plane tools only.
- PostgreSQL durable system of record remains the durable system of record.
- Redis queue/lock/short-lived execution state remains queue, lock, and
  short-lived execution state only.
- prod has exactly one live Telegram collector instance.
- replay creates new runs or versions and never overwrites historical truth.
- production rollout remains unauthorized.

## 2. Scope

Allowed in this plan:

- DB/Redis binding policy.
- firewall policy.
- future provisioning sequence.
- future validation sequence.
- rollback boundary.
- non-goal list.
- acceptance checklist.

This plan may be used to prepare a later separately approved provisioning slice,
but it is not the provisioning slice.

## 3. Non-goals and unauthorized actions

This plan does not allow:

- actual install commands that imply immediate execution.
- PostgreSQL installation.
- Redis installation.
- Docker installation.
- Docker Compose execution.
- systemd app service execution.
- Alembic migration.
- database creation.
- database user creation.
- DB/Redis connectivity checks.
- secret values.
- `.env` content.
- connection strings.
- raw IP values.
- database passwords.
- Redis passwords.
- secret placement; secret placement remains unauthorized.
- `.env` creation; `.env` creation remains unauthorized. The exact policy marker
  is .env creation remains unauthorized.
- TDLib setup or auth.
- Telegram setup or connection.
- live collector startup; live collector remains unauthorized.
- notifier transport startup; notifier transport remains unauthorized.
- app runtime startup.
- production rollout.

## 4. Binding and firewall policy

PostgreSQL must never be publicly exposed. PostgreSQL must be bound to localhost
or Docker/internal network only.

Redis must never be publicly exposed. Redis must be bound to localhost or
Docker/internal network only.

Public inbound `5432` is forbidden. Public inbound `6379` is forbidden. The
plain-language acceptance markers are no public 5432 and no public 6379.

Hetzner Cloud Firewall must not expose DB/Redis ports. UFW must not expose
DB/Redis ports. Neither Hetzner Cloud Firewall nor UFW may open public inbound
DB/Redis access.

External clients should not connect directly to PostgreSQL or Redis. Future app
services should connect over localhost or internal service network only.

## 5. PostgreSQL provisioning boundary

PostgreSQL is the durable system of record.

Database creation and user creation are later explicit provisioning-slice tasks.
This plan does not create databases or users.

Alembic migration is not part of this plan. Alembic migration remains
unauthorized until a separately approved migration slice.

Backups must be considered before production data exists. Once real production
data exists, deletion or destructive rebuild is not allowed without separate
approval.

Future provisioning must constrain listener and host-authentication policy so
PostgreSQL accepts only localhost or internal service-network clients. Remote
superuser access is not allowed.

No password may be printed in logs, docs, terminal output, or validation
reports.

## 6. Redis provisioning boundary

Redis is queue/lock/short-lived execution state only.

Redis must not become durable source of truth. Redis persistence settings must
not be treated as a recovery source for canonical data.

Redis external exposure is forbidden. Redis must be reachable only from
localhost or internal service network.

Redis password or ACL decisions are later provisioning details and are not
authorized by this plan.

Queue rebuild must rely on PostgreSQL and the append-only/replay contracts, not
on Redis persistence.

## 7. Secret and environment policy

Secret placement remains unauthorized.

`.env` creation remains unauthorized. The exact policy marker is .env creation
remains unauthorized.

Future slices should prefer `_FILE` or file-based secret injection. Future secret
directories and files must use restricted permissions.

No secrets may be committed to git. No raw connection strings may be written in
docs, logs, or validation reports.

This plan does not read, place, print, validate, or rotate secrets.

## 8. Validation sequence

Future validation must happen in this order:

1. Provision DB/Redis in a separately approved slice.
2. Verify processes are bound only to localhost/internal network.
3. Verify Hetzner Cloud Firewall and UFW still expose no DB/Redis ports.
4. Verify DB/Redis service health locally only.
5. Verify DB connectivity with a non-secret redacted URL or secret-file based
   method.
6. Run Alembic current/upgrade only in a separately approved migration slice.
7. Run app DB/Redis smoke only after provisioning and migration approval.
8. Do not start live collector or notifier transport.

This plan does not perform any of those runtime checks.

## 9. Rollback and recovery boundary

Safe rollback ideas for future provisioning slices:

- stop DB/Redis services if they are misconfigured.
- remove accidental firewall rules.
- revert provisioning package/service changes if needed.
- before real data exists, re-provisioning is possible but must be documented.
- after real data exists, do not delete production data unless separately
  approved.

Rollback documentation must preserve what changed, why it changed, how it was
verified, and whether any data existed at the time.

## 10. Follow-up slices

Recommended sequence:

1. Commit this plan.
2. Optional metadata-only plan checker.
3. PostgreSQL/Redis package or Docker decision slice.
4. Actual DB/Redis provisioning slice.
5. DB/Redis local connectivity verification slice.
6. Alembic migration slice.
7. Runtime service environment/secrets slice.
8. TDLib auth slice.
9. Live collector one-channel smoke slice.
10. Silent/restricted notifier slice.

Each follow-up slice requires separate explicit approval.

## 11. Acceptance checklist

- PASS: This is a planning/runbook boundary only.
- PASS: PostgreSQL and Redis installation remain unauthorized.
- PASS: Docker installation and Docker Compose execution remain unauthorized.
- PASS: systemd app service execution remains unauthorized.
- PASS: PostgreSQL durable system of record responsibility is preserved.
- PASS: Redis queue/lock/short-lived execution state responsibility is
  preserved.
- PASS: PostgreSQL binding is localhost/internal network only.
- PASS: Redis binding is localhost/internal network only.
- PASS: no public 5432.
- PASS: no public 6379.
- PASS: Hetzner Cloud Firewall must not expose DB/Redis ports.
- PASS: UFW must not expose DB/Redis ports.
- PASS: secret placement remains unauthorized.
- PASS: `.env` creation remains unauthorized.
- PASS: Alembic migration remains unauthorized.
- PASS: DB/Redis connectivity checks require a later approved slice.
- PASS: live collector remains unauthorized.
- PASS: notifier transport remains unauthorized.
- PASS: no raw IP values, connection strings, passwords, tokens, or secret values
  belong in this document.
