# Dedicated VPS DB/Redis package-vs-Docker decision

## Scope

This is a decision/checker/runbook slice only for the next dedicated single VPS
DB/Redis infrastructure provisioning boundary.

This slice does not install, start, connect to, provision, or mutate
PostgreSQL, Redis, Docker, systemd services, TDLib, Telegram, app runtime,
notifier transport, secrets, `.env`, Alembic, or production rollout state.

The decision applies only to DB/Redis package-vs-Docker for the next
provisioning slice. It does not change runtime service code, migrations,
project-source docs, secrets, or deployment state.

## Source-of-truth handling

The canonical architecture remains:

```text
SourceMessage -> Artifact -> CandidateGroup -> EvidenceBundle -> JudgeOutput -> Analysis -> Notification
```

README v20 and the locked project-source bundles remain the source-of-truth set.
This runbook records the minimal-change interpretation for the immediate first
DB/Redis infrastructure provisioning step only.

Production has exactly one live Telegram collector instance. Replay creates new
runs or versions and never overwrites historical truth. Production rollout
remains unauthorized. `recommended_flag_patch` is output-only and must not be
auto-applied.

## Prior locked default

- Single VPS + Docker Compose remains the eventual multi-service runtime
  direction.
- PostgreSQL durable system of record remains the durable system of record.
- Redis queue/lock/short-lived execution state remains queue, lock, and
  short-lived execution state only.

## Decision

First DB/Redis provisioning should use host apt/systemd PostgreSQL and host
apt/systemd Redis.

Selected immediate DB/Redis provisioning path:

```text
host apt/systemd PostgreSQL + host apt/systemd Redis
```

The selected decision is host apt/systemd PostgreSQL plus host apt/systemd
Redis for first actual DB/Redis provisioning on the dedicated single VPS.

## Minimal-change interpretation

- This is a bounded infrastructure decision for DB/Redis provisioning only.
- Docker Compose remains available for later full app stack work.
- Docker Compose remains a future full app stack candidate and is not
  discarded.
- This is not an architecture rewrite.
- This does not discard Docker Compose.
- This does not change service ownership, queue semantics, durable storage
  semantics, replay semantics, collector singleton policy, notifier policy, or
  rollout gates.

## Rationale

- Current VPS repo bootstrap is repo-local venv based, not app-container based.
- Actual app containerization is not yet implemented or accepted.
- Host packages reduce moving parts for the first DB/Redis binding/provisioning
  slice.
- PostgreSQL and Redis must bind locally/private-only, not public.
- The immediate validation target is DB/Redis local/private binding and
  no-public-exposure evidence before TDLib auth, live collector smoke, shadow
  analysis, silent delivery, restricted delivery, and go-live gate.

## Rejected alternatives for this immediate slice

### Docker Compose DB/Redis now

Rejected for the immediate first DB/Redis provisioning slice because the app
runtime is still repo-local venv based and full app containerization has not
been implemented or accepted.

This is not a permanent rejection. Docker Compose remains a future full app
stack candidate and is not discarded.

### Managed DB/Redis now

Rejected for the immediate first DB/Redis provisioning slice because the current
target is a dedicated single VPS with local/private-only service binding and a
minimal first infrastructure footprint.

### App containerization now

Rejected for the immediate first DB/Redis provisioning slice because app
containerization is a separate full-stack deployment scope and is not authorized
by this decision/checker slice.

## Required safety constraints

- no public 5432.
- no public 6379.
- local/private binding only.
- no secret values in repo docs.
- no `.env`.
- no Alembic in this slice.
- no app runtime.
- no TDLib/Telegram.
- no live collector.
- no notifier transport.
- no production rollout.
- no install/start/connect side effects in this slice.
- no PostgreSQL installation in this slice.
- no Redis installation in this slice.
- no Docker installation in this slice.
- no Docker Compose execution in this slice.
- no systemd service start or restart in this slice.
- no DB connection in this slice.
- no Redis connection in this slice.
- no secret placement in this slice.
- no `.env` creation in this slice.

## Next provisioning slice acceptance criteria

The next provisioning slice must be separately approved before any operator
commands run.

- PostgreSQL installed by explicit operator command only.
- Redis installed by explicit operator command only.
- bind/listen addresses verified.
- firewall verified.
- no public DB/Redis exposure.
- local-only health checks.
- secrets/database passwords not printed or committed.
- Alembic not run until separately authorized.

## Rollback / failure handling

If DB/Redis package provisioning fails, do not fall back to Docker
automatically.

Stop, record the failure, and request the next explicit operator decision.
Failure in the host package path is not permission to install Docker, start
Docker Compose, expose DB/Redis publicly, create `.env`, print secrets, run
Alembic, start app runtime, perform TDLib auth, connect Telegram, start the live
collector, enable notifier transport, or authorize production rollout.

## Anti-overconservatism note

After this decision/checker passes, move to the actual DB/Redis provisioning
slice.

Do not add more docs/checkers unless a concrete boundary gap, contradiction, or
failed validation appears.
