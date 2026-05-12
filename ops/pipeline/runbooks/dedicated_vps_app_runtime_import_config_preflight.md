# Dedicated VPS app runtime import/config preflight

## Purpose

Prepare the repo-local package for a future separately approved app/runtime
import and config preflight on the dedicated VPS before any runtime start.

This package validates import/config surfaces only. It verifies that selected
application runtime/config import surfaces are loadable without side effects
and that safe non-secret config loaders can consume the already validated
runtime environment shape.

This package does not prove service readiness. It does not authorize runtime
start.

The locked architecture remains unchanged:

```text
SourceMessage -> Artifact -> CandidateGroup -> EvidenceBundle -> JudgeOutput -> Analysis -> Notification
```

Layer boundaries remain unchanged:

- collector preserves raw Telegram source messages and revisions only.
- outbox-relay publishes thin ID-only Redis Stream messages only.
- router-normalizer is deterministic and non-LLM.
- enrichers gather evidence only.
- evidence-assembler assembles candidate-centered evidence bundles and may
  reroot only within its contract.
- LLM judge produces structured `judge_output_v1` only.
- deterministic policy-engine computes final `analysis_v1` verdict and
  `delivery_decision`.
- notifier is presentation and delivery only.
- PostgreSQL is durable system of record.
- Redis is queue, lock, and short-lived execution state only.
- replay creates new runs or versions and never overwrites historical truth.
- production has exactly one live Telegram collector instance.
- `recommended_flag_patch` is output-only and must not be auto-applied.
- production rollout remains unauthorized.

## Source-of-truth handling

`docs/project-source/README_replacement_consolidated_v0_20.md` remains
authoritative. The locked project-source bundles `00~10` remain the
source-of-truth set and are interpreted in filename order. The GitHub AI
application plan remains advisory only.

This package consumes the already closed dedicated VPS operational sequence:

- dedicated VPS hardening/repo bootstrap passed.
- DB/Redis provisioning passed.
- runtime secret placement passed.
- Alembic upgrade passed.
- post-migration DB acceptance smoke passed.
- runtime environment consumer preflight package/execution/result record passed.

This package does not reopen those decisions and does not re-run the runtime
environment consumer preflight.

## Scope

The runner is safe-by-default and requires
`--approved-app-runtime-import-config-preflight`.

Without approval, the runner reads no runtime env file, imports no application
service modules, inspects no process env vars, and connects nowhere. It returns
redacted JSON with `contract_status: approval_required`.

With approval, the runner reads only the specified runtime env file inside
Python. It parses key/value lines locally. It does not source, dot-source, or
export runtime env values. It does not mutate global environment permanently.

The approved runner:

- builds a redacted runtime env shape snapshot.
- imports allowlisted service config modules only.
- exercises only allowlisted safe non-secret config loader surfaces.
- defers secret-bound config loaders.
- skips forbidden runtime, client, service, worker, repository, Redis stream,
  and `main.py` surfaces.
- returns JSON report data only.

## Non-authorizations

This package does not authorize:

- runtime start.
- TDLib auth.
- Telegram connection.
- live collector startup.
- notifier transport.
- production rollout.
- PostgreSQL connection.
- Redis connection.
- DB writes.
- Redis mutation.
- Alembic.
- Docker or Docker Compose.
- systemd changes.
- migration file modification.
- runtime env mutation.
- secret file reads.
- secret printing.
- full connection URL printing.

Passing this preflight does not authorize service readiness or runtime startup.

## Redaction guarantees

The runner output must not include:

- actual `DATABASE_URL`.
- actual `REDIS_URL`.
- DB password.
- Redis credential.
- Telegram, OpenAI, GitHub, X, or TDLib secrets.
- raw server public IP.
- raw operator IP.
- runtime env contents.
- secret file contents.
- full connection URLs.

The runner may report:

- module names.
- class/function names.
- import/config status booleans.
- safe shape metadata already validated in the previous slice.
- loopback host shape.
- port numbers.
- database name.
- username.
- scheme names.
- secret-bound key categories.

Secret-bound loader categories are reported as deferred. Secret-bound loaders
are not executed.

## Future approved command

Future/separately approved only:

```bash
python scripts/ops/dedicated_vps_app_runtime_import_config_preflight_runner.py \
  --approved-app-runtime-import-config-preflight \
  --format json
```

Codex must not run that command against the real VPS runtime env during
implementation.

## Completed redacted result record

The separately approved app/runtime import-config preflight execution passed
and is recorded in
`ops/pipeline/runbooks/dedicated_vps_app_runtime_import_config_preflight_result_record.md`.

That result record is redacted and records only the already-approved execution
facts. It does not authorize TDLib auth, Telegram connection, live collector
startup, notifier transport, or production rollout.

## Failure handling

If the future approved preflight fails, stop and bring the redacted JSON back to
review.

Do not edit runtime env based only on this runbook. Do not proceed to TDLib
auth, Telegram connection, live collector startup, notifier transport,
production rollout, Alembic, DB mutation, Redis mutation, Docker, or systemd
changes.

## Passing result boundary

A passing result means only:

- allowlisted config modules imported without detected side effects.
- forbidden runtime/client/service surfaces were skipped.
- selected safe non-secret config loaders consumed a redacted runtime env shape.
- secret-bound config loaders were deferred.
- no runtime, DB, Redis, network, TDLib, Telegram, notifier, Docker, systemd,
  migration, or rollout side effects were performed.

It does not prove service readiness.

## Next bounded slice

After the result record is committed, pushed, pulled, and repo-locally validated
on the dedicated VPS, the next bounded slice is Telegram preparation only:

```text
dedicated_vps_telegram_credentials_acquisition_plan
```

Do not skip directly to TDLib auth, Telegram connection, live collector startup,
notifier transport, or production rollout.

## Anti-overconservatism check

If this package and its focused tests pass with no boundary violation, do not
add another diagnostic, checker, or preflight for marginal certainty. Move next
to `dedicated_vps_telegram_credentials_acquisition_plan` only after the result
record is committed, pushed, pulled, and validated.
