# Dedicated VPS Repo Bootstrap Result Record

## 0. Status

Dedicated VPS repo bootstrap passed for `github_ai_catchbot` on
`github-ai-catchbot-prod-1`.

This document is an operational result record, not an implementation design.
It records an already completed repository bootstrap and validation result.

This record is additive only. It does not replace
`docs/project-source/README_replacement_consolidated_v0_20.md`, and it does not
replace the stage 0~44 contracts captured in the locked project-source bundles.

This record does not authorize production rollout. It does not authorize live
collector startup, TDLib auth, Telegram connection, DB/Redis production
connectivity, Docker/systemd app execution, schema/migration changes, new
runtime workers, secret placement, `.env` creation, app runtime startup, or
notifier transport startup.

## 1. Source-of-truth alignment

README v20 remains authoritative. The locked stage 0~44 project-source bundles
remain the implementation-contract source of truth. The value-expansion scope
lock remains additive product scope only. The dedicated VPS initial hardening
record and this repo bootstrap record are operational result records only.
`docs/project-source/03_GitHub_AI_application_plan.md` remains advisory only.

This record preserves the canonical pipeline:

SourceMessage -> Artifact -> CandidateGroup -> EvidenceBundle -> JudgeOutput -> Analysis -> Notification

The locked responsibility boundaries remain unchanged:

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
- PostgreSQL is the durable system of record.
- Redis is queue, lock, and short-lived execution state only.
- prod has exactly one live Telegram collector instance.
- replay creates new runs or versions and never overwrites historical truth.
- production rollout remains unauthorized.

PostgreSQL remains the durable system of record and Redis remains queue, lock,
and short-lived execution state. Neither PostgreSQL nor Redis was installed,
provisioned, or connected in this slice.

## 2. Repo bootstrap identity and redaction policy

- Server name: `github-ai-catchbot-prod-1`.
- VPS repo path: `~/workspace/bots/github_ai_catchbot`.
- Verified HEAD: `dea133f test(ops): harden VPS preflight redaction assertions`.
- GitHub read-only deploy key authentication succeeded.
- Repository clone and pull succeeded.
- Operator SSH alias configured outside repository.
- Exact server IP values are intentionally omitted from repository
  documentation.
- Exact operator IP values are intentionally omitted from repository
  documentation.
- SSH key paths, fingerprints, key material, and passphrases are intentionally
  omitted from repository documentation.
- GitHub deploy key material is intentionally omitted from repository
  documentation.

The public server address is retained in private operator notes. The operator
IP allowlist remains outside repository documentation. GitHub read-only deploy
key configuration is recorded here only as a result, without recording key
material.

## 3. Verified repo bootstrap results

- PASS: GitHub SSH deploy key authentication succeeded.
- PASS: clone/pull uses the read-only deploy key route.
- PASS: repo cloned under deploy user.
- PASS: repo path exists at `~/workspace/bots/github_ai_catchbot`.
- PASS: repo is aligned with `origin/main`.
- PASS: HEAD verified at `dea133f`.
- PASS: repo-local venv created.
- PASS: Python 3.12.3 verified.
- PASS: pip upgraded to 26.1.1.
- PASS: pytest 9.0.3 available.
- PASS: editable package install completed.
- PASS: `pip check` passed.
- PASS: compileall passed.
- PASS: dedicated VPS baseline preflight schema mode passed.
- PASS: previously failing redaction test passed after `dea133f`.
- PASS: no app runtime service detected.

## 4. Validation results

The VPS validation categories were:

- `git status --short --branch` on VPS reported clean/aligned
  `main...origin/main`.
- `git log --oneline -5` showed `dea133f` at HEAD, followed by recent history
  including `70cea32`, `72c9ef4`, `23be68d`, and `a62fa6f`.
- `python -m pip check` passed.
- `python -m compileall -q src scripts tests migrations` passed.
- focused dedicated VPS preflight tests passed after the test hardening fix.
- `scripts/ops/dedicated_vps_baseline_preflight_check.py --format json` passed
  in schema mode with `contract_status: passed` and `checks_failed: []`.
- side-effect booleans remained false for DB, Redis, Docker, systemd, TDLib,
  and Telegram.
- redaction booleans remained false for username, home path, hostname, IP, raw
  path, secret, and provider metadata.
- no catchbot, collector, telegram, postgres, redis, or docker runtime service
  was running.
- root disk and memory checks were healthy at the end of the repo bootstrap
  check, with approximately 150G root disk capacity, about 142G free, about
  7.6GiB memory total, and about 7.1GiB available.

This section summarizes validation categories only. It intentionally does not
store raw full command logs, server addresses, operator addresses, key paths,
fingerprints, key material, passphrases, provider account identifiers, or secret
values.

## 5. Current non-started runtime components

The following were not performed in this repo bootstrap slice:

- Docker installation
- PostgreSQL provisioning
- Redis provisioning
- Alembic migration
- TDLib auth
- Telegram connection
- OpenAI/GitHub/X secret placement
- `.env` creation
- systemd app service creation
- app runtime startup
- live collector startup
- notifier transport startup
- production rollout

Repo clone and repo-local venv setup were performed and are intentionally not
listed as non-started runtime components.

## 6. Known issue resolved during bootstrap

VPS validation initially exposed a test false-positive: username `deploy`
matched the JSON key `deployment_topology`.

This was not an actual redaction leak. The test hardening fix was committed as:

`dea133f test(ops): harden VPS preflight redaction assertions`

The VPS pulled this commit with a fast-forward pull from `origin/main`. The
originally failing test,
`test_current_host_mode_does_not_print_username_home_hostname_or_ip`, then
passed on VPS with `1 passed in 0.04s`.

## 7. Security boundaries preserved

- No exact public server IP values were added to repository documentation.
- No exact operator IP values were added to repository documentation.
- No SSH fingerprints were added to repository documentation.
- No SSH private or public key contents were added to repository documentation.
- No SSH private key path was added to repository documentation.
- No passphrase was added to repository documentation.
- No GitHub deploy key contents were added to repository documentation.
- No Hetzner account identifiers were added to repository documentation.
- No OpenAI, GitHub, X, Telegram, PostgreSQL, Redis, TDLib, or other secret
  values were added to repository documentation.
- Operator-local SSH alias setup remains outside repository scope.
- Operator IP allowlist details remain outside repository scope.

## 8. Follow-up sequence

1. Keep VPS repo bootstrap result record committed.
2. Do not install Docker, PostgreSQL, Redis, or TDLib yet.
3. Plan PostgreSQL/Redis provisioning as a separate explicit slice.
4. Before DB/Redis provisioning, define local-only or Docker-internal binding
   policy.
5. Run DB/Redis connectivity verification only after provisioning.
6. Perform Alembic migration only after DB provisioning and explicit approval.
7. Perform TDLib auth only in a separate explicit slice.
8. Start live collector only after TDLib/auth/runtime gates are explicitly
   approved.
9. Start notifier transport only after silent/restricted delivery gates are
   explicitly approved.

## 9. Acceptance checklist

- PASS: This document is an operational result record, not an implementation
  design.
- PASS: The record is additive only.
- PASS: README v20 remains authoritative.
- PASS: Stage 0~44 contracts remain authoritative for implementation contracts.
- PASS: The source pipeline invariant is preserved.
- PASS: Locked service responsibility boundaries are preserved.
- PASS: PostgreSQL and Redis responsibilities remain unchanged.
- PASS: PostgreSQL and Redis were not installed, provisioned, or connected.
- PASS: Production rollout is not authorized.
- PASS: Live collector startup is not authorized.
- PASS: TDLib auth is not authorized.
- PASS: Telegram connection is not authorized.
- PASS: DB/Redis production connectivity is not authorized.
- PASS: Docker/systemd app execution is not authorized.
- PASS: Schema/migration changes are not authorized.
- PASS: New runtime workers are not authorized.
- PASS: Secret placement is not authorized.
- PASS: `.env` creation is not authorized.
- PASS: App runtime startup is not authorized.
- PASS: Notifier transport startup is not authorized.
- PASS: Raw server IP values are omitted.
- PASS: Raw operator IP values are omitted.
- PASS: SSH key paths, fingerprints, key material, and passphrases are omitted.
- PASS: GitHub deploy key material is omitted.

## 10. Operator notes

Future VPS access should use the operator-local SSH alias, not raw IP values in
repository documentation. The SSH alias is outside repository scope.

If the operator moves or changes Wi-Fi/network, SSH keys do not change. Hetzner
firewall and UFW operator IP allowlists must be updated outside repository
documentation before relying on remote access from the new network.

Do not add raw old or new IP values to repository documentation.
