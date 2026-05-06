# Dedicated VPS Initial Hardening Result Record

## 0. Status

Initial dedicated VPS provisioning and baseline hardening passed for the first
dedicated `github_ai_catchbot` production host.

This document is an operational result record, not an implementation design.
It is additive only. It does not replace
`docs/project-source/README_replacement_consolidated_v0_20.md`, and it does not
replace the stage 0~44 contracts captured in the locked project-source bundles.

This record does not authorize production rollout. It does not authorize live
collector startup, TDLib auth, Telegram connection, DB/Redis production
connectivity, Docker/systemd app execution, schema/migration changes, new
runtime workers, secret placement, `.env` creation, or app runtime startup.

## 1. Source-of-truth alignment

The authoritative source order remains:

1. `docs/project-source/README_replacement_consolidated_v0_20.md`
2. Locked design bundles `00~04`
3. Implementation bundles `05~10`
4. Advisory-only `docs/project-source/03_GitHub_AI_application_plan.md`

This operational result record preserves the existing architecture invariant:

```text
SourceMessage -> Artifact -> CandidateGroup -> EvidenceBundle -> JudgeOutput -> Analysis -> Notification
```

The existing responsibility boundaries remain locked:

- collector preserves raw Telegram source messages and revisions only.
- normalization and triggering are deterministic and non-LLM.
- enrichers gather evidence only.
- evidence-assembler assembles candidate-centered EvidenceBundles and may
  reroot only within its contract.
- analysis-router is the deterministic judge-pipeline entry gate.
- judge-openai calls OpenAI and stores structured `judge_output_v1` only.
- analysis-validator validates `judge_output_v1` and controls policy handoff only.
- policy-engine computes final `analysis_v1.verdict` and `delivery_decision`
  deterministically.
- notifier is presentation, delivery, and Telegram transport only.
- maintenance is retry/replay orchestration plus explicitly requested one-shot
  delivery control-plane tools only.
- PostgreSQL is the durable system of record.
- Redis is queue, lock, and short-lived execution state only.
- prod has exactly one live Telegram collector instance.
- replay creates new runs or versions and never overwrites historical truth.
- precision-first and negative-first remain governing product philosophy.

PostgreSQL remains the durable system of record and Redis remains queue, lock,
and short-lived execution state. Neither PostgreSQL nor Redis was installed,
provisioned, or connected in this slice.

## 2. Server identity and redaction policy

- Server name: `github-ai-catchbot-prod-1`
- Provider/location: Hetzner / Falkenstein, Germany
- Plan: CPX32
- OS: Ubuntu 24.04.x LTS
- Backups: enabled
- Hetzner Cloud Firewall: attached
- public IPv4 assigned and retained in private operator notes.
- public IP values intentionally omitted from repo documentation.
- operator IPv4 /32 allowlist configured.
- operator allowlisted IP intentionally omitted from repo documentation.
- SSH key auth configured with operator-managed key.
- SSH key fingerprint and private key path intentionally omitted from repo
  documentation.

Exact public IPv4, public IPv6, operator home/public IP, SSH key fingerprint,
SSH private key path, passphrase, Hetzner account identifiers, and secret values
must remain outside repository documentation.

## 3. Verified hardening results

- PASS: deploy user exists.
- PASS: deploy user has sudo capability.
- PASS: SSH key auth works for deploy.
- PASS: root SSH blocked.
- PASS: SSH password authentication disabled.
- PASS: ssh.socket enabled and active after reboot.
- PASS: ssh.service socket-triggered and active after connection.
- PASS: UFW active.
- PASS: UFW default deny incoming / allow outgoing.
- PASS: UFW TCP 22 allowlist limited to operator IPv4 /32.
- PASS: fail2ban active.
- PASS: fail2ban sshd jail active.
- PASS: unattended-upgrades active.
- PASS: Hetzner Cloud Firewall attached.
- PASS: backups enabled.
- PASS: reboot survival test passed.
- PASS: no unexpected external attack evidence observed in supplied SSH logs.

## 4. Reboot survival result

The reboot survival test passed.

After reboot, SSH public key auth for the deploy user still worked. `ssh.socket`
was enabled and active after reboot, and `ssh.service` was socket-triggered and
active after connection. The hardened SSH posture survived reboot with root SSH
blocked and password authentication disabled.

## 5. Current non-started runtime components

The following were not performed in this slice:

- repo clone
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

## 6. Security boundaries preserved

This record keeps the dedicated `github_ai_catchbot` VPS separate from the
existing trading-bot VPS.

The following remain unauthorized by this document:

- production rollout
- live collector startup
- TDLib auth
- Telegram connection
- DB/Redis production connectivity
- Docker/systemd app execution
- schema/migration changes
- new runtime workers
- secret placement
- `.env` creation
- app runtime startup

No exact public IP values, operator allowlisted IP values, SSH fingerprints,
private key paths, passphrases, Hetzner account identifiers, or secret values
are recorded in this repository document.

## 7. Follow-up sequence

Recommended next sequence:

1. Keep VPS hardening result record committed.
2. Add a bounded repo/VPS bootstrap runbook or dry-run checker if needed.
3. Clone repo on VPS only after explicit approval.
4. Set up Python/venv only after repo clone approval.
5. Provision PostgreSQL/Redis only in a separate explicit slice.
6. Run DB/Redis connectivity verification only after provisioning.
7. Perform TDLib auth only in a separate explicit slice.
8. Start live collector only after TDLib/auth/runtime gates are explicitly
   approved.
9. Start notifier transport only after silent/restricted delivery gates are
   explicitly approved.

## 8. Acceptance checklist

- PASS: This is an operational result record, not an implementation design.
- PASS: The record is additive only.
- PASS: The record does not replace README v20.
- PASS: The record does not replace stage 0~44 contracts.
- PASS: The architecture invariant is preserved.
- PASS: PostgreSQL and Redis responsibilities remain unchanged.
- PASS: PostgreSQL and Redis were not installed, provisioned, or connected.
- PASS: No production rollout is authorized.
- PASS: No live collector startup is authorized.
- PASS: No TDLib auth is authorized.
- PASS: No Telegram connection is authorized.
- PASS: No DB/Redis production connectivity is authorized.
- PASS: No Docker/systemd app execution is authorized.
- PASS: No schema/migration changes are authorized.
- PASS: No new runtime workers are authorized.
- PASS: No secret placement is authorized.
- PASS: No `.env` creation is authorized.
- PASS: No app runtime startup is authorized.
- PASS: Raw IP addresses and SSH-sensitive details are intentionally omitted.

## 9. Operator notes

If the operator moves or changes Wi-Fi/network, SSH keys do not change. The
Hetzner Cloud Firewall and UFW operator IPv4 /32 allowlist must be updated to
the new operator network before relying on remote access from that network.

Do not add the old or new raw IP values to repository documentation.
