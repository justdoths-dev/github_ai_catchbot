# Dedicated VPS Telegram runtime secret placement update package

## Purpose

Create a repo-local, redaction-safe operator package for a later dedicated VPS
update to the host-local runtime secret file.

This package documents how a future operator may add Telegram-related runtime
keys after ChatGPT review, commit/push, VPS pull, repo-local validation, and a
separate explicit approval to execute the update.

This package does not execute secret placement. It does not read, print, or
mutate `/etc/github-ai-catchbot/runtime.env`.

## Source-of-truth / architecture boundary

`docs/project-source/README_replacement_consolidated_v0_20.md` remains
authoritative. The locked project-source bundles `00` through `10` remain the
source-of-truth set and are interpreted in filename order. The GitHub AI
application plan remains advisory only.

This package preserves the canonical architecture invariant:

```text
SourceMessage -> Artifact -> CandidateGroup -> EvidenceBundle -> JudgeOutput -> Analysis -> Notification
```

Layer boundaries remain unchanged:

- collector preserves raw Telegram source messages and revisions only.
- outbox-relay publishes thin ID-only Redis Stream messages only.
- router-normalizer is deterministic and non-LLM.
- enrichers gather evidence only.
- evidence-assembler assembles candidate-centered EvidenceBundles and may
  reroot only within its contract.
- LLM judge produces structured `judge_output_v1` only.
- deterministic policy-engine computes final `analysis_v1` verdict and
  `delivery_decision`.
- notifier is presentation and delivery only.
- PostgreSQL is durable system of record.
- Redis is queue, lock, and short-lived execution state only.
- replay creates new runs or versions and never overwrites historical truth.
- production has exactly one live Telegram collector instance.
- production rollout remains unauthorized.

## Current closed prerequisites

- Latest expected repo HEAD is `bd3f483 test(ops): declare pytest asyncio
  dependency`.
- Previous Telegram credentials acquisition plan is
  `8aa6f73 docs(ops): add telegram credentials acquisition plan`.
- Telegram credentials acquisition plan is PASS.
- pytest async dependency contract is PASS.
- VPS test parity is restored.

These conclusions are not reopened by this package.

## Scope

This package covers only future update planning for Telegram-related runtime
keys in the dedicated VPS runtime secret file.

Allowed repo-local content is limited to key names, placeholder labels,
operator checklist language, redacted validation logic, and status-only
examples.

The current implementation config surfaces are:

- collector reader account / TDLib / MTProto:
  - `TELEGRAM_API_ID`
  - `TELEGRAM_API_HASH`
  - `TELEGRAM_PHONE_NUMBER`
  - `TELEGRAM_2FA_PASSWORD`
  - `TDLIB_DB_ENCRYPTION_KEY`
  - `TDLIB_STATE_DIR`
  - `TDLIB_FILES_DIR`
- notifier bot / Telegram Bot API:
  - `TELEGRAM_BOT_TOKEN`

The current collector config also accepts secret-file variants through its
secret reader helper for `TELEGRAM_API_HASH_FILE`,
`TELEGRAM_2FA_PASSWORD_FILE`, and `TDLIB_DB_ENCRYPTION_KEY_FILE`. This package
uses the direct runtime env keys above as the minimal-change interpretation for
one host-local runtime secret file. It does not change config loading.

## Non-authorizations

This package does not execute the operator package.
This package does not mutate `/etc/github-ai-catchbot/runtime.env`.
This package does not read or print runtime env values.
This package does not store actual secrets.
This package does not run TDLib auth.
This package does not connect Telegram.
This package does not start live collector.
This package does not enable notifier transport.
This package does not start app runtime.
This package does not connect to DB or Redis.
This package does not run Alembic.
This package does not modify Docker or systemd.
This package does not perform production rollout.

## Runtime secret file target

Future approved operator execution may update only this host-local file:

```text
/etc/github-ai-catchbot/runtime.env
```

The file is outside the repository. Do not create repo `.env` files and do not
write secrets into git-tracked files.

## Collector reader account / TDLib / MTProto keys

Collector reader account credentials and TDLib state belong to the collection
side only.

Required collector-side runtime key names:

```text
TELEGRAM_API_ID=<paste-from-password-manager-in-editor>
TELEGRAM_API_HASH=<paste-from-password-manager-in-editor>
TELEGRAM_PHONE_NUMBER=<paste-from-password-manager-in-editor>
TELEGRAM_2FA_PASSWORD=<paste-from-password-manager-in-editor-or-leave-empty-if-not-enabled>
TDLIB_DB_ENCRYPTION_KEY=<paste-from-password-manager-in-editor>
TDLIB_STATE_DIR=<dedicated-vps-tdlib-state-path-label>
TDLIB_FILES_DIR=<dedicated-vps-tdlib-files-path-label>
```

Collector rules:

- collector reader account and notifier bot are separate credentials.
- collector uses TDLib/MTProto reader account credentials.
- bot token does not authorize channel collection.
- TDLib auth remains later and separately reviewed.
- Telegram connection remains unauthorized.
- live collector startup remains unauthorized.

## Notifier bot / Bot API keys

Notifier credentials belong to the delivery side only.

Required notifier-side runtime key name:

```text
TELEGRAM_BOT_TOKEN=<paste-from-password-manager-in-editor>
```

Notifier rules:

- notifier uses Telegram Bot API bot token.
- reader account credentials do not authorize notifier transport.
- notifier transport remains unauthorized.
- production rollout remains unauthorized.

## Operator pre-checklist

Before any later execution, the operator must confirm:

- ChatGPT review approved this package.
- the package was committed and pushed after approval.
- dedicated VPS repository checkout was pulled to the approved commit.
- repo-local validation for this package passed on the VPS.
- separate explicit approval was granted to execute secret placement update.
- credentials are available only in the operator password manager or approved
  secret store.
- no secret value is pasted into ChatGPT, Codex, GitHub, repository files,
  markdown, or shell history.
- collector reader account and notifier bot are still separate credentials.
- production rollout remains unauthorized.

## Safe edit procedure

These commands are documentation for later approved operator execution only.
Codex must not run them during this package slice.

Confirm context without printing secrets:

```bash
pwd
whoami
id
git status --short --branch
git log --oneline -3
test "$(whoami)" = "deploy"
```

Edit the runtime secret file in an editor so values do not appear in shell
history or command-line arguments:

```bash
sudoedit /etc/github-ai-catchbot/runtime.env
```

Inside the editor only, add or update the Telegram key lines listed in the
collector and notifier sections. Placeholder labels such as
`<paste-from-password-manager-in-editor>` are examples for the runbook only and
must not be saved as final values.

Do not display the runtime secret file in the terminal. Do not load the runtime
secret file into the current shell. Do not append secret assignments with shell
commands. Do not pipe secret file contents into environment export commands.

## Redacted validation procedure

Validation may read `/etc/github-ai-catchbot/runtime.env` only during the later
approved operator execution. It must print only redacted JSON/status.

The validation command must not connect to DB or Redis, must not run Alembic,
must not import or start app runtime services, must not run TDLib auth, must not
connect Telegram, must not start live collector, must not enable notifier
transport, and must not perform production rollout.

The later operator may run a local redacted validator shaped like this:

```bash
sudo python3 - <<'PY'
from __future__ import annotations

import json
from pathlib import Path

target = Path("/etc/github-ai-catchbot/runtime.env")
required_keys = [
    "TELEGRAM_API_ID",
    "TELEGRAM_API_HASH",
    "TELEGRAM_PHONE_NUMBER",
    "TDLIB_DB_ENCRYPTION_KEY",
    "TDLIB_STATE_DIR",
    "TDLIB_FILES_DIR",
    "TELEGRAM_BOT_TOKEN",
]
optional_keys = ["TELEGRAM_2FA_PASSWORD"]
placeholder_markers = ("<", "paste-from-password-manager", "replace-inside-editor")

def classify(raw: str | None) -> str:
    if raw is None:
        return "missing"
    value = raw.strip()
    if not value:
        return "empty"
    lowered = value.lower()
    if any(marker in lowered for marker in placeholder_markers):
        return "placeholder"
    return "present_redacted"

values: dict[str, str] = {}
if target.exists():
    for line in target.read_text(encoding="utf-8").splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#") or "=" not in stripped:
            continue
        key, value = stripped.split("=", 1)
        values[key.strip()] = value

status = {
    "runtime_env_path": "/etc/github-ai-catchbot/runtime.env",
    "runtime_env_read": target.exists(),
    "key_status": {
        key: classify(values.get(key)) for key in [*required_keys, *optional_keys]
    },
    "runtime_env_values_printed": False,
    "database_connected": False,
    "redis_connected": False,
    "alembic_run": False,
    "app_runtime_started": False,
    "tdlib_auth_performed": False,
    "telegram_connected": False,
    "live_collector_started": False,
    "notifier_transport_enabled": False,
    "production_rollout_performed": False,
}
status["contract_status"] = (
    "passed"
    if all(status["key_status"][key] == "present_redacted" for key in required_keys)
    else "failed"
)
print(json.dumps(status, sort_keys=True))
PY
```

## Expected redacted validation output shape

The output must be one JSON object only.

Expected fields:

```json
{
  "alembic_run": false,
  "app_runtime_started": false,
  "contract_status": "passed-or-failed",
  "database_connected": false,
  "key_status": {
    "TDLIB_DB_ENCRYPTION_KEY": "present_redacted-or-empty-or-placeholder-or-missing",
    "TDLIB_FILES_DIR": "present_redacted-or-empty-or-placeholder-or-missing",
    "TDLIB_STATE_DIR": "present_redacted-or-empty-or-placeholder-or-missing",
    "TELEGRAM_2FA_PASSWORD": "present_redacted-or-empty-or-placeholder-or-missing",
    "TELEGRAM_API_HASH": "present_redacted-or-empty-or-placeholder-or-missing",
    "TELEGRAM_API_ID": "present_redacted-or-empty-or-placeholder-or-missing",
    "TELEGRAM_BOT_TOKEN": "present_redacted-or-empty-or-placeholder-or-missing",
    "TELEGRAM_PHONE_NUMBER": "present_redacted-or-empty-or-placeholder-or-missing"
  },
  "live_collector_started": false,
  "notifier_transport_enabled": false,
  "production_rollout_performed": false,
  "redis_connected": false,
  "runtime_env_path": "/etc/github-ai-catchbot/runtime.env",
  "runtime_env_read": true,
  "runtime_env_values_printed": false,
  "tdlib_auth_performed": false,
  "telegram_connected": false
}
```

The JSON must not include raw values, DB URLs, Redis URLs, phone numbers,
tokens, hashes, invite links, VPS IPs, or operator IPs.

## Rollback / recovery notes

If later approved execution changes the runtime secret file incorrectly:

- reopen the file with the approved editor path.
- remove only the incorrect Telegram key lines or restore the prior
  password-manager backed values.
- do not print the file while troubleshooting.
- do not start TDLib auth, Telegram connection, live collector, notifier
  transport, or production rollout as part of rollback.
- create a result record only after the approved operator execution completes.

If a Telegram value is exposed, rotate only the affected credential surface:

- exposed bot token: rotate through BotFather and keep collector credentials
  untouched unless separate evidence requires rotation.
- exposed reader account credential, phone number, 2FA password, or TDLib
  encryption key: recover the reader-account path and keep notifier bot
  credentials untouched unless separate evidence requires rotation.
- exposed invite link or delivery target: rotate that access path with the
  channel owner or administrator.

## Acceptance criteria

- No actual Telegram API ID, API hash, phone number, 2FA password, TDLib DB
  encryption key, bot token, chat ID, delivery target ID, invite link, runtime
  env content, DB URL, Redis URL, VPS IP, operator IP, or secret value appears
  in this package.
- The package keeps collector reader account credentials separate from notifier
  bot credentials.
- The package defines a future operator-safe edit path using an editor.
- The package defines redacted validation only.
- The package does not authorize TDLib auth, Telegram connection, live
  collector startup, notifier transport, app runtime startup, DB/Redis
  connection, Alembic, Docker/systemd changes, or production rollout.
- The package checker passes.
- Focused tests pass.

## Next bounded action

Proceed only in this order:

1. ChatGPT review.
2. Commit/push if approved.
3. VPS pull and repo-local validation.
4. Separate explicit approval to execute the operator update.
5. Create `dedicated_vps_telegram_runtime_secret_placement_result_record`.
6. Only after that consider a TDLib auth package.

Do not make TDLib auth, Telegram connection, live collector startup, notifier
transport, or production rollout the next immediate action.
