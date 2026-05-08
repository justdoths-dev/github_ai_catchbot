# Dedicated VPS runtime secret placement

## Scope

This is a concrete operator package for placing the first runtime secret file
on the dedicated VPS after PostgreSQL and Redis provisioning has completed.

The operator runs these commands manually on the dedicated VPS while already
logged in as `deploy` and positioned in the `github_ai_catchbot` repository
checkout. Codex and reviewers must not run the commands from this repository
slice.

This slice only authorizes creation and redacted validation of one host-local
runtime secret file:

```text
/etc/github-ai-catchbot/runtime.env
```

This slice does not authorize SSH, VPS command execution by Codex, repo `.env`
creation, repo `env/*.env` creation, secret printing, PostgreSQL connections,
Redis connections, Alembic, app runtime startup, TDLib auth, Telegram
connection, live collector startup, notifier transport, production rollout,
Docker, Docker Compose, or systemd unit modification.

## Source-of-truth handling

`docs/project-source/README_replacement_consolidated_v0_20.md` remains
authoritative. The locked project-source bundles `00~10` remain the
source-of-truth set and are interpreted in filename order. The GitHub AI
application plan remains advisory only.

This package preserves the canonical architecture invariant:

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

## Current prerequisite state

The dedicated VPS DB/Redis provisioning result record says the following state
has already been reached:

- PostgreSQL and Redis provisioning on the dedicated VPS is complete.
- PostgreSQL and Redis are host apt/systemd services.
- PostgreSQL app role exists: `github_ai_catchbot_app`.
- PostgreSQL database exists: `github_ai_catchbot`.
- PostgreSQL password was set interactively and must not be printed or stored
  in the repository.
- PostgreSQL listens locally only.
- Redis listens locally only.
- UFW is active with no DB/Redis public allow rule.

This package is the next boundary asset before any later Alembic preflight or
runtime environment consumer can safely read production DB/Redis runtime
configuration.

## Secret storage decision

Use a host-local runtime secret file outside the repository.

Do not create repo `.env`. Do not create `env/*.env` under the repository. Do
not write secrets into git-tracked files. Do not paste secrets into ChatGPT.

This decision is a minimal-change interpretation for the current dedicated VPS
state: DB/Redis are host apt/systemd services, while full app-stack Docker
Compose remains future deployment scope and is not authorized by this slice.

## Allowed secret file path

Allowed runtime secret file:

```text
/etc/github-ai-catchbot/runtime.env
```

Required ownership and permissions:

```text
/etc/github-ai-catchbot              root:deploy 0750
/etc/github-ai-catchbot/runtime.env  root:deploy 0640
```

No other runtime secret file path is authorized by this package.

## Allowed keys

Required keys for this first runtime secret placement package:

```text
APP_ENV=prod
DATABASE_URL=<secret value, not printed>
REDIS_URL=redis://127.0.0.1:6379/0
ENABLE_NOTIFICATION_SEND=false
NOTIFIER_TELEGRAM_DRY_RUN=true
NOTIFIER_TELEGRAM_ALLOW_EDITS=false
ENABLE_REPLAY_TO_PROD_DB=false
MAINTENANCE_ENABLE_NOTIFICATION_RETRY_PROMOTION=false
```

Optional keys are allowed only if already explicitly available to the operator
and not printed:

```text
OPENAI_API_KEY=<secret value, not printed>
OPENAI_PROJECT=<non-secret or secret-adjacent value, not printed if present>
TELEGRAM_BOT_TOKEN=<secret value, not printed>
GITHUB_APP_ID=<non-secret value>
GITHUB_INSTALLATION_ID=<non-secret value>
GITHUB_PRIVATE_KEY=<secret value, not printed>
X_BEARER_TOKEN=<secret value, not printed>
TELEGRAM_API_ID=<secret or sensitive value, not printed>
TELEGRAM_API_HASH=<secret value, not printed>
TELEGRAM_PHONE_NUMBER=<sensitive value, not printed>
TELEGRAM_2FA_PASSWORD=<secret value, not printed>
TDLIB_DB_ENCRYPTION_KEY=<secret value, not printed>
TDLIB_STATE_DIR=<path, no raw secret>
TDLIB_FILES_DIR=<path, no raw secret>
```

Optional collector, judge, GitHub, X, TDLib, and notifier secrets may be left
absent until their respective runtime slices. This package must at minimum
place the DB/Redis runtime secret boundary safely enough for a future separately
approved Alembic preflight.

## Explicitly forbidden keys/actions

Forbidden keys in `/etc/github-ai-catchbot/runtime.env` for this slice:

- `ENABLE_NOTIFICATION_SEND=true`
- `NOTIFIER_TELEGRAM_DRY_RUN=false`
- `NOTIFIER_TELEGRAM_ALLOW_EDITS=true`
- `ENABLE_REPLAY_TO_PROD_DB=true`
- `MAINTENANCE_ENABLE_NOTIFICATION_RETRY_PROMOTION=true`
- any key not listed in the allowed required or optional key lists above.

Forbidden actions:

- Do not create repo `.env`.
- Do not create `env/*.env` under the repository.
- Do not `cat /etc/github-ai-catchbot/runtime.env`.
- Do not `source /etc/github-ai-catchbot/runtime.env`.
- Do not `. /etc/github-ai-catchbot/runtime.env`.
- Do not `export DATABASE_URL`.
- Do not `export REDIS_URL`.
- Do not print `DATABASE_URL`.
- Do not print `REDIS_URL` if it ever contains credentials.
- Do not print any secret values.
- Do not paste secrets into ChatGPT.
- Do not print or paste the contents of `/etc/github-ai-catchbot/runtime.env`.
- Do not write secrets into git-tracked files.
- Do not run Alembic.
- Do not start app runtime.
- Do not run TDLib auth.
- Do not connect Telegram.
- Do not start live collector.
- Do not enable notifier transport.
- Do not perform production rollout.
- Do not use Docker or Docker Compose.
- Do not modify systemd units in this slice.
- Do not connect to PostgreSQL.
- Do not connect to Redis.

## Operator command blocks

Run each block manually on the dedicated VPS. Stop on the first unexpected
result.

### Block 0: context confirmation

```bash
pwd
whoami
id
git status --short --branch
git log --oneline -3
test "$(whoami)" = "deploy"
```

Expected safe output:

- current user is `deploy`.
- repository path is the dedicated VPS checkout.
- branch is `main`.
- recent git log matches the reviewed state.
- no secret values are displayed.

### Block 1: create secret directory and file safely

```bash
sudo install -d -o root -g deploy -m 0750 /etc/github-ai-catchbot
sudo install -o root -g deploy -m 0640 /dev/null /etc/github-ai-catchbot/runtime.env
```

Expected safe output:

- directory exists as `root:deploy` with mode `0750`.
- file exists as `root:deploy` with mode `0640`.
- no secret value appears on the command line.
- no repo `.env` or repo `env/*.env` is created.

### Block 2: edit secret file safely

Prefer an editor command so secret values are typed inside the editor and do
not appear in shell history or command-line arguments:

```bash
sudoedit /etc/github-ai-catchbot/runtime.env
```

Paste this template into the editor, then replace
`<DB_PASSWORD_FROM_PASSWORD_MANAGER>` inside the editor only. Do not paste the
password into ChatGPT. Do not print the full file. Do not place the file under
the repository.

```dotenv
APP_ENV=prod
DATABASE_URL=postgresql+psycopg://github_ai_catchbot_app:<DB_PASSWORD_FROM_PASSWORD_MANAGER>@127.0.0.1:5432/github_ai_catchbot
REDIS_URL=redis://127.0.0.1:6379/0
ENABLE_NOTIFICATION_SEND=false
NOTIFIER_TELEGRAM_DRY_RUN=true
NOTIFIER_TELEGRAM_ALLOW_EDITS=false
ENABLE_REPLAY_TO_PROD_DB=false
MAINTENANCE_ENABLE_NOTIFICATION_RETRY_PROMOTION=false

# Optional only if explicitly available now; leave absent otherwise.
# OPENAI_API_KEY=<replace inside editor; do not print>
# OPENAI_PROJECT=<replace inside editor if present; do not print if secret-adjacent>
# TELEGRAM_BOT_TOKEN=<replace inside editor; do not print>
# GITHUB_APP_ID=<replace inside editor if present>
# GITHUB_INSTALLATION_ID=<replace inside editor if present>
# GITHUB_PRIVATE_KEY=<replace inside editor; do not print>
# X_BEARER_TOKEN=<replace inside editor; do not print>
# TELEGRAM_API_ID=<replace inside editor; do not print>
# TELEGRAM_API_HASH=<replace inside editor; do not print>
# TELEGRAM_PHONE_NUMBER=<replace inside editor; do not print>
# TELEGRAM_2FA_PASSWORD=<replace inside editor; do not print>
# TDLIB_DB_ENCRYPTION_KEY=<replace inside editor; do not print>
# TDLIB_STATE_DIR=<path only; no raw secret>
# TDLIB_FILES_DIR=<path only; no raw secret>
```

Expected safe result:

- required keys are present.
- optional keys may be absent.
- production delivery/replay/promotion gates remain disabled.
- no secret value is printed.
- no secret value is stored in the repository.

### Block 3: redacted validation only

This block validates path, ownership, permissions, allowed key names, required
key presence, and disabled gate values. It reads the local secret file only on
the VPS and prints key names/status only, never values.

```bash
sudo python3 - <<'PY'
from pathlib import Path
import grp
import os
import pwd
import stat
import sys

secret_dir = Path("/etc/github-ai-catchbot")
secret_file = secret_dir / "runtime.env"
required = {
    "APP_ENV",
    "DATABASE_URL",
    "REDIS_URL",
    "ENABLE_NOTIFICATION_SEND",
    "NOTIFIER_TELEGRAM_DRY_RUN",
    "NOTIFIER_TELEGRAM_ALLOW_EDITS",
    "ENABLE_REPLAY_TO_PROD_DB",
    "MAINTENANCE_ENABLE_NOTIFICATION_RETRY_PROMOTION",
}
optional = {
    "OPENAI_API_KEY",
    "OPENAI_PROJECT",
    "TELEGRAM_BOT_TOKEN",
    "GITHUB_APP_ID",
    "GITHUB_INSTALLATION_ID",
    "GITHUB_PRIVATE_KEY",
    "X_BEARER_TOKEN",
    "TELEGRAM_API_ID",
    "TELEGRAM_API_HASH",
    "TELEGRAM_PHONE_NUMBER",
    "TELEGRAM_2FA_PASSWORD",
    "TDLIB_DB_ENCRYPTION_KEY",
    "TDLIB_STATE_DIR",
    "TDLIB_FILES_DIR",
}
expected_values = {
    "APP_ENV": "prod",
    "REDIS_URL": "redis://127.0.0.1:6379/0",
    "ENABLE_NOTIFICATION_SEND": "false",
    "NOTIFIER_TELEGRAM_DRY_RUN": "true",
    "NOTIFIER_TELEGRAM_ALLOW_EDITS": "false",
    "ENABLE_REPLAY_TO_PROD_DB": "false",
    "MAINTENANCE_ENABLE_NOTIFICATION_RETRY_PROMOTION": "false",
}


def owner_group_mode(path: Path) -> tuple[str, str, str]:
    info = path.stat()
    owner = pwd.getpwuid(info.st_uid).pw_name
    group = grp.getgrgid(info.st_gid).gr_name
    mode = stat.filemode(info.st_mode)[-3:]
    return owner, group, mode


def fail(message: str) -> None:
    print(f"FAIL: {message}")
    sys.exit(1)


if not secret_dir.is_dir():
    fail("secret directory missing")
if not secret_file.is_file():
    fail("runtime.env missing")

dir_owner, dir_group, dir_mode = owner_group_mode(secret_dir)
file_owner, file_group, file_mode = owner_group_mode(secret_file)
if (dir_owner, dir_group, dir_mode) != ("root", "deploy", "750"):
    fail("secret directory ownership or mode mismatch")
if (file_owner, file_group, file_mode) != ("root", "deploy", "640"):
    fail("runtime.env ownership or mode mismatch")

parsed = {}
for raw_line in secret_file.read_text(encoding="utf-8").splitlines():
    line = raw_line.strip()
    if not line or line.startswith("#"):
        continue
    if "=" not in line:
        fail("non-comment line without key/value separator")
    key, value = line.split("=", 1)
    key = key.strip()
    value = value.strip()
    if key not in required | optional:
        fail(f"unauthorized key present: {key}")
    if key in parsed:
        fail(f"duplicate key present: {key}")
    parsed[key] = value

missing = sorted(required - parsed.keys())
if missing:
    fail("missing required keys: " + ",".join(missing))

for key, expected in expected_values.items():
    if parsed.get(key) != expected:
        fail(f"{key} has unexpected redacted-safe gate value")

if not parsed.get("DATABASE_URL"):
    fail("DATABASE_URL is empty")
for key, value in parsed.items():
    if "<DB_PASSWORD_FROM_PASSWORD_MANAGER>" in value:
        if key == "DATABASE_URL":
            fail("DATABASE_URL placeholder remains")
        fail("placeholder value remains")
    if "<replace inside editor; do not print>" in value:
        fail("placeholder value remains")
    if "<" in value and ">" in value:
        fail("placeholder value remains")
database_url = parsed["DATABASE_URL"]
if not database_url.startswith("postgresql+psycopg://github_ai_catchbot_app:"):
    fail("DATABASE_URL shape mismatch")
if "@127.0.0.1:5432/github_ai_catchbot" not in database_url:
    fail("DATABASE_URL shape mismatch")

print("PASS: runtime.env path, ownership, permissions, allowed keys, and disabled gates validated")
print("runtime_env_path=/etc/github-ai-catchbot/runtime.env")
print("runtime_env_owner_group_mode=root:deploy 0640")
print("runtime_env_parent_owner_group_mode=root:deploy 0750")
print("required_keys_present=" + ",".join(sorted(required)))
optional_present = sorted(key for key in optional if key in parsed)
print("optional_keys_present=" + (",".join(optional_present) if optional_present else "none"))
print("secret_values_printed=false")
print("database_connected=false")
print("redis_connected=false")
print("alembic_run=false")
print("app_runtime_started=false")
print("tdlib_auth_performed=false")
print("telegram_connected=false")
print("live_collector_started=false")
print("notifier_transport_enabled=false")
PY
```

Expected safe output:

- PASS line is present.
- path and permission facts are printed.
- allowed key names and optional key names may be printed.
- no secret values are printed.
- no PostgreSQL or Redis connection is made.
- no Alembic or app runtime is run.

## Redacted validation

Allowed redacted validation:

- file path existence.
- parent directory ownership and mode.
- file ownership and mode.
- allowed key-name validation.
- required key-name presence validation.
- placeholder failure validation without printing values.
- `DATABASE_URL` prefix and loopback host/database suffix shape validation
  without printing the value.
- fixed non-secret gate values.
- optional key-name presence only.
- explicit booleans showing no DB connection, no Redis connection, no Alembic,
  no runtime startup, no TDLib auth, no Telegram connection, no live collector,
  no notifier transport, and no production rollout.

Forbidden validation output:

- raw `DATABASE_URL`.
- any credential-bearing `REDIS_URL`.
- OpenAI, Telegram, GitHub, X, TDLib, PostgreSQL, or Redis secret values.
- copied contents of `/etc/github-ai-catchbot/runtime.env`.
- Do not print command output from `cat /etc/github-ai-catchbot/runtime.env`.
- Do not print values loaded by `source /etc/github-ai-catchbot/runtime.env`.
- Do not print values injected by `export DATABASE_URL` or `export REDIS_URL`.
- repo `.env` contents.

## Failure handling

If any command fails or the validation reports `FAIL`, stop.

Do not fall forward into Alembic, app runtime, TDLib auth, Telegram connection,
live collector startup, notifier transport, production rollout, Docker, Docker
Compose, or systemd unit modification.

Record only the failed check name, non-secret path facts, and redacted status.
Do not paste secret values into ChatGPT.

## Rollback/cleanup

If the file was created with wrong ownership or permissions and no future
runtime slice has consumed it yet, the operator may fix ownership and
permissions with:

```bash
sudo chown root:deploy /etc/github-ai-catchbot /etc/github-ai-catchbot/runtime.env
sudo chmod 0750 /etc/github-ai-catchbot
sudo chmod 0640 /etc/github-ai-catchbot/runtime.env
```

If an unauthorized key or wrong gate value was placed, reopen the file with:

```bash
sudoedit /etc/github-ai-catchbot/runtime.env
```

If cleanup is required before any runtime consumer has used the file, remove
only the authorized runtime secret file after separately confirming that no
secret value will be printed:

```bash
sudo rm -f /etc/github-ai-catchbot/runtime.env
```

Do not delete PostgreSQL data, Redis state, TDLib state, repository files, or
systemd units in this slice.

## What output to bring back to ChatGPT

Bring back only redacted output:

- Block 0: non-secret context summary, branch, and short commit lines.
- Block 1: directory/file creation success.
- Block 3: PASS/FAIL line and redacted validation lines.
- If failed: failed check name and non-secret path facts only.

Do not bring back:

- contents of `/etc/github-ai-catchbot/runtime.env`.
- `DATABASE_URL`.
- any credential-bearing `REDIS_URL`.
- PostgreSQL password.
- OpenAI, Telegram, GitHub, X, or TDLib secrets.
- raw server IP/operator IP/SSH private key path.
- repo `.env` or `env/*.env` contents.

## What remains unauthorized

After this secret placement package, the following remain unauthorized until
separately approved:

- SSH or VPS command execution by Codex.
- repo `.env` creation.
- repo `env/*.env` creation.
- secret printing.
- PostgreSQL connection.
- Redis connection.
- Alembic.
- app runtime startup.
- TDLib auth.
- Telegram connection.
- live collector startup.
- notifier transport.
- production rollout.
- Docker or Docker Compose execution.
- systemd unit modification.
- applying `recommended_flag_patch`.

## Next step

After this package is reviewed and the operator has completed it manually, the
next eligible slice is a separately approved redacted Alembic preflight package
or runtime environment consumer preflight that may read
`/etc/github-ai-catchbot/runtime.env` without printing values.

This runbook does not authorize that next slice by itself.
