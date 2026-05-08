# Dedicated VPS redacted Alembic migration preflight

## Scope

This is a repo-local operator package for a future manual read-only Alembic
preflight on the dedicated VPS.

The operator is already logged into the dedicated VPS as `deploy` and is
positioned in the `github_ai_catchbot` repository checkout. Codex and reviewers
must not run the commands from this repository slice.

This package prepares only:

- repo-local Alembic asset checks.
- redacted runtime environment shape and safe-gate validation.
- a separately approved read-only `python -m alembic current` command template.

This package does not execute the preflight. It does not authorize Alembic
upgrade. It does not authorize Alembic stamp. It does not authorize Alembic
revision generation. It does not authorize app runtime, TDLib/Telegram, live
collector, notifier transport, or production rollout.

It does not authorize Docker, Docker Compose, systemd unit changes, repo `.env`
creation, or repo `env/*.env` creation.

## Source-of-truth handling

`docs/project-source/README_replacement_consolidated_v0_20.md` remains
authoritative. The locked project-source bundles `00~10` remain the
source-of-truth set and are interpreted in filename order. The GitHub AI
application plan remains advisory only.

Checked runbook path:

```text
ops/pipeline/runbooks/dedicated_vps_alembic_preflight.md
```

The canonical architecture invariant remains unchanged:

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

Source interpretation for this package:

- The current DB/Redis records select host apt/systemd PostgreSQL and host
  apt/systemd Redis for the immediate dedicated VPS state.
- Docker Compose remains a future full app stack candidate and is not
  discarded.
- The runtime secret placement record says `/etc/github-ai-catchbot/runtime.env`
  exists and validated with required key names present, optional keys absent,
  and secret values not printed.
- This package does not silently override source contracts; it only prepares the
  next narrow operator preflight after the recorded DB/Redis and runtime secret
  placement results.

## Current prerequisite state

The current recorded prerequisite state is:

- Dedicated VPS DB/Redis provisioning is complete and recorded.
- PostgreSQL and Redis are host apt/systemd services.
- PostgreSQL role exists: `github_ai_catchbot_app`.
- PostgreSQL database exists: `github_ai_catchbot`.
- Runtime secret placement is complete and recorded.
- `/etc/github-ai-catchbot/runtime.env` exists on the VPS.
- Runtime secret file owner/mode was recorded as `root:deploy 0640`.
- Runtime secret parent directory owner/mode was recorded as `root:deploy 0750`.
- Required runtime keys are present by name.
- Optional runtime keys are currently absent.
- Actual `DATABASE_URL` and DB password must never be printed.

No repo `.env`, Alembic execution, app runtime, TDLib/Telegram, live collector,
notifier transport, or production rollout has been authorized by those records.

## Allowed preflight checks

Allowed future operator preflight checks are limited to:

- Confirm repo path and HEAD.
- Confirm `alembic.ini` exists.
- Confirm `migrations` exists.
- Confirm `migrations/env.py` exists.
- Confirm `migrations/versions` exists.
- Print only migration filenames using `find migrations/versions`.
- Read `/etc/github-ai-catchbot/runtime.env` only inside a redacted Python
  helper.
- Validate required runtime key names are present.
- Validate `DATABASE_URL` is present and has this shape only:
  - starts with `postgresql+psycopg://github_ai_catchbot_app:`
  - contains `@127.0.0.1:5432/github_ai_catchbot`
- Validate `REDIS_URL=redis://127.0.0.1:6379/0`.
- Validate safe gates:
  - `ENABLE_NOTIFICATION_SEND=false`
  - `NOTIFIER_TELEGRAM_DRY_RUN=true`
  - `NOTIFIER_TELEGRAM_ALLOW_EDITS=false`
  - `ENABLE_REPLAY_TO_PROD_DB=false`
  - `MAINTENANCE_ENABLE_NOTIFICATION_RETRY_PROMOTION=false`
- Optionally, after separate approval only, run read-only
  `python -m alembic current` inside a redacted Python subprocess wrapper that
  passes `DATABASE_URL` only in the child process environment and does not print
  the value.

## Explicitly forbidden actions

- Do not `cat /etc/github-ai-catchbot/runtime.env`.
- Do not `source /etc/github-ai-catchbot/runtime.env`.
- Do not `. /etc/github-ai-catchbot/runtime.env`.
- Do not `export DATABASE_URL`.
- Do not `export REDIS_URL`.
- Do not print `DATABASE_URL`.
- Do not print any secret value.
- Do not run `alembic upgrade`.
- Do not run `alembic stamp`.
- Do not run `alembic revision`.
- Do not run app runtime.
- Do not run TDLib auth.
- Do not connect Telegram.
- Do not start live collector.
- Do not enable notifier transport.
- Do not perform production rollout.
- Do not use Docker or Docker Compose.
- Do not modify systemd units.
- Do not create repo `.env`.
- Do not create repo `env/*.env`.

## Operator command blocks

Run each block manually on the dedicated VPS only after this package has been
reviewed. Stop on the first unexpected result.

### Block 0: context confirmation

```bash
pwd
whoami
id
git status --short --branch
git log --oneline -5
test "$(whoami)" = "deploy"
```

Expected safe output:

- current user is `deploy`.
- repository path is the dedicated VPS checkout.
- branch is `main`.
- recent git log matches the reviewed state.
- no secret values are displayed.

### Block 1: repo-local Alembic asset check

This block prints only repo-local paths and migration filenames. It does not
print file contents.

```bash
test -f alembic.ini
test -d migrations
test -f migrations/env.py
test -d migrations/versions
find migrations/versions -maxdepth 1 -type f -name '*.py' -printf '%f\n' | sort
```

Expected safe output:

- `alembic.ini` exists.
- `migrations` exists.
- `migrations/env.py` exists.
- `migrations/versions` exists.
- migration filenames are printed, not file contents.
- no DB/Redis connection is made.
- no Alembic command is run.

### Block 2: redacted runtime env shape and gate validation

This block reads `/etc/github-ai-catchbot/runtime.env` only inside the helper.
It prints only key presence, shape labels, and boolean statuses. It never prints
values.

```bash
python3 - <<'PY'
from pathlib import Path
import sys

runtime_env = Path("/etc/github-ai-catchbot/runtime.env")
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
expected = {
    "APP_ENV": "prod",
    "REDIS_URL": "redis://127.0.0.1:6379/0",
    "ENABLE_NOTIFICATION_SEND": "false",
    "NOTIFIER_TELEGRAM_DRY_RUN": "true",
    "NOTIFIER_TELEGRAM_ALLOW_EDITS": "false",
    "ENABLE_REPLAY_TO_PROD_DB": "false",
    "MAINTENANCE_ENABLE_NOTIFICATION_RETRY_PROMOTION": "false",
}


def fail(message: str) -> None:
    print(f"FAIL: {message}")
    sys.exit(1)


if not runtime_env.is_file():
    fail("runtime.env missing")

parsed = {}
for raw_line in runtime_env.read_text(encoding="utf-8").splitlines():
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
    if "<" in value and ">" in value:
        fail("placeholder value remains")
    parsed[key] = value

missing = sorted(required - parsed.keys())
if missing:
    fail("missing required keys: " + ",".join(missing))

for key, value in expected.items():
    if parsed.get(key) != value:
        fail(f"{key} has unexpected redacted-safe gate value")

database_url = parsed["DATABASE_URL"]
if not database_url.startswith("postgresql+psycopg://github_ai_catchbot_app:"):
    fail("DATABASE_URL shape mismatch")
if "@127.0.0.1:5432/github_ai_catchbot" not in database_url:
    fail("DATABASE_URL shape mismatch")

print("PASS: runtime.env required keys, DATABASE_URL shape, Redis URL, and disabled gates validated")
print("runtime_env_path=/etc/github-ai-catchbot/runtime.env")
print("required_keys_present=" + ",".join(sorted(required)))
optional_present = sorted(key for key in optional if key in parsed)
print("optional_keys_present=" + (",".join(optional_present) if optional_present else "none"))
print("database_url_shape_valid=true")
print("REDIS_URL=redis://127.0.0.1:6379/0")
print("ENABLE_NOTIFICATION_SEND=false")
print("NOTIFIER_TELEGRAM_DRY_RUN=true")
print("NOTIFIER_TELEGRAM_ALLOW_EDITS=false")
print("ENABLE_REPLAY_TO_PROD_DB=false")
print("MAINTENANCE_ENABLE_NOTIFICATION_RETRY_PROMOTION=false")
print("secret_values_printed=false")
print("database_url_printed=false")
print("db_connection_performed=false")
print("redis_connection_performed=false")
print("alembic_current_performed=false")
print("alembic_upgrade_run=false")
print("alembic_stamp_run=false")
print("alembic_revision_run=false")
print("app_runtime_started=false")
print("tdlib_auth_performed=false")
print("telegram_connected=false")
print("live_collector_started=false")
print("notifier_transport_enabled=false")
print("production_rollout_performed=false")
PY
```

The helper must not connect to DB/Redis, run Alembic, import
`psycopg`/`redis`/`requests`/`http`/`socket`, or print `DATABASE_URL`.

### Block 3: future read-only Alembic current preflight command template

Run only after separate approval. Do not run during package implementation.
Read-only Alembic current only. No upgrade/stamp/revision.

This block is a template for a future manual preflight. It must not be run by
Codex in this slice.

```bash
python3 - <<'PY'
from pathlib import Path
import re
import subprocess
import sys

runtime_env = Path("/etc/github-ai-catchbot/runtime.env")


def fail(message: str) -> None:
    print(f"FAIL: {message}")
    sys.exit(1)


parsed = {}
for raw_line in runtime_env.read_text(encoding="utf-8").splitlines():
    line = raw_line.strip()
    if not line or line.startswith("#"):
        continue
    if "=" not in line:
        fail("non-comment line without key/value separator")
    key, value = line.split("=", 1)
    parsed[key.strip()] = value.strip()

database_url = parsed.get("DATABASE_URL", "")
if not database_url.startswith("postgresql+psycopg://github_ai_catchbot_app:"):
    fail("DATABASE_URL shape mismatch")
if "@127.0.0.1:5432/github_ai_catchbot" not in database_url:
    fail("DATABASE_URL shape mismatch")

child_env = {"DATABASE_URL": database_url}
result = subprocess.run(
    [sys.executable, "-m", "alembic", "current"],
    check=False,
    capture_output=True,
    text=True,
    env=child_env,
    timeout=30,
)
combined = (result.stdout or "") + (result.stderr or "")
database_url_printed = database_url in combined
redacted = combined.replace(database_url, "<DATABASE_URL_REDACTED>")
redacted = re.sub(
    r"postgresql\+psycopg://[^\\s]+@127\\.0\\.0\\.1:5432/github_ai_catchbot",
    "postgresql+psycopg://<REDACTED>@127.0.0.1:5432/github_ai_catchbot",
    redacted,
)
if database_url_printed:
    fail("DATABASE_URL appeared in Alembic output")

print(f"alembic_current_exit_code={result.returncode}")
print("alembic_current_output_redacted=" + repr(redacted))
print("database_url_printed=false")
print("alembic_upgrade_run=false")
print("alembic_stamp_run=false")
print("alembic_revision_run=false")
PY
```

Do not document direct shell `DATABASE_URL=... alembic current`. Do not
document `export DATABASE_URL`. Do not document `source runtime.env`. Do not
document `cat runtime.env`.

## Redacted validation rules

Allowed redacted validation output:

- repo alembic assets present: PASS/FAIL.
- runtime env required keys present: PASS/FAIL.
- database URL shape valid: PASS/FAIL.
- `secret_values_printed=false`.
- secret values printed: NO.
- DB connection performed during Block 2: NO.
- Alembic current performed: NO unless separately approved later.
- Do not perform Alembic upgrade/stamp/revision.
- app runtime started: NO.
- TDLib/Telegram/live collector/notifier/production rollout performed: NO.

Forbidden validation output:

- raw `DATABASE_URL`.
- DB password.
- credential-bearing `REDIS_URL`.
- OpenAI, Telegram, GitHub, X, TDLib, PostgreSQL, or Redis secret values.
- copied contents of `/etc/github-ai-catchbot/runtime.env`.
- repo `.env` contents.
- repo `env/*.env` contents.

## Failure handling

If any block reports `FAIL`, stop. Do not continue into Alembic current unless
that exact read-only command has already been separately approved.

If Block 1 fails, bring back only the missing repo-local asset name. Do not
print migration file contents.

If Block 2 fails, bring back only the failed check name, required key-name
status, and redacted boolean output. Do not paste secret values into ChatGPT.

Failure is not permission to run upgrade/stamp/revision or create repo `.env`.
Do not modify migrations, start app runtime, perform TDLib auth, or connect
Telegram.

Do not start live collector, enable notifier transport, or run Docker/Docker
Compose.

Do not modify systemd units or perform production rollout.

## What output to bring back to ChatGPT

Bring back only:

- Block 0: non-secret context summary, branch, and short commit lines.
- Block 1: PASS/FAIL summary and migration filenames only.
- Block 2: PASS/FAIL line, key-name presence, shape labels, and boolean
  statuses.
- Block 3, only if separately approved later: `alembic_current_exit_code=<int>`,
  `alembic_current_output_redacted=<safe text>`, `database_url_printed=false`,
  `alembic_upgrade_run=false`, `alembic_stamp_run=false`, and
  `alembic_revision_run=false`.

Do not bring back:

- contents of `/etc/github-ai-catchbot/runtime.env`.
- `DATABASE_URL`.
- DB password.
- any credential-bearing `REDIS_URL`.
- OpenAI, Telegram, GitHub, X, or TDLib secrets.
- repo `.env` or repo `env/*.env` contents.
- raw server IP/operator IP/SSH private key path.

## What remains unauthorized

After this package, the following remain unauthorized until separately
approved:

- SSH or VPS command execution by Codex.
- reading or writing secret files by Codex.
- repo `.env` creation.
- repo `env/*.env` creation.
- secret printing.
- direct shell runtime env sourcing, dot-sourcing, or export.
- PostgreSQL connection by Codex.
- Redis connection by Codex.
- Alembic current execution by Codex.
- Alembic upgrade remains unauthorized.
- Alembic stamp remains unauthorized.
- Alembic revision remains unauthorized.
- app runtime startup remains unauthorized.
- TDLib auth remains unauthorized.
- Telegram connection remains unauthorized.
- live collector startup remains unauthorized.
- notifier transport remains unauthorized.
- production rollout remains unauthorized.
- Docker or Docker Compose execution.
- systemd unit modification remains unauthorized.
- applying `recommended_flag_patch`.

## Next step

Review this runbook, checker, and tests with ChatGPT. If accepted, commit this
package. The next operational action is a separately approved manual redacted
Alembic current preflight on the dedicated VPS, run by the operator only.
