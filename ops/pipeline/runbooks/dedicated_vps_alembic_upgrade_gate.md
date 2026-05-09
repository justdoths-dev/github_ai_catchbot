# Dedicated VPS Alembic upgrade execution gate

## Scope

This is a repo-local operator package for a future manual Alembic
`upgrade head` execution gate on the dedicated VPS.

The operator is already logged into the dedicated VPS as `deploy` and is
positioned in the `github_ai_catchbot` repository checkout. Codex and reviewers
must not run the commands from this repository slice.

This package prepares only:

- repo-local Alembic asset checks.
- redacted runtime environment shape and safe-gate validation.
- a read-only pre-upgrade `python -m alembic current` template for the later
  separately approved upgrade execution.
- an explicit approval checkpoint before any mutation.
- a future `python -m alembic upgrade head` execution template that may be run
  only after explicit user approval.
- a read-only post-upgrade `python -m alembic current` template.

This package does not execute the upgrade. It does not authorize the operator
to run the upgrade until the user separately approves Alembic upgrade execution
after this package review.

It does not authorize Alembic downgrade, Alembic stamp, Alembic revision
generation, app runtime, TDLib/Telegram, live collector, notifier transport, or
production rollout.

It does not authorize Docker, Docker Compose, systemd unit changes, repo `.env`
creation, repo `env/*.env` creation, or migration file editing in this slice.

## Source-of-truth handling

`docs/project-source/README_replacement_consolidated_v0_20.md` remains
authoritative. The locked project-source bundles `00~10` remain the
source-of-truth set and are interpreted in filename order. The GitHub AI
application plan remains advisory only.

Checked runbook path:

```text
ops/pipeline/runbooks/dedicated_vps_alembic_upgrade_gate.md
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

- The DB/Redis result record selects host apt/systemd PostgreSQL and host
  apt/systemd Redis for the immediate dedicated VPS state.
- Docker Compose remains a future full app stack candidate and is not
  discarded.
- The runtime secret placement result record says
  `/etc/github-ai-catchbot/runtime.env` exists and validated with required key
  names present, optional keys absent, and secret values not printed.
- The Alembic current preflight result record says the redacted manual current
  preflight completed successfully and is committed at
  `1daa7ed docs(ops): record Alembic current preflight result`.
- This package does not silently override source contracts; it only prepares
  the next narrow operator gate after the recorded DB/Redis, runtime secret,
  and Alembic current preflight results.

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
- Manual redacted Alembic current preflight completed successfully.
- Alembic current result record is committed at
  `1daa7ed docs(ops): record Alembic current preflight result`.
- Manual current preflight evidence recorded:
  - `alembic_current_exit_code=0`
  - `database_url_printed=false`
  - `alembic_upgrade_run=false`
  - `alembic_stamp_run=false`
  - `alembic_revision_run=false`
- Alembic asset check passed with four migration files:
  - `0001_ingest_core.py`
  - `0002_normalization_candidates.py`
  - `0003_enrichment_bundles.py`
  - `0004_judge_delivery_observability.py`
- Duplicate Alembic log lines were observed and recorded as non-blocking.

No app runtime, TDLib/Telegram, live collector, notifier transport, production
rollout, Docker, Docker Compose, or systemd unit modification has been
authorized by those records.

## Allowed upgrade gate checks

Allowed future operator upgrade gate checks are limited to:

- Confirm repo path and HEAD.
- Confirm `alembic.ini` exists.
- Confirm `migrations` exists.
- Confirm `migrations/env.py` exists.
- Confirm `migrations/versions` exists.
- Print only migration filenames using `find migrations/versions`.
- Read `/etc/github-ai-catchbot/runtime.env` only inside a redacted Python
  helper.
- Validate required runtime key names are present.
- Validate that placeholders are absent.
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
- Run read-only `python -m alembic current` immediately before upgrade, inside
  a redacted Python subprocess wrapper, only as part of the later separately
  approved upgrade execution.
- Stop for an explicit user approval checkpoint before the mutation step.
- After separate approval only, run exactly `python -m alembic upgrade head`
  inside a redacted Python subprocess wrapper.
- After upgrade, run read-only `python -m alembic current` again inside the
  same redaction pattern.
- Capture only redacted outputs:
  - pre-upgrade current exit code/output
  - upgrade exit code/output
  - post-upgrade current exit code/output
  - safety booleans

## Explicitly forbidden actions

- Do not `cat /etc/github-ai-catchbot/runtime.env`.
- Do not `source /etc/github-ai-catchbot/runtime.env`.
- Do not `. /etc/github-ai-catchbot/runtime.env`.
- Do not `export DATABASE_URL`.
- Do not `export REDIS_URL`.
- Do not print `DATABASE_URL`.
- Do not print any secret value.
- Do not run direct shell `DATABASE_URL=... alembic upgrade head`.
- Do not run direct shell `DATABASE_URL=... alembic current`.
- Do not run `alembic downgrade`.
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
- Do not edit migration files in this slice.

## Operator command blocks

Run each block manually on the dedicated VPS only after this package has been
reviewed. Stop on the first unexpected result.

### Block 0: context confirmation

```bash
pwd
whoami
id
git status --short --branch
git log --oneline -7
test "$(whoami)" = "deploy"
```

Expected safe output:

- current user is `deploy`.
- repository path is the dedicated VPS checkout.
- branch is `main`.
- recent git log includes
  `1daa7ed docs(ops): record Alembic current preflight result`.
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

### Block 2: redacted runtime env shape/gate validation

This block reads `/etc/github-ai-catchbot/runtime.env` only inside the helper.
It prints only key presence, shape labels, and boolean statuses. It never prints
values. It does not connect to DB/Redis, does not run Alembic, and does not
import psycopg, redis, requests, http, socket, or urllib.

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
    if key in parsed:
        fail(f"duplicate key: {key}")
    parsed[key] = value.strip()

missing = sorted(required - parsed.keys())
if missing:
    fail("missing required keys: " + ",".join(missing))

unauthorized = sorted(set(parsed) - required - optional)
if unauthorized:
    fail("unauthorized keys present: " + ",".join(unauthorized))

for key, value in parsed.items():
    if "PLACEHOLDER" in value or value.startswith("<") or value.endswith(">"):
        fail(f"placeholder remains for key: {key}")

database_url = parsed["DATABASE_URL"]
database_url_shape_valid = (
    database_url.startswith("postgresql+psycopg://github_ai_catchbot_app:")
    and "@127.0.0.1:5432/github_ai_catchbot" in database_url
)
if not database_url_shape_valid:
    fail("DATABASE_URL shape mismatch")

for key, expected_value in expected.items():
    if parsed[key] != expected_value:
        fail(f"unexpected safe gate value: {key}")

print("runtime_env_required_keys_present=PASS")
print("runtime_env_optional_keys_policy=PASS")
print("runtime_env_unauthorized_keys_absent=PASS")
print("runtime_env_placeholders_absent=PASS")
print("database_url_shape_valid=PASS")
print("redis_url_shape_valid=PASS")
print("safe_gates_disabled=PASS")
print("secret_values_printed=false")
print("database_url_printed=false")
print("db_connection_performed=false")
print("redis_connection_performed=false")
print("alembic_execution_performed=false")
PY
```

### Block 3: pre-upgrade read-only Alembic current

Run only as part of the separately approved upgrade execution later. This block
does not mutate schema.

It reads runtime.env inside Python, sets `DATABASE_URL` only in the child
process environment, runs `[sys.executable, "-m", "alembic", "current"]`,
captures stdout/stderr, redacts the exact `DATABASE_URL`, redacts URL-shaped
PostgreSQL strings, and fails without printing raw output if URL redaction
fails.

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


def parse_runtime_env() -> dict[str, str]:
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
        parsed[key.strip()] = value.strip()
    return parsed


def redact_output(text: str, database_url: str) -> str:
    redacted = text.replace(database_url, "<REDACTED_DATABASE_URL>")
    redacted = re.sub(
        r"postgresql(?:\+psycopg)?://[^\s`'\"<>]+",
        "<REDACTED_POSTGRESQL_URL>",
        redacted,
    )
    if database_url in redacted or re.search(r"postgresql(?:\+psycopg)?://[^\s`'\"<>]+", redacted):
        fail("unsafe Alembic output redaction")
    return redacted.strip().replace("\n", "\\n")


parsed = parse_runtime_env()
database_url = parsed.get("DATABASE_URL", "")
if not (
    database_url.startswith("postgresql+psycopg://github_ai_catchbot_app:")
    and "@127.0.0.1:5432/github_ai_catchbot" in database_url
):
    fail("DATABASE_URL shape mismatch")
if "PLACEHOLDER" in database_url or database_url.startswith("<") or database_url.endswith(">"):
    fail("DATABASE_URL placeholder remains")

completed = subprocess.run(
    [sys.executable, "-m", "alembic", "current"],
    check=False,
    capture_output=True,
    text=True,
    env={"DATABASE_URL": database_url},
)
combined = "\n".join(part for part in (completed.stdout, completed.stderr) if part)
print(f"pre_upgrade_alembic_current_exit_code={completed.returncode}")
print(f"pre_upgrade_alembic_current_output_redacted={redact_output(combined, database_url)}")
print("database_url_printed=false")
print("alembic_upgrade_run=false")
print("alembic_stamp_run=false")
print("alembic_revision_run=false")
if completed.returncode != 0:
    sys.exit(completed.returncode)
PY
```

### Block 4: explicit approval checkpoint

STOP: do not run Block 5 unless the user explicitly approves Alembic upgrade
execution now.

Block 5 mutates the PostgreSQL schema. If any production data exists and backup
state is uncertain, stop and ask for backup/rollback approval before continuing.
This package does not add backup commands.

### Block 5: Alembic upgrade head execution template

Run only after explicit user approval.

DB schema mutation. No stamp/revision/downgrade. No app runtime.

This block reads runtime.env inside Python, validates shape and placeholders,
creates a subprocess environment with `DATABASE_URL` only inside the child
process, runs `[sys.executable, "-m", "alembic", "upgrade", "head"]`, captures
stdout/stderr, redacts the exact `DATABASE_URL`, redacts URL-shaped PostgreSQL
strings, and fails without printing raw output if URL redaction fails.

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


def parse_runtime_env() -> dict[str, str]:
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
        parsed[key.strip()] = value.strip()
    return parsed


def redact_output(text: str, database_url: str) -> str:
    redacted = text.replace(database_url, "<REDACTED_DATABASE_URL>")
    redacted = re.sub(
        r"postgresql(?:\+psycopg)?://[^\s`'\"<>]+",
        "<REDACTED_POSTGRESQL_URL>",
        redacted,
    )
    if database_url in redacted or re.search(r"postgresql(?:\+psycopg)?://[^\s`'\"<>]+", redacted):
        fail("unsafe Alembic output redaction")
    return redacted.strip().replace("\n", "\\n")


parsed = parse_runtime_env()
database_url = parsed.get("DATABASE_URL", "")
if not (
    database_url.startswith("postgresql+psycopg://github_ai_catchbot_app:")
    and "@127.0.0.1:5432/github_ai_catchbot" in database_url
):
    fail("DATABASE_URL shape mismatch")
if "PLACEHOLDER" in database_url or database_url.startswith("<") or database_url.endswith(">"):
    fail("DATABASE_URL placeholder remains")

completed = subprocess.run(
    [sys.executable, "-m", "alembic", "upgrade", "head"],
    check=False,
    capture_output=True,
    text=True,
    env={"DATABASE_URL": database_url},
)
combined = "\n".join(part for part in (completed.stdout, completed.stderr) if part)
print(f"alembic_upgrade_exit_code={completed.returncode}")
print(f"alembic_upgrade_output_redacted={redact_output(combined, database_url)}")
print("database_url_printed=false")
print("alembic_upgrade_run=true")
print("alembic_stamp_run=false")
print("alembic_revision_run=false")
print("alembic_downgrade_run=false")
print("app_runtime_started=false")
print("live_collector_started=false")
print("notifier_transport_enabled=false")
print("production_rollout_performed=false")
if completed.returncode != 0:
    sys.exit(completed.returncode)
PY
```

The exact authorized Alembic mutation command after separate approval is
`python -m alembic upgrade head`. No direct shell `DATABASE_URL=... alembic
upgrade head` form is authorized.

### Block 6: post-upgrade read-only Alembic current

Run only if Block 5 completed with exit code 0.

This block runs read-only `python -m alembic current` in the same redacted
wrapper pattern. It does not start app runtime.

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


def parse_runtime_env() -> dict[str, str]:
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
        parsed[key.strip()] = value.strip()
    return parsed


def redact_output(text: str, database_url: str) -> str:
    redacted = text.replace(database_url, "<REDACTED_DATABASE_URL>")
    redacted = re.sub(
        r"postgresql(?:\+psycopg)?://[^\s`'\"<>]+",
        "<REDACTED_POSTGRESQL_URL>",
        redacted,
    )
    if database_url in redacted or re.search(r"postgresql(?:\+psycopg)?://[^\s`'\"<>]+", redacted):
        fail("unsafe Alembic output redaction")
    return redacted.strip().replace("\n", "\\n")


parsed = parse_runtime_env()
database_url = parsed.get("DATABASE_URL", "")
if not (
    database_url.startswith("postgresql+psycopg://github_ai_catchbot_app:")
    and "@127.0.0.1:5432/github_ai_catchbot" in database_url
):
    fail("DATABASE_URL shape mismatch")

completed = subprocess.run(
    [sys.executable, "-m", "alembic", "current"],
    check=False,
    capture_output=True,
    text=True,
    env={"DATABASE_URL": database_url},
)
combined = "\n".join(part for part in (completed.stdout, completed.stderr) if part)
print(f"post_upgrade_alembic_current_exit_code={completed.returncode}")
print(f"post_upgrade_alembic_current_output_redacted={redact_output(combined, database_url)}")
print("database_url_printed=false")
print("alembic_stamp_run=false")
print("alembic_revision_run=false")
print("alembic_downgrade_run=false")
print("app_runtime_started=false")
if completed.returncode != 0:
    sys.exit(completed.returncode)
PY
```

## Redacted validation rules

The operator may bring back only the following redacted summary fields:

```text
repo alembic assets present: PASS/FAIL
runtime env required keys present: PASS/FAIL
database URL shape valid: PASS/FAIL
secret values printed: NO
pre-upgrade current performed: YES/NO
upgrade performed: YES/NO
post-upgrade current performed: YES/NO
Alembic stamp/revision/downgrade performed: NO
app runtime started: NO
TDLib/Telegram/live collector/notifier/production rollout performed: NO
```

No raw `DATABASE_URL`, DB password, credential-bearing URL, token, private key,
runtime.env contents, public server IP, operator IP, or SSH key path may be
returned.

## Failure handling

If any block fails:

- stop immediately.
- do not run later blocks.
- do not try a different Alembic command.
- do not run downgrade, stamp, or revision.
- do not start app runtime as a workaround.
- do not enable notifier transport.
- bring back only the failed block name, exit code if available, and redacted
  output.

If Block 5 fails after separate approval:

- do not run Block 6 unless the failure output clearly indicates no mutation
  occurred and the user explicitly asks for another read-only current check.
- do not attempt rollback, downgrade, stamp, or manual schema edits without
  separate approval.
- preserve the redacted `alembic_upgrade_exit_code=<int>` and
  `alembic_upgrade_output_redacted=<safe text>` evidence.

## What output to bring back to ChatGPT

Bring back only:

- Block 0 context summary without raw IPs or secrets.
- Block 1 Alembic asset pass/fail and migration filenames only.
- Block 2 redacted runtime env validation booleans.
- Block 3 `pre_upgrade_alembic_current_exit_code=<int>` and
  `pre_upgrade_alembic_current_output_redacted=<safe text>`.
- Block 5 `alembic_upgrade_exit_code=<int>` and
  `alembic_upgrade_output_redacted=<safe text>` only if Block 5 was separately
  approved and actually run later.
- Block 6 `post_upgrade_alembic_current_exit_code=<int>` and
  `post_upgrade_alembic_current_output_redacted=<safe text>` only if Block 5
  completed with exit code 0 and Block 6 was actually run later.
- Safety booleans:
  - `database_url_printed=false`
  - `secret_values_printed=false`
  - `alembic_upgrade_run=true` only if Block 5 was separately approved and run
    later
  - `alembic_stamp_run=false`
  - `alembic_revision_run=false`
  - `alembic_downgrade_run=false`
  - `app_runtime_started=false`
  - `tdlib_auth_performed=false`
  - `telegram_connected=false`
  - `live_collector_started=false`
  - `notifier_transport_enabled=false`
  - `production_rollout_performed=false`

## What remains unauthorized

The following remain unauthorized after this package review and also remain
unauthorized after any later successful Alembic upgrade unless separately
approved:

- Alembic downgrade remains unauthorized.
- Alembic stamp remains unauthorized.
- Alembic revision generation remains unauthorized.
- migration file editing remains unauthorized.
- app runtime startup remains unauthorized.
- TDLib auth remains unauthorized.
- Telegram connection remains unauthorized.
- live collector startup remains unauthorized.
- notifier transport remains unauthorized.
- production rollout remains unauthorized.
- Docker or Docker Compose remains unauthorized.
- systemd unit changes remain unauthorized.
- repo `.env` creation remains unauthorized.
- repo `env/*.env` creation remains unauthorized.
- secret printing or secret file pasteback remains unauthorized.

## Next step

Review this runbook, checker, and tests. If accepted, commit the package.

Only after that review and commit, request separate explicit user approval for
manual dedicated VPS Alembic upgrade execution. Until that later approval is
given, do not run Block 5 and do not run Alembic upgrade.
