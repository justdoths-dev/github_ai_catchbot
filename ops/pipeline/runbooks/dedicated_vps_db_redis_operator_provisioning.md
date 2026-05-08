# Dedicated VPS DB/Redis operator provisioning

## Scope

This is a concrete operator-command package for the separately approved first
dedicated VPS DB/Redis provisioning path.

The operator runs these commands manually on the dedicated VPS after review.
Codex and reviewers must not run the commands from this repository slice.

The operator is assumed to already be logged into the dedicated VPS as the
deploy user and positioned in the `github_ai_catchbot` repository checkout.

This runbook only covers:

- installing PostgreSQL packages using apt.
- installing Redis packages using apt.
- configuring PostgreSQL local/private binding.
- configuring Redis local/private binding.
- creating the PostgreSQL app role and database without printing or committing
  the password.
- restarting/enabling PostgreSQL and Redis host services.
- running local-only health checks.
- inspecting local listen sockets and firewall status to prove no public 5432
  and no public 6379.

This runbook does not authorize app deployment or production rollout.

## Source-of-truth handling

README v20 remains authoritative. The locked project-source bundles `00~10`
remain the source-of-truth set, read in filename order. The GitHub AI
application plan remains advisory only.

The architecture invariant remains unchanged:

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

## Prior decision

Prior locked immediate DB/Redis decision:

- immediate DB/Redis provisioning uses host apt/systemd PostgreSQL + host
  apt/systemd Redis.
- Docker Compose remains a future full app stack candidate and is not
  discarded.
- this is not an architecture rewrite.

Decision record:

- `ops/pipeline/runbooks/dedicated_vps_db_redis_package_vs_docker_decision.md`

## Explicit authorized operator actions

The following actions are authorized for the operator on the dedicated VPS only:

- install PostgreSQL packages using apt.
- install Redis packages using apt.
- configure PostgreSQL local/private binding.
- configure Redis local/private binding.
- create PostgreSQL app role `github_ai_catchbot_app` and database
  `github_ai_catchbot` without printing or committing the password.
- restart/enable PostgreSQL and Redis host services.
- run local-only health checks.
- inspect local listen sockets and firewall status to prove no public 5432 and
  no public 6379.

## Explicit still-unauthorized actions

The following remain unauthorized:

- Docker install.
- Docker Compose execution.
- `.env` creation.
- secret value printing or committing.
- Alembic.
- app runtime.
- TDLib auth.
- Telegram connection.
- live collector.
- notifier transport.
- production rollout.
- raw server IP/operator IP/SSH key path/secret values in repo docs.

Do not create `.env`. Do not export or write `DATABASE_URL`. Do not export or
write `REDIS_URL`. Runtime secret placement is a later separately approved
slice.

Do not paste passwords, connection strings, Telegram/OpenAI/GitHub/X secrets,
SSH private key paths, raw server IPs, or operator IPs back into ChatGPT or into
repo docs.

## Prerequisites

- You are the operator, logged into the dedicated VPS as the deploy user.
- You are inside the `github_ai_catchbot` repository checkout on branch `main`.
- You have reviewed this runbook with ChatGPT before running it.
- You have sudo access for package/service/config commands.
- You have a password manager ready for the PostgreSQL app-role password.
- No production app runtime, live collector, notifier transport, TDLib auth, or
  Telegram connection is running as part of this slice.

## Operator command blocks

Run each block manually. Stop on the first unexpected result.

### Command block 0: context confirmation

```bash
pwd
hostname
whoami
id
lsb_release -a || cat /etc/os-release
git status --short --branch
git log --oneline -3
```

Expected safe output:

- repository path is the dedicated VPS checkout.
- user is the intended deploy user.
- branch is `main`.
- recent git log matches the reviewed state.
- do not paste raw IP output. This block does not require IP commands.

### Command block 1: package install

```bash
sudo apt-get update
sudo apt-get install -y postgresql postgresql-contrib redis-server redis-tools
sudo systemctl enable postgresql redis-server
```

Expected safe output:

- apt completes without package errors.
- `postgresql`, `postgresql-contrib`, `redis-server`, and `redis-tools` are
  installed.
- PostgreSQL and Redis host services are enabled.
- no Docker package is installed by this block.

### Command block 2: PostgreSQL local/private binding configuration

```bash
PG_CLUSTER_COUNT="$(pg_lsclusters --no-header | awk 'END {print NR}')"
if [ "$PG_CLUSTER_COUNT" -ne 1 ]; then
  echo "FAIL: expected exactly one PostgreSQL cluster, found ${PG_CLUSTER_COUNT}"
  pg_lsclusters
  exit 1
fi

PG_VERSION="$(pg_lsclusters --no-header | awk 'NR==1 {print $1}')"
PG_CLUSTER="$(pg_lsclusters --no-header | awk 'NR==1 {print $2}')"
test -n "$PG_VERSION"
test -n "$PG_CLUSTER"

PG_CONF="/etc/postgresql/${PG_VERSION}/${PG_CLUSTER}/postgresql.conf"
sudo cp -a "$PG_CONF" "${PG_CONF}.operator-package.$(date -u +%Y%m%dT%H%M%SZ).bak"

sudo pg_conftool "$PG_VERSION" "$PG_CLUSTER" set listen_addresses '127.0.0.1'
sudo pg_conftool "$PG_VERSION" "$PG_CLUSTER" set password_encryption 'scram-sha-256'
sudo pg_conftool "$PG_VERSION" "$PG_CLUSTER" show listen_addresses
sudo pg_conftool "$PG_VERSION" "$PG_CLUSTER" show password_encryption
```

Expected safe output:

- `pg_lsclusters --no-header` returns exactly one PostgreSQL cluster; if not,
  `pg_lsclusters` is printed and the operator stops.
- detected PostgreSQL version and cluster are non-empty.
- `listen_addresses` reports `127.0.0.1`.
- `password_encryption` reports `scram-sha-256`.
- no PostgreSQL version number is hardcoded.
- no public bind address is configured.
- no raw IP beyond local loopback appears in repo docs.
- no `.env` is created.

### Command block 3: Redis local/private binding configuration

```bash
sudo cp -a /etc/redis/redis.conf "/etc/redis/redis.conf.operator-package.$(date -u +%Y%m%dT%H%M%SZ).bak"

if grep -qE '^[#[:space:]]*bind ' /etc/redis/redis.conf; then
  sudo sed -i -E 's/^[#[:space:]]*bind .*/bind 127.0.0.1 ::1/' /etc/redis/redis.conf
else
  printf '%s\n' 'bind 127.0.0.1 ::1' | sudo tee -a /etc/redis/redis.conf >/dev/null
fi

if grep -qE '^[#[:space:]]*protected-mode ' /etc/redis/redis.conf; then
  sudo sed -i -E 's/^[#[:space:]]*protected-mode .*/protected-mode yes/' /etc/redis/redis.conf
else
  printf '%s\n' 'protected-mode yes' | sudo tee -a /etc/redis/redis.conf >/dev/null
fi

if grep -qE '^[#[:space:]]*supervised ' /etc/redis/redis.conf; then
  sudo sed -i -E 's/^[#[:space:]]*supervised .*/supervised systemd/' /etc/redis/redis.conf
else
  printf '%s\n' 'supervised systemd' | sudo tee -a /etc/redis/redis.conf >/dev/null
fi

grep -E '^(bind|protected-mode|supervised) ' /etc/redis/redis.conf
```

Expected safe output:

- Redis config contains `bind 127.0.0.1 ::1`.
- Redis config contains `protected-mode yes`.
- Redis config contains `supervised systemd`.
- Redis public bind is absent.
- Redis password/secret setup is not performed in this slice.
- no `.env` is created.

### Command block 4: service restart and local status

```bash
sudo systemctl restart postgresql redis-server
sudo systemctl is-active postgresql redis-server
sudo systemctl is-enabled postgresql redis-server
```

Expected safe output:

- both services report `active`.
- both services report `enabled`.
- no app runtime is started.

### Command block 5: PostgreSQL app role and database creation

The password must be stored outside the repo in the operator's password
manager. Do not paste the password back into ChatGPT. Do not place the password
into `.env` in this slice. Runtime secret placement is a later separately
approved slice.

```bash
sudo -u postgres psql -v ON_ERROR_STOP=1 <<'SQL'
DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_catalog.pg_roles
    WHERE rolname = 'github_ai_catchbot_app'
  ) THEN
    CREATE ROLE github_ai_catchbot_app LOGIN;
  END IF;
END
$$;
SQL

sudo -u postgres psql -c '\password github_ai_catchbot_app'

if ! sudo -u postgres psql -tAc "SELECT 1 FROM pg_database WHERE datname = 'github_ai_catchbot'" | grep -q 1; then
  sudo -u postgres createdb --owner=github_ai_catchbot_app github_ai_catchbot
fi

sudo -u postgres psql -d github_ai_catchbot -v ON_ERROR_STOP=1 <<'SQL'
GRANT CONNECT ON DATABASE github_ai_catchbot TO github_ai_catchbot_app;
GRANT USAGE, CREATE ON SCHEMA public TO github_ai_catchbot_app;
SQL
```

Expected safe output:

- role `github_ai_catchbot_app` exists.
- database `github_ai_catchbot` exists.
- the operator entered the password interactively.
- password value is not printed.
- password value is not committed.
- no `.env` is created.
- `DATABASE_URL` is not exported or written.
- Alembic is not run.

### Command block 6: local-only verification

```bash
pg_isready -h 127.0.0.1 -p 5432
sudo -u postgres psql -tAc "SELECT 1"
sudo -u postgres psql -tAc "SELECT 1 FROM pg_database WHERE datname = 'github_ai_catchbot'"
sudo -u postgres psql -tAc "SELECT 1 FROM pg_roles WHERE rolname = 'github_ai_catchbot_app'"
redis-cli -h 127.0.0.1 -p 6379 PING

ss -ltn | awk '$4 ~ /(:5432|:6379)$/ {print $4}'
if ss -ltn | awk '$4 ~ /(:5432|:6379)$/ {print $4}' | grep -Eq '^(0\.0\.0\.0:5432|\[::\]:5432|\*:5432|:::5432|0\.0\.0\.0:6379|\[::\]:6379|\*:6379|:::6379)$'; then
  echo "FAIL: public DB/Redis bind detected"
  exit 1
fi
echo "PASS: no public 5432 or 6379 listen bind detected"

UFW_STATUS="$(sudo ufw status verbose)"
printf '%s\n' "$UFW_STATUS"
if ! printf '%s\n' "$UFW_STATUS" | grep -Fq 'Status: active'; then
  echo "FAIL: UFW is not active"
  exit 1
fi
if printf '%s\n' "$UFW_STATUS" | grep -E '(^|[[:space:]])(5432|6379)(/tcp)?([[:space:]]|$)' | grep -Eiq 'allow'; then
  echo "FAIL: UFW appears to allow public 5432 or 6379"
  exit 1
fi
echo "PASS: UFW is active and has no public 5432/6379 allow rule"
```

Expected safe output:

- `pg_isready -h 127.0.0.1 -p 5432` succeeds.
- `SELECT 1` returns `1`.
- database existence check for `github_ai_catchbot` returns `1`.
- role existence check for `github_ai_catchbot_app` returns `1`.
- `redis-cli -h 127.0.0.1 -p 6379 PING` returns `PONG`.
- listen-socket check reports no `0.0.0.0:5432`, `[::]:5432`,
  `*:5432`, `:::5432`, `0.0.0.0:6379`, `[::]:6379`, `*:6379`, or
  `:::6379`.
- UFW status contains `Status: active` and confirms no public 5432 and no
  public 6379 allow rules.
- do not paste raw public IPs or operator IPs back for review.

### Command block 7: post-run summary

Produce this redacted result summary and paste only this summary back for
review. Fill statuses from the preceding command output. Do not include raw
public IP, DB password, `DATABASE_URL`, credential-bearing `REDIS_URL`,
Telegram/OpenAI/GitHub/X secrets, or SSH private key paths.

```bash
cat <<'SUMMARY'
Dedicated VPS DB/Redis operator provisioning summary

- package install status: PASS/FAIL
- service active status:
  - postgresql: active/inactive
  - redis-server: active/inactive
- service enabled status:
  - postgresql: enabled/disabled
  - redis-server: enabled/disabled
- PostgreSQL local readiness: PASS/FAIL
- Redis local PING: PASS/FAIL
- DB existence github_ai_catchbot: PASS/FAIL
- role existence github_ai_catchbot_app: PASS/FAIL
- local bind/public exposure result: PASS/FAIL, no public 5432/no public 6379
- UFW no-public-DB/Redis confirmation: PASS/FAIL
- password pasted into ChatGPT: NO
- `.env` created: NO
- Alembic run: NO
- app runtime started: NO
- TDLib/Telegram/live collector/notifier/production rollout performed: NO
SUMMARY
```

## Verification checklist

- [ ] Command block 0 confirms the reviewed repo context.
- [ ] Command block 1 installs only PostgreSQL and Redis packages listed here.
- [ ] Command block 2 confirms exactly one PostgreSQL cluster before selecting
      version/cluster.
- [ ] Command block 2 sets PostgreSQL `listen_addresses` to `127.0.0.1`.
- [ ] Command block 2 sets PostgreSQL `password_encryption` to
      `scram-sha-256`.
- [ ] Command block 3 sets Redis `bind 127.0.0.1 ::1`.
- [ ] Command block 3 sets Redis `protected-mode yes`.
- [ ] Command block 4 reports PostgreSQL and Redis active/enabled.
- [ ] Command block 5 creates or confirms `github_ai_catchbot_app`.
- [ ] Command block 5 creates or confirms `github_ai_catchbot`.
- [ ] Command block 5 keeps the password outside the repo and out of ChatGPT.
- [ ] Command block 6 confirms local PostgreSQL readiness.
- [ ] Command block 6 confirms local Redis PING.
- [ ] Command block 6 confirms no public 5432.
- [ ] Command block 6 confirms no public 6379.
- [ ] Command block 6 confirms UFW is active and has no public 5432/6379 allow
      rules.
- [ ] No Docker install occurred.
- [ ] No Docker Compose execution occurred.
- [ ] No `.env` creation occurred.
- [ ] No Alembic/app runtime/TDLib/Telegram/live collector/notifier/production
      rollout occurred.

## Failure handling

Stop immediately if any of the following happens:

- package installation fails.
- PostgreSQL cluster count is not exactly one.
- PostgreSQL version/cluster detection is empty.
- PostgreSQL config cannot be backed up.
- Redis config cannot be backed up.
- PostgreSQL or Redis fails to restart.
- PostgreSQL is not reachable locally.
- Redis does not return `PONG` locally.
- `ss -ltn` shows `0.0.0.0:5432`, `[::]:5432`, `*:5432`, `:::5432`,
  `0.0.0.0:6379`, `[::]:6379`, `*:6379`, or `:::6379`.
- UFW status does not contain `Status: active`.
- UFW appears to allow public 5432 or public 6379.
- a password or secret appears in terminal output intended for review.

Do not improvise a Docker fallback. Do not expose DB/Redis publicly to work
around local configuration failures.

## Rollback/stop conditions

If a misconfiguration is detected before production data exists:

- stop and record the failed block.
- keep config backups created by this runbook.
- if exposure is detected, stop the affected service and remove any accidental
  public firewall rule before asking for review.
- request review with the redacted failure summary.

After real production data exists, do not delete PostgreSQL data or rebuild the
database without separate explicit approval.

## What output to bring back to ChatGPT for review

Bring back:

- Command block 0 git status and git log summary, without raw IPs.
- package install status.
- PostgreSQL and Redis active/enabled status.
- PostgreSQL local readiness result.
- Redis local PING result.
- DB/role existence results.
- local bind/public exposure result.
- UFW no-public-DB/Redis confirmation.
- any failed command block number and redacted error text.

Do not bring back:

- raw public IP.
- operator IP.
- DB password.
- `DATABASE_URL`.
- credential-bearing `REDIS_URL`.
- Telegram/OpenAI/GitHub/X secrets.
- SSH private key paths.
- `.env` content.

Docker, docker compose, app service start, Alembic, TDLib, Telegram, notifier,
live collector, and production rollout remain explicitly forbidden by this
operator-command package.
