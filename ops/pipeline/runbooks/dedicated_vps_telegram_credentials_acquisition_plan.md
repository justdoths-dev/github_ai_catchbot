# Dedicated VPS Telegram credentials acquisition plan

## Purpose

Create a repo-local operator acquisition plan for Telegram credentials and
Telegram channel/bootstrap preparation for the dedicated VPS deployment path.

This is a planning and checklist slice only. It prepares the operator to
collect the credential inventory safely before a later reviewed runtime secret
placement update package.

## Source-of-truth / architecture boundary

`docs/project-source/README_replacement_consolidated_v0_20.md` remains
authoritative. The locked project-source bundles `00` through `10` remain the
source-of-truth set and are interpreted in filename order. The GitHub AI
application plan remains advisory only.

This plan preserves the canonical architecture invariant:

```text
SourceMessage -> Artifact -> CandidateGroup -> EvidenceBundle -> JudgeOutput -> Analysis -> Notification
```

Service boundaries remain unchanged:

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
- `recommended_flag_patch` is output-only and must not be auto-applied.
- production rollout remains unauthorized.

## Scope

This slice documents how the operator should acquire and inventory two separate
Telegram credential surfaces before any later placement or runtime action:

1. collector reader account / TDLib / MTProto credentials and channel source
   inventory.
2. notifier bot / Telegram Bot API credentials and delivery target inventory.

The plan may name key names and inventory fields. The plan must not contain
actual credential values, actual invite links, runtime environment contents, or
server-specific secrets.

## Non-authorizations

This slice does not mutate `/etc/github-ai-catchbot/runtime.env`.
This slice does not read or print runtime env values.
This slice does not connect to DB or Redis.
This slice does not run Alembic.
This slice does not modify Docker or systemd.
This slice does not start any app runtime.
This slice does not run TDLib auth.
This slice does not connect Telegram.
This slice does not enable live collector.
This slice does not enable notifier transport.
This slice does not perform production rollout.

It also does not place credentials, validate credentials, test Telegram
connectivity, start a collector, send notifications, or change feature flags.

## Credential surface A - collector reader account / TDLib / MTProto

The collector uses a Telegram reader account through TDLib/MTProto. The reader
account should be separate from the operator personal or main Telegram account
if possible.

Required or planned collector-side items:

- Telegram reader account identity and ownership status.
- `TELEGRAM_API_ID`
- `TELEGRAM_API_HASH`
- `TELEGRAM_PHONE_NUMBER`
- `TELEGRAM_2FA_PASSWORD`, if 2FA is enabled.
- `TDLIB_DB_ENCRYPTION_KEY`
- `TDLIB_STATE_DIR`
- `TDLIB_FILES_DIR`
- tracked channel source inventory with `public username`, `invite link`,
  `source_kind`, `source_value`, `desired_state`, `notes`, and
  `priority_weight`.

Collector rules:

- Access-sensitive invite links must not be committed.
- Record only whether each item has been acquired and where it is stored, not
  the value.
- The collector uses TDLib/MTProto reader account credentials.
- TDLib auth remains later and separately reviewed.
- Telegram connection remains unauthorized.
- Live collector startup remains unauthorized.

## Credential surface B - notifier bot / Telegram Bot API

The notifier uses a Telegram notification bot created by the operator through
BotFather and authenticates through the Telegram Bot API.

Required or planned notifier-side items:

- Telegram notification bot creation status.
- `TELEGRAM_BOT_TOKEN`
- operator target chat ID / delivery target ID acquisition plan.
- optional debug or digest target only if the operator later needs one.

Notifier rules:

- No bot token is to be committed or printed.
- Record only whether the target ID acquisition path is ready, not sensitive
  target values.
- The notifier uses Telegram Bot API bot token credentials.
- Notifier target verification remains a later separately reviewed slice.
- Notifier transport remains unauthorized.
- Production rollout remains unauthorized.

## Operator acquisition checklist

- Confirm the reader account is intended for collection and is separate from
  the notifier bot.
- Confirm the notifier bot is intended for delivery only and is separate from
  the reader account.
- Acquire collector credential values only through Telegram-approved operator
  flows.
- Acquire the notifier bot token only through BotFather.
- Store acquired values in the operator password manager.
- Record only presence/status, owner, storage location label, and follow-up
  blocker notes in repo-local materials.
- Confirm no secret value, phone number, invite link, target ID, runtime env
  contents, DB URL, Redis URL, VPS IP, or operator IP is written to repository
  files.
- Confirm production rollout remains unauthorized after acquisition is
  complete.

## Channel source inventory checklist/template

Maintain the channel source inventory outside committed secrets. Repo-local
notes may describe field names and redacted status only.

Template fields:

| Field | Meaning | Repo-safe value |
| --- | --- | --- |
| public username | Public channel username if available | `<redacted-or-public-handle-status>` |
| invite link | Access-sensitive invite link if needed | `<store-in-password-manager>` |
| source_kind | Source identifier type | `<public_username-or-invite_link-or-channel_id-label>` |
| source_value | Source identifier value | `<redacted-or-status-only>` |
| desired_state | Desired operator state | `<candidate-or-active-after-review>` |
| notes | Non-secret acquisition notes | `<no-secret-notes>` |
| priority_weight | Relative source priority | `<integer-label-after-review>` |

Access-sensitive invite links must not be committed. If a source is public, the
operator may record only a status label in this plan until a later reviewed
source inventory package defines the canonical storage contract.

## Secure storage / password manager expectation

All acquired values belong in the operator password manager or another approved
secret store outside the repository. Store in the operator password manager and
keep the repository limited to key names, placeholder labels, and acquisition
status only.

Use placeholders such as `<store-in-password-manager>`,
`<presence-recorded-only>`, and `<future-placement-label>` when repo-local
documentation needs to refer to future placement.

## No-secret / redaction rules

Do not provide secret values to ChatGPT, Codex, GitHub issues or PRs,
repository files, markdown runbooks, or terminal history.

Do not commit or print:

- real Telegram API ID.
- real Telegram API hash.
- real phone number.
- real 2FA password.
- real TDLib DB encryption key.
- real bot token.
- real target chat ID or delivery target ID if sensitive.
- real invite links.
- raw runtime env contents.
- DB or Redis URLs or passwords.
- public VPS IP or operator IP.
- any actual secret value.

Allowed repo-local content is limited to key names, placeholders, acquisition
status, storage-location labels, and redacted examples that cannot be mistaken
for real values.

## Later secret placement boundary

The later secret placement package will define operator commands for updating
the dedicated VPS runtime secret boundary without printing values. This plan
does not mutate `/etc/github-ai-catchbot/runtime.env`, does not read that file,
and does not define final placement commands.

The later placement package must preserve the separation between collector
credentials and notifier credentials.

## Later TDLib auth package boundary

TDLib auth remains later and separately reviewed. This plan does not run TDLib
auth, does not initialize TDLib state, does not create a session, and does not
connect Telegram.

The collector reader account credentials authorize only the collector-side
TDLib/MTProto preparation path. Reader account credentials do not authorize
notifier transport.

## Later notifier target verification boundary

Notifier target verification remains later and separately reviewed. This plan
does not verify a chat ID, does not send a Telegram message, does not enable
notifier transport, and does not change delivery flags.

The notifier bot token authorizes only the Bot API notifier preparation path.
Bot token does not authorize channel collection.

## Rotation / recovery notes

The collector reader account and notifier bot are separate credentials and
separate blast-radius domains.

- If the notifier bot token is exposed, rotate it through BotFather and keep
  collector credentials untouched unless separate evidence requires rotation.
- If the reader account credentials, phone number, 2FA password, or TDLib
  encryption key are exposed, rotate or recover the reader account path and
  keep notifier bot credentials untouched unless separate evidence requires
  rotation.
- If an invite link is exposed, rotate that channel access path with the
  channel owner or administrator and keep unrelated credentials untouched.
- Record only rotation status and blocker notes in repo-local materials.

## Acceptance criteria

- The operator has a password-manager record plan for collector reader account
  credentials.
- The operator has a password-manager record plan for notifier bot credentials.
- The collector reader account / TDLib / MTProto surface is documented
  separately from the notifier bot / Telegram Bot API surface.
- The channel source inventory field list is documented without actual values.
- No credential values, invite links, runtime env contents, DB/Redis URLs,
  server IPs, or operator IPs are committed or printed.
- TDLib auth remains later and separately reviewed.
- Telegram connection remains unauthorized.
- Live collector startup remains unauthorized.
- Notifier transport remains unauthorized.
- Production rollout remains unauthorized.

## Next bounded slice

After this plan is reviewed and approved, proceed only to:

```text
dedicated_vps_telegram_runtime_secret_placement_update_package
```

Do not replace the next bounded slice with TDLib auth, Telegram connection,
live collector startup, notifier transport, or production rollout.
