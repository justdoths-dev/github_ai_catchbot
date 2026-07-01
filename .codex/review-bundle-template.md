# github_ai_catchbot Codex Review Bundle Template

Use this template for ChatGPT review before commit or push. Generate review
bundles outside the repo, normally under
`/mnt/c/Users/dev/Desktop/codex-review-bundles`.

Do not run `git add`, `git commit`, or `git push` during review bundle
generation.

Exclude accidental or sensitive noise:

- `.codex` files outside the exact intended repo-ops files
- `runtime.env`
- private stderr
- logs
- `venv/` and `.venv/`
- `__pycache__/` and `.pytest_cache/`
- review bundle Markdown outputs
- `/tmp` outputs and seed files

## 1. Metadata

- Repository: `justdoths-dev/github_ai_catchbot`
- Branch:
- Task name:
- Task type:
- Bundle generated at:
- Bundle path:
- Codex/model:
- Commit/push status: no git add, no commit, no push during bundle generation

## 2. Original Task Summary

Paste the original user task or a faithful summary. Include explicit scope
limits, forbidden areas, and requested validation.

## 3. Canonical Source Docs Read

Record whether each required source file was read and any task-relevant notes:

- `AGENTS.md`
- `.codex/config.toml`, if present
- `.codex/review-bundle-template.md`, if present
- current repo HEAD: code, tests, migrations, repo-local instructions, and commits
- PASS Review Bundles and VPS/runtime readbacks, if available to the task
- `github_ai_catchbot_chatgpt_master_roadmap_progress_authority_completion_v3.md`, if present as active project source/local doc
- `docs/project-source/00_foundations_stage0_stage1_bundle_v0_1.md`
- `docs/project-source/01_runtime_collector_design_stage2_stage3_bundle_v0_1.md`
- `docs/project-source/02_normalization_enrichment_design_stage4_stage5_bundle_v0_1.md`
- `docs/project-source/03_judge_delivery_operations_stage6_stage10_bundle_v0_1.md`
- `docs/project-source/04_execution_contracts_migrations_stage11_stage12_bundle_v0_1.md`
- `docs/project-source/README_replacement_consolidated_v0_20.md`, only if explicitly requested for archaeology
- implementation bundles `05` through `10`, only if explicitly requested for archaeology
- `docs/project-source/03_GitHub_AI_application_plan.md`, advisory only if explicitly relevant
- stale sources avoided as active next-step authority: yes/no, with notes

State the applied source priority:

1. Current repo HEAD: code, tests, migrations, `AGENTS.md`, `.codex/*`, and commits
2. PASS Review Bundles and VPS/runtime readbacks
3. `github_ai_catchbot_chatgpt_master_roadmap_progress_authority_completion_v3.md`, if present as active project source/local doc
4. Locked bundles `00` through `04`
5. Historical/archaeology docs only when explicitly requested

Do not require README v20 or implementation bundles `05` through `10` as active
next-step authority unless the task explicitly asks for archaeology.

## 4. Project Boundary Check

State whether the task touched any forbidden area:

- `src/`
- `scripts/ops/`
- `tests/`
- `migrations/`
- `docs/project-source/`
- live collector, notifier, TDLib auth/code-entry, Telegram API
- PostgreSQL or Redis mutation
- Docker, systemd, or Alembic execution
- `source_messages` or `event_outbox` writes
- secrets, runtime env files, private stderr, logs, or VPS state

## 5. Git State Commands

Paste exact command outputs.

### `git status --short --branch`

```text
<exact output>
```

### `git log --oneline -5 --decorate`

```text
<exact output>
```

### `git diff --stat`

```text
<exact output>
```

### `git diff --name-status`

```text
<exact output>
```

### `git diff --check`

```text
<exact output>
```

### `git ls-files --others --exclude-standard`

```text
<exact output>
```

## 6. Changed Files Overview

List each changed, deleted, or untracked project file. Mark whether it is
tracked or untracked and why it belongs in scope.

## 7. Full Current Contents Of Changed And Untracked Project Files

Include the full current contents of every changed or untracked project file in
scope. This is required for untracked files because normal `git diff` omits
them.

### `<path/to/file>`

```text
<full current file contents>
```

Do not include contents from excluded sensitive/noise paths.

## 8. Full Unified Diff

Paste the full unified diff for tracked changes:

```text
<git diff output>
```

For untracked project files, include either full contents in section 7 plus a
clear note that normal `git diff` omits them, or a safe `git diff --no-index`
view against `/dev/null`.

## 9. Validation Commands And Output

Paste each validation command and exact output. Include skipped validations with
the reason.

```text
<command>
<exact output>
```

## 10. Contract Compliance Assessment

Assess compliance with the project contracts:

- Architecture invariant:
  `SourceMessage -> Artifact -> CandidateGroup -> EvidenceBundle -> JudgeOutput -> Analysis -> Notification`
- Collector raw-message preservation boundary
- PostgreSQL durable record boundary
- Redis transient state boundary
- Deterministic non-LLM router-normalizer boundary
- Evidence-only enricher boundary
- `judge_output_v1` LLM judge boundary
- Deterministic policy-engine final-decision boundary
- Notifier render/deliver-only boundary
- Replay append-only/new-version boundary
- Exact-file staging and no forbidden `git add` commands

## 11. Highest-Risk Areas

List the highest-risk changed areas and what the reviewer should inspect first.

## 12. Dependency And Environment Changes

State whether dependencies, environment variables, Docker/systemd files,
migrations, runtime credentials, or VPS assumptions changed. If none changed,
say so explicitly.

## 13. Backward Compatibility And Data Risks

State whether the change can affect stored data, migrations, queue contracts,
historical truth, replay behavior, or notification behavior. If none apply,
say so explicitly.

## 14. Recommended Next Action

Choose one:

- Approve exact-file staging.
- Request changes before staging.
- Run additional validation first.
- Do not stage.

Include exact suggested staging commands only if approval is recommended.

## 15. Reviewer Notes For ChatGPT

Call out anything the reviewer should know, including known environment limits,
untracked files, skipped broad tests, shell quoting issues, or intentionally
excluded files.
