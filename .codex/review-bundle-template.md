# github_ai_catchbot External Review Bundle Template

Use this template for Won/operator review before any commit or push. Generate
review bundles outside the repo, normally under
`/mnt/c/Users/dev/Desktop/codex-review-bundles`.

The `codex-review-bundles` directory name is a compatibility path and does not
identify the executor.

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

## 1. Task Identity

- Project: `github_ai_catchbot`
- Repository: `justdoths-dev/github_ai_catchbot`
- Task:
- Task Packet path or identity:
- Task Packet SHA-256:
- Route Addendum identity/SHA, when applicable:
- Operator exception identity/SHA, when applicable:
- Bundle generated at:
- Bundle path:

## 2. Worker Execution Metadata

- executor:
- provider:
- provider_route:
- exact_model_id or model_id_unavailable:
- display_model_name:
- reasoning_effort:
- client_surface:
- Plan/Act mode:
- auto_approve_state (Cline auto-approve state):
- operator exception scope:
- Cline version, when applicable:

## 3. Original Task Summary

Paste the original user task or a faithful summary. Include explicit scope
limits, forbidden areas, and requested validation.

## 4. Source Read Receipt

Record whether each authority was read, unavailable, or not applicable, with a
task-relevant note. Never claim a remote source was read when only a local
remote-tracking ref was inspected.

- agent-llm-wiki project state:
- latest Won Verdict:
- `AGENTS.md`
- `.codex/config.toml`, if present
- review template
- current repo HEAD: code, tests, migrations, repo-local instructions, and commits
- accepted task-related Review Bundles and runtime readbacks, if available to the task
- active v5 roadmap/progress delta
- `docs/project-source/00_foundations_stage0_stage1_bundle_v0_1.md`
- `docs/project-source/01_runtime_collector_design_stage2_stage3_bundle_v0_1.md`
- `docs/project-source/02_normalization_enrichment_design_stage4_stage5_bundle_v0_1.md`
- `docs/project-source/03_judge_delivery_operations_stage6_stage10_bundle_v0_1.md`
- `docs/project-source/04_execution_contracts_migrations_stage11_stage12_bundle_v0_1.md`
- `docs/project-source/README_replacement_consolidated_v0_20.md`, only if explicitly requested for archaeology
- implementation bundles `05` through `10`, only if explicitly requested for archaeology
- `docs/project-source/03_GitHub_AI_application_plan.md`, advisory only if explicitly relevant
- archaeology avoided: yes/no, with notes
- unavailable sources and reason:

State the applied source priority:

1. current local repository HEAD/code/tests/migrations/repo-local instructions
2. accepted task-related Review Bundle and runtime readback
3. agent-llm-wiki accepted project state/latest Won Verdict, when actually readable
4. active v5 roadmap and current progress delta
5. locked bundles `00` through `04`
6. archaeology only when explicitly requested

Do not require README v20 or implementation bundles `05` through `10` as active
next-step authority unless the task explicitly asks for archaeology.

## 5. Repository Evidence

- workspace:
- repository:
- branch:
- initial HEAD:
- current HEAD:
- local origin/main relation:
- initial worktree/index/untracked:
- final worktree/index/untracked:

Do not claim remote GitHub synchronization when only the local remote-tracking
ref was read.

## 6. Reuse Map And Contract Mapping

- existing owner reused:
- changed exact files:
- downstream consumer:
- forbidden equivalent surfaces not created:
- contract-to-file mapping:
- design deviations:

## 7. Project Boundary Check

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

## 8. Git State Commands

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

## 9. Changed Files Overview

List each changed, deleted, or untracked project file. Mark whether it is
tracked or untracked and why it belongs in scope.

## 10. Full Current Contents Of Changed And Untracked Project Files

Include the full current contents of every changed or untracked project file in
scope. This is required for untracked files because normal `git diff` omits
them.

### `<path/to/file>`

```text
<full current file contents>
```

Do not include contents from excluded sensitive/noise paths.

## 11. Full Unified Diff

Paste the full unified diff for tracked changes:

```text
<git diff output>
```

For untracked project files, include either full contents in section 10 plus a
clear note that normal `git diff` omits them, or a safe `git diff --no-index`
view against `/dev/null`.

## 12. Validation And Authority Evidence

Paste each validation command, exit code, and exact sanitized output. Include
tests skipped and reasons, and state whether network, secret/env reads,
DB/Redis, Docker/systemd, Git mutation, and product/runtime code were not
attempted or touched.

```text
<command>
<exact output>
```

## 13. Contract Compliance Assessment

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

## 14. Worker Outcome

Fill exactly one worker outcome and leave every reviewer-only field empty:

```text
IMPLEMENTED_FOR_REVIEW
BLOCKED
INTERRUPTED
TESTS_FAILED
```

Worker outcome:

### Won Review Verdict (reviewer-only; worker leaves empty)

- PASS_PENDING_PUBLICATION
- CONDITIONAL
- FAIL

### Publication Readback (reviewer-only; worker leaves empty)

- exact files staged
- commit SHA
- push result
- origin readback
- required runtime validation

### Final Won Verdict (reviewer-only; worker leaves empty)

- FINAL_PASS

## 15. Highest-Risk Areas

List the highest-risk changed areas and what the reviewer should inspect first.

## 16. Dependency And Environment Changes

State whether dependencies, environment variables, Docker/systemd files,
migrations, runtime credentials, or VPS assumptions changed. If none changed,
say so explicitly.

## 17. Backward Compatibility And Data Risks

State whether the change can affect stored data, migrations, queue contracts,
historical truth, replay behavior, or notification behavior. If none apply,
say so explicitly.

## 18. Recommended Next Action

Choose one:

- Approve exact-file staging.
- Request changes before staging.
- Run additional validation first.
- Do not stage.

Include exact suggested staging commands only if approval is recommended.

## 19. Reviewer Notes For Won/Operator

Call out anything the reviewer should know, including known environment limits,
untracked files, skipped broad tests, shell quoting issues, or intentionally
excluded files.
