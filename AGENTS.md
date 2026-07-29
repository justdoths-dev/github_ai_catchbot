# Automated Worker Operating Instructions: github_ai_catchbot

These instructions apply to the whole repository. They are the canonical
repo-local operating contract for authorized automated workers and the
operator working on `justdoths-dev/github_ai_catchbot`.

## Project Mission

Build and maintain `github_ai_catchbot`.

This project is a precision-first GitHub/X/AI-development opportunity
intelligence bot. It ingests Telegram development channels, preserves raw source
truth, creates deterministic candidates, gathers external evidence, assembles an
`EvidenceBundle`, calls an LLM judge for structured `judge_output_v1` only,
applies deterministic policy, and delivers Telegram notifications.

It is not a generic news summarizer. It is not an autonomous agent loop. It is
not a trading bot.

## Repo Map

Known project-source and historical references:

- `docs/project-source/00_foundations_stage0_stage1_bundle_v0_1.md`
- `docs/project-source/01_runtime_collector_design_stage2_stage3_bundle_v0_1.md`
- `docs/project-source/02_normalization_enrichment_design_stage4_stage5_bundle_v0_1.md`
- `docs/project-source/03_judge_delivery_operations_stage6_stage10_bundle_v0_1.md`
- `docs/project-source/04_execution_contracts_migrations_stage11_stage12_bundle_v0_1.md`
- `github_ai_catchbot_plus_gpt56_high_handoff_master_roadmap_v5.md`, when present as an active local source
- `github_ai_catchbot_chatgpt_project_source_refresh_progress_delta_2026-07-23.md`, when present as the current local progress delta
- `docs/project-source/README_replacement_consolidated_v0_20.md` as a historical index only
- `docs/project-source/05_migration_code_drafts_stage13_stage16_bundle_v0_1.md`
- `docs/project-source/06_collector_implementation_stage17_stage25_bundle_v0_1.md`
- `docs/project-source/07_outbox_normalizer_stage26_stage28_bundle_v0_1.md`
- `docs/project-source/08_enrichers_assembler_stage29_stage32_bundle_v0_1.md`
- `docs/project-source/09_analysis_pipeline_stage33_stage38_bundle_v0_1.md`
- `docs/project-source/10_delivery_hardening_stage39_plus_v0_1.md`
- `docs/project-source/03_GitHub_AI_application_plan.md`

Implementation bundles `05` through `10`, older README snapshots, and advisory
application plans are historical/archaeology sources only unless the current
task explicitly asks to inspect them. They are not active next-step authority.

Known implementation areas, if present:

- `src/services/collector_telegram/`
- `src/services/outbox_relay/`
- `src/services/router_normalizer/`
- `src/services/gh_enricher/`
- `src/services/x_enricher/`
- `src/services/web_enricher/`
- `src/services/evidence_assembler/`
- `src/services/analysis_router/`
- `src/services/judge_openai/`
- `src/services/analysis_validator/`
- `src/services/policy_engine/`
- `src/services/notifier_telegram/`
- `scripts/ops/`
- `tests/unit/`
- `tests/component/`

If a listed path is absent, treat it as unimplemented or pending. Do not invent
files, commands, schemas, policy names, hooks, or runtime surfaces.

## Source of Truth

Mandatory read order when required sources are available:

1. operator-provided current project instructions and exact Task Packet
2. agent-llm-wiki global workflow, source-of-truth, Task Packet, and review contracts
3. `agent-llm-wiki/projects/github_ai_catchbot/`: `INDEX`, `PROJECT_PURPOSE`, `CURRENT_STATE`, `ROADMAP`, `OPERATIONS`, `DECISIONS`, and latest Won Verdict
4. root/scoped `AGENTS.md`
5. `.codex/config.toml` and `.codex/review-bundle-template.md`
6. current repository HEAD, source, tests, migrations, commits, and harness files
7. accepted task-related Review Bundles and runtime readbacks
8. active v5 roadmap and current progress delta
9. locked bundles `00` through `04`
10. archaeology only when explicitly requested

Mandatory read order and conflict-resolution priority are different contracts.

Conflict-resolution/source priority:

1. current repository/runtime evidence
2. accepted Review Bundle/runtime readback
3. agent-llm-wiki accepted state
4. active v5/progress delta
5. locked bundles `00` through `04`
6. archaeology

`README_replacement_consolidated_v0_20.md` is a historical index only.
Implementation bundles `05` through `10` are not active next-step authority.
`03_GitHub_AI_application_plan.md` is advisory only. These documents do not
override current code/tests, accepted task-related Review Bundles/readbacks,
actual agent-llm-wiki state, active v5/progress authority, locked bundles `00`
through `04`, service ownership, or contracts unless the current task explicitly
asks for archaeology.

`OPEN`, `UX_OPEN`, `AUTHORITY_OPEN`, or `ROLLOUT_OPEN` never means code is
absent. Re-check current HEAD and reviewed readbacks before adding a new
surface. If sources conflict and the conflict blocks the bounded task, stop and
report it.

During a GitHub synchronization outage, the local-only exception permits only
narrow repair/review work and requires all of: explicit operator approval;
exact local branch/HEAD/origin relation; known worktree/index/untracked state;
exact defect, test failure, or demonstrated contract gap; bounded allowed files
and commands; no new authority; and closed publication and final-verdict
authority. This is an outage-specific exception, not a normal Wiki bypass.

## Automated Worker Authority, Routes, And Recovery

Won/operator owns architecture, contracts, task scope, the final review verdict,
and publication approval. A worker owns only bounded implementation, authorized
tests, and Review Bundle evidence.

Workers may claim only:

```text
IMPLEMENTED_FOR_REVIEW
BLOCKED
INTERRUPTED
TESTS_FAILED
```

Only Won/operator may issue:

```text
PASS_PENDING_PUBLICATION
CONDITIONAL
FAIL
FINAL_PASS
```

Select the implementation route, provider, and model immediately before work in
this exact order:

```text
1. Codex
2. Cline + ClinePass + operator-selected hosted coding model
3. Cline + direct hosted provider + operator-selected model
4. local model + Ollama/Cline
```

- Do not permanently assume a provider offers a particular model or version.
- Never perform automatic provider/model fallback.
- Do not ask again when the operator already selected the route.
- Do not pass a hosted-model Task Packet unchanged to a local model.
- Do not automatically inherit dirty state between providers or models.

Every worker execution and Review Bundle must record: executor; provider;
provider_route; `exact_model_id` or `model_id_unavailable`; display_model_name;
reasoning_effort; Plan/Act mode; auto_approve_state; Cline version when
applicable; workspace; repository; branch; HEAD; local origin relation; initial
worktree, index, and untracked state; and Task Packet SHA-256.

Repository-modification Cline tasks default to `auto-approve=off`. An exception
must be explicitly operator-approved, task-specific, and recorded in both the
Task Packet and Review Bundle. It must not expand allowed files, commands,
network, secret, runtime, DB, systemd, Git publication, or product authority.

For `INTERRUPTED`, the worker must provide the partial diff, `git status`, index
state, untracked state, tests attempted and results, actual provider/model
metadata, and the last completed action. Won/operator must then choose one of:
continue, revert, discard, or a smaller route-specific Task Packet. No automatic
dirty-state handoff is allowed.

## Architecture Invariants

Preserve this pipeline:

```text
SourceMessage -> Artifact -> CandidateGroup -> EvidenceBundle -> JudgeOutput -> Analysis -> Notification
```

Layer rules:

- Telegram collector preserves raw source messages and revisions only.
- Normalization and triggering are deterministic and non-LLM.
- External enrichers gather evidence only.
- Evidence assembler builds `EvidenceBundle`.
- Analysis-router is deterministic routing only.
- LLM judge produces structured `judge_output_v1` only.
- Validator gates LLM output before policy.
- Policy engine computes final verdict and `delivery_decision`.
- Notifier renders and delivers only.
- PostgreSQL is durable system of record.
- Redis is queue, lock, and short-lived execution state only.
- Redis messages must stay thin.
- Business state must be rehydrated from PostgreSQL.
- Replay creates new runs/versions and never overwrites historical truth.
- Preserve append-only history where the schema requires it.
- Do not collapse collector, normalizer, enricher, assembler, judge, policy, and notifier responsibilities.

## Coding Rules

- Prefer minimal bounded implementation slices.
- Use the largest safe bounded macro-slice when adjacent work shares the same
  opened authority and validation boundary; do not fragment safe adjacent work.
- Do not redesign architecture unless source documents explicitly require it.
- Do not add comfort docs, extra diagrams, diagnostics, reports, or runbooks as substitutes for implementation.
- If a safety issue is found, connect it to a contract, test, fix, stop condition, or implementation slice.
- Implement safety through guards, readback, idempotency, redaction, focused
  tests, and explicit bounds, not wrapper/readiness theater.
- Keep service ownership narrow.
- Do not duplicate business logic in ops scripts when an existing service boundary is available.
- Ops smoke scripts must be bounded, sanitized, and explicit about approvals.
- Default smoke mode must be read-only unless the task explicitly states otherwise.
- Approved live mutations require explicit operator approval flags.
- Use sanitized bucketed reports for live/runtime outputs.
- Do not print raw IDs, stream IDs, dedupe keys, URLs, source text, tokens, DB URLs, Redis URLs, exception bodies, stderr, or raw origin remote URLs.
- Repository-operating, docs-only, review-only, and local-only tasks must stay inside their named files and must not drift into product/runtime code.

## Testing / Verification

Use focused verification for each bounded slice.

Common verified commands include:

- `venv/bin/python -m compileall <changed-python-files>`
- `venv/bin/python -m pytest -q <focused-test-path>`
- `venv/bin/python -m pytest -q tests/unit/services/outbox_relay`
- `venv/bin/python -m pytest -q tests/unit/services/router_normalizer`
- `venv/bin/python -m pytest -q tests/unit/services/evidence_assembler tests/component/services/evidence_assembler`

Only run wider tests when they are cheap and relevant. Do not claim validation
that was not run.

Use `git diff --check` before review. Use `git diff --cached --check` only when
staging is intentionally part of the user-approved step.

Do not treat garbled pasted shell text as failure if JSON/test output and final
git status are complete. If command semantics are ambiguous, rerun the smallest
read-only verification.

For repo-operations and `AGENTS.md`-only slices:

- Do not run pytest.
- Do not run compileall.
- Do not run Docker, systemd, Alembic, live API, DB, or Redis commands.
- Run only text, diff, and allowed-file checks.

## Review Bundle Lifecycle

Full external Review Bundles must be written first to:

```text
/mnt/c/Users/dev/Desktop/codex-review-bundles/01_new
```

Windows equivalent:

```text
C:\Users\dev\Desktop\codex-review-bundles\01_new
```

Do not write newly generated Review Bundles directly to `02_reviewed_pass`. The
directory name `codex-review-bundles` is a compatibility path; it does not prove
that Codex was the executor.

Lifecycle folders:

- `01_new`: newly generated full Review Bundles before Won/operator review.
- `02_reviewed_pass`: only Review Bundles manually moved after Won/operator review.
- `03_captured_finish`: Review Bundles whose capture/archive lifecycle is complete.
- `04_quarantine`: FAIL, CONDITIONAL, non-full, malformed, ambiguous, or wrong-project artifacts.

Compact final reports are not capture artifacts unless the user explicitly says
otherwise.

Full Review Bundles must include safe top-level metadata:

```text
Project: github_ai_catchbot
Target repo: justdoths-dev/github_ai_catchbot
Task: <task-slug>
```

Do not add duplicate task labels such as both `Task:` and `Task slug:` in the
same safe metadata block.

A Review Bundle must include:

- summary
- repository state
- files changed
- contract and boundary evidence
- validation commands and outputs
- full diff or no-index diff for untracked files
- full changed file contents
- boundary check
- worker outcome and empty reviewer-only verdict fields
- explicit no staging/commit/push statement

Review bundle generation must not run `git add`, `git commit`, or `git push`.
Untracked project files must be included by direct full-content capture because
normal `git diff` omits them.

## Git / Staging Policy

- Do not stage, commit, or push unless the operator explicitly authorizes exact-file publication after Won review.
- Never use git add .
- Never use git add -A.
- Stage exact files only.
- After explicit Won/operator staging approval, use only an exact reviewed-file
  staging command, such as `git add -- AGENTS.md` when that file alone is approved.

Do not stage:

- `.env`
- `.env.*`
- `runtime.env`
- logs
- private stderr
- private locator files
- `venv/` or `.venv/`
- `__pycache__/`
- `.pytest_cache/`
- generated review bundles
- live server configs
- unrelated scratch files
- zero-byte accidental root artifacts

## Security / Secrets

Never print, commit, or include in Review Bundles:

- tokens
- credentials
- runtime env values
- DB URLs
- Redis URLs
- X bearer token
- Telegram credentials
- OpenAI API key
- raw Telegram source text
- private URLs
- raw IDs when the task requires sanitized output
- exception bodies
- stderr logs
- raw origin remote URL

Runtime env files must stay out of git.

If a runtime command can expose stderr or secrets, redirect stderr to a
DO-NOT-PASTE file and report sanitized stdout JSON only.

## Do Not

- Do not collapse pipeline layers.
- Do not let the LLM gather evidence.
- Do not let the LLM decide final verdict or delivery.
- Do not let notifier reinterpret policy output.
- Do not use Redis as durable truth.
- Do not overwrite historical records during replay.
- Do not rerun closed approved live smoke gates without new failing evidence.
- Do not start downstream services in a route-publish smoke.
- Do not run a live collector.
- Do not run a notifier.
- Do not perform TDLib auth, TDLib code entry, or Telegram API work.
- Do not mutate PostgreSQL or Redis.
- Do not write `source_messages` or `event_outbox` records.
- Do not run Docker, systemd, Alembic, external API calls, live DB/Redis writes, or downstream services unless the task explicitly allows them.
- Do not touch runtime secrets, production logs, private stderr, or live VPS state.
- Do not create, modify, or run Alembic migrations unless explicitly scoped.
- Do not invent repo structure, commands, config keys, or policy files.
- Do not add `.codex/rules`, hooks, or skills just because they look useful.
- Do not put long philosophy text in `.codex/`.
- Do not create comfort artifacts.

## Done Criteria

### Worker Review-Stage Completion

A worker may complete a task for review only when:

- only exact task-authorized tracked and untracked changes exist
- the index is clean unless staging was explicitly authorized
- no unexpected path exists
- required validation passed
- an external Review Bundle was generated
- the worker returned one allowed worker outcome

### Publication And Final Closure

Publication and final closure are distinct from worker review-stage completion.
When publication or runtime execution is required, closure requires:

- exact reviewed files were staged and committed only after operator approval
- push/origin readback completed when publication is required
- required runtime readback completed when applicable
- the final worktree and index are clean after publication
- only Won/operator may issue `FINAL_PASS`
