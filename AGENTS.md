# Codex Operating Instructions: github_ai_catchbot

These instructions apply to the whole repository. They are repo-local operating
rules for Codex and other automated agents working on `justdoths-dev/github_ai_catchbot`.

## Required Source Read

Before changing files for any bounded task, Codex must read these files in the
repo and treat them as the current source set:

- `AGENTS.md`
- `docs/project-source/README_replacement_consolidated_v0_20.md`
- `docs/project-source/00_foundations_stage0_stage1_bundle_v0_1.md`
- `docs/project-source/01_runtime_collector_design_stage2_stage3_bundle_v0_1.md`
- `docs/project-source/02_normalization_enrichment_design_stage4_stage5_bundle_v0_1.md`
- `docs/project-source/03_judge_delivery_operations_stage6_stage10_bundle_v0_1.md`
- `docs/project-source/04_execution_contracts_migrations_stage11_stage12_bundle_v0_1.md`
- `docs/project-source/05_migration_code_drafts_stage13_stage16_bundle_v0_1.md`
- `docs/project-source/06_collector_implementation_stage17_stage25_bundle_v0_1.md`
- `docs/project-source/07_outbox_normalizer_stage26_stage28_bundle_v0_1.md`
- `docs/project-source/08_enrichers_assembler_stage29_stage32_bundle_v0_1.md`
- `docs/project-source/09_analysis_pipeline_stage33_stage38_bundle_v0_1.md`
- `docs/project-source/10_delivery_hardening_stage39_plus_v0_1.md`
- `docs/project-source/03_GitHub_AI_application_plan.md` as advisory only

Source priority:

1. `README_replacement_consolidated_v0_20.md`
2. `00` through `04` design and locked bundles
3. `05` through `10` implementation bundles
4. `03_GitHub_AI_application_plan.md` as advisory only

If source docs conflict, the newer README and locked design bundles win over
implementation drafts and advisory notes.

## Architecture Invariant

Preserve this pipeline shape:

```text
SourceMessage -> Artifact -> CandidateGroup -> EvidenceBundle -> JudgeOutput -> Analysis -> Notification
```

Do not collapse stages, skip durable records, or move authority across service
boundaries unless the current bounded task explicitly approves that contract
change.

## Service Boundaries

- The collector preserves raw Telegram source messages and revisions only.
- PostgreSQL is the durable system of record.
- Redis is for queues, locks, and transient state only.
- The router-normalizer is deterministic and non-LLM.
- Enrichers gather evidence only.
- The LLM judge produces `judge_output_v1` only.
- The deterministic policy-engine computes the final verdict and delivery decision.
- The notifier renders and delivers only.
- Replay creates new runs and versions; it never overwrites historical truth.

## Bounded Execution Gates

Do not perform any of the following unless explicitly approved by the current
bounded task:

- Run a live collector.
- Run a notifier.
- Perform TDLib auth, TDLib code entry, or Telegram API work.
- Mutate PostgreSQL or Redis.
- Run Docker or systemd changes.
- Create, modify, or run Alembic migrations.
- Write `source_messages` or `event_outbox` records.
- Touch runtime secrets, production logs, private stderr, or live VPS state.

Repository-operating, docs-only, review-only, and local-only tasks must stay
inside their named files and must not drift into product/runtime code.

## Git And Staging Hygiene

Use exact-file staging only. Never run:

- `git add .`
- `git add -A`

Do not stage:

- Accidental `.codex` noise outside an explicitly approved repo-ops task.
- `runtime.env` or any file containing secrets.
- Logs.
- Private stderr.
- `venv/` or `.venv/`.
- `__pycache__/` or `.pytest_cache/`.
- Review bundle Markdown outputs.
- `/tmp` outputs or seed files.

If staging is requested after review, stage only the exact approved paths, for
example:

```bash
git add AGENTS.md .codex/config.toml .codex/review-bundle-template.md
```

## Review Bundle Rule

For implementation tasks, a Review Bundle is mandatory before commit or push.
The bundle must be generated outside the repo, normally under:

```text
/mnt/c/Users/dev/Desktop/codex-review-bundles
```

Review bundle generation must not run `git add`, `git commit`, or `git push`.
Untracked project files must be included by direct full-content capture because
normal `git diff` omits them.

## Anti-Overconservatism

Docs are tools, not deliverables. Do not add docs, diagnostics, reports, or
extra runbooks unless they close a concrete contract gap, failed test,
ambiguity, safety risk, or security risk in the current bounded task.

