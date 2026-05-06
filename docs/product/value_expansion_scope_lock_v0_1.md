# Value Expansion Scope Lock v0.1

## 0. Status

This document is a scope lock, not an implementation design.

This document records an accepted future product direction for selected value-expansion capabilities. It is additive only. It does not replace `docs/project-source/README_replacement_consolidated_v0_20.md`, and it does not replace the stage 0~44 contracts captured in the locked project-source bundles.

This document does not authorize production rollout, live collector startup, TDLib auth, Telegram connection, DB/Redis production connectivity, Docker/systemd execution, schema/migration changes, new runtime workers, automatic Codex execution, automatic PR creation, or any self-learning/autonomous closed loop.

## 1. Source-of-truth alignment

The authoritative source order remains:

1. `docs/project-source/README_replacement_consolidated_v0_20.md`
2. Locked design bundles `00~04`
3. Implementation bundles `05~10`
4. Advisory-only `docs/project-source/03_GitHub_AI_application_plan.md`

This document must preserve the existing architecture invariant:

```text
SourceMessage -> Artifact -> CandidateGroup -> EvidenceBundle -> JudgeOutput -> Analysis -> Notification
```

The existing responsibility boundaries remain locked:

- collector preserves raw Telegram source messages and revisions only.
- normalization and triggering are deterministic and non-LLM.
- enrichers gather evidence only.
- evidence-assembler assembles candidate-centered EvidenceBundles and may reroot only within its contract.
- analysis-router is the deterministic judge-pipeline entry gate.
- judge-openai calls OpenAI and stores structured `judge_output_v1` only.
- analysis-validator validates `judge_output_v1` and controls policy handoff only.
- policy-engine computes final `analysis_v1.verdict` and `delivery_decision` deterministically.
- notifier is presentation, delivery, and Telegram transport only.
- PostgreSQL is the durable system of record.
- Redis is queue, lock, and short-lived execution state only.
- prod has exactly one live Telegram collector instance.
- replay creates new runs or versions and never overwrites historical truth.
- precision-first and negative-first remain governing product philosophy.

## 2. Accepted value-expansion capabilities

The following capabilities are accepted as intended future product direction. Acceptance here means scope inclusion only, not implementation approval.

### 2.1 Project Relevance Layer

Project Relevance Layer means mapping analyzed candidates to the operator's known projects.

Likely future inputs are manually defined `ProjectProfile` records or tags. The expected output is project relevance, applicable project or projects, and bounded next-step hints.

Likely project examples include `github_ai_catchbot`, `trading-bot`, `stock-auto-trading-bot`, `economic-finance-event-intelligence-bot`, and `crypto-memory-vault-dev`. These are examples only, not hard-coded implementation requirements.

This capability must not let judge-openai browse local repos. It must not let the LLM mutate project plans. It must not override the policy-engine verdict. It must not let notifier reinterpret verdict or `delivery_decision`.

### 2.2 Watchlist / Follow-up Monitor

Watchlist / Follow-up Monitor means artifact and candidate follow-up tracking.

It may support periodic or operator-triggered refresh of previously analyzed artifacts. It should be based on append-only snapshots and material-change detection. It should reuse existing enrichment, evidence, and analysis contracts where possible.

It must not overwrite historical analyses. It must not create duplicate noisy notifications. It must not become a crawler.

### 2.3 Weekly Development Opportunity Report

Weekly Development Opportunity Report means a periodic or operator-triggered report summarizing high-value development opportunities.

The initial form should be read-only and report-only. It should summarize candidates, skipped patterns, project relevance, watchlist changes, and risk patterns.

It must not activate digest runtime prematurely. It must not send automatically until separately approved. It must not modify source truth.

### 2.4 Feedback -> Eval Case Loop

Feedback -> Eval Case Loop means capturing operator feedback on delivered or reviewed candidates and converting that feedback into reviewed eval or golden-set case candidates.

The loop must preserve manual review before policy or prompt changes. It allows reviewed eval-case candidate creation, but no automatic self-learning, no automatic prompt rewriting, no automatic threshold changes, and no automatic rollout.

### 2.5 Technical Debt / Security / Operational Risk Detection

Technical Debt / Security / Operational Risk Detection means structured risk signals about candidate projects or tools.

Examples include no tests, no CI, stale maintenance, wrapper risk, suspicious install behavior, prompt-injection surface, license ambiguity, and supply-chain smell.

This is explicitly not a full security audit. It must stay evidence-based. It must not claim exploit verification unless exploit verification is separately implemented and authorized. It should feed judge_output, analysis, and notification as risk evidence, not bypass policy-engine.

## 3. Currently excluded capability

### 3.1 Codex Prompt Seed Generation

Codex Prompt Seed Generation is currently excluded and not accepted in this scope lock.

It may be reconsidered later, but it must not be implemented by inference. This exclusion means no automatic Codex run, no automatic branch/commit/PR generation, and no repo mutation from bot output.

## 4. Non-goals and forbidden interpretations

This document explicitly forbids:

- replacing the existing pipeline with an agent runtime.
- moving final verdict or delivery decisions into the LLM.
- letting notifier reinterpret analysis.
- using Redis as durable memory.
- overwriting historical truth during follow-up.
- automatic self-learning.
- automatic Codex execution.
- automatic PR generation.
- a full autonomous agent loop.
- broad web crawling.
- full GitHub clone as the default.
- security-audit claims.
- production rollout.
- live TDLib or Telegram use.
- TDLib auth.
- Telegram connection.
- DB/Redis production connectivity.
- Docker/systemd execution.
- schema or migration changes.
- new runtime workers.
- automatic PR creation.

## 5. Architecture boundary mapping

Accepted value-expansion capabilities must map onto the existing pipeline rather than replacing it.

| Capability | Boundary mapping |
| --- | --- |
| Project Relevance Layer | Analysis-side or policy-adjacent metadata derived from existing candidate/evidence outputs; it cannot override deterministic verdict or delivery decisions. |
| Watchlist / Follow-up Monitor | Append-only artifact/candidate follow-up snapshots and material-change detection; replay and refresh must create new runs or versions. |
| Weekly Development Opportunity Report | Read-only reporting over analyses, skipped patterns, project relevance, watchlist changes, and risk patterns. |
| Feedback -> Eval Case Loop | Operator-reviewed feedback capture into eval/golden-set candidate records; no automatic policy, prompt, threshold, or rollout mutation. |
| Technical Debt / Security / Operational Risk Detection | Evidence-based risk signals that can inform JudgeOutput, Analysis, and Notification without bypassing policy-engine. |

The LLM remains limited to structured judge output. Final verdict and delivery decision remain deterministic policy-engine responsibilities. Notifier remains presentation and transport only.

## 6. Future implementation sequencing

Recommended safe order:

1. Scope lock only - this slice.
2. ProjectProfile contract / fixture / matcher plan.
3. Watchlist contract / artifact follow-up plan.
4. Weekly report read-only CLI/report plan.
5. Feedback capture contract and eval-case candidate flow.
6. Risk signal taxonomy extension for EvidenceBundle/JudgeOutput/Analysis.
7. Only after the above, consider runtime delivery integration.

This sequence is not an implementation approval. It is a safe ordering note for future separately approved slices.

## 7. Acceptance criteria for future slices

Future slices that touch this scope must satisfy all of the following:

- They must cite README v20 and the relevant locked stage bundle before proposing behavior.
- They must preserve `SourceMessage -> Artifact -> CandidateGroup -> EvidenceBundle -> JudgeOutput -> Analysis -> Notification`.
- They must state whether the slice is docs-only, contract-only, fixture-only, test-only, CLI/report-only, or runtime-affecting.
- They must keep production rollout, live TDLib/Telegram use, DB/Redis production connectivity, Docker/systemd execution, and secret access unauthorized unless explicitly approved.
- They must keep final verdict and delivery decisions outside the LLM.
- They must keep notifier from reinterpreting analysis.
- They must keep Redis out of durable memory responsibilities.
- They must preserve append-only historical truth for replay, watchlist refresh, and follow-up.
- They must include explicit non-goals for automatic self-learning, automatic Codex execution, automatic PR generation, and autonomous closed-loop behavior.
- They must include validation appropriate to the approved slice type.
