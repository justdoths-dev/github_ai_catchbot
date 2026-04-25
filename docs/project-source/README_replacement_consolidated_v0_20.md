# Markdown 정리 폴더

이 README는 프로젝트 소스 파일 수 제한(40개)을 피하기 위한 **통합형 인덱스**다.  
현재 기준은 기존 통합 번들 세트(`00~10`)에 **application-plan 보조 문서 1개**를 추가한 상태다.

## 현재 해석 기준

- 최신 진행 상태는 `44_delivery_gate_and_recovery_acceptance_hardening_v0_1.md`까지 반영된 상태다.
- stage 5 evidence layer는 `33_evidence_assembler_integration_hardening_v0_1.md`에서 operationally 닫힌 상태로 본다.
- stage 6 judge pipeline은
  - `34_analysis_router_skeleton_and_code_draft_v0_1.md`
  - `35_judge_openai_skeleton_and_code_draft_v0_1.md`
  - `36_analysis_validator_skeleton_and_code_draft_v0_1.md`
  - `37_policy_engine_skeleton_and_code_draft_v0_1.md`
  까지 닫힌 상태로 본다.
- stage 7 delivery layer는
  - `38_notifier_telegram_skeleton_and_code_draft_v0_1.md`
  - `39_notifier_telegram_integration_hardening_v0_1.md`
  - `40_end_to_end_delivery_acceptance_and_compose_hardening_v0_1.md`
  - `41_delivery_retry_promotion_and_replay_hardening_v0_1.md`
  - `42_delivery_operations_observability_and_dead_letter_hardening_v0_1.md`
  - `43_delivery_gate_runner_and_batch_recovery_code_draft_v0_1.md`
  - `44_delivery_gate_and_recovery_acceptance_hardening_v0_1.md`
  까지 닫힌 상태로 본다.
- 아키텍처 불변식은 유지한다.

```text
SourceMessage -> Artifact -> CandidateGroup -> EvidenceBundle -> JudgeOutput -> Analysis -> Notification
```

## 현재 소스 해석 우선순위

1. 최신 README (`README_replacement_consolidated_v0_20.md`)
2. 정본 단계 번들 (`00~04`)
3. 구현 초안 번들 (`05~10`)
4. advisory design note (`03_GitHub_AI_application_plan.md`)

중요:
- `03_GitHub_AI_application_plan.md`는 **적용 가능 자산 검토 문서**다.
- 이 문서는 architecture/phase authority가 아니다.
- phase ordering이나 service ownership이 충돌할 때는 **최신 README와 정본 단계 문서가 우선**이다.

## 권장 현재 업로드 세트 (13 files)

### 1. README
- `README_replacement_consolidated_v0_20.md`

### 2. 설계/정본 번들
- `00_foundations_stage0_stage1_bundle_v0_1.md`
- `01_runtime_collector_design_stage2_stage3_bundle_v0_1.md`
- `02_normalization_enrichment_design_stage4_stage5_bundle_v0_1.md`
- `03_judge_delivery_operations_stage6_stage10_bundle_v0_1.md`
- `04_execution_contracts_migrations_stage11_stage12_bundle_v0_1.md`

### 3. 구현 초안 번들
- `05_migration_code_drafts_stage13_stage16_bundle_v0_1.md`
- `06_collector_implementation_stage17_stage25_bundle_v0_1.md`
- `07_outbox_normalizer_stage26_stage28_bundle_v0_1.md`
- `08_enrichers_assembler_stage29_stage32_bundle_v0_1.md`
- `09_analysis_pipeline_stage33_stage38_bundle_v0_1.md`
- `10_delivery_hardening_stage39_plus_v0_1.md`

### 4. 추가 문서
- `03_GitHub_AI_application_plan.md`

## 파일 수 상태

- 권장 현재 세트: 13 files
- 40개 제한까지 여유: 27 files

## 현재 단계 요약

### 완료된 범위
- stage 0~12 설계/계약/DDL 정본
- stage 13~16 migration code draft
- stage 17~25 collector 구현 초안
- stage 26 outbox-relay
- stage 27~28 router-normalizer
- stage 29 gh-enricher
- stage 30 x-enricher
- stage 31 web-enricher
- stage 32 evidence-assembler skeleton
- stage 33 evidence-assembler integration hardening
- stage 34 analysis-router skeleton and code draft
- stage 35 judge-openai skeleton and code draft
- stage 36 analysis-validator skeleton and code draft
- stage 37 policy-engine skeleton and code draft
- stage 38 notifier-telegram skeleton and code draft
- stage 39 notifier-telegram integration hardening
- stage 40 end-to-end delivery acceptance and compose hardening
- stage 41 delivery retry promotion and replay hardening
- stage 42 delivery operations observability and dead-letter hardening
- stage 43 delivery gate runner and batch recovery code draft
- stage 44 delivery gate and recovery acceptance hardening

## 구현 착수 판단

현재 소스 세트는 **실제 repo 구현 코딩을 시작하기에 충분한 상태**로 본다.

이 판단의 의미는 아래와 같다.

- 아키텍처 불변식이 문서상 잠겨 있다.
- 실행 계약 / migration 정본이 durable schema 와 queue contract 를 고정한다.
- stage 17~44 구현 초안이 서비스별 skeleton / hardening / delivery control-plane acceptance 까지 내려와 있다.
- 이후에는 새 구조 설계보다 **문서에 잠긴 내용을 코드로 옮기는 작업** 이 우선이다.

## 다음 권장 순서

이제 자연스러운 다음 순서는 **실제 repo 구현 코딩 시작** 이다.

1. migration 실제 파일 정리
2. collector → outbox-relay → router-normalizer
3. gh/x/web enricher → evidence-assembler
4. analysis-router → judge-openai → validator → policy-engine → notifier
5. maintenance / delivery control-plane CLI

즉, stage 44 이후에는 **문서 확장보다 구현 전개가 우선** 이다.

## 기존 파일 → 현재 세트 매핑

### A. 기존 정본 단계 설계
- `00_overview_10_steps.md`
- `00_stage0_product_contract.md`
- `01_stage1_accounts_permissions_keys.md`
  - → `00_foundations_stage0_stage1_bundle_v0_1.md`

- `02_stage2_vps_runtime.md`
- `03_stage3_telegram_collector.md`
  - → `01_runtime_collector_design_stage2_stage3_bundle_v0_1.md`

- `04_stage4_trigger_normalization.md`
- `05_stage5_external_enrichers.md`
  - → `02_normalization_enrichment_design_stage4_stage5_bundle_v0_1.md`

- `06_stage6_llm_judge.md`
- `07_stage7_telegram_delivery_policy.md`
- `08_stage8_observability_replay_recovery.md`
- `09_stage9_quality_tuning_eval_framework.md`
- `10_stage10_rollout_cutover_governance.md`
  - → `03_judge_delivery_operations_stage6_stage10_bundle_v0_1.md`

- `11_stage11_execution_contracts_v0_1.md`
- `12_migration_spec_0001_0004_v0_1.md`
  - → `04_execution_contracts_migrations_stage11_stage12_bundle_v0_1.md`

### B. migration 구현 초안
- `13_response_0001_ingest_core_draft.md`
- `14_response_0002_normalization_candidates_draft.md`
- `15_response_0003_enrichment_bundles_draft.md`
- `16_response_0004_judge_delivery_observability_draft.md`
  - → `05_migration_code_drafts_stage13_stage16_bundle_v0_1.md`

### C. collector 구현 문서
- `17_collector_telegram_skeleton_spec_v0_1.md`
- `18_collector_telegram_code_skeleton_package_v0_1.md`
- `19_collector_telegram_bootstrap_code_draft_v0_1.md`
- `20_collector_tdlib_auth_code_draft_v0_1.md`
- `21_collector_repository_outbox_idempotency_code_draft_v0_1.md`
- `22_collector_projection_dispatch_handlers_code_draft_v0_1.md`
- `23_collector_reconcile_registry_sync_code_draft_v0_1.md`
- `24_collector_health_observability_code_draft_v0_1.md`
- `25_collector_acceptance_hardening_code_draft_v0_1.md`
  - → `06_collector_implementation_stage17_stage25_bundle_v0_1.md`

### D. collector 이후 구현 문서
- `26_outbox_relay_skeleton_and_code_draft_v0_1.md`
- `27_router_normalizer_skeleton_and_code_draft_v0_1.md`
- `28_router_normalizer_consumer_integration_hardening_v0_1.md`
  - → `07_outbox_normalizer_stage26_stage28_bundle_v0_1.md`

- `29_gh_enricher_skeleton_and_code_draft_v0_1.md`
- `30_x_enricher_skeleton_and_code_draft_v0_1.md`
- `31_web_enricher_skeleton_and_code_draft_v0_1.md`
- `32_evidence_assembler_skeleton_and_code_draft_v0_1.md`
  - → `08_enrichers_assembler_stage29_stage32_bundle_v0_1.md`

- `33_evidence_assembler_integration_hardening_v0_1.md`
- `34_analysis_router_skeleton_and_code_draft_v0_1.md`
- `35_judge_openai_skeleton_and_code_draft_v0_1.md`
- `36_analysis_validator_skeleton_and_code_draft_v0_1.md`
- `37_policy_engine_skeleton_and_code_draft_v0_1.md`
- `38_notifier_telegram_skeleton_and_code_draft_v0_1.md`
  - → `09_analysis_pipeline_stage33_stage38_bundle_v0_1.md`

- `39_notifier_telegram_integration_hardening_v0_1.md`
- `40_end_to_end_delivery_acceptance_and_compose_hardening_v0_1.md`
- `41_delivery_retry_promotion_and_replay_hardening_v0_1.md`
- `42_delivery_operations_observability_and_dead_letter_hardening_v0_1.md`
- `43_delivery_gate_runner_and_batch_recovery_code_draft_v0_1.md`
- `44_delivery_gate_and_recovery_acceptance_hardening_v0_1.md`
  - → `10_delivery_hardening_stage39_plus_v0_1.md`

## README 계열 처리 규칙

프로젝트 소스 업로드 대상에서는 아래 README 중간본은 제외하는 것을 권장한다.

- `README.md`
- `README_minimal_update_v0_1.md`
- `README_minimal_update_v0_2.md`
- `README_minimal_update_v0_3.md`
- `README_minimal_update_v0_4.md`
- `README_minimal_update_v0_5.md`
- `README_replacement_consolidated_v0_6.md`
- `README_replacement_consolidated_v0_7.md`
- `README_replacement_consolidated_v0_8.md`
- `README_replacement_consolidated_v0_9.md`
- `README_replacement_consolidated_v0_10.md`
- `README_replacement_consolidated_v0_11.md`
- `README_replacement_consolidated_v0_12.md`
- `README_replacement_consolidated_v0_13.md`
- `README_replacement_consolidated_v0_14.md`
- `README_replacement_consolidated_v0_15.md`
- `README_replacement_consolidated_v0_16.md`
- `README_replacement_consolidated_v0_17.md`
- `README_replacement_consolidated_v0_18.md`
- `README_replacement_consolidated_v0_19.md`

즉, 프로젝트 소스에는 **이 문서 1개만 authoritative README로 유지** 하는 것이 맞다.

## 현재 소스 충돌 정리

### A. README v6 ~ v19 동시 존재
- v6: latest = 32
- v7: latest = 33
- v8: latest = 34
- v9: latest = 35
- v10: latest = 36
- v11: latest = 37
- v12: latest = 38
- v13: latest = 38 + next step declaration
- v14: latest = 39
- v15: latest = 40
- v16: latest = 41
- v17: latest = 42
- v18: latest = 43
- v19: latest = 44

현재 기준에서는 **v20만 authoritative** 다.  
v6 ~ v19는 이력성 중간본으로 본다.

### B. standalone 33~38와 새 09 bundle의 공존
현재부터는 **`09_analysis_pipeline_stage33_stage38_bundle_v0_1.md`를 우선** 한다.
- 이유: 내용은 그대로 유지한 채 파일 수를 줄이기 위함이다.
- standalone `33~38`는 이력성 중간 산출물로 보고, 프로젝트 소스 업로드 대상에서는 제외하는 편이 맞다.

### C. standalone 39~44와 새 10 bundle의 공존
현재부터는 **`10_delivery_hardening_stage39_plus_v0_1.md`를 우선** 한다.
- 이유: 내용은 그대로 유지한 채 파일 수를 줄이기 위함이다.
- standalone `39~44`는 이력성 중간 산출물로 보고, 프로젝트 소스 업로드 대상에서는 제외하는 편이 맞다.

### D. application plan의 phase snapshot
`03_GitHub_AI_application_plan.md`는 외부 자산 적용 검토 문서다.  
이 문서의 phase snapshot이 현재보다 늦거나 빨라도, phase ordering authority는 아니다.

## 다음 통합 원칙

- 기존 번들 `00~10`은 다시 쪼개지 않는다.
- 이후에는 delivery 문서를 추가로 늘리기보다 **실제 repo 구현 코딩** 을 우선한다.
- implementation 진행 중 구조 충돌이 실제로 드러날 때만 별도 corrective markdown을 추가한다.
