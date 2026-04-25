# 04 execution contracts migrations stage11 stage12 bundle v0 1
이 문서는 프로젝트 소스 파일 수 제한(40개)을 피하기 위한 **통합 번들**이다.
구조 변경 문서가 아니며, 아래 원본 문서를 **순서와 내용을 보존한 채** 묶는다.

## 포함 원본 파일
- `11_stage11_execution_contracts_v0_1.md`
- `12_migration_spec_0001_0004_v0_1.md`

## 통합 원칙
- 아키텍처 불변식은 유지한다.
- 원본 파일명은 아래 섹션 제목으로 보존한다.
- 충돌 시 해석 우선순위는 기존과 동일하게 정본 단계 문서 → 실행 계약 → migration 정본 → 구현 초안이다.


---

## Source file: `11_stage11_execution_contracts_v0_1.md`

# 11단계: 실행 계약 패키지 초안 (Execution Contracts v0.1)

## 0. 문서 목적

이 문서는 0~10단계 설계를 다시 논의하는 문서가 아니다. 목적은 이미 잠긴 제품/아키텍처 결정을 **실제 구현 가능한 계약**으로 내리는 것이다. 따라서 이 문서는 다음 네 가지를 고정한다.

1. PostgreSQL durable schema  
2. Redis queue / lock / retry 계약  
3. 서비스 간 이벤트 / 입출력 계약  
4. config / feature flag / rollout / rollback 표준  

이 문서는 설계를 바꾸지 않는다. 상세 문서 간 충돌이 있으면 최소 변경 해석을 따른다. 전체 단계 파일 목록과 단계 간 선후 제약은 README 인덱스와 각 단계 문서를 정본으로 본다.

---

## 1. 고정 전제

### 1-1. 아키텍처 불변식

아래 경계는 구현 중에도 유지한다.

`SourceMessage -> Artifact -> CandidateGroup -> EvidenceBundle -> JudgeOutput -> Analysis -> Notification`

그리고 각 계층 책임은 다음처럼 고정한다.

- **collector**: Telegram 원문/수정/삭제/재조정 보존만 수행
- **normalizer**: deterministic, non-LLM trigger/canonicalization/proposal만 수행
- **enricher**: 외부 증거 수집만 수행
- **judge**: structured `judge_output_v1`만 생성
- **policy engine**: 최종 `analysis_v1.verdict`, `delivery_decision` 계산
- **notifier**: presentation / delivery만 수행

### 1-2. 런타임 불변식

- 초기 배포는 **단일 VPS + Docker Compose**
- **PostgreSQL = system of record**
- **Redis = queue / lock / short-lived execution state**
- **prod에는 live Telegram collector 1개만 존재**
- **dev는 live ingest 금지, replay 중심**
- **replay는 overwrite가 아니라 new run / new version 생성**

### 1-3. 제품 운영 철학

- **precision-first**
- **negative-first**
- **inspect_now 정확도 우선**
- **`ai` 단독은 hard trigger 금지**
- **text-only idea를 구조적으로 배제하지 않음**

---

## 2. MVP 경계

### 2-1. MVP에 포함

초기 runnable MVP는 아래 범위만 포함한다.

- `collector-telegram`
- `outbox-relay`
- `router-normalizer`
- `gh-enricher`
- `x-enricher`
- `text_idea` snapshot 경로
- `evidence-assembler`
- `analysis-router`
- `judge-openai` (`gpt-5.4-mini` 기본)
- `analysis-validator`
- `policy-engine`
- `notifier-telegram`
- 최소 observability / replay / DLQ / retry 기반 객체

### 2-2. MVP에서 제외 또는 flag-off

- inbound bot command plane
- digest 기본 운영
- OCR/media understanding
- headless browser web crawling
- GitHub full clone 기본 전략
- multi-node/Kubernetes
- full eval/governance UI
- `gpt-5.4` escalation 기본 활성화

---

## 3. 서비스 책임 매트릭스

아래 표는 실제 구현 단위다. 서비스 경계는 런타임/큐/secret 주입 경계와 동일하게 취급한다.

| 서비스 | 핵심 책임 | 직접 쓰는 durable 테이블 | 허용 secret |
|---|---|---|---|
| `collector-telegram` | TDLib 세션 유지, live ingest, reconcile/backfill, versioning, outbox 적재 | `telegram_channel_registry`, `telegram_raw_updates`, `source_messages`, `source_message_versions`, `event_outbox` | Telegram reader 계정 자격증명만 |
| `outbox-relay` | pending outbox 발행 | `event_outbox`, `job_attempts` | 없음 |
| `router-normalizer` | deterministic trigger, canonicalization, artifact upsert, suppression trace, candidate proposal 생성 | `normalization_runs`, `normalization_suppression_traces`, `artifact_registry`, `artifact_observations`, `candidate_group_proposals`, `candidate_group_members`, `event_outbox` | 없음 |
| `gh-enricher` | GitHub repo/subpath/page/gist snapshot 수집 | `artifact_enrichment_runs`, `artifact_snapshots`, `artifact_snapshot_github_repo`, `artifact_snapshot_github_file_samples`, `discovered_url_observations`, `event_outbox` | GitHub App만 |
| `x-enricher` | X post snapshot 수집 | `artifact_enrichment_runs`, `artifact_snapshots`, `artifact_snapshot_x_post`, `discovered_url_observations`, `event_outbox` | X bearer token만 |
| `web-enricher` | article metadata/excerpt 수집 | `artifact_enrichment_runs`, `artifact_snapshots`, `artifact_snapshot_web_article`, `discovered_url_observations`, `event_outbox` | 없음 |
| `evidence-assembler` | bundle 조립, reroot, `text_idea` snapshot, ready-for-analysis 판정 | `artifact_snapshot_text_idea`, `candidate_reroot_events`, `candidate_evidence_bundles`, `candidate_evidence_members`, `event_outbox` | 없음 |
| `analysis-router` | judge profile/model 선택, escalation 여부 결정, re-enrich 경로 분기 | `judge_runs`, `event_outbox` | 없음 |
| `judge-openai` | Responses API 호출, structured output 수집 | `judge_runs`, `judge_outputs`, `event_outbox` | OpenAI key만 |
| `analysis-validator` | schema/business validation, retry/refusal 처리 | `judge_runs`, `state_transitions`, `event_outbox` | 없음 |
| `policy-engine` | final verdict/delivery decision 계산 | `analyses`, `state_transitions`, `event_outbox` | 없음 |
| `notifier-telegram` | render, send/edit, delivery 기록 | `notification_plans`, `notification_renders`, `notification_delivery_records`, `state_transitions`, `event_outbox` | Telegram bot token만 |
| `maintenance` | retry promotion, rebuild, replay, stale job cleanup | `pipeline_runs`, `job_attempts`, `dead_letter_entries`, `replay_requests`, `event_outbox` | 기본적으로 없음 |

### 구현 주의

- **canonicalization 규칙은 공유 라이브러리 1개**로 유지한다.
- enricher는 URL을 발견할 수는 있지만, **자기 방식으로 artifact를 정의하면 안 된다**.
- reroot는 fetcher가 아니라 **evidence-assembler 단일 지점**에서만 반영한다.

---

## 4. PostgreSQL schema v1

### 4-1. 공통 원칙

- 내부 PK는 초안 기본값으로 `uuid` 사용
- raw/current/history를 분리한다
- current aggregate는 pointer를 가지되, history/snapshot은 append-only로 유지한다
- 큰 raw payload는 blob cache ref를 우선 사용하고, 본문 전체 inline 저장을 최소화한다
- Redis job payload는 얇게 유지하고, 실제 데이터는 PostgreSQL에서 재조회한다

### 4-2. migration 패키지 분할

1. `0001_ingest_core`
2. `0002_normalization_candidates`
3. `0003_enrichment_bundles`
4. `0004_judge_delivery_observability`
5. `0005_eval_governance`  
   - 첫 runnable MVP에서는 제외 가능

---

### 4-3. enum / 상태 카탈로그

#### 명시적으로 문서에서 잠긴 enum

- `artifact_type`
  - `github_repo`
  - `github_subpath`
  - `github_gist`
  - `github_repo_page`
  - `x_post`
  - `web_article`
  - `text_idea`
  - `unknown_link`
  - `short_url_unresolved`
- `verdict`
  - `inspect_now`
  - `later`
  - `skip`
- `delivery_decision`
  - `send_now`
  - `send_digest`
  - `suppress`
- `urgency_profile`
  - `high`
  - `normal_silent`
  - `digest`
  - `suppressed`

#### 초안 기본값으로 두는 상태 enum

아래는 실행 계약을 위해 추가하는 **초안 기본값**이다.

- `outbox_status`
  - `pending`, `published`, `failed`
- `snapshot_status`
  - `pending`, `fetching`, `ready`, `partial_ready`, `failed_transient`, `failed_permanent`, `rate_limited`, `access_denied`, `unsupported`, `low_evidence`
- `notification_status`
  - `planned`, `rendered`, `queued`, `sent`, `edited`, `suppressed`, `failed_retryable`, `failed_terminal`
- `job_attempt_status`
  - `pending`, `running`, `succeeded`, `failed_retryable`, `failed_terminal`, `abandoned`
- `replay_type`
  - `source`, `enrich`, `judge`, `delivery`, `full_pipeline`

---

### 4-4. `0001_ingest_core`

#### `telegram_channel_registry`

역할:
- 추적 대상 채널 registry
- onboarding / access / history sync anchor
- prod tracked channel의 단일 기준점

핵심 필드:
- `registry_id`
- `source_kind`
- `source_value`
- `desired_state`
- `access_state`
- `chat_id`
- `username_snapshot`
- `title_snapshot`
- `chat_type`
- `last_resolved_at`
- `last_join_attempt_at`
- `last_history_sync_at`
- `last_seen_message_id`
- `last_seen_message_date`
- `priority_weight`
- `notes`
- `created_at`
- `updated_at`

제약:
- active row 기준 `(source_kind, source_value)` unique
- `chat_id` nullable 허용, resolve 후 채움
- `chat_id` 인덱스 필수

#### `telegram_raw_updates`

역할:
- TDLib raw update journal
- replay/debug/reconcile 근거
- 장기 시스템 오브 레코드는 아니지만, 단기 재현 근거

핵심 필드:
- `update_seq` bigserial PK
- `received_at`
- `update_type`
- `chat_id`
- `message_id`
- `payload_json`
- `apply_status`
- `applied_at`
- `error_text`

인덱스:
- `(apply_status, received_at)`
- `(chat_id, message_id)`
- retention purge용 `received_at`

#### `source_messages`

역할:
- Telegram message current canonical row
- 후단 normalizer가 재조회하는 기준 row

핵심 필드:
- `source_message_id`
- `platform` default `telegram`
- `chat_id`
- `message_id`
- `logical_post_key`
- `is_channel_post`
- `posted_at`
- `edited_at`
- `deleted_at`
- `delete_kind`
- `current_version_no`
- `message_link`
- `author_signature`
- `forward_info_json`
- `content_type`
- `text_body`
- `caption_text`
- `text_surface`
- `entities_json`
- `url_surface_json`
- `raw_message_json`
- `first_seen_at`
- `last_seen_at`

제약:
- unique `(platform, chat_id, message_id)`
- 인덱스 `(logical_post_key)`
- 인덱스 `(deleted_at)`
- 인덱스 `(last_seen_at desc)`

#### `source_message_versions`

역할:
- immutable revision history
- edit/content_change/reconcile/delete_marker 추적

핵심 필드:
- `source_message_version_id`
- `source_message_id`
- `version_no`
- `version_reason`
- `observed_at`
- `telegram_edit_date`
- `text_surface`
- `entities_json`
- `raw_message_json`
- `content_hash`

제약:
- unique `(source_message_id, version_no)`
- unique `(source_message_id, content_hash)`는 강제하지 않음  
  - 같은 본문이 다른 이유로 관측될 수 있으므로
- 인덱스 `(source_message_id, observed_at desc)`

#### `event_outbox`

역할:
- collector 및 각 서비스의 transactional outbox
- DB commit과 Redis publish를 분리하는 안전 경계

핵심 필드:
- `event_id`
- `event_type`
- `aggregate_type`
- `aggregate_id`
- `dedupe_key`
- `payload_json`
- `status`
- `published_at`
- `fail_count`
- `last_error`
- `created_at`

제약:
- unique `(dedupe_key)` 권장
- 인덱스 `(status, created_at)`

---

### 4-5. `0002_normalization_candidates`

#### `normalization_runs`

역할:
- deterministic normalization execution row
- source version + normalizer version 단위의 idempotent 결과 기록

핵심 필드:
- `normalization_run_id`
- `source_message_id`
- `source_version_no`
- `normalizer_version`
- `signal_detected`
- `candidate_eligible`
- `trigger_strength`
- `completed_at`
- `result_hash`

제약:
- unique `(source_message_id, source_version_no, normalizer_version)`

#### `normalization_suppression_traces`

역할:
- suppress 이유를 버리지 않고 구조화 저장
- weak AI noise나 low-context text suppress 근거 보존

핵심 필드:
- `suppression_trace_id`
- `normalization_run_id`
- `reason_code`
- `trigger_strength`
- `notes_json`
- `created_at`

#### `artifact_registry`

역할:
- canonical artifact registry
- artifact 1층 dedupe 기준점

핵심 필드:
- `artifact_id`
- `artifact_type`
- `canonical_id`
- `canonical_url`
- `normalized_host`
- `artifact_key_json`
- `current_snapshot_id`
- `current_status`
- `created_at`
- `updated_at`

제약:
- unique `(canonical_id)`
- 인덱스 `(artifact_type, normalized_host)`

#### `artifact_observations`

역할:
- source message version이 특정 artifact를 어떻게 관측했는지 기록
- URL provenance를 설명 가능한 상태로 유지

핵심 필드:
- `artifact_observation_id`
- `artifact_id`
- `source_message_id`
- `source_version_no`
- `observed_url`
- `source_kind`
- `normalized_url`
- `resolved_url`
- `canonical_url`
- `classification`
- `context_path`
- `created_at`

제약:
- 인덱스 `(artifact_id, created_at desc)`
- 인덱스 `(source_message_id, source_version_no)`

#### `candidate_group_proposals`

역할:
- 이름은 `proposal`이지만, 구현상 **durable candidate-group aggregate**로 사용
- proposal origin과 current state를 함께 가진다

핵심 필드:
- `candidate_group_id`
- `source_message_id`
- `source_version_no`
- `initial_primary_artifact_id`
- `current_primary_artifact_id`
- `proposal_status`
- `normalizer_version`
- `dedupe_subject_key`
- `current_bundle_id`
- `current_analysis_id`
- `created_at`
- `updated_at`

제약:
- unique `(source_message_id, source_version_no, dedupe_subject_key)`
- 인덱스 `(current_primary_artifact_id)`
- 인덱스 `(proposal_status, created_at)`

#### `candidate_group_members`

역할:
- candidate membership 구조 보존
- primary/supporting/inferred anchor 구분 유지

핵심 필드:
- `candidate_group_member_id`
- `candidate_group_id`
- `artifact_id`
- `member_role`
- `member_order`
- `created_at`

제약:
- unique `(candidate_group_id, artifact_id, member_role)`

---

### 4-6. `0003_enrichment_bundles`

#### `artifact_enrichment_runs`

역할:
- artifact별 enrichment execution record
- provider/job profile/refresh mode 추적

핵심 필드:
- `artifact_enrichment_run_id`
- `artifact_id`
- `provider`
- `refresh_mode`
- `depth_budget`
- `requested_at`
- `started_at`
- `finished_at`
- `status`
- `content_anchor`
- `job_idempotency_key`

제약:
- unique `(job_idempotency_key)`

#### `artifact_snapshots`

역할:
- 모든 snapshot의 공통 append-only 부모 테이블
- content anchor와 status를 source-independent하게 보존

핵심 필드:
- `snapshot_id`
- `artifact_id`
- `provider`
- `snapshot_type`
- `status`
- `fetched_at`
- `content_anchor`
- `auth_mode`
- `normalized_projection`
- `raw_payload_ref`
- `evidence_limitations`
- `fetch_anomalies`

제약:
- unique `(artifact_id, provider, content_anchor, snapshot_type)`
- 인덱스 `(artifact_id, fetched_at desc)`

#### `artifact_snapshot_github_repo`

역할:
- GitHub repo/subpath/page/gist 관련 structured projection
- shallow evidence를 역할 기반으로 저장

핵심 필드:
- `snapshot_id` FK
- `repo_full_name`
- `default_branch`
- `resolved_ref`
- `content_anchor_commit_sha`
- `repo_flags_json`
- `license_spdx`
- `topics_json`
- `readme_excerpt`
- `detected_build_systems_json`
- `detected_languages_json`
- `key_paths_json`
- `test_paths_json`
- `ci_paths_json`
- `examples_paths_json`
- `docs_paths_json`
- `release_summary_json`

#### `artifact_snapshot_github_file_samples`

역할:
- GitHub sampled file detail child table
- 전체 repo 내용을 inline하지 않고 역할 기반 excerpt만 보존

핵심 필드:
- `file_sample_id`
- `snapshot_id`
- `path`
- `role`
- `size_bytes`
- `content_hash`
- `excerpt`
- `raw_blob_ref`

#### `artifact_snapshot_x_post`

역할:
- X post structured snapshot
- post ID, edit lineage, referenced posts, discovered links, metrics 보존

핵심 필드:
- `snapshot_id`
- `post_id`
- `content_anchor_post_version`
- `author_summary_json`
- `text_full`
- `text_excerpt`
- `conversation_id`
- `referenced_post_ids_json`
- `discovered_links_json`
- `media_summary_json`
- `metrics_summary_json`

#### `artifact_snapshot_web_article`

역할:
- article metadata/excerpt 중심의 보수적 snapshot
- headless browser path는 포함하지 않음

핵심 필드:
- `snapshot_id`
- `final_url`
- `canonical_url_candidate`
- `site_name`
- `title`
- `description`
- `author`
- `published_at`
- `content_hash`
- `main_text_excerpt`
- `outbound_links_json`

#### `artifact_snapshot_text_idea`

역할:
- text-only idea 후보의 local structured snapshot
- source message version anchored representation

핵심 필드:
- `snapshot_id`
- `source_message_id`
- `source_version_no`
- `hash_surface`
- `display_surface`
- `dev_context_signals_json`

#### `discovered_url_observations`

역할:
- GitHub/X/web evidence 내부에서 발견된 URL 기록
- canonicalization은 공유 계층에서 재사용하며, 이 테이블은 observation만 보존

핵심 필드:
- `discovered_url_observation_id`
- `parent_candidate_group_id`
- `parent_artifact_id`
- `parent_snapshot_id`
- `observed_url`
- `context_path`
- `discovery_reason`
- `depth_remaining`
- `created_at`

#### `candidate_reroot_events`

역할:
- primary 변경 lineage table
- reroot는 overwrite가 아니라 history event로 저장

핵심 필드:
- `candidate_reroot_event_id`
- `candidate_group_id`
- `from_artifact_id`
- `to_artifact_id`
- `reason_code`
- `trigger_snapshot_id`
- `created_at`

#### `candidate_evidence_bundles`

역할:
- LLM 입력 직전의 candidate-centered bundle
- current primary와 supporting summaries를 묶는 append-only row

핵심 필드:
- `bundle_id`
- `candidate_group_id`
- `initial_primary_artifact_id`
- `current_primary_artifact_id`
- `bundle_version`
- `bundle_profile_version`
- `bundle_input_hash`
- `reroot_count`
- `primary_summary`
- `supporting_summaries_json`
- `discovered_links_summary_json`
- `evidence_limitations`
- `ready_for_analysis`
- `token_budget_profile`
- `created_at`

제약:
- unique `(candidate_group_id, bundle_profile_version, bundle_input_hash)`

#### `candidate_evidence_members`

역할:
- bundle 구성 member tracking
- 어떤 snapshot들이 특정 bundle에 포함됐는지 보존

핵심 필드:
- `candidate_evidence_member_id`
- `bundle_id`
- `artifact_id`
- `snapshot_id`
- `member_role`
- `member_order`

---

### 4-7. `0004_judge_delivery_observability`

#### `judge_runs`

역할:
- judge execution root row
- profile/model/prompt/policy/usage telemetry를 한곳에 모음

핵심 필드:
- `judge_run_id`
- `bundle_id`
- `judge_profile`
- `model`
- `reasoning_effort`
- `prompt_version`
- `schema_version`
- `policy_version`
- `prompt_cache_key`
- `status`
- `schema_retry_count`
- `escalated_from_judge_run_id`
- `input_tokens`
- `cached_input_tokens`
- `output_tokens`
- `reasoning_tokens`
- `latency_ms`
- `finish_reason`
- `refusal_detected`
- `started_at`
- `finished_at`

제약:
- unique judge idempotency key  
  예: `(bundle_id, prompt_version, model, reasoning_effort)`

#### `judge_outputs`

역할:
- structured `judge_output_v1` append-only 저장
- model output과 final analysis를 분리하는 핵심 테이블

핵심 필드:
- `judge_output_id`
- `judge_run_id`
- `candidate_group_id`
- `judge_schema_version`
- `payload_json`
- `model_proposed_verdict`
- `model_confidence_band`
- `created_at`

#### `analyses`

역할:
- deterministic policy engine이 확정한 최종 `analysis_v1`
- verdict/delivery 결정의 유일한 durable 결과

핵심 필드:
- `analysis_id`
- `candidate_group_id`
- `judge_output_id`
- `schema_version`
- `policy_version`
- `prompt_version`
- `delivery_policy_version`
- `verdict`
- `delivery_decision`
- `scores_json`
- `reason_codes_json`
- `evidence_limitations_ko`
- `recommended_action_ko`
- `freshness_note_ko`
- `model_proposed_verdict`
- `policy_reconciled_flag`
- `created_at`

#### `notification_plans`

역할:
- 전달 의도 row
- analysis와 telegram send를 분리하는 첫 계층

핵심 필드:
- `notification_plan_id`
- `analysis_id`
- `candidate_group_id`
- `delivery_decision`
- `urgency_profile`
- `target_chat_id`
- `target_thread_id`
- `render_profile`
- `dedupe_subject_key`
- `material_change_hash`
- `send_after`
- `suppress_reason_code`
- `created_at`
- `status`

#### `notification_renders`

역할:
- Telegram 전송 직전 payload row
- 템플릿/render와 delivery result를 분리

핵심 필드:
- `notification_render_id`
- `notification_plan_id`
- `message_text`
- `entities_json`
- `link_preview_options_json`
- `reply_markup_json`
- `disable_notification`
- `protect_content`
- `parse_strategy`
- `render_hash`
- `created_at`

#### `notification_delivery_records`

역할:
- 실제 Telegram transport 결과 저장
- send/edit/failure를 분리 기록

핵심 필드:
- `notification_delivery_record_id`
- `notification_plan_id`
- `telegram_chat_id`
- `telegram_message_id`
- `delivery_status`
- `sent_at`
- `edited_at`
- `attempt_count`
- `transport_error_code`
- `transport_error_class`
- `telegram_response_json`

#### `pipeline_runs`

역할:
- live/bootstrap/replay/manual 전체 파이프라인 추적 루트

핵심 필드:
- `pipeline_run_id`
- `trigger_source`
- `run_kind`
- `root_object_type`
- `root_object_id`
- `started_at`
- `finished_at`
- `terminal_status`

#### `job_attempts`

역할:
- queue consumer 단위 실행/재시도 기록
- Redis 유실 후 재구성 기준 일부 제공

핵심 필드:
- `job_attempt_id`
- `stage_name`
- `queue_name`
- `root_object_type`
- `root_object_id`
- `attempt_no`
- `lease_owner`
- `started_at`
- `finished_at`
- `attempt_status`
- `error_code`
- `retry_after_at`

#### `state_transitions`

역할:
- aggregate lifecycle audit trail
- candidate, analysis, notification 상태 전이를 설명 가능하게 함

핵심 필드:
- `state_transition_id`
- `object_type`
- `object_id`
- `from_state`
- `to_state`
- `reason_code`
- `created_at`

#### `dead_letter_entries`

역할:
- stage별 DLQ durable record
- replay/manual intervention 근거 row

핵심 필드:
- `dead_letter_entry_id`
- `stage_name`
- `queue_name`
- `root_object_type`
- `root_object_id`
- `last_error_code`
- `last_error_snippet`
- `retry_count`
- `first_failed_at`
- `last_failed_at`
- `next_manual_action`
- `replay_hint`

#### `replay_requests`

역할:
- explicit replay intent row
- replay 종류와 root object 기준점 보존

핵심 필드:
- `replay_request_id`
- `replay_type`
- `root_object_type`
- `root_object_id`
- `requested_by`
- `requested_at`
- `status`

---

## 5. Redis 계약

### 5-1. queue primitive

초안 기본값은 **Redis Streams + consumer groups**다. 이유는 다음 세 가지다.

- low-to-moderate concurrency에 적합
- pending/ack/reclaim 흐름이 명확함
- stage별 queue를 쉽게 분리 가능

단, Redis는 durable source가 아니다. Streams에 남아 있는 메시지는 편의 상태일 뿐, 복구 기준은 PostgreSQL이다.

### 5-2. queue 이름

- `q.source.normalize`
- `q.artifact.enrich.github`
- `q.artifact.enrich.x`
- `q.artifact.enrich.web`
- `q.candidate.bundle`
- `q.analysis.route`
- `q.analysis.judge`
- `q.analysis.validate`
- `q.analysis.policy`
- `q.notification.send`
- `q.replay`
- `q.maintenance`

### 5-3. queue payload 원칙

Redis payload는 **ID만 싣는다**. 큰 JSON 본문을 넣지 않는다.

공통 최소 필드:
- `job_id`
- `stage_name`
- `root_object_type`
- `root_object_id`
- `idempotency_key`
- `pipeline_run_id`
- `not_before`
- `trigger_event_id`

### 5-4. lock scope

잠금은 `SET NX PX` 기반 짧은 TTL lock으로 충분하다.

권장 lock key:
- `singleton:collector:prod`
- `reconcile:chat:{chat_id}`
- `normalize:source_message:{source_message_id}:{version_no}`
- `enrich:{provider}:{artifact_id}`
- `bundle:{candidate_group_id}`
- `judge:{bundle_id}:{prompt_version}:{model}`
- `notify:{analysis_id}:{target_chat_id}`

### 5-5. delayed retry

지연 재시도는 queue별 sorted set을 둔다.

예:
- `z.retry.q.analysis.judge`
- `z.retry.q.notification.send`

`maintenance`가 due job을 원 queue로 승격한다.

### 5-6. Redis rebuild 원칙

Redis 유실 시 절차는 아래로 고정한다.

1. 모든 worker의 in-memory lease를 폐기
2. PostgreSQL의 `event_outbox`, `job_attempts`, `notification_plans`, `replay_requests`를 기준으로 pending work 재구성
3. Streams를 다시 채움
4. stale in-progress row는 `abandoned` 또는 retryable 상태로 전환

즉, **Redis 복구는 restore가 아니라 rebuild**다.

---

## 6. 서비스 간 이벤트 계약

모든 이벤트는 outbox를 통해 발행하고, version suffix를 갖는다. payload는 얇아야 하고, 수신 서비스는 PostgreSQL에서 canonical row를 재조회한다.

### 6-1. source ingest 계열

#### `source_message.created.v1`
- `event_id`
- `source_message_id`
- `current_version_no`
- `logical_post_key`
- `occurred_at`

#### `source_message.edited.v1`
- 위와 동일

#### `source_message.deleted.v1`
- 위와 동일 + `delete_kind`

#### `source_message.reconciled.v1`
- 위와 동일 + `reconcile_reason`

### 6-2. normalization / candidate 계열

#### `artifact.enrich.requested.v1`
- `event_id`
- `candidate_group_id`
- `artifact_id`
- `artifact_type`
- `provider_route`
- `refresh_mode`
- `depth_budget`

#### `candidate.bundle.refresh.v1`
- `event_id`
- `candidate_group_id`
- `trigger_kind`
- `trigger_object_type`
- `trigger_object_id`

### 6-3. enrichment 계열

#### `artifact.snapshot.updated.v1`
- `event_id`
- `artifact_id`
- `snapshot_id`
- `provider`
- `status`
- `content_anchor`

### 6-4. analysis 계열

#### `analysis.requested.v1`
- `event_id`
- `candidate_group_id`
- `bundle_id`
- `judge_profile`
- `escalation_allowed`

#### `judge.call.requested.v1`
- `event_id`
- `judge_run_id`
- `bundle_id`
- `model`
- `reasoning_effort`
- `prompt_version`
- `prompt_cache_key`

#### `judge.output.ready.v1`
- `event_id`
- `judge_run_id`
- `judge_output_id`
- `finish_reason`
- `refusal_detected`

#### `analysis.policy.apply.v1`
- `event_id`
- `judge_run_id`
- `judge_output_id`
- `candidate_group_id`
- `bundle_id`

### 6-5. delivery 계열

#### `notification.plan.created.v1`
- `event_id`
- `notification_plan_id`
- `analysis_id`
- `target_chat_id`
- `send_after`

#### `notification.delivery.result.v1`
- `event_id`
- `notification_plan_id`
- `delivery_status`
- `telegram_chat_id`
- `telegram_message_id`

### 6-6. replay 계열

#### `replay.requested.v1`
- `event_id`
- `replay_request_id`
- `replay_type`
- `root_object_type`
- `root_object_id`

---

## 7. Config / secret / feature flag 표준

### 7-1. secret 주입 방식

초안 기본값은 **Docker secrets**다.  
원칙은 아래로 고정한다.

- production long-lived secret은 plain `.env`에 두지 않음
- secret은 `_FILE` 기반 환경변수로 주입
- 서비스는 자기 secret만 읽음
- collector와 notifier secret은 절대 섞지 않음

### 7-2. 공통 환경변수

- `APP_ENV`
- `APP_TIMEZONE`
- `LOG_LEVEL`
- `DATABASE_URL`
- `REDIS_URL`
- `BLOB_CACHE_DIR`
- `STATE_DIR`

### 7-3. Telegram collector

- `TELEGRAM_API_ID`
- `TELEGRAM_API_HASH_FILE`
- `TELEGRAM_PHONE_NUMBER`
- `TELEGRAM_2FA_PASSWORD_FILE`
- `TDLIB_STATE_DIR`
- `COLLECTOR_MODE=live|replay`
- `RECONCILE_INTERVAL_SEC`
- `RECONCILE_BACKFILL_LIMIT`

### 7-4. Telegram notifier

- `TELEGRAM_BOT_TOKEN_FILE`
- `TELEGRAM_OPERATOR_CHAT_ID`
- `TELEGRAM_DEBUG_CHAT_ID`
- `TELEGRAM_DIGEST_CHAT_ID`

### 7-5. GitHub

- `GITHUB_APP_ID`
- `GITHUB_INSTALLATION_ID`
- `GITHUB_PRIVATE_KEY_FILE`

### 7-6. X

- `X_BEARER_TOKEN_FILE`

### 7-7. OpenAI judge

- `OPENAI_API_KEY_FILE`
- `OPENAI_PROJECT`
- `JUDGE_DEFAULT_MODEL`
- `JUDGE_ESCALATION_MODEL`
- `JUDGE_REASONING_EFFORT_DEFAULT`
- `JUDGE_REASONING_EFFORT_ESCALATION`
- `JUDGE_DAILY_CANDIDATE_CAP`
- `JUDGE_DAILY_INPUT_TOKEN_CAP`
- `JUDGE_MAX_ESCALATION_RATIO`

### 7-8. web enricher

- `WEB_FETCH_USER_AGENT`
- `WEB_FETCH_TIMEOUT_MS`
- `WEB_FETCH_MAX_BYTES`

---

## 8. Feature flag 계획

feature flag는 선택사항이 아니다. 이 시스템은 single-prod + staged rollout 구조라서, 단계별 on/off가 가능해야 한다.

### 8-1. 필수 flag

- `ENABLE_LIVE_COLLECTOR`
- `ENABLE_BACKFILL_ONBOARDING`
- `ENABLE_GAP_SCAN`
- `ENABLE_GITHUB_ENRICH`
- `ENABLE_X_ENRICH`
- `ENABLE_WEB_ENRICH`
- `ENABLE_TEXT_IDEA`
- `ENABLE_MODEL_ESCALATION`
- `ENABLE_LATER_DELIVERY`
- `ENABLE_SILENT_LATER`
- `ENABLE_NOTIFICATION_SEND`
- `ENABLE_CHANNEL_OVERRIDES`
- `ENABLE_REPLAY_TO_PROD_DB`

### 8-2. 초안 기본값

#### Phase 0 / offline
- live collector: off
- notify: off

#### Phase 1 / live ingest
- live collector: on
- notify: off
- escalation: off
- web enrich: off

#### Phase 2 / shadow analysis
- GitHub/X enrich: on
- text idea: on
- notify: off

#### Phase 3 / silent delivery
- notify: on
- later delivery: on
- silent later: on

#### Phase 4 / restricted live
- channel overrides: on 가능
- escalation: 필요 시만 on

#### Phase 5 / full go-live
- release gate 통과 후 전체 활성

---

## 9. 저장소 / 모듈 구조 초안

초안 기본값은 Python monorepo다. 구조는 아래처럼 분리한다.

```text
repo/
  README.md
  compose/
  docker/
  ops/
    runbooks/
    backups/
    rollout/
  migrations/
    0001_ingest_core/
    0002_normalization_candidates/
    0003_enrichment_bundles/
    0004_judge_delivery_observability/
    0005_eval_governance/
  contracts/
    events/
    schemas/
      source_message_v1/
      artifact_snapshot_v1/
      judge_output_v1/
      analysis_v1/
      notification_plan_v1/
    policies/
      trigger_rules_v1.yaml
      verdict_policy_v1.yaml
      delivery_policy_v1.yaml
    prompts/
      judge/
        github_primary/
        x_primary/
        text_idea_primary/
  src/
    common/
      config/
      db/
      redis/
      outbox/
      logging/
      ids/
      locks/
      blobstore/
      canonicalization/
    domain/
      source_messages/
      artifacts/
      candidates/
      snapshots/
      bundles/
      judge/
      analysis/
      notifications/
      replay/
      evals/
    services/
      collector_telegram/
      outbox_relay/
      router_normalizer/
      gh_enricher/
      x_enricher/
      web_enricher/
      evidence_assembler/
      analysis_router/
      judge_openai/
      analysis_validator/
      policy_engine/
      notifier_telegram/
      maintenance/
  tests/
    unit/
    component/
    replay/
    golden/
```

주의:
- **schema / policy / prompt / delivery template를 섞지 않는다**
- contracts 디렉터리는 코드 구현보다 먼저 정리한다

---

## 10. 구현 순서

### M1. contracts + migrations
산출물:
- `0001~0004` migration
- enum catalog
- idempotency key catalog
- event contract files
- config loader skeleton

종료 조건:
- aggregate ownership ambiguity 없음
- replay/overwrite 관련 구조적 충돌 없음

### M2. collector
산출물:
- TDLib boot
- channel registry onboarding
- raw update persistence
- source/current/version upsert
- reconcile/backfill
- outbox emit

종료 조건:
- duplicate/no-op 처리 안정
- live update + reconcile 동시 동작 검증

### M3. normalizer
산출물:
- surface normalization
- entity-first URL extraction
- short URL expansion
- artifact canonicalization
- suppression trace
- candidate group proposal

종료 조건:
- 같은 source version에 대해 deterministic replay 가능

### M4. enrich + bundle
산출물:
- GitHub snapshot
- X snapshot
- text_idea snapshot
- discovered URL observation
- reroot event
- evidence bundle

종료 조건:
- `ready / partial_ready / low_evidence` 구분 가능

### M5. judge pipeline
산출물:
- analysis-router
- judge-openai
- validator
- policy-engine
- analysis persistence
- usage telemetry

종료 조건:
- `judge_output_v1 -> analysis_v1` 승격 안정

### M6. notifier
산출물:
- plan / render / delivery record
- inline keyboard
- silent later
- dedupe subject / material change

종료 조건:
- operator DM single-alert flow 정상

### M7. observability / replay
산출물:
- pipeline_runs
- job_attempts
- DLQ
- replay_requests
- Redis rebuild
- stale job recovery

종료 조건:
- Redis flush 후 recovery 가능

### M8. rollout hardening
산출물:
- release gate runner
- rollout flag matrix
- restricted-live checklist

종료 조건:
- Phase 0~5 rollout 문서와 실제 flag가 연결됨

---

## 11. rollout / rollback 통제

### 11-1. rollout 고정 순서

1. **Offline fixture validation**
2. **Live ingest, no delivery**
3. **Shadow analysis**
4. **Silent delivery**
5. **Restricted live delivery**
6. **Full v1 go-live**

### 11-2. rollback 경계

#### collector 문제
- `ENABLE_LIVE_COLLECTOR=false`
- TDLib state 복구
- recent reconcile 재실행

#### enricher 문제
- 해당 source enricher flag만 off
- 다른 source는 계속 운영

#### judge 문제
- prompt rollback 또는 escalation off
- analysis path는 stable profile만 유지

#### notifier 문제
- `ENABLE_NOTIFICATION_SEND=false`
- analysis는 계속 생성
- delivery replay로 나중에 복구

#### Redis 문제
- restore 개념 금지
- PostgreSQL 기준 rebuild

이 원칙은 “문제 난 단계만 멈추고 다른 단계는 가능한 한 살린다”로 이해하면 된다.

---

## 12. idempotency key 카탈로그

아래 키들은 PostgreSQL unique guard와 Redis lock 모두에 반영한다.

- normalization  
  `norm:{source_message_id}:{version_no}:{normalizer_version}`

- GitHub enrich  
  `enrich:github:{artifact_id}:{refresh_mode}:{content_anchor_hint}`

- X enrich  
  `enrich:x:{artifact_id}:{refresh_mode}:{content_anchor_hint}`

- web enrich  
  `enrich:web:{artifact_id}:{refresh_mode}:{content_anchor_hint}`

- bundle  
  `bundle:{candidate_group_id}:{bundle_profile_version}:{bundle_input_hash}`

- judge  
  `judge:{bundle_id}:{prompt_version}:{model}:{reasoning_effort}`

- notify  
  `notify:{analysis_id}:{delivery_policy_version}:{target_chat_id}:{template_version}`

---

## 13. bounded assumptions register

아래는 문서에 명시돼 있지 않아 초안 기본값으로 둔 항목이다.

1. **Python 3.12 + SQLAlchemy/Alembic**
2. **Redis Streams**
3. **Docker secrets**
4. **초기 restricted rollout은 high-signal 5~8개 채널**
5. **`candidate_group_proposals`를 durable candidate aggregate로 해석**
6. **`artifact_snapshot_text_idea`는 evidence-assembler가 생성**
7. **`web-enricher`는 metadata/excerpt만 수집하고 기본 flag-off**
8. **`eval/governance` 계열 테이블은 첫 runnable migration에서 제외**

이 중 5번은 나중에 이름을 바꿀 수는 있어도, 지금은 기존 문서와 충돌을 피하려고 **테이블명은 유지**하고 **의미만 명확히 고정**하는 게 맞다.

---

## 14. 이 초안 기준에서 다음 턴에 바로 할 일

이 초안이 승인되면 다음 단계는 선택지가 두 개다.

### 선택지 A. 바로 migration 상세화
가장 추천한다.

다음 턴에서 바로 아래를 작성하면 된다.

- `0001_ingest_core` 상세 DDL
- `0002_normalization_candidates` 상세 DDL
- `0003_enrichment_bundles` 상세 DDL
- `0004_judge_delivery_observability` 상세 DDL

### 선택지 B. 먼저 contracts 디렉터리 초안
- event schema 파일명
- policy 파일명
- prompt 버전명
- repo tree skeleton

지금 순서상 더 맞는 건 **선택지 A**다. 이유는 PostgreSQL이 durable truth이고, 나머지 서비스 경계가 전부 그 위에 걸려 있기 때문이다.

---

## 최종 정리

이 초안의 핵심은 세 줄이다.

1. **DB가 먼저다.**
2. **queue는 얇게, durable truth는 Postgres에 둔다.**
3. **collector / normalizer / enricher / judge / policy / notifier 경계를 코드보다 먼저 계약으로 고정한다.**

다음 턴에서는 바로 **`0001_ingest_core`부터 `0004_judge_delivery_observability`까지 migration 상세안**으로 들어가는 게 맞다.


---

## Source file: `12_migration_spec_0001_0004_v0_1.md`

# 12단계: PostgreSQL Migration 상세안 v0.1 (`0001`~`0004`)

## 0. 목적

이 문서는 `11_stage11_execution_contracts.md`를 실제 migration 설계로 내리는 문서다. 범위는 다음 네 묶음이다.

- `0001_ingest_core`
- `0002_normalization_candidates`
- `0003_enrichment_bundles`
- `0004_judge_delivery_observability`

이 문서는 **구현 순서와 DDL 설계 원칙**을 고정한다. 아직 Alembic 코드 파일 자체를 작성하는 단계는 아니다. 다만 여기 적힌 구조는 Alembic migration으로 바로 옮길 수 있어야 한다.

---

## 1. 공통 설계 원칙

### 1-1. 확장 기능

초기 migration 공통 prelude:

- `pgcrypto` 활성화
  - `gen_random_uuid()` 사용 목적

초안 기본값:

```sql
CREATE EXTENSION IF NOT EXISTS pgcrypto;
```

### 1-2. 타입 정책

- ID: `uuid`
- Telegram `chat_id`, `message_id`: `bigint`
- version / attempt number: `integer`
- raw payload / structured output: `jsonb`
- 시간: `timestamptz`
- short enum: PostgreSQL native enum 사용
- 긴 상태/자유 키: `text`
- content hash / material hash: `char(64)` 또는 `text`
  - 구현 단순성을 위해 v1에서는 `text` 허용 가능

### 1-3. enum 전략

v1은 **핵심 closed set만 native enum**으로 둔다.

권장 native enum:
- `artifact_type_enum`
- `verdict_enum`
- `delivery_decision_enum`
- `urgency_profile_enum`
- `outbox_status_enum`
- `snapshot_status_enum`
- `notification_status_enum`
- `job_attempt_status_enum`
- `replay_type_enum`

이유:
- 문서상 의미가 잠겨 있음
- 분석/알림 분류에 직접 쓰임
- enum 변경 빈도가 낮음

### 1-4. timestamp 기본 정책

가능하면 아래 기본값을 사용한다.

- `created_at timestamptz NOT NULL DEFAULT now()`
- `updated_at timestamptz NOT NULL DEFAULT now()`

단, append-only history/snapshot row는 `updated_at` 불필요하다.

### 1-5. current pointer 정책

아래 aggregate는 current pointer를 가진다.

- `artifact_registry.current_snapshot_id`
- `candidate_group_proposals.current_bundle_id`
- `candidate_group_proposals.current_analysis_id`

하지만 이 포인터들은 참조 대상이 뒤 migration에서 생긴다. 따라서 다음 원칙으로 처리한다.

1. 초기 migration에서는 **nullable column만 먼저 생성**
2. 참조 대상 테이블이 만들어진 이후 migration에서 FK 추가
3. current pointer 업데이트는 application/service 책임

### 1-6. append-only 정책

아래는 append-only로 유지한다.

- `telegram_raw_updates`
- `source_message_versions`
- `normalization_runs`
- `normalization_suppression_traces`
- `artifact_observations`
- `artifact_enrichment_runs`
- `artifact_snapshots`
- source-specific snapshot tables
- `discovered_url_observations`
- `candidate_reroot_events`
- `candidate_evidence_bundles`
- `judge_runs`
- `judge_outputs`
- `analyses`
- `notification_delivery_records`
- `pipeline_runs`
- `job_attempts`
- `state_transitions`
- `dead_letter_entries`
- `replay_requests`

### 1-7. mutable current row 정책

아래는 mutable current row다.

- `telegram_channel_registry`
- `source_messages`
- `artifact_registry`
- `candidate_group_proposals`
- `notification_plans`
- `notification_renders`

---

## 2. Enum 상세안

### 2-1. `artifact_type_enum`

```text
github_repo
github_subpath
github_gist
github_repo_page
x_post
web_article
text_idea
unknown_link
short_url_unresolved
```

### 2-2. `verdict_enum`

```text
inspect_now
later
skip
```

### 2-3. `delivery_decision_enum`

```text
send_now
send_digest
suppress
```

### 2-4. `urgency_profile_enum`

```text
high
normal_silent
digest
suppressed
```

### 2-5. `outbox_status_enum`

```text
pending
published
failed
```

### 2-6. `snapshot_status_enum`

```text
pending
fetching
ready
partial_ready
failed_transient
failed_permanent
rate_limited
access_denied
unsupported
low_evidence
```

### 2-7. `notification_status_enum`

```text
planned
rendered
queued
sent
edited
suppressed
failed_retryable
failed_terminal
```

### 2-8. `job_attempt_status_enum`

```text
pending
running
succeeded
failed_retryable
failed_terminal
abandoned
```

### 2-9. `replay_type_enum`

```text
source
enrich
judge
delivery
full_pipeline
```

---

## 3. `0001_ingest_core` 상세안

## 3-1. 생성 순서

1. enum 생성
2. `telegram_channel_registry`
3. `telegram_raw_updates`
4. `source_messages`
5. `source_message_versions`
6. `event_outbox`
7. 기본 인덱스 생성

## 3-2. `telegram_channel_registry`

### 컬럼

| 컬럼 | 타입 | NULL | 기본값 | 비고 |
|---|---|---:|---|---|
| `registry_id` | `uuid` | N | `gen_random_uuid()` | PK |
| `source_kind` | `text` | N |  | `public_username` / `invite_link` / `chat_id` |
| `source_value` | `text` | N |  | 외부 식별 입력 |
| `desired_state` | `text` | N | `'active'` | `active` / `paused` / `removed` |
| `access_state` | `text` | N | `'unresolved'` | onboarding/access 상태 |
| `chat_id` | `bigint` | Y |  | 최종 anchor |
| `username_snapshot` | `text` | Y |  | |
| `title_snapshot` | `text` | Y |  | |
| `chat_type` | `text` | Y |  | channel / supergroup |
| `last_resolved_at` | `timestamptz` | Y |  | |
| `last_join_attempt_at` | `timestamptz` | Y |  | |
| `last_history_sync_at` | `timestamptz` | Y |  | |
| `last_seen_message_id` | `bigint` | Y |  | |
| `last_seen_message_date` | `timestamptz` | Y |  | |
| `priority_weight` | `integer` | N | `100` | 낮을수록 우선 아님. 운영 가중치 용도 |
| `notes` | `text` | Y |  | |
| `created_at` | `timestamptz` | N | `now()` | |
| `updated_at` | `timestamptz` | N | `now()` | |

### 제약

- PK: `registry_id`
- partial unique index:
  - `(source_kind, source_value)` where `desired_state <> 'removed'`

### 인덱스

- `idx_channel_registry_chat_id` on `(chat_id)`
- `idx_channel_registry_state` on `(desired_state, access_state)`

### 주의

`chat_id`는 resolve 후 채워진다. 따라서 초기에 nullable이 맞다.

---

## 3-3. `telegram_raw_updates`

### 컬럼

| 컬럼 | 타입 | NULL | 기본값 | 비고 |
|---|---|---:|---|---|
| `update_seq` | `bigserial` | N |  | PK |
| `received_at` | `timestamptz` | N | `now()` | |
| `update_type` | `text` | N |  | |
| `chat_id` | `bigint` | Y |  | 파싱 가능 시 저장 |
| `message_id` | `bigint` | Y |  | 파싱 가능 시 저장 |
| `payload_json` | `jsonb` | N |  | raw TDLib update |
| `apply_status` | `text` | N | `'pending'` | `pending/applied/failed` |
| `applied_at` | `timestamptz` | Y |  | |
| `error_text` | `text` | Y |  | |

### 제약

- PK: `update_seq`

### 인덱스

- `idx_raw_updates_apply_status_received_at` on `(apply_status, received_at)`
- `idx_raw_updates_chat_message` on `(chat_id, message_id)`
- `idx_raw_updates_received_at` on `(received_at)`

### 주의

이 테이블은 retention purge 대상이다. partitioning은 v1에서 보류한다.

---

## 3-4. `source_messages`

### 컬럼

| 컬럼 | 타입 | NULL | 기본값 | 비고 |
|---|---|---:|---|---|
| `source_message_id` | `uuid` | N | `gen_random_uuid()` | PK |
| `platform` | `text` | N | `'telegram'` | v1 고정 |
| `chat_id` | `bigint` | N |  | |
| `message_id` | `bigint` | N |  | |
| `logical_post_key` | `text` | N |  | album 고려 |
| `is_channel_post` | `boolean` | N | `false` | |
| `posted_at` | `timestamptz` | N |  | Telegram publish 시각 |
| `edited_at` | `timestamptz` | Y |  | |
| `deleted_at` | `timestamptz` | Y |  | tombstone |
| `delete_kind` | `text` | N | `'none'` | `none/permanent/cache_only` |
| `current_version_no` | `integer` | N | `1` | |
| `message_link` | `text` | Y |  | |
| `author_signature` | `text` | Y |  | |
| `forward_info_json` | `jsonb` | Y |  | |
| `content_type` | `text` | Y |  | text/photo/video/document/... |
| `text_body` | `text` | Y |  | |
| `caption_text` | `text` | Y |  | |
| `text_surface` | `text` | Y |  | searchable combined text |
| `entities_json` | `jsonb` | Y |  | |
| `url_surface_json` | `jsonb` | Y |  | entity/preview/regex surface |
| `raw_message_json` | `jsonb` | N |  | latest canonical raw message |
| `first_seen_at` | `timestamptz` | N | `now()` | |
| `last_seen_at` | `timestamptz` | N | `now()` | |

### 제약

- PK: `source_message_id`
- unique: `(platform, chat_id, message_id)`
- check: `platform = 'telegram'` optional

### 인덱스

- `idx_source_messages_logical_post_key` on `(logical_post_key)`
- `idx_source_messages_deleted_at` on `(deleted_at)`
- `idx_source_messages_last_seen_at` on `(last_seen_at desc)`
- `idx_source_messages_chat_posted` on `(chat_id, posted_at desc)`

### 주의

`text_surface`는 current canonical row 용도다. 정규화용 다른 surface는 0002 이후 normalizer 결과에서 다룬다.

---

## 3-5. `source_message_versions`

### 컬럼

| 컬럼 | 타입 | NULL | 기본값 | 비고 |
|---|---|---:|---|---|
| `source_message_version_id` | `uuid` | N | `gen_random_uuid()` | PK |
| `source_message_id` | `uuid` | N |  | FK |
| `version_no` | `integer` | N |  | |
| `version_reason` | `text` | N |  | `new/edit/content_change/reconcile/delete_marker` |
| `observed_at` | `timestamptz` | N | `now()` | |
| `telegram_edit_date` | `timestamptz` | Y |  | |
| `text_surface` | `text` | Y |  | |
| `entities_json` | `jsonb` | Y |  | |
| `raw_message_json` | `jsonb` | N |  | point-in-time snapshot |
| `content_hash` | `text` | N |  | sha256 hex 권장 |

### 제약

- PK: `source_message_version_id`
- FK: `source_message_id -> source_messages(source_message_id)` on delete restrict
- unique: `(source_message_id, version_no)`

### 인덱스

- `idx_source_message_versions_source_observed` on `(source_message_id, observed_at desc)`
- `idx_source_message_versions_content_hash` on `(source_message_id, content_hash)`

### 주의

같은 `content_hash`라도 새로운 reconcile 관측이 가능하므로 unique `(source_message_id, content_hash)`는 두지 않는다.

---

## 3-6. `event_outbox`

### 컬럼

| 컬럼 | 타입 | NULL | 기본값 | 비고 |
|---|---|---:|---|---|
| `event_id` | `uuid` | N | `gen_random_uuid()` | PK |
| `event_type` | `text` | N |  | versioned event name |
| `aggregate_type` | `text` | N |  | |
| `aggregate_id` | `uuid` | N |  | |
| `dedupe_key` | `text` | N |  | logical uniqueness |
| `payload_json` | `jsonb` | N |  | thin payload |
| `status` | `outbox_status_enum` | N | `'pending'` | |
| `published_at` | `timestamptz` | Y |  | |
| `fail_count` | `integer` | N | `0` | |
| `last_error` | `text` | Y |  | |
| `created_at` | `timestamptz` | N | `now()` | |

### 제약

- PK: `event_id`
- unique: `(dedupe_key)`

### 인덱스

- `idx_event_outbox_status_created` on `(status, created_at)`
- `idx_event_outbox_aggregate` on `(aggregate_type, aggregate_id)`

---

## 4. `0002_normalization_candidates` 상세안

## 4-1. 생성 순서

1. `normalization_runs`
2. `normalization_suppression_traces`
3. `artifact_registry`
4. `artifact_observations`
5. `candidate_group_proposals`
6. `candidate_group_members`
7. 관련 인덱스

## 4-2. `normalization_runs`

### 컬럼

| 컬럼 | 타입 | NULL | 기본값 |
|---|---|---:|---|
| `normalization_run_id` | `uuid` | N | `gen_random_uuid()` |
| `source_message_id` | `uuid` | N |  |
| `source_version_no` | `integer` | N |  |
| `normalizer_version` | `text` | N |  |
| `signal_detected` | `boolean` | N | `false` |
| `candidate_eligible` | `boolean` | N | `false` |
| `trigger_strength` | `text` | Y |  |
| `result_hash` | `text` | Y |  |
| `completed_at` | `timestamptz` | N | `now()` |

### 제약

- PK: `normalization_run_id`
- FK: `source_message_id -> source_messages(source_message_id)`
- unique: `(source_message_id, source_version_no, normalizer_version)`

### 인덱스

- `idx_normalization_runs_source_version` on `(source_message_id, source_version_no)`

---

## 4-3. `normalization_suppression_traces`

### 컬럼

| 컬럼 | 타입 | NULL | 기본값 |
|---|---|---:|---|
| `suppression_trace_id` | `uuid` | N | `gen_random_uuid()` |
| `normalization_run_id` | `uuid` | N |  |
| `reason_code` | `text` | N |  |
| `trigger_strength` | `text` | Y |  |
| `notes_json` | `jsonb` | Y |  |
| `created_at` | `timestamptz` | N | `now()` |

### 제약

- PK: `suppression_trace_id`
- FK: `normalization_run_id -> normalization_runs(normalization_run_id)`

### 인덱스

- `idx_suppression_traces_run` on `(normalization_run_id)`
- `idx_suppression_traces_reason` on `(reason_code)`

---

## 4-4. `artifact_registry`

### 컬럼

| 컬럼 | 타입 | NULL | 기본값 |
|---|---|---:|---|
| `artifact_id` | `uuid` | N | `gen_random_uuid()` |
| `artifact_type` | `artifact_type_enum` | N |  |
| `canonical_id` | `text` | N |  |
| `canonical_url` | `text` | Y |  |
| `normalized_host` | `text` | Y |  |
| `artifact_key_json` | `jsonb` | Y |  |
| `current_snapshot_id` | `uuid` | Y |  |
| `current_status` | `snapshot_status_enum` | Y |  |
| `created_at` | `timestamptz` | N | `now()` |
| `updated_at` | `timestamptz` | N | `now()` |

### 제약

- PK: `artifact_id`
- unique: `(canonical_id)`
- FK to snapshot은 `0003` 이후 추가

### 인덱스

- `idx_artifact_registry_type_host` on `(artifact_type, normalized_host)`
- `idx_artifact_registry_updated_at` on `(updated_at desc)`

---

## 4-5. `artifact_observations`

### 컬럼

| 컬럼 | 타입 | NULL | 기본값 |
|---|---|---:|---|
| `artifact_observation_id` | `uuid` | N | `gen_random_uuid()` |
| `artifact_id` | `uuid` | N |  |
| `source_message_id` | `uuid` | N |  |
| `source_version_no` | `integer` | N |  |
| `observed_url` | `text` | Y |  |
| `source_kind` | `text` | N |  |
| `normalized_url` | `text` | Y |  |
| `resolved_url` | `text` | Y |  |
| `canonical_url` | `text` | Y |  |
| `classification` | `text` | Y |  |
| `context_path` | `text` | Y |  |
| `created_at` | `timestamptz` | N | `now()` |

### 제약

- PK: `artifact_observation_id`
- FK: `artifact_id -> artifact_registry(artifact_id)`
- FK: `source_message_id -> source_messages(source_message_id)`

### 인덱스

- `idx_artifact_observations_artifact_created` on `(artifact_id, created_at desc)`
- `idx_artifact_observations_source_version` on `(source_message_id, source_version_no)`

---

## 4-6. `candidate_group_proposals`

### 컬럼

| 컬럼 | 타입 | NULL | 기본값 |
|---|---|---:|---|
| `candidate_group_id` | `uuid` | N | `gen_random_uuid()` |
| `source_message_id` | `uuid` | N |  |
| `source_version_no` | `integer` | N |  |
| `initial_primary_artifact_id` | `uuid` | N |  |
| `current_primary_artifact_id` | `uuid` | N |  |
| `proposal_status` | `text` | N | `'proposed'` |
| `normalizer_version` | `text` | N |  |
| `dedupe_subject_key` | `text` | N |  |
| `current_bundle_id` | `uuid` | Y |  |
| `current_analysis_id` | `uuid` | Y |  |
| `created_at` | `timestamptz` | N | `now()` |
| `updated_at` | `timestamptz` | N | `now()` |

### 제약

- PK: `candidate_group_id`
- FK: `source_message_id -> source_messages(source_message_id)`
- FK: `initial_primary_artifact_id -> artifact_registry(artifact_id)`
- FK: `current_primary_artifact_id -> artifact_registry(artifact_id)`
- `current_bundle_id` / `current_analysis_id` FK는 뒤 migration에서 추가
- unique: `(source_message_id, source_version_no, dedupe_subject_key)`

### 인덱스

- `idx_candidate_groups_current_primary` on `(current_primary_artifact_id)`
- `idx_candidate_groups_status_created` on `(proposal_status, created_at)`
- `idx_candidate_groups_source` on `(source_message_id, source_version_no)`

### 주의

이 테이블은 이름은 proposal이지만 v1에서는 durable aggregate로 사용한다. proposal history 자체를 별도 table로 쪼개지 않는 대신, 이후 reroot/bundle/analysis/state_transition이 history 역할을 보완한다.

---

## 4-7. `candidate_group_members`

### 컬럼

| 컬럼 | 타입 | NULL | 기본값 |
|---|---|---:|---|
| `candidate_group_member_id` | `uuid` | N | `gen_random_uuid()` |
| `candidate_group_id` | `uuid` | N |  |
| `artifact_id` | `uuid` | N |  |
| `member_role` | `text` | N |  |
| `member_order` | `integer` | Y |  |
| `created_at` | `timestamptz` | N | `now()` |

### 제약

- PK: `candidate_group_member_id`
- FK: `candidate_group_id -> candidate_group_proposals(candidate_group_id)`
- FK: `artifact_id -> artifact_registry(artifact_id)`
- unique: `(candidate_group_id, artifact_id, member_role)`

### 인덱스

- `idx_candidate_members_group` on `(candidate_group_id)`
- `idx_candidate_members_artifact` on `(artifact_id)`

---

## 5. `0003_enrichment_bundles` 상세안

## 5-1. 생성 순서

1. `artifact_enrichment_runs`
2. `artifact_snapshots`
3. source-specific snapshot tables
4. `discovered_url_observations`
5. `candidate_reroot_events`
6. `candidate_evidence_bundles`
7. `candidate_evidence_members`
8. cross-migration FK patch

## 5-2. `artifact_enrichment_runs`

### 컬럼

| 컬럼 | 타입 | NULL | 기본값 |
|---|---|---:|---|
| `artifact_enrichment_run_id` | `uuid` | N | `gen_random_uuid()` |
| `artifact_id` | `uuid` | N |  |
| `provider` | `text` | N |  |
| `refresh_mode` | `text` | N |  |
| `depth_budget` | `integer` | N | `1` |
| `status` | `snapshot_status_enum` | N | `'pending'` |
| `content_anchor` | `text` | Y |  |
| `job_idempotency_key` | `text` | N |  |
| `requested_at` | `timestamptz` | N | `now()` |
| `started_at` | `timestamptz` | Y |  |
| `finished_at` | `timestamptz` | Y |  |

### 제약

- PK: `artifact_enrichment_run_id`
- FK: `artifact_id -> artifact_registry(artifact_id)`
- unique: `(job_idempotency_key)`

### 인덱스

- `idx_enrichment_runs_artifact_provider` on `(artifact_id, provider)`
- `idx_enrichment_runs_status_requested` on `(status, requested_at)`

---

## 5-3. `artifact_snapshots`

### 컬럼

| 컬럼 | 타입 | NULL | 기본값 |
|---|---|---:|---|
| `snapshot_id` | `uuid` | N | `gen_random_uuid()` |
| `artifact_id` | `uuid` | N |  |
| `provider` | `text` | N |  |
| `snapshot_type` | `text` | N |  |
| `status` | `snapshot_status_enum` | N |  |
| `fetched_at` | `timestamptz` | N | `now()` |
| `content_anchor` | `text` | N |  |
| `auth_mode` | `text` | Y |  |
| `normalized_projection` | `jsonb` | Y |  |
| `raw_payload_ref` | `text` | Y |  |
| `evidence_limitations` | `jsonb` | Y |  |
| `fetch_anomalies` | `jsonb` | Y |  |

### 제약

- PK: `snapshot_id`
- FK: `artifact_id -> artifact_registry(artifact_id)`
- unique: `(artifact_id, provider, content_anchor, snapshot_type)`

### 인덱스

- `idx_artifact_snapshots_artifact_fetched` on `(artifact_id, fetched_at desc)`
- `idx_artifact_snapshots_status` on `(status)`

---

## 5-4. `artifact_snapshot_github_repo`

### 컬럼

| 컬럼 | 타입 | NULL |
|---|---|---:|
| `snapshot_id` | `uuid` | N |
| `repo_full_name` | `text` | N |
| `default_branch` | `text` | Y |
| `resolved_ref` | `text` | Y |
| `content_anchor_commit_sha` | `text` | Y |
| `repo_flags_json` | `jsonb` | Y |
| `license_spdx` | `text` | Y |
| `topics_json` | `jsonb` | Y |
| `readme_excerpt` | `text` | Y |
| `detected_build_systems_json` | `jsonb` | Y |
| `detected_languages_json` | `jsonb` | Y |
| `key_paths_json` | `jsonb` | Y |
| `test_paths_json` | `jsonb` | Y |
| `ci_paths_json` | `jsonb` | Y |
| `examples_paths_json` | `jsonb` | Y |
| `docs_paths_json` | `jsonb` | Y |
| `release_summary_json` | `jsonb` | Y |

### 제약

- PK/FK: `snapshot_id -> artifact_snapshots(snapshot_id)` on delete cascade

### 인덱스

- `idx_snapshot_gh_repo_full_name` on `(repo_full_name)`

---

## 5-5. `artifact_snapshot_github_file_samples`

### 컬럼

| 컬럼 | 타입 | NULL |
|---|---|---:|
| `file_sample_id` | `uuid` | N |
| `snapshot_id` | `uuid` | N |
| `path` | `text` | N |
| `role` | `text` | N |
| `size_bytes` | `integer` | Y |
| `content_hash` | `text` | Y |
| `excerpt` | `text` | Y |
| `raw_blob_ref` | `text` | Y |

### 제약

- PK: `file_sample_id`
- FK: `snapshot_id -> artifact_snapshots(snapshot_id)` on delete cascade
- unique: `(snapshot_id, path, role)`

### 인덱스

- `idx_snapshot_gh_file_samples_snapshot` on `(snapshot_id)`

---

## 5-6. `artifact_snapshot_x_post`

### 컬럼

| 컬럼 | 타입 | NULL |
|---|---|---:|
| `snapshot_id` | `uuid` | N |
| `post_id` | `text` | N |
| `content_anchor_post_version` | `text` | N |
| `author_summary_json` | `jsonb` | Y |
| `text_full` | `text` | Y |
| `text_excerpt` | `text` | Y |
| `conversation_id` | `text` | Y |
| `referenced_post_ids_json` | `jsonb` | Y |
| `discovered_links_json` | `jsonb` | Y |
| `media_summary_json` | `jsonb` | Y |
| `metrics_summary_json` | `jsonb` | Y |

### 제약

- PK/FK: `snapshot_id -> artifact_snapshots(snapshot_id)` on delete cascade

### 인덱스

- `idx_snapshot_x_post_id` on `(post_id)`

---

## 5-7. `artifact_snapshot_web_article`

### 컬럼

| 컬럼 | 타입 | NULL |
|---|---|---:|
| `snapshot_id` | `uuid` | N |
| `final_url` | `text` | N |
| `canonical_url_candidate` | `text` | Y |
| `site_name` | `text` | Y |
| `title` | `text` | Y |
| `description` | `text` | Y |
| `author` | `text` | Y |
| `published_at` | `timestamptz` | Y |
| `content_hash` | `text` | Y |
| `main_text_excerpt` | `text` | Y |
| `outbound_links_json` | `jsonb` | Y |

### 제약

- PK/FK: `snapshot_id -> artifact_snapshots(snapshot_id)` on delete cascade

---

## 5-8. `artifact_snapshot_text_idea`

### 컬럼

| 컬럼 | 타입 | NULL |
|---|---|---:|
| `snapshot_id` | `uuid` | N |
| `source_message_id` | `uuid` | N |
| `source_version_no` | `integer` | N |
| `hash_surface` | `text` | N |
| `display_surface` | `text` | Y |
| `dev_context_signals_json` | `jsonb` | Y |

### 제약

- PK/FK: `snapshot_id -> artifact_snapshots(snapshot_id)` on delete cascade
- FK: `source_message_id -> source_messages(source_message_id)`

### 인덱스

- `idx_snapshot_text_idea_source` on `(source_message_id, source_version_no)`

---

## 5-9. `discovered_url_observations`

### 컬럼

| 컬럼 | 타입 | NULL | 기본값 |
|---|---|---:|---|
| `discovered_url_observation_id` | `uuid` | N | `gen_random_uuid()` |
| `parent_candidate_group_id` | `uuid` | N |  |
| `parent_artifact_id` | `uuid` | N |  |
| `parent_snapshot_id` | `uuid` | N |  |
| `observed_url` | `text` | N |  |
| `context_path` | `text` | Y |  |
| `discovery_reason` | `text` | N |  |
| `depth_remaining` | `integer` | N | `0` |
| `created_at` | `timestamptz` | N | `now()` |

### 제약

- PK: `discovered_url_observation_id`
- FK: `parent_candidate_group_id -> candidate_group_proposals(candidate_group_id)`
- FK: `parent_artifact_id -> artifact_registry(artifact_id)`
- FK: `parent_snapshot_id -> artifact_snapshots(snapshot_id)`

### 인덱스

- `idx_discovered_urls_parent_candidate` on `(parent_candidate_group_id, created_at desc)`

---

## 5-10. `candidate_reroot_events`

### 컬럼

| 컬럼 | 타입 | NULL | 기본값 |
|---|---|---:|---|
| `candidate_reroot_event_id` | `uuid` | N | `gen_random_uuid()` |
| `candidate_group_id` | `uuid` | N |  |
| `from_artifact_id` | `uuid` | N |  |
| `to_artifact_id` | `uuid` | N |  |
| `reason_code` | `text` | N |  |
| `trigger_snapshot_id` | `uuid` | Y |  |
| `created_at` | `timestamptz` | N | `now()` |

### 제약

- PK: `candidate_reroot_event_id`
- FK: `candidate_group_id -> candidate_group_proposals(candidate_group_id)`
- FK: `from_artifact_id -> artifact_registry(artifact_id)`
- FK: `to_artifact_id -> artifact_registry(artifact_id)`
- FK: `trigger_snapshot_id -> artifact_snapshots(snapshot_id)`

### 인덱스

- `idx_reroot_events_candidate_created` on `(candidate_group_id, created_at desc)`

---

## 5-11. `candidate_evidence_bundles`

### 컬럼

| 컬럼 | 타입 | NULL | 기본값 |
|---|---|---:|---|
| `bundle_id` | `uuid` | N | `gen_random_uuid()` |
| `candidate_group_id` | `uuid` | N |  |
| `initial_primary_artifact_id` | `uuid` | N |  |
| `current_primary_artifact_id` | `uuid` | N |  |
| `bundle_version` | `integer` | N | `1` |
| `bundle_profile_version` | `text` | N |  |
| `bundle_input_hash` | `text` | N |  |
| `reroot_count` | `integer` | N | `0` |
| `primary_summary` | `jsonb` | Y |  |
| `supporting_summaries_json` | `jsonb` | Y |  |
| `discovered_links_summary_json` | `jsonb` | Y |  |
| `evidence_limitations` | `jsonb` | Y |  |
| `ready_for_analysis` | `boolean` | N | `false` |
| `token_budget_profile` | `text` | Y |  |
| `created_at` | `timestamptz` | N | `now()` |

### 제약

- PK: `bundle_id`
- FK: `candidate_group_id -> candidate_group_proposals(candidate_group_id)`
- FK: `initial_primary_artifact_id -> artifact_registry(artifact_id)`
- FK: `current_primary_artifact_id -> artifact_registry(artifact_id)`
- unique: `(candidate_group_id, bundle_profile_version, bundle_input_hash)`

### 인덱스

- `idx_bundles_candidate_created` on `(candidate_group_id, created_at desc)`
- `idx_bundles_ready` on `(ready_for_analysis, created_at)`

---

## 5-12. `candidate_evidence_members`

### 컬럼

| 컬럼 | 타입 | NULL | 기본값 |
|---|---|---:|---|
| `candidate_evidence_member_id` | `uuid` | N | `gen_random_uuid()` |
| `bundle_id` | `uuid` | N |  |
| `artifact_id` | `uuid` | N |  |
| `snapshot_id` | `uuid` | N |  |
| `member_role` | `text` | N |  |
| `member_order` | `integer` | Y |  |

### 제약

- PK: `candidate_evidence_member_id`
- FK: `bundle_id -> candidate_evidence_bundles(bundle_id)` on delete cascade
- FK: `artifact_id -> artifact_registry(artifact_id)`
- FK: `snapshot_id -> artifact_snapshots(snapshot_id)`
- unique: `(bundle_id, artifact_id, snapshot_id, member_role)`

### 인덱스

- `idx_bundle_members_bundle` on `(bundle_id)`

---

## 5-13. `0003`에서 수행할 cross-migration FK patch

`artifact_registry.current_snapshot_id`에 FK 추가:

- `artifact_registry.current_snapshot_id -> artifact_snapshots(snapshot_id)`

이 FK는 `0002`에서는 불가하므로 `0003`에서 추가한다.

---

## 6. `0004_judge_delivery_observability` 상세안

## 6-1. 생성 순서

1. `judge_runs`
2. `judge_outputs`
3. `analyses`
4. `notification_plans`
5. `notification_renders`
6. `notification_delivery_records`
7. `pipeline_runs`
8. `job_attempts`
9. `state_transitions`
10. `dead_letter_entries`
11. `replay_requests`
12. cross-migration FK patch

## 6-2. `judge_runs`

### 컬럼

| 컬럼 | 타입 | NULL | 기본값 |
|---|---|---:|---|
| `judge_run_id` | `uuid` | N | `gen_random_uuid()` |
| `bundle_id` | `uuid` | N |  |
| `judge_profile` | `text` | N |  |
| `model` | `text` | N |  |
| `reasoning_effort` | `text` | N |  |
| `prompt_version` | `text` | N |  |
| `schema_version` | `text` | N |  |
| `policy_version` | `text` | N |  |
| `prompt_cache_key` | `text` | Y |  |
| `status` | `text` | N | `'pending'` |
| `schema_retry_count` | `integer` | N | `0` |
| `escalated_from_judge_run_id` | `uuid` | Y |  |
| `input_tokens` | `integer` | Y |  |
| `cached_input_tokens` | `integer` | Y |  |
| `output_tokens` | `integer` | Y |  |
| `reasoning_tokens` | `integer` | Y |  |
| `latency_ms` | `integer` | Y |  |
| `finish_reason` | `text` | Y |  |
| `refusal_detected` | `boolean` | N | `false` |
| `started_at` | `timestamptz` | Y |  |
| `finished_at` | `timestamptz` | Y |  |

### 제약

- PK: `judge_run_id`
- FK: `bundle_id -> candidate_evidence_bundles(bundle_id)`
- FK: `escalated_from_judge_run_id -> judge_runs(judge_run_id)`
- unique judge key: `(bundle_id, prompt_version, model, reasoning_effort)`

### 인덱스

- `idx_judge_runs_bundle` on `(bundle_id)`
- `idx_judge_runs_status_started` on `(status, started_at)`

---

## 6-3. `judge_outputs`

### 컬럼

| 컬럼 | 타입 | NULL | 기본값 |
|---|---|---:|---|
| `judge_output_id` | `uuid` | N | `gen_random_uuid()` |
| `judge_run_id` | `uuid` | N |  |
| `candidate_group_id` | `uuid` | N |  |
| `judge_schema_version` | `text` | N |  |
| `payload_json` | `jsonb` | N |  |
| `model_proposed_verdict` | `verdict_enum` | Y |  |
| `model_confidence_band` | `text` | Y |  |
| `created_at` | `timestamptz` | N | `now()` |

### 제약

- PK: `judge_output_id`
- FK: `judge_run_id -> judge_runs(judge_run_id)`
- FK: `candidate_group_id -> candidate_group_proposals(candidate_group_id)`

### 인덱스

- `idx_judge_outputs_candidate_created` on `(candidate_group_id, created_at desc)`

---

## 6-4. `analyses`

### 컬럼

| 컬럼 | 타입 | NULL | 기본값 |
|---|---|---:|---|
| `analysis_id` | `uuid` | N | `gen_random_uuid()` |
| `candidate_group_id` | `uuid` | N |  |
| `judge_output_id` | `uuid` | N |  |
| `schema_version` | `text` | N |  |
| `policy_version` | `text` | N |  |
| `prompt_version` | `text` | N |  |
| `delivery_policy_version` | `text` | N |  |
| `verdict` | `verdict_enum` | N |  |
| `delivery_decision` | `delivery_decision_enum` | N |  |
| `scores_json` | `jsonb` | N |  |
| `reason_codes_json` | `jsonb` | N |  |
| `evidence_limitations_ko` | `text` | Y |  |
| `recommended_action_ko` | `text` | Y |  |
| `freshness_note_ko` | `text` | Y |  |
| `model_proposed_verdict` | `verdict_enum` | Y |  |
| `policy_reconciled_flag` | `boolean` | N | `true` |
| `created_at` | `timestamptz` | N | `now()` |

### 제약

- PK: `analysis_id`
- FK: `candidate_group_id -> candidate_group_proposals(candidate_group_id)`
- FK: `judge_output_id -> judge_outputs(judge_output_id)`
- unique: `(judge_output_id, policy_version, delivery_policy_version)`

### 인덱스

- `idx_analyses_candidate_created` on `(candidate_group_id, created_at desc)`
- `idx_analyses_verdict_created` on `(verdict, created_at desc)`

---

## 6-5. `notification_plans`

### 컬럼

| 컬럼 | 타입 | NULL | 기본값 |
|---|---|---:|---|
| `notification_plan_id` | `uuid` | N | `gen_random_uuid()` |
| `analysis_id` | `uuid` | N |  |
| `candidate_group_id` | `uuid` | N |  |
| `delivery_decision` | `delivery_decision_enum` | N |  |
| `urgency_profile` | `urgency_profile_enum` | N |  |
| `target_chat_id` | `bigint` | N |  |
| `target_thread_id` | `bigint` | Y |  |
| `render_profile` | `text` | Y |  |
| `dedupe_subject_key` | `text` | N |  |
| `material_change_hash` | `text` | N |  |
| `send_after` | `timestamptz` | Y |  |
| `suppress_reason_code` | `text` | Y |  |
| `status` | `notification_status_enum` | N | `'planned'` |
| `created_at` | `timestamptz` | N | `now()` |

### 제약

- PK: `notification_plan_id`
- FK: `analysis_id -> analyses(analysis_id)`
- FK: `candidate_group_id -> candidate_group_proposals(candidate_group_id)`
- unique notify key 권장: `(analysis_id, target_chat_id, material_change_hash)`

### 인덱스

- `idx_notification_plans_status_send_after` on `(status, send_after)`
- `idx_notification_plans_dedupe_subject` on `(dedupe_subject_key)`

---

## 6-6. `notification_renders`

### 컬럼

| 컬럼 | 타입 | NULL | 기본값 |
|---|---|---:|---|
| `notification_render_id` | `uuid` | N | `gen_random_uuid()` |
| `notification_plan_id` | `uuid` | N |  |
| `message_text` | `text` | N |  |
| `entities_json` | `jsonb` | Y |  |
| `link_preview_options_json` | `jsonb` | Y |  |
| `reply_markup_json` | `jsonb` | Y |  |
| `disable_notification` | `boolean` | N | `false` |
| `protect_content` | `boolean` | N | `false` |
| `parse_strategy` | `text` | N | `'entities'` |
| `render_hash` | `text` | N |  |
| `created_at` | `timestamptz` | N | `now()` |

### 제약

- PK: `notification_render_id`
- FK: `notification_plan_id -> notification_plans(notification_plan_id)`
- unique: `(notification_plan_id, render_hash)`

---

## 6-7. `notification_delivery_records`

### 컬럼

| 컬럼 | 타입 | NULL | 기본값 |
|---|---|---:|---|
| `notification_delivery_record_id` | `uuid` | N | `gen_random_uuid()` |
| `notification_plan_id` | `uuid` | N |  |
| `telegram_chat_id` | `bigint` | Y |  |
| `telegram_message_id` | `bigint` | Y |  |
| `delivery_status` | `notification_status_enum` | N |  |
| `sent_at` | `timestamptz` | Y |  |
| `edited_at` | `timestamptz` | Y |  |
| `attempt_count` | `integer` | N | `1` |
| `transport_error_code` | `text` | Y |  |
| `transport_error_class` | `text` | Y |  |
| `telegram_response_json` | `jsonb` | Y |  |
| `created_at` | `timestamptz` | N | `now()` |

### 제약

- PK: `notification_delivery_record_id`
- FK: `notification_plan_id -> notification_plans(notification_plan_id)`

### 인덱스

- `idx_notification_delivery_plan_created` on `(notification_plan_id, created_at desc)`
- `idx_notification_delivery_status_created` on `(delivery_status, created_at desc)`

---

## 6-8. `pipeline_runs`

### 컬럼

| 컬럼 | 타입 | NULL | 기본값 |
|---|---|---:|---|
| `pipeline_run_id` | `uuid` | N | `gen_random_uuid()` |
| `trigger_source` | `text` | N |  |
| `run_kind` | `text` | N |  |
| `root_object_type` | `text` | N |  |
| `root_object_id` | `uuid` | N |  |
| `started_at` | `timestamptz` | N | `now()` |
| `finished_at` | `timestamptz` | Y |  |
| `terminal_status` | `text` | Y |  |

### 인덱스

- `idx_pipeline_runs_root` on `(root_object_type, root_object_id)`
- `idx_pipeline_runs_started` on `(started_at desc)`

---

## 6-9. `job_attempts`

### 컬럼

| 컬럼 | 타입 | NULL | 기본값 |
|---|---|---:|---|
| `job_attempt_id` | `uuid` | N | `gen_random_uuid()` |
| `stage_name` | `text` | N |  |
| `queue_name` | `text` | N |  |
| `root_object_type` | `text` | N |  |
| `root_object_id` | `uuid` | N |  |
| `attempt_no` | `integer` | N | `1` |
| `lease_owner` | `text` | Y |  |
| `started_at` | `timestamptz` | Y |  |
| `finished_at` | `timestamptz` | Y |  |
| `attempt_status` | `job_attempt_status_enum` | N | `'pending'` |
| `error_code` | `text` | Y |  |
| `retry_after_at` | `timestamptz` | Y |  |
| `created_at` | `timestamptz` | N | `now()` |

### 인덱스

- `idx_job_attempts_queue_status_retry_after` on `(queue_name, attempt_status, retry_after_at)`
- `idx_job_attempts_root` on `(root_object_type, root_object_id)`

---

## 6-10. `state_transitions`

### 컬럼

| 컬럼 | 타입 | NULL | 기본값 |
|---|---|---:|---|
| `state_transition_id` | `uuid` | N | `gen_random_uuid()` |
| `object_type` | `text` | N |  |
| `object_id` | `uuid` | N |  |
| `from_state` | `text` | Y |  |
| `to_state` | `text` | N |  |
| `reason_code` | `text` | Y |  |
| `created_at` | `timestamptz` | N | `now()` |

### 인덱스

- `idx_state_transitions_object_created` on `(object_type, object_id, created_at desc)`

---

## 6-11. `dead_letter_entries`

### 컬럼

| 컬럼 | 타입 | NULL | 기본값 |
|---|---|---:|---|
| `dead_letter_entry_id` | `uuid` | N | `gen_random_uuid()` |
| `stage_name` | `text` | N |  |
| `queue_name` | `text` | N |  |
| `root_object_type` | `text` | N |  |
| `root_object_id` | `uuid` | N |  |
| `last_error_code` | `text` | Y |  |
| `last_error_snippet` | `text` | Y |  |
| `retry_count` | `integer` | N | `0` |
| `first_failed_at` | `timestamptz` | N | `now()` |
| `last_failed_at` | `timestamptz` | N | `now()` |
| `next_manual_action` | `text` | Y |  |
| `replay_hint` | `text` | Y |  |

### 인덱스

- `idx_dlq_stage_last_failed` on `(stage_name, last_failed_at desc)`

---

## 6-12. `replay_requests`

### 컬럼

| 컬럼 | 타입 | NULL | 기본값 |
|---|---|---:|---|
| `replay_request_id` | `uuid` | N | `gen_random_uuid()` |
| `replay_type` | `replay_type_enum` | N |  |
| `root_object_type` | `text` | N |  |
| `root_object_id` | `uuid` | N |  |
| `requested_by` | `text` | Y |  |
| `requested_at` | `timestamptz` | N | `now()` |
| `status` | `text` | N | `'pending'` |

### 인덱스

- `idx_replay_requests_status_requested` on `(status, requested_at)`

---

## 6-13. `0004`에서 수행할 cross-migration FK patch

아래 FK를 추가한다.

1. `candidate_group_proposals.current_bundle_id -> candidate_evidence_bundles(bundle_id)`
2. `candidate_group_proposals.current_analysis_id -> analyses(analysis_id)`

주의:
- 이 FK는 nullable이어야 한다.
- current pointer는 service code가 관리한다.

---

## 7. 인덱스 / 성능 메모

### 7-1. v1에서 바로 넣는 인덱스

- 모든 unique key
- status + time 기반 운영 큐 인덱스
- artifact/candidate/bundle/analysis/notification 역추적용 인덱스
- source_message `(chat_id, posted_at desc)`

### 7-2. v1에서 보류하는 것

- full text search index
- trigram index
- jsonb GIN index 대량 도입
- table partitioning

이유:
- 먼저 correctness와 replayability를 고정해야 한다.
- 과도한 인덱스는 migration 복잡도만 올린다.

---

## 8. Alembic 구현 시 주의점

### 8-1. migration 순서

`0001 -> 0002 -> 0003 -> 0004`

중간 migration을 합치지 않는다. 이유:
- rollback 단위 유지
- cross-migration FK patch 명확화
- 단계별 diff 검토 용이

### 8-2. `updated_at` 처리

v1에서는 DB trigger보다 application-side update가 단순하다.  
즉:
- table에는 `updated_at` 컬럼만 둔다.
- 값 갱신은 service layer 책임으로 둔다.

### 8-3. enum 변경 정책

새 enum value 추가는 migration으로 허용한다.  
기존 enum value 삭제/이름 변경은 v1 운영 중 금지한다.

---

## 9. 구현 우선순위 재확인

이 문서 기준으로 실제 다음 구현 순서는 아래가 맞다.

1. `0001_ingest_core` Alembic 작성
2. `0002_normalization_candidates` Alembic 작성
3. `0003_enrichment_bundles` Alembic 작성
4. `0004_judge_delivery_observability` Alembic 작성
5. migration dry-run / upgrade / downgrade 검증
6. 그 다음에 collector 구현 시작

즉, **다음 실제 작업은 `0001_ingest_core` Alembic 초안 작성**이다.

---

## 10. 최종 요약

이 migration 상세안의 핵심은 아래 네 가지다.

1. **append-only history/snapshot과 mutable current row를 분리한다.**
2. **cross-stage current pointer는 뒤 migration에서 FK를 보강한다.**
3. **Redis 재구성의 기준점이 되도록 PostgreSQL schema를 먼저 완성한다.**
4. **candidate / bundle / judge / analysis / notification lineage를 모두 DB에서 추적 가능하게 만든다.**

다음 턴에서는 이 문서를 바탕으로 바로 **`0001_ingest_core` Alembic/SQL 초안**으로 들어가면 된다.
