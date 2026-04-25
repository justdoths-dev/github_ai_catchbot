# 03 judge delivery operations stage6 stage10 bundle v0 1
이 문서는 프로젝트 소스 파일 수 제한(40개)을 피하기 위한 **통합 번들**이다.
구조 변경 문서가 아니며, 아래 원본 문서를 **순서와 내용을 보존한 채** 묶는다.

## 포함 원본 파일
- `06_stage6_llm_judge.md`
- `07_stage7_telegram_delivery_policy.md`
- `08_stage8_observability_replay_recovery.md`
- `09_stage9_quality_tuning_eval_framework.md`
- `10_stage10_rollout_cutover_governance.md`

## 통합 원칙
- 아키텍처 불변식은 유지한다.
- 원본 파일명은 아래 섹션 제목으로 보존한다.
- 충돌 시 해석 우선순위는 기존과 동일하게 정본 단계 문서 → 실행 계약 → migration 정본 → 구현 초안이다.


---

## Source file: `06_stage6_llm_judge.md`

# 6단계: LLM 판정기 설계

이번 단계는 앞선 구조를 그대로 유지한 채, **`CandidateEvidenceBundle`을 최종 `Analysis`로 바꾸는 판단 계층**을 설계하는 단계다.

이 단계에서도 이전 단계에서 잠근 구조를 깨지 않는다.

- **0단계**: `SourceMessage → Artifact → CandidateGroup → Analysis → Notification`
- **1단계**: 계정/권한/키 분리
- **2단계**: `PostgreSQL = system of record`, `Redis = queue/lock`, `prod 단일 live collector`
- **3단계**: collector는 Telegram 원문 보존만 담당
- **4단계**: router-normalizer는 결정적으로 `Artifact`와 `CandidateGroup proposal`만 생성
- **5단계**: enrichers는 외부 증거를 `ArtifactSnapshot`과 `EvidenceBundle`로 조립

따라서 6단계의 핵심 원칙은 명확하다.

> **LLM 판정기는 탐지기가 아니다.**  
> **LLM 판정기는 크롤러도 아니다.**  
> **LLM 판정기는 이미 수집된 증거를, 구조화된 점수·사유·요약으로 변환하는 계층이다.**

---

## 6단계에서 고정할 핵심 결정

| 구분 | 고정 결정 |
|---|---|
| 기본 API | **OpenAI Responses API** |
| 기본 출력 방식 | **Structured Outputs (`json_schema`, `strict: true`)** |
| 기본 모델 | **`gpt-5.4-mini`** |
| 승급 모델 | **`gpt-5.4`** |
| hot path reasoning | `mini + reasoning.effort=low` |
| escalation reasoning | `gpt-5.4 + reasoning.effort=medium` |
| 최종 verdict 산출 | **모델 점수 출력 + 앱의 deterministic policy engine 재계산** |
| delivery 결정 | **LLM이 아니라 앱 정책이 결정** |
| judge 입력 | **`CandidateEvidenceBundle`만 허용**, 외부 검색/도구 호출 금지 |
| judge 출력 | **`judge_output_v1`** → 검증 후 **`analysis_v1`** 로 승격 |
| 실패 처리 | schema retry 1회, 이후 `analysis_failed` 또는 enrichment 재요청 |
| 캐시 전략 | static prefix 고정 + `prompt_cache_key` 사용 |

핵심은 이것이다.

1. **모델은 분석자이고, 정책 엔진은 판정 집행자다.**
2. **모델이 구조화된 사실·점수·사유를 내고, 최종 verdict/delivery는 앱이 확정한다.**

이 분리를 안 하면, 0단계에서 잠근 `verdict_policy_v1`이 유명무실해진다.

---

## 왜 Responses API + Structured Outputs로 고정하는가

이 프로젝트의 LLM 판정기는 “자유문장 생성”보다 **안정적인 구조화 출력**이 중요하다. 그래서 `Chat Completions`의 자유 텍스트보다, **Responses API + Structured Outputs**가 구조적으로 더 잘 맞는다.

설계 이유는 아래 세 가지다.

1. **구조 안정성**  
   판정기는 `scores`, `reason_codes`, `comparables`, `skeptical_take_ko` 같은 필드를 정확히 채워야 한다. 이 프로젝트는 출력 형식이 조금만 흔들려도 후단 policy engine과 notifier가 함께 흔들린다.

2. **정책 분리**  
   모델의 출력과 정책 집행 결과를 분리하려면, 모델 출력이 항상 같은 JSON 구조여야 한다.

3. **실패 감지 용이성**  
   `refusal`, `schema invalid`, `missing required field`를 프로그램적으로 다루기 쉬워야 한다.

---

## Judge 계층을 4개 서비스로 나눈다

LLM 호출을 한 서비스에 모두 몰아넣으면 이후에 디버깅이 어려워진다. 6단계에서는 최소 아래처럼 나누는 편이 맞다.

- `analysis-router`
- `judge-openai`
- `analysis-validator`
- `policy-engine`

### 1) `analysis-router`
역할:
- bundle readiness 확인
- model/profile 선택
- escalation 필요성 판정
- enrich 재요청 여부 결정

### 2) `judge-openai`
역할:
- OpenAI Responses API 호출
- structured output 수신
- usage / latency / cache hit 정보 기록

### 3) `analysis-validator`
역할:
- JSON Schema 검증
- enum / 길이 / 비어있음 검증
- 필수 필드 보정 가능 여부 판정
- refusal / schema failure / truncation 처리

### 4) `policy-engine`
역할:
- 0단계의 `verdict_policy_v1` 적용
- 최종 `verdict` 산출
- 최종 `delivery_decision` 산출
- model proposed verdict와 policy verdict의 불일치 기록

핵심은 이렇다.

> **OpenAI 응답이 곧 최종 Analysis가 아니다.**  
> **OpenAI 응답은 `judge_output_v1`이고, 검증과 정책 적용을 거쳐야만 `analysis_v1`이 된다.**

---

## LLM judge의 입력 경계를 고정한다

6단계는 5단계의 `CandidateEvidenceBundle`을 그대로 입력으로 받는다.  
여기서 중요한 규칙은 **judge가 bundle 밖으로 나가지 않는 것**이다.

금지 사항:
- 웹 검색
- GitHub/X 추가 fetch
- 외부 tool 호출
- raw collector 데이터 재탐색

허용 사항:
- `CandidateEvidenceBundle`
- bundle에 포함된 `limitations`
- bundle에 포함된 `source_lineage`

이 경계를 지켜야 하는 이유는 두 가지다.

1. **재현성**  
   같은 bundle이면 같은 분석이 나와야 한다.

2. **책임 분리**  
   최신성 확보는 5단계 enrichers 책임이고, 6단계는 판단 책임이다.

---

## 모델 선택 정책

현재 OpenAI 공식 문서는 `gpt-5.4`를 복잡한 reasoning과 coding의 기본 선택지로, `gpt-5.4-mini`를 더 빠르고 저렴한 고볼륨 코딩/에이전트 워크로드용 모델로 설명한다. 두 모델 모두 Responses API와 Structured Outputs를 지원한다. 따라서 이 봇의 hot path는 `gpt-5.4-mini`, 승급 경로는 `gpt-5.4`로 고정하는 편이 가장 자연스럽다.

### 기본 모델: `gpt-5.4-mini`
사용 대상:
- 일반 GitHub repo 후보
- X post + supporting repo 후보
- text-only idea 후보
- 대부분의 hot path 분석

이유:
- 비용/지연 균형이 좋음
- 코딩/도구 판단에 충분한 기본 성능
- high-volume judge 경로에 적합

### 승급 모델: `gpt-5.4`
사용 대상:
- mini 결과가 애매함
- HIGH 가능성이 있는데 확신이 낮음
- reroot가 일어남
- 후보의 실질 가치가 큰데 bundle 구조가 복합적임

이유:
- 복잡한 professional/coding 판단에서 상위 성능
- ambiguous case 재판정에 적합

### 도입하지 않는 것
- hot path에 제3의 분류 모델 추가
- reasoning model(o-series) 추가
- source별 멀티벤더 judge

이유:
- 0~5단계까지 이미 경계를 잘라뒀으므로, 지금 필요한 것은 복잡한 모델 스택이 아니라 **안정적인 정책 분리**다.

---

## reasoning.effort 정책

OpenAI의 reasoning 가이드는 `reasoning.effort`를 `none/minimal/low/medium/high/xhigh` 중 모델별 지원값으로 조정할 수 있고, 낮은 effort는 속도와 토큰 절감을, 높은 effort는 더 완전한 추론을 지향한다고 설명한다. 또한 reasoning tokens는 사용자에게 보이지 않지만 컨텍스트를 차지하고 과금 대상이 된다.

따라서 이 봇의 hot path는 보수적으로 가는 편이 맞다.

### hot path
- 모델: `gpt-5.4-mini`
- `reasoning.effort = low`

이유:
- 이 judge는 구조화 extraction + 비교 평가의 성격이 강함
- `none`은 nuance 손실 위험이 있고
- `medium` 이상은 비용 대비 이득이 작을 가능성이 큼

### escalation path
- 모델: `gpt-5.4`
- `reasoning.effort = medium`

이유:
- 복합 후보, 다중 supporting artifact, reroot 이후 판단처럼 ambiguity가 있는 경우에만 추가 reasoning을 허용

### 금지
- hot path에서 `high` 또는 `xhigh`
- 근거 없이 기본 effort 상향

이건 단순 비용 문제가 아니라 **응답 지연과 정책 일관성** 문제다.

---

## judge 출력은 `analysis_v1`이 아니라 `judge_output_v1`

이 부분이 매우 중요하다.

0단계에서 `analysis_v1`을 잠갔지만, 6단계에서는 **모델 출력과 최종 저장 구조를 동일시하지 않는 편이 더 안전하다.**

권장 구조는 아래와 같다.

### 1) `judge_output_v1` — 모델의 구조화 응답

권장 필드:

- `judge_schema_version`
- `candidate_group_id`
- `headline`
- `summary_one_line_ko`
- `skeptical_take_ko`
- `why_it_might_matter_ko`
- `comparables`
- `scores`
- `reason_codes`
- `red_flags_ko`
- `evidence_limitations_ko`
- `recommended_action_ko`
- `freshness_note_ko`
- `model_proposed_verdict`
- `model_confidence_band`

### 2) `analysis_v1` — 앱이 확정한 최종 Analysis

여기에는 0단계에서 잠근 필드를 유지한다.

- `schema_version`
- `policy_version`
- `prompt_version`
- `primary_artifact`
- `supporting_artifacts`
- `scores`
- `verdict`
- `delivery_decision`
- `reason_codes`
- `evidence_limitations_ko`
- 기타 필드

### 왜 둘을 나누는가

이유는 명확하다.

1. **모델 drift가 verdict를 직접 흔들지 못하게**
2. **정책 변경 시 과거 judge_output 재평가 가능하게**
3. **model proposed verdict와 policy verdict의 괴리를 측정 가능하게**

즉, **LLM은 점수/설명/제안까지만** 내고, 최종 집행은 policy engine이 한다.

---

## 최종 verdict와 delivery는 policy engine이 계산한다

0단계에서 이미 `verdict`와 `delivery_decision`은 의미론과 운영 정책을 분리하기로 잠갔다.  
따라서 6단계에서는 이 결정을 강화해야 한다.

### verdict
입력:
- `scores`
- `reason_codes`
- `artifact_type`
- `bundle_limitations`

산출:
- `inspect_now`
- `later`
- `skip`

### delivery decision
입력:
- `verdict`
- source priority
- channel preference
- dedupe 상태
- alert fatigue 정책

산출:
- `send_now`
- `send_digest`
- `suppress`

핵심 원칙:

- **모델은 delivery를 결정하지 않는다.**
- **모델은 verdict도 제안만 하고, 최종 확정은 policy engine이 한다.**

이걸 분리하지 않으면 0단계 구조가 깨진다.

---

## prompt 구조는 캐시와 버전관리를 중심으로 짠다

OpenAI의 Prompt Caching은 최근 모델들에서 자동으로 동작하며, 동일한 **정확한 prefix**에 대해 비용과 지연을 줄인다. 캐시 hit를 높이려면 정적 지시문과 예시를 프롬프트 앞부분에 두고, 변동하는 candidate evidence는 뒤로 보내야 한다. `prompt_cache_key`를 함께 사용하면 공통 prefix 라우팅과 cache hit를 더 안정적으로 유도할 수 있다.

따라서 judge prompt는 아래처럼 고정하는 편이 좋다.

```text
<role_and_objective>
<hard_rules>
<score_rubric>
<verdict_policy_summary>
<reason_code_catalog>
<profile_specific_guidance>
<output_contract>
<evidence_bundle>
```

### static prefix
항상 동일해야 하는 부분:
- 역할 정의
- negative-first 규칙
- 점수 정의
- 금지 사항
- 출력 스키마 설명
- 비교 기준

### variable suffix
매 요청마다 바뀌는 부분:
- primary snapshot 요약
- supporting snapshot 요약
- limitations
- source lineage
- token budget hints

### `prompt_cache_key` 권장 규칙

```text
judge:{profile}:{prompt_version}:{schema_version}:{policy_version}
```

이렇게 두면 prompt 버전이 바뀌었을 때 cache 오염도 막고, 실험군 분리도 쉬워진다.

---

## profile은 3개면 충분하다

너무 많은 judge profile을 두면 프롬프트와 평가가 분산된다. v1은 아래 3개만 두는 편이 맞다.

### Profile A: `github_primary`
대상:
- `github_repo`
- `github_subpath`
- `github_gist` (단, gist는 내부 규칙에서 보수 평가)

강조 포인트:
- 기존 도구 대비 차별성
- README claim과 코드 구조의 일치 여부
- 테스트/CI/examples/docs 유무
- wrapper risk

### Profile B: `x_primary`
대상:
- `x_post` primary
- X + GitHub supporting
- X + article supporting

강조 포인트:
- 아이디어의 구체성
- 링크 본체가 따로 있는지
- 허세 vs 실제 workflow 가치
- reroot 필요성

### Profile C: `text_idea_primary`
대상:
- 링크가 약하거나 없는 vibe coding / dev workflow 아이디어

강조 포인트:
- 절차성
- 실행 가능성
- 이미 흔한 이야기인지
- 과장/모호성

핵심은 **모든 profile이 같은 output schema를 사용**한다는 점이다.

---

## few-shot은 기본값이 아니다

구조화 judge에서 흔한 실수는 calibration examples를 과도하게 넣는 것이다.  
그러면 다음 문제가 생긴다.

- 프롬프트 길이 증가
- cache 효율 저하
- 특정 사례에 과적합
- profile 간 drift

따라서 v1 기본값은 **zero-shot + schema-first**다.

예외:
- 골든셋 평가에서 특정 systematic error가 반복될 때
- 아주 짧은 profile-specific counterexample 1~2개가 실질적 이득을 보일 때

즉, 예시는 “기본 구성품”이 아니라 **교정 장치**다.

---

## judge가 반드시 지켜야 할 hard rule

이 규칙은 prompt 안에 명시적으로 들어가야 한다.

1. **좋은 점보다 먼저 왜 별로일 수 있는지를 요약할 것**
2. **evidence 부족이면 HIGH 성격의 결론을 피할 것**
3. **비교 대상을 억지로 지어내지 말 것**
4. **bundle 밖 정보에 의존한 단정 금지**
5. **모른다면 limitation으로 남길 것**
6. **마케팅 문구 재서술 금지**
7. **점수와 서술이 충돌하면 낮은 쪽으로 보수적으로 정렬할 것**

이 중 2번과 7번이 중요하다.  
이 judge는 추천기가 아니라 필터이므로 **보수적 오판**이 낫다.

---

## 점수 체계는 0단계를 그대로 따른다

judge는 아래 점수를 모두 0~100으로 출력한다.

### 공통 필수 점수
- `novelty`
- `practical_usefulness`
- `evidence_strength`
- `hype_penalty`
- `confidence`

### GitHub 조건부 점수
- `code_quality`
- `maintenance_signal`

### X / text idea 조건부 점수
- `specificity`
- `reproducibility_signal`

여기서 중요한 규칙 두 가지:

1. **조건부 점수가 비대상 artifact에 대해 0으로 채워져서는 안 된다.**  
   `not_applicable` 또는 `null` 정책을 schema 수준에서 분리하는 편이 좋다.

2. **confidence는 output style confidence가 아니라 판단 신뢰도다.**  
   즉, 근거가 빈약하면 summary 문장이 매끈해도 confidence는 낮아야 한다.

---

## comparables는 optional이지만, GitHub primary에서는 강하게 요구한다

사용자 요구상 “이미 있는 도구인지, 말만 거창한 wrapper인지”를 봐야 한다.  
그래서 GitHub primary 후보에서는 비교 항목이 빠지면 judge의 가치가 크게 떨어진다.

권장 규칙:
- `github_primary`: `comparables` 1~3개 권장
- `x_primary` / `text_idea_primary`: 없을 수 있음
- 확실하지 않으면 빈 배열 허용, 대신 `comparison_gap` reason code 부여

중요한 점:
- obscure tool을 스타일용으로 지어내면 안 됨
- 비교 대상을 확신 못 하면 limitation으로 남겨야 함

---

## escalation 규칙은 “증거 부족”과 “판단 애매함”을 분리한다

이 구조를 안 잡으면, 실제로는 deep enrich가 필요한 케이스를 더 비싼 모델로 해결하려 들게 된다.

### A. enrichment 재요청 대상
아래면 `gpt-5.4`로 올리지 말고 5단계로 되돌리는 편이 맞다.

- `evidence_strength`를 낼 재료가 구조적으로 부족함
- README claim 확인용 key file이 없음
- GitHub subpath만 있고 repo shape가 없음
- X post가 edit window 안인데 핵심 링크가 아직 불안정함

즉, **증거 부족은 모델 upgrade로 해결하지 않는다.**

### B. model escalation 대상
아래면 `gpt-5.4` 재판정을 허용한다.

- `confidence < 60`
- `inspect_now` 제안인데 `hype_penalty >= 50`
- `later` 제안인데 `novelty >= 70` 또는 `practical_usefulness >= 75`
- `reroot_applied = true`
- supporting artifact가 많아 서술 충돌 가능성이 큼

즉, **증거는 충분하지만 해석이 애매할 때만** 상위 모델로 올린다.

---

## validator 계층은 schema 검증만 하면 안 된다

Structured Outputs를 쓰더라도 validator는 필요하다.

이유:
- schema는 맞지만 business rule을 위반할 수 있음
- 필드는 채워졌지만 의미가 모순될 수 있음
- 너무 긴 배열/문자열이 들어올 수 있음
- verdict와 score가 충돌할 수 있음

권장 validator 순서:

1. JSON Schema 검증
2. enum 검증
3. 길이/개수 검증
4. nullability 검증
5. semantic validation
   - `inspect_now` 제안인데 `evidence_strength`가 지나치게 낮은가
   - `skeptical_take_ko`가 비어 있는가
   - GitHub primary인데 comparables가 빈 배열인가
6. policy reconciliation
7. 저장 가능 여부 판정

즉, validator는 단순한 JSON parser가 아니라 **LLM 출력 방화벽**이다.

---

## refusal / truncation / schema failure 처리

Structured Outputs는 스키마 안정성을 크게 높이지만, 여전히 다음은 다뤄야 한다.

### refusal
- safety refusal 또는 정책 refusal
- 내부 상태: `analysis_refused`
- 사용자 알림 기본 금지
- 운영 로그에만 기록

### truncation
- output 길이 초과
- 1회만 재시도
- 재시도 시 bundle 축약 또는 output budget 축소 검토

### schema failure
- 1회 same model retry
- 반복 시 `analysis_failed_schema`
- 특정 prompt version 문제인지 집계

### transient API error
- exponential backoff
- 제한된 retry
- queue 재적재

핵심은 **같은 실패를 무한 반복하지 않는 것**이다.

---

## prompt와 schema는 버전 고정이 필수다

이 단계에서 반드시 버전 분리를 고정해야 한다.

- `judge_prompt_v1`
- `judge_output_schema_v1`
- `verdict_policy_v1`
- `bundle_schema_v1`

이유는 간단하다.

- prompt를 바꿔도 policy는 유지될 수 있음
- policy를 바꿔도 prompt는 유지될 수 있음
- schema를 바꾸면 validator와 notifier가 영향 받음

즉, **세 가지를 같은 버전으로 묶지 않는다.**

---

## usage telemetry를 반드시 남긴다

OpenAI 응답에는 usage 정보가 포함되며, reasoning 모델 계열에서는 `output_tokens_details`를 통해 reasoning token 사용량을 확인할 수 있다. Prompt Caching이 적용되면 cached input token도 usage에 반영된다. 이 judge는 비용과 latency가 핵심이므로, 최소 아래는 전부 저장하는 편이 좋다.

- `model`
- `reasoning.effort`
- `input_tokens`
- `cached_input_tokens`
- `output_tokens`
- `reasoning_tokens`
- `latency_ms`
- `finish_reason`
- `refusal_detected`
- `schema_retry_count`
- `escalated`

이 데이터를 안 남기면, 이후에 mini/full 비율 최적화가 불가능해진다.

---

## prompt caching 전략은 hot path에서 가치가 크다

judge 프롬프트는 정적 규칙이 길고, evidence bundle만 바뀌는 구조다.  
즉, Prompt Caching의 전형적인 대상이다.

권장 정책:
- 긴 static rubric는 prefix에 고정
- profile-specific appendix도 prefix에 포함
- evidence bundle만 suffix로 첨부
- `prompt_cache_key` 사용
- `gpt-5.4` 승급 경로에는 필요 시 `prompt_cache_retention=24h` 검토

중요한 점:
- 현재 OpenAI 문서상 Prompt Caching은 recent models에서 자동으로 동작한다.
- 다만 **extended 24h retention**은 현재 명시된 일부 모델에만 제공되며, 가이드 기준 `gpt-5.4`는 포함되지만 `gpt-5.4-mini`는 목록에 없다.

즉,
- **mini는 in-memory cache 기대**
- **full은 필요 시 24h retention 후보**

정도로 설계하는 편이 현재 문서와 맞다.

---

## 이 단계에서 반드시 피해야 할 반패턴

1. LLM이 최종 verdict와 delivery를 직접 확정하게 만드는 것  
2. Structured Outputs 대신 자유문장 JSON 흉내에 의존하는 것  
3. JSON mode만 쓰고 schema 보장을 기대하는 것  
4. 증거 부족 문제를 상위 모델 승급으로 덮는 것  
5. prompt / schema / policy 버전을 한 덩어리로 묶는 것  
6. judge가 bundle 밖 정보를 찾아 나가게 허용하는 것  
7. `skeptical_take_ko`를 optional로 두는 것  
8. comparables를 무조건 강제해 hallucination을 유도하는 것  
9. usage / cache / reasoning token telemetry를 저장하지 않는 것  
10. hot path에서 high/xhigh reasoning effort를 쓰는 것

특히 1, 4, 6은 0~5단계 구조를 직접 흔든다.

---

## 6단계 완료 기준

아래가 확정되면 6단계는 끝이다.

- `analysis-router` / `judge-openai` / `analysis-validator` / `policy-engine` 분리
- `judge_output_v1` 스키마 확정
- `analysis_v1` 승격 규칙 확정
- profile 3종 확정
- model selection / escalation 규칙 확정
- `reasoning.effort` 정책 확정
- prompt prefix/suffix 구조 확정
- `prompt_cache_key` 규칙 확정
- validator / refusal / retry 정책 확정
- usage telemetry 필드 확정

이번 단계의 한 줄 결론은 이거다.

**6단계는 `EvidenceBundle`을 구조화된 점수·비교·냉정 요약으로 바꾸는 판단 계층이며, 모델은 `judge_output_v1`만 생성하고 최종 `verdict`와 `delivery_decision`은 deterministic policy engine이 확정해야 앞선 단계들의 구조가 흔들리지 않는다.**

---

## 공식 참고 문서

- OpenAI Text generation guide (Responses API 권장)
- OpenAI Structured Outputs guide
- OpenAI Models guide / GPT-5.4 / GPT-5.4 mini
- OpenAI Reasoning guide
- OpenAI Prompt Caching guide


---

## Source file: `07_stage7_telegram_delivery_policy.md`

# 7단계: 텔레그램 알림 및 전달 정책 설계

## 단계 목적

7단계의 역할은 단순한 `sendMessage` 호출이 아니다.

이 단계의 실제 목적은 **6단계에서 확정된 `analysis_v1`을, 사용자가 즉시 해석 가능한 텔레그램 전달물로 변환하고, 중복·소음·수정·재전송을 통제하는 운영 계층을 설계하는 것**이다.

즉, 이 단계는 다음을 동시에 만족해야 한다.

- `inspect_now / later / skip` 구조를 깨지 않는다.
- `send_now / send_digest / suppress` 전달 결정을 구조적으로 유지한다.
- 텔레그램 전송 포맷이 분석 로직을 침범하지 않게 한다.
- 한 후보가 여러 채널에서 반복 관측되어도 알림 폭주가 나지 않게 한다.
- 나중에 `/pause`, `/why`, `/force` 같은 제어면을 붙여도 스키마를 뜯지 않게 한다.

핵심은 이것이다.

> **7단계는 “메시지 생성기”가 아니라, `Analysis → Notification` 변환 경계다.**

---

## 이전 단계와의 연결 고정

이번 단계는 아래 구조를 전제로 한다.

- **0단계**
  - `SourceMessage → Artifact → CandidateGroup → Analysis → Notification`
  - `verdict`와 `delivery_decision` 분리
- **1단계**
  - Telegram reader account와 notifier bot 분리
- **2단계**
  - `notifier-telegram`은 별도 서비스
  - 초기에는 outbound-only
- **3단계**
  - collector는 원문 보존만 담당
- **4단계**
  - candidate grouping은 이미 deterministic하게 끝남
- **5단계**
  - bundle은 이미 재현 가능한 evidence snapshot으로 고정됨
- **6단계**
  - LLM은 `judge_output_v1`만 만들고
  - policy-engine이 최종 `analysis_v1.verdict`와 `delivery_decision`을 확정함

즉, **7단계의 notifier는 점수를 다시 계산하면 안 된다.**
notifier는 이미 확정된 분석 결과를 **전달 가능한 표면으로 렌더링**해야 한다.

---

## 이번 단계에서 고정할 핵심 결정

| 구분 | 고정 결정 |
|---|---|
| 알림 서비스 | `notifier-telegram` 1개 |
| 입력 | `analysis_v1` + `delivery_policy_applied` |
| 출력 | `notification_plan_v1` + `notification_delivery_record_v1` |
| 기본 채널 | **운영자 개인 DM 1개** |
| 메시지 단위 | **single alert = candidate 1개** |
| 기본 전송 수단 | **`sendMessage` 텍스트 메시지** |
| 기본 링크 표면 | **inline keyboard URL 버튼** |
| 기본 포맷 방식 | **`parse_mode`보다 explicit entities 우선** |
| 기본 링크 미리보기 | **disabled** |
| 기본 수정 전략 | **single-shot send**, 필요 시 material update만 `editMessageText` |
| 기본 전달 정책 | `inspect_now → send_now(알림 on)` / `later → send_now(알림 off)` / `skip → suppress` |
| digest | v1에서 구조는 설계하되 기본 경로는 비활성 |
| protect_content | 기본 `false` |
| allow_paid_broadcast | 기본 `false` |

핵심 결정은 세 가지다.

1. **v1은 즉시 전송 중심**으로 간다.  
   사용자가 “하나 올라올 때 바로 분석해서 전송”을 원했으므로, `later`도 기본은 immediate send로 두되 `disable_notification=true`로 소음을 낮춘다.

2. **single alert는 candidate 1개**로 고정한다.  
   그래야 0단계의 `CandidateGroup` 구조가 그대로 유지되고, edit/update/dedupe도 단순해진다.

3. **text message + inline keyboard**를 기본으로 한다.  
   사진/캡션 중심으로 가면 편집성과 텍스트 길이 여유가 크게 나빠진다.

---

## 왜 텍스트 메시지 중심으로 가는가

Telegram Bot API의 `sendMessage`는 텍스트 메시지를 보내며, 메시지 본문은 entities 파싱 후 1~4096자까지 허용된다. 반면 `editMessageText`도 텍스트 메시지 수정에 맞춰져 있고, 메시지 편집은 reply markup이 없거나 inline keyboard가 있을 때 가능하다고 명시돼 있다. 따라서 분석 알림은 **caption 기반 media message보다 text message가 훨씬 안정적**이다. 또한 Bot API는 `entities`를 직접 넘기거나 `parse_mode`를 사용할 수 있어, 정교한 링크/강조 제어가 가능하다. ([Telegram Bot API](https://core.telegram.org/bots/api))

실무적으로 이 판단은 더 강해진다.

- text message는 수정이 쉽다.
- inline keyboard와 궁합이 좋다.
- 4096자 한도가 caption보다 넓다.
- GitHub/X 링크를 버튼으로 빼면 본문 가독성이 좋아진다.
- rich preview가 없어도 triage에 필요한 정보는 충분하다.

즉, 이 봇은 **“보기 좋은 카드”보다 “빠르게 읽히는 triage note”**가 더 중요하다.

---

## Telegram 기능 선택 원칙

Telegram Bot API는 `sendMessage`에서 `entities`, `link_preview_options`, `disable_notification`, `protect_content`, `reply_parameters`, `reply_markup`을 지원하고, `editMessageText`로 본문과 inline keyboard를 수정할 수 있다. 또한 Bot API 문서는 getUpdates와 webhook이 상호배타적이며, incoming updates는 최대 24시간만 저장된다고 설명한다. 따라서 v1에서 outbound-only notifier는 웹훅 없이도 충분하고, 추후 제어 명령을 붙일 때만 inbound update 수신 방식을 결정하면 된다. ([Telegram Bot API](https://core.telegram.org/bots/api))

이 문서 기반으로 고정할 원칙은 아래다.

### 사용
- `sendMessage`
- `editMessageText`
- `InlineKeyboardMarkup`
- `link_preview_options`
- `disable_notification`

### 제한적 사용
- `reply_parameters`  
  향후 follow-up reply 용도

### v1 비사용
- `ReplyKeyboardMarkup`
- media caption 중심 알림
- `allow_paid_broadcast`
- webhook 기반 inbound control plane

---

## `Notification` 도메인 모델을 세 층으로 나눈다

0단계에서 `Notification`은 최종 객체였지만, 실제 구현에서는 아래 세 층으로 나눠야 한다.

### 1) `notification_plan_v1`
정책이 확정된 전달 의도다.

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

역할:
- 무엇을
- 어디로
- 어떤 우선순위로
- 어떤 형식으로
보낼지 결정하는 계약

### 2) `notification_render_v1`
실제 Telegram 전송 payload 직전 상태다.

핵심 필드:
- `notification_plan_id`
- `message_text`
- `entities_json`
- `link_preview_options_json`
- `reply_markup_json`
- `disable_notification`
- `protect_content`
- `parse_strategy`
- `render_hash`

역할:
- 전송 가능한 텍스트/엔티티/버튼 구성

### 3) `notification_delivery_record_v1`
실제 Telegram 전송 결과다.

핵심 필드:
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

역할:
- 실제 어떤 message_id로 전송됐는지
- 이후 edit 가능한지
- 실패했는지
를 기록

핵심은 이것이다.

> **알림 의도와, 알림 렌더링과, 실제 전송 결과를 분리해야 한다.**

이걸 안 나누면 나중에 템플릿 수정, 재전송, edit 정책이 서로 엉킨다.

---

## 전달 대상(topology)은 단순하게 고정한다

v1은 아래처럼 고정하는 편이 좋다.

### 기본 대상
- `operator_chat_id` 1개
- 유형: 운영자 개인 DM

### 선택 대상
- `debug_chat_id`
- `digest_chat_id`

하지만 초기 운영에서는 **개인 DM 1개만 활성화**하는 것이 맞다.

그 이유는 명확하다.

- 사용자 요구가 “나에게 전송”이다.
- 다중 대상은 중복 억제와 실패 처리 복잡도를 급증시킨다.
- private DM은 topic/thread 고려가 거의 없다.
- 템플릿 튜닝이 가장 쉽다.

다만 스키마에는 `target_thread_id`를 남겨둬야 한다.  
나중에 supergroup topic으로 옮겨도 스키마 변경이 없게 하기 위해서다.

---

## 전달 결정과 우선순위를 다시 분리한다

0단계에서 `verdict`와 `delivery_decision`을 분리했다.  
7단계에서는 여기에 **urgency profile**을 하나 더 둬야 한다.

### 1) `verdict`
- `inspect_now`
- `later`
- `skip`

### 2) `delivery_decision`
- `send_now`
- `send_digest`
- `suppress`

### 3) `urgency_profile`
- `high`
- `normal_silent`
- `digest`
- `suppressed`

이 세 층을 분리해야 하는 이유는 다음과 같다.

- `later`라도 immediate send를 할 수 있다.
- `send_now`라도 소리 없이 보낼 수 있다.
- `inspect_now`라도 특정 시간대에는 digest로 내릴 수 있다.
- `skip`이더라도 debug 채널에는 남길 수 있다.

---

## v1 기본 전달 정책

사용자 요구를 반영하면 v1 기본 정책은 아래가 맞다.

### `inspect_now`
- `delivery_decision = send_now`
- `urgency_profile = high`
- `disable_notification = false`

### `later`
- `delivery_decision = send_now`
- `urgency_profile = normal_silent`
- `disable_notification = true`

### `skip`
- `delivery_decision = suppress`
- `urgency_profile = suppressed`

### `send_digest`
구조는 유지하되 기본 운영에서는 비활성.
아래와 같은 상황에서만 활성화 가능:
- 일시적 볼륨 폭증
- 채널 확장 이후 later 과다
- 테스트/리뷰 모드

이 방식이 좋은 이유는,
**즉시성 요구를 살리면서도 알림 소음은 urgency와 notification sound로 제어할 수 있기 때문**이다.

즉, `later`를 digest로 미루지 않아도 된다.  
대신 **조용하게 보내면 된다.**

---

## 알림 상태 머신을 지금 고정해야 한다

권장 상태는 아래다.

- `planned`
- `rendered`
- `queued`
- `sent`
- `edited`
- `suppressed`
- `failed_retryable`
- `failed_terminal`

상태 전이는 아래처럼 단순하게 유지한다.

```text
analysis_v1
  ↓
planned
  ↓
rendered
  ↓
queued
  ↓
sent
  ├─> edited
  ├─> failed_retryable
  └─> failed_terminal
```

핵심은 `suppressed`도 명시적 상태로 남긴다는 점이다.  
알림을 안 보낸 것도 시스템 결정의 일부이기 때문이다.

---

## 메시지 렌더링 포맷은 템플릿화해야 한다

0단계에서 이미 사용자 메시지 포맷을 잠갔다.  
7단계에서는 그걸 Telegram용으로 더 엄격하게 고정한다.

### 기본 single alert 템플릿

```text
[HIGH|MID] [GitHub|X|Idea]
제목: <headline>

판정: inspect_now | confidence 74
한줄 요약: ...
냉정 평가: ...
기존 도구 대비: ...
리스크: ...
추천 행동: ...

출처:
- 채널 관측 N건
- primary/source 타입
```

여기서 중요한 규칙은 아래다.

- 첫 3줄 안에 **severity + source type + headline + verdict**가 보여야 한다.
- `냉정 평가`는 항상 포함
- GitHub primary면 `기존 도구 대비`는 필수
- 링크는 본문에 길게 노출하지 않고 버튼으로 뺌
- 한 메시지에는 한 candidate만 다룸

### 버튼 템플릿

1행:
- `원문 Telegram`
- `Primary Link`

2행:
- `GitHub` 또는 `X`
- `Supporting`

3행:
- `왜 이렇게 판정했나`는 v1에서는 미사용  
  추후 callback/query 또는 follow-up command용 예약 슬롯

즉, 버튼은 **외부 원문 접근**만 담당하고,
설명은 본문 텍스트가 담당한다.

---

## explicit entities를 기본으로 한다

Bot API는 `parse_mode`를 사용할 수도 있고, `entities` 배열을 직접 넘길 수도 있다. 이 프로젝트는 rendering determinism이 중요하므로, 기본 렌더 전략은 **explicit entities builder**로 두는 편이 맞다. Telegram은 bold, italic, underline, strikethrough, spoiler, block quote, inline link, code 등 다양한 formatting entity를 지원한다. ([Telegram Bot API](https://core.telegram.org/bots/api))

이 설계가 좋은 이유는 다음과 같다.

- MarkdownV2 escaping 실수로 인한 전송 실패 위험이 줄어든다.
- 렌더 결과가 안정적이다.
- 추후 localization이나 필드 순서 변경 시 안전하다.
- 편집 시 동일 엔티티를 다시 계산하기 쉽다.

즉, `parse_mode`는 fallback이고, 기본은 entity builder다.

---

## 링크 미리보기는 기본적으로 끈다

Bot API는 `link_preview_options`에서 preview 비활성화와 일부 렌더 옵션을 지원한다. 이 시스템은 triage 속도가 중요하므로, 기본적으로 `is_disabled = true`를 두는 편이 맞다. preview를 켜면 메시지 표면이 길어지고, 버튼 기반 링크 구조와도 충돌하기 쉽다. ([Telegram Bot API](https://core.telegram.org/bots/api))

권장 기본값:
- `link_preview_options.is_disabled = true`

예외:
- 나중에 digest 메시지에서 대표 링크 하나만 preview로 보여주는 실험 가능
- 그러나 single alert 기본값은 disabled 고정

---

## `protect_content`는 기본 false로 둔다

Bot API는 `protect_content`로 전달물의 forwarding/saving을 막을 수 있다. 하지만 이 봇은 운영자 개인 workflow 도구이므로, 요약을 저장하거나 다른 채팅으로 옮기거나 노트로 남길 가능성이 높다. 따라서 기본값은 `false`가 맞다. ([Telegram Bot API](https://core.telegram.org/bots/api))

즉, 이 시스템의 목표는 보안 통제가 아니라 **개인 triage 효율**이다.

---

## `allow_paid_broadcast`는 구조에는 남기되 기본 금지

Bot API는 `allow_paid_broadcast=true`일 때 비용을 내고 매우 높은 초당 전송량을 허용한다. 그러나 이 프로젝트는 운영자 1명에게 보내는 저볼륨 봇이므로 전혀 필요 없다. 기본값은 무조건 `false`로 두는 것이 맞다. ([Telegram Bot API](https://core.telegram.org/bots/api))

즉,
이 옵션은 스키마 호환성 때문에 필드는 남겨두되,
운영 정책에서는 금지한다.

---

## single-shot send가 기본, edit는 예외

Bot API는 `editMessageText`를 지원하고, 메시지 편집은 reply markup이 없거나 inline keyboard가 있을 때 가능하다. 따라서 수정 자체는 가능하지만, v1 기본 전략을 “먼저 보내고 계속 수정”으로 두면 사용자가 흐름을 읽기 어렵고, message history도 지저분해진다. ([Telegram Bot API](https://core.telegram.org/bots/api))

권장 기본 정책은 아래다.

### 기본
- evidence bundle 완성
- analysis_v1 확정
- render 완료
- **한 번에 최종본 전송**

### 예외적으로 edit 허용
- material reroot 발생
- verdict upgrade (`later → inspect_now`)
- 핵심 링크 정정
- Telegram 전송 직후 템플릿 렌더 오류 보정
- X edit-window 안정화 후 headline/summary가 의미 있게 바뀜

### edit 금지
- 관측 채널 수 증가만 있음
- supporting source가 하나 더 붙었지만 결론은 동일
- wording만 미세 조정
- cosmetic formatting 수정

핵심은 **내용이 바뀌는 경우만 수정**하는 것이다.

---

## material change 기준을 명시적으로 둔다

수정/재전송 정책이 흔들리지 않게, `material_change_hash`를 지금부터 고정해야 한다.

권장 입력:
- `primary_artifact_canonical_id`
- `reroot_applied`
- `verdict`
- `severity_band`
- `summary_one_line_ko`
- `skeptical_take_ko`
- `recommended_action_ko`
- `primary_snapshot_fingerprint`

이 해시가 변했더라도 무조건 재전송하면 안 된다.  
그래서 아래 룰을 추가로 둔다.

### 편집 대상
- 같은 `dedupe_subject_key`
- 최근 알림
- material change 발생
- 아직 같은 메시지를 계속 보는 게 사용자에게 더 유리함

### 새 메시지 대상
- 같은 후보라도 `later → inspect_now`
- primary reroot 발생
- GitHub repo 주체가 바뀜
- 기존 메시지가 너무 오래되어 문맥 단절이 큼

---

## dedupe subject key는 “사용자 체감 기준”으로 잡아야 한다

이 부분이 중요하다.

알림 dedupe를 snapshot 단위로 잡으면, 같은 repo의 commit이 바뀔 때마다 새 알림이 나간다.  
반대로 너무 넓게 잡으면 중요한 변경도 묻힌다.

그래서 권장 subject key는 아래처럼 잡는 것이 맞다.

### GitHub primary
- `notify:github_repo:{owner}/{repo}`

### X primary
- `notify:x_post:{post_id}`

### text_idea primary
- `notify:text_idea:{source_message_id}`  
  또는 source-scoped hash

즉, **사용자가 “같은 대상”이라고 느끼는 단위**로 dedupe해야 한다.

snapshot fingerprint는 재전송 여부 판단에 쓰고,
subject key는 알림 주체 통합에 쓴다.

---

## 채널 관측 수는 보내되, 알림을 새로 만들 이유는 아니다

같은 repo가 여러 채널에 올라오는 일은 자주 생긴다.  
하지만 그 자체로 새 알림을 만들면 노이즈가 급증한다.

권장 규칙:
- candidate가 이미 전송됨
- 새 source message가 같은 subject key를 참조
- 기존 분석이 유효

이 경우:
- observation count만 증가
- 필요 시 기존 message edit로 `채널 관측 N건`만 갱신
- 기본은 무수정

즉,
**재관측은 evidence이며, 재알림 조건은 아니다.**

---

## digest 구조는 지금 설계만 해둔다

v1 기본은 immediate send지만, `send_digest` 경로는 아예 없애면 나중에 다시 뜯게 된다.

권장 digest 정책:
- key: `digest:{target_chat_id}:{time_bucket}`
- bucket duration: 3h 또는 6h
- 포함 대상: `delivery_decision = send_digest`
- 정렬: severity desc → freshness desc
- 한 digest당 최대 항목 수 상한

digest 메시지는 여러 candidate를 담기 때문에, single alert와 다른 템플릿을 써야 한다.

예:
```text
[Digest] 후보 7건
1. [GitHub] ...
2. [X] ...
3. [Idea] ...
```

다만 이건 **구조만 유지**하고, v1 운영 기본값에서는 비활성이다.

---

## notifier는 분석을 재해석하면 안 된다

이건 명시적으로 적어둘 필요가 있다.

`notifier-telegram`이 하면 안 되는 것:
- 점수 다시 계산
- verdict override
- comparables 삭제/추가
- summary 재작성
- supporting artifact 중요도 재판정

notifier가 해도 되는 것:
- 템플릿 적용
- 길이 예산 맞춤
- 문장 압축
- 버튼 구성
- alert level 배지화

즉, notifier는 **의미를 바꾸지 않는 presentation 계층**이어야 한다.

---

## 길이 예산과 절단 규칙

Telegram `sendMessage`는 4096자 제한이 있다. 따라서 렌더러는 길이 예산을 명시적으로 관리해야 한다. ([Telegram Bot API](https://core.telegram.org/bots/api))

권장 예산:
- headline: 짧게
- summary_one_line: 1문장
- skeptical_take: 1~2문장
- comparables: 최대 2~3개
- red_flags: 최대 2개
- evidence_limitations: 필요 시 1개만 노출
- 링크는 버튼으로 외부화

권장 절단 우선순위:
1. limitations 축소
2. comparables 개수 축소
3. why_it_might_matter 축소
4. red_flags 축소
5. skeptical_take는 마지막까지 유지

이 규칙이 좋은 이유는,
이 봇의 핵심 가치는 **냉정 평가와 추천 행동**에 있기 때문이다.

---

## 실패 처리 정책

전송 실패는 크게 두 종류로 나눈다.

### retryable
- 일시적 네트워크 장애
- Telegram API 일시 오류
- 시간 지연 후 재시도해볼 가치가 있는 경우

### terminal
- chat access 상실
- 잘못된 chat_id
- 렌더 payload 자체가 잘못됨
- 운영 정책상 보내면 안 되는 경우

핵심 규칙:
- render bug는 `failed_terminal`
- transport glitch는 `failed_retryable`
- retry는 지수 백오프
- 동일 render_hash에 대한 무한 재시도 금지

---

## outbound-only 원칙은 유지한다

Telegram Bot API는 updates 수신 방식으로 `getUpdates`와 webhook을 제공하지만, 둘은 상호배타적이다. 또 updates는 서버에 최대 24시간만 저장된다. 현재 이 프로젝트의 notifier는 outbound-only이므로, v1에서는 둘 다 필요하지 않다. 추후 `/pause`, `/resume`, `/why` 같은 제어 명령을 붙일 때 low-volume 운영이면 `getUpdates`가 단순한 선택지다. ([Telegram Bot API](https://core.telegram.org/bots/api))

즉,
7단계에서도 **inbound control plane은 설계만 고려하고 구현은 보류**한다.

---

## v1에서 권장하는 실제 템플릿

### HIGH / inspect_now
```text
[HIGH] [GitHub]
제목: owner/repo

판정: inspect_now | confidence 78
한줄 요약: ...
냉정 평가: ...
기존 도구 대비: ...
리스크: ...
추천 행동: 지금 5분 확인
```

옵션:
- `disable_notification = false`

### MID / later
```text
[MID] [X]
제목: ...

판정: later | confidence 64
한줄 요약: ...
냉정 평가: ...
아이디어 가치: ...
리스크: ...
추천 행동: 저장만 해둘 가치 있음
```

옵션:
- `disable_notification = true`

### suppressed
- 전송 안 함
- plan/record만 남김

---

## 이번 단계에서 반드시 피해야 할 반패턴

1. notifier가 verdict를 다시 바꾸는 것  
2. 한 메시지에 여러 candidate를 억지로 넣는 것  
3. `parse_mode` 문자열 조합에만 의존하는 것  
4. preview를 기본 켜는 것  
5. same subject 재관측마다 새 알림 보내는 것  
6. single-shot 대신 placeholder send 후 과도한 edit 남발  
7. `later`를 무조건 digest로 미루는 것  
8. `protect_content=true`로 운영자 workflow를 막는 것  
9. `allow_paid_broadcast`를 켜서 해결하려는 것  
10. 알림 의도 / 렌더 / 전송 결과를 한 테이블에 섞는 것

특히 1, 5, 10은 앞선 0~6단계 구조를 직접 깨뜨린다.

---

## 7단계 완료 기준

아래가 고정되면 7단계는 끝이다.

- `notification_plan_v1` 스키마 확정
- `notification_render_v1` 스키마 확정
- `notification_delivery_record_v1` 스키마 확정
- single alert 템플릿 확정
- urgency profile 확정
- immediate send 기본 정책 확정
- digest 구조 예약 확정
- dedupe subject key / material change 기준 확정
- single-shot vs edit 정책 확정
- render length budget 확정
- failure classification 확정

---

## 이번 단계의 한 줄 결론

**7단계는 `analysis_v1`을 Telegram용 triage note로 안정적으로 렌더링하고, 즉시 전송·조용한 전송·억제·수정·재전송을 deterministic하게 통제하는 전달 계층이다.**

즉,  
**좋은 분석이 있어도 전달 정책이 흔들리면 전체 시스템은 금방 스팸 봇처럼 보인다.**  
7단계의 역할은 그걸 막는 것이다.


---

## Source file: `08_stage8_observability_replay_recovery.md`

# 8단계: 로그, 모니터링, 재처리/복구 정책 설계

이번 단계의 목적은 하나다.

**0~7단계에서 설계한 파이프라인이 실제 운영 중에도 설명 가능하고, 다시 돌릴 수 있고, 장애 후 복원 가능하도록 만드는 것**이다.

이 단계가 빠지면 앞선 단계에서 아무리 구조를 잘 나눠도 운영 중에는 결국 아래 문제가 생긴다.

- 왜 이 알림이 왔는지 설명 못함
- 어디서 막혔는지 추적 못함
- 재부팅 후 어떤 작업을 다시 돌려야 하는지 모름
- 프롬프트/정책 변경 후 과거 후보를 안전하게 재판정 못함
- 외부 API 장애가 났을 때 어디까지 손상됐는지 복기 못함

따라서 8단계는 부가 기능이 아니라, **0~7단계를 실제 시스템으로 고정시키는 운영 계층**이다.

---

## 이번 단계에서 고정할 핵심 결정

| 구분 | 고정 결정 |
|---|---|
| 운영 진실의 원천 | **PostgreSQL** |
| 실시간 실행 상태 | **Redis** |
| 로그 형식 | **JSON structured logs** |
| 추적 기준 | **end-to-end correlation IDs** |
| 메트릭 계층 | **service metrics + pipeline metrics + business metrics** |
| 재처리 단위 | **source / artifact / bundle / judge / notify** |
| 복구 원칙 | **Redis는 버려도 되고, Postgres로 재구성 가능해야 함** |
| 장애 격리 | **stage별 dead-letter / retry budget 분리** |
| 대량 재판정 | **live path와 분리된 offline replay path** |
| 알림 정책 | **운영 알림과 사용자 알림 분리** |

핵심은 이것이다.

> **이 시스템은 로그를 읽어서 복구하는 구조가 아니라, Postgres에 저장된 durable state를 기준으로 복구하는 구조여야 한다.**

2단계에서 이미 `PostgreSQL = system of record`, `Redis = queue/lock`을 잠갔다.  
8단계는 이 원칙을 실제 복구 절차까지 밀어붙이는 단계다.

---

## 전체 구조 안에서의 위치

8단계는 특정 서비스 하나를 추가하는 단계가 아니다.  
전체 파이프라인에 횡단면으로 들어간다.

```text
SourceMessage
  ↓
Artifact / CandidateGroup
  ↓
ArtifactSnapshot / EvidenceBundle
  ↓
JudgeOutput / Analysis
  ↓
NotificationPlan / DeliveryRecord

↑ 이 전 구간에 observability, replay, recovery가 걸쳐야 한다.
```

즉, 8단계는 새 비즈니스 로직이 아니라 **운영 제어면(control plane)** 이다.

---

## 1. 추적 단위는 “로그 라인”이 아니라 “파이프라인 객체”여야 한다

이 시스템에서 추적해야 할 기본 개체는 0단계에서 이미 고정했다.

- `SourceMessage`
- `Artifact`
- `CandidateGroup`
- `ArtifactSnapshot`
- `CandidateEvidenceBundle`
- `JudgeOutput`
- `Analysis`
- `Notification`

8단계에서는 여기에 운영 객체를 추가한다.

### 추가 운영 객체

#### `pipeline_run`
특정 입력이 파이프라인을 통과한 1회 실행.

핵심 필드:
- `pipeline_run_id`
- `trigger_source`
- `run_kind` (`live`, `bootstrap`, `replay`, `manual`)
- `root_object_type`
- `root_object_id`
- `started_at`
- `finished_at`
- `terminal_status`

#### `job_attempt`
큐 기반 작업의 개별 시도 기록.

핵심 필드:
- `job_attempt_id`
- `stage_name`
- `queue_name`
- `attempt_no`
- `lease_owner`
- `started_at`
- `finished_at`
- `attempt_status`
- `error_code`
- `retry_after_at`

#### `state_transition`
핵심 객체 상태 변화의 감사 로그.

예:
- `tracked_chat`: `resolving → active`
- `candidate_group`: `proposed → ready_for_enrich → ready_for_analysis`
- `analysis`: `draft → validated → finalized`

#### `dead_letter_entry`
retry budget를 초과한 작업의 격리 레코드.

#### `replay_request`
운영자 또는 정책이 재처리를 요청한 기록.

---

## 2. correlation ID는 처음부터 끝까지 살아 있어야 한다

이 단계에서 가장 중요하게 잠가야 할 운영 규칙은 이것이다.

> **모든 로그, 메트릭, 상태 전이는 공통 correlation ID 집합을 공유해야 한다.**

권장 핵심 ID:

- `pipeline_run_id`
- `source_message_id`
- `candidate_group_id`
- `artifact_id`
- `artifact_snapshot_id`
- `bundle_id`
- `judge_output_id`
- `analysis_id`
- `notification_id`

서비스별로 일부만 갖고 있어도 되지만, 최소한 `pipeline_run_id`와 자기 단계의 핵심 객체 ID는 반드시 찍혀야 한다.

이걸 안 하면 다음이 불가능해진다.

- “이 텔레그램 글이 어떤 EvidenceBundle이 됐는가?”
- “이 Analysis는 어떤 prompt/policy 버전에서 나왔는가?”
- “이 알림은 왜 suppress되지 않았는가?”
- “어느 단계에서 retry storm가 났는가?”

---

## 3. 텔레메트리는 3층으로 나눈다

이 프로젝트는 텔레메트리를 한 종류로 처리하면 안 된다.  
최소 아래 세 층으로 나누는 편이 맞다.

### A. structured logs
목적:
- 디버깅
- 원인 분석
- 개별 실행 복기

권장 공통 필드:
- `ts`
- `level`
- `service`
- `env`
- `pipeline_run_id`
- `stage`
- `event`
- `object_type`
- `object_id`
- `status`
- `duration_ms`
- `error_code`

### B. metrics
목적:
- 모니터링
- 알림 조건 평가
- 장기 추세 관찰

유형:
- counter
- gauge
- histogram

### C. durable audit rows
목적:
- 재처리
- 정책 재적용
- 복구 기준점 확보

이 3가지를 섞으면 안 된다.  
로그는 상세하지만 영속 기준점이 아니고, 메트릭은 집계용이며, durable audit row만이 replay와 recovery의 기준이다.

---

## 4. 메트릭은 “서비스 건강”과 “제품 건강”을 분리해야 한다

운영에서 자주 망가지는 부분이다.  
CPU, 메모리, 에러율만 보면 시스템은 멀쩡해 보이는데, 실제로는 좋은 후보를 하나도 못 보내는 상황이 생긴다.

그래서 메트릭은 최소 세 분류로 나눈다.

### 4-1. service health metrics

예:
- collector heartbeat age
- queue depth
- retry count
- external API latency
- 429/5xx rate
- DB connection saturation
- Redis lock contention

### 4-2. pipeline metrics

예:
- `source_message_ingested_total`
- `normalization_suppressed_total`
- `candidate_group_proposed_total`
- `artifact_snapshot_partial_total`
- `bundle_ready_total`
- `judge_schema_retry_total`
- `analysis_policy_override_total`
- `notification_send_failed_total`

### 4-3. product quality metrics

예:
- `inspect_now_sent_total`
- `later_sent_total`
- `skip_suppressed_total`
- reroot 비율
- GitHub/X/text_idea 비중
- HIGH 알림 후 실제 열람률
- false positive 피드백률
- 채널별 유효 후보 비율

핵심은 이것이다.

> **운영 정상과 제품 유효성은 별개다. 둘 다 따로 봐야 한다.**

---

## 5. 단계별 SLO/SLA 후보를 지금 잠근다

SLO를 너무 정교하게 잡을 필요는 없지만, 방향은 지금 정해야 한다.

권장 SLO v1:

- `inspect_now` 후보의 **source_message → notification sent** p95: 120초 이내
- collector freshness lag p95: 30초 이내
- bundle assembly success rate: 95% 이상
- structured output schema valid rate: 99% 이상
- notification send success rate: 99% 이상
- replay success rate: 95% 이상

중요한 점은 **later**와 **offline replay**는 같은 SLO를 쓰지 않는다는 점이다.  
live path와 replay path는 목표 시간이 달라야 한다.

---

## 6. 경보(alert)는 사용자 알림과 완전히 분리한다

7단계에서 이미 사용자 알림과 notifier를 정의했다.  
8단계에서는 운영자 경보 채널을 별도로 둬야 한다.

### 사용자 알림
목적:
- 좋은 후보 전달

### 운영자 경보
목적:
- 시스템 장애 전달

운영자 경보 예시:
- collector heartbeat stale
- tracked chat gap scan failure
- GitHub 429 burst
- X quota exhaustion
- OpenAI 429/503 burst
- schema validation failure spike
- notification failure burst
- dead-letter backlog 증가
- Postgres replication/backup failure
- TDLib state load failure

이걸 분리하지 않으면 사용자 채팅방이 운영 오류로 오염된다.

---

## 7. 외부 API별 backoff/alert 정책을 지금 고정한다

외부 의존성이 많은 시스템이라, stage마다 다른 backoff 규칙이 있으면 운영이 금방 꼬인다.

### Telegram notifier
Telegram Bot API는 실패 응답의 `ResponseParameters.retry_after`로 flood control 재시도 대기 시간을 제공한다. 따라서 notifier는 일반 retry가 아니라 **Telegram이 준 retry window를 우선 존중**해야 한다. 또한 메시지 편집은 text message 기준 `editMessageText`로 가능하므로, resend와 edit를 다른 실패 종류로 구분해야 한다.  
운영 정책:
- `retry_after` 수신 시 해당 notification attempt를 지연 재큐잉
- repeated flood control이면 `later`/digest 계열 전송 우선순위 하향
- edit 실패는 resend fallback 정책 별도 유지

### GitHub enricher
GitHub는 `retry-after`가 있으면 그 시간까지 재시도하지 말고, `x-ratelimit-remaining=0`이면 `x-ratelimit-reset` 시각까지 멈추라고 권고한다. secondary rate limit 상황에서도 exponential backoff를 적용하라고 명시한다.  
운영 정책:
- per-host circuit breaker
- rate-limit 상태를 서비스 전체 health metric으로 집계
- retry budget 소진 시 `partial_snapshot`로 종료하고 dead-letter가 아니라 deferred retry queue로 이동

### X enricher
X는 모든 응답에 `x-rate-limit-limit`, `x-rate-limit-remaining`, `x-rate-limit-reset` 헤더를 포함한다고 문서화하고, 429/5xx에는 exponential backoff와 partial error 검사를 권장한다. multi-resource 요청은 200이어도 `errors` 배열이 같이 올 수 있으므로, 성공/실패를 이진으로 보지 말아야 한다.  
운영 정책:
- 200 + partial errors를 `partial_snapshot`로 기록
- `x-rate-limit-reset` 기준 sleep
- usage cap과 rate limit을 구분해 경보

### OpenAI judge
OpenAI는 API key usage를 Usage page에서 추적할 수 있고, 비용 알림 threshold를 둘 수 있다. 또한 rate limit 가이드는 exponential backoff를 권장하고, 실패 요청도 per-minute limit에 기여할 수 있다고 설명한다.  
운영 정책:
- 429/503는 exponential backoff + jitter
- project hard cap은 앱 내부에서 강제
- mini/full 승급 비율 초과 시 자동 강등
- OpenAI usage/cost 대시보드는 참고용, live 차단은 앱 내부 budget guard가 담당

---

## 8. OpenAI는 절대 audit source가 아니다

이건 이번 단계에서 꼭 잠가야 한다.

OpenAI 문서상 API key usage 추적은 Usage page에서 볼 수 있고 비용 알림 threshold도 설정할 수 있다. 하지만 Responses API의 application state 보존은 기본적으로 30일 수준이고, abuse monitoring logs도 기본적으로 최대 30일 보존이다. 따라서 장기 재현, replay, 감사 추적이 중요하다면 **애플리케이션 자체 저장소에 증거와 결과를 남겨야 한다.**

즉:
- OpenAI dashboard = 운영 참고 지표
- OpenAI stored response state = 보조 상태
- **PostgreSQL의 bundle / judge / analysis rows = 실제 audit source**

이 원칙을 안 박아두면 나중에 “OpenAI에 남아 있으니 괜찮다”는 잘못된 전제가 생긴다.

---

## 9. replay는 5종류로 명시적으로 나눈다

“재처리”를 하나의 버튼으로 만들면 나중에 비용과 혼란이 폭발한다.  
replay는 최소 아래처럼 나눠야 한다.

### 9-1. source replay
대상:
- `SourceMessage`
목적:
- normalization 규칙 변경 검증
- candidate 생성 로직 재평가

다시 도는 단계:
- 4단계부터

### 9-2. enrich replay
대상:
- `Artifact` 또는 `CandidateGroup`
목적:
- GitHub/X/web snapshot 갱신
- broken/partial snapshot 복구

다시 도는 단계:
- 5단계부터

### 9-3. judge replay
대상:
- `CandidateEvidenceBundle`
목적:
- prompt / model / policy 버전 변경 실험
- offline 품질 재평가

다시 도는 단계:
- 6단계부터

### 9-4. delivery replay
대상:
- `NotificationPlan`
목적:
- 전송 실패 복구
- 텔레그램 flood control 이후 재전송

다시 도는 단계:
- 7단계부터

### 9-5. full pipeline replay
대상:
- 특정 `pipeline root`
목적:
- 큰 구조 변경 후 end-to-end 검증

이 구분이 중요한 이유는, **다시 읽을 필요가 없는 단계까지 매번 재실행하지 않기 위해서**다.

---

## 10. replay는 overwrite가 아니라 새 버전을 만든다

이건 아주 중요하다.

> **replay는 기존 결과를 덮어쓰지 않고, 새 run / 새 snapshot / 새 judge / 새 analysis를 만든다.**

그래야 아래가 가능하다.

- 정책 버전별 비교
- 모델 버전별 비교
- false positive 원인 추적
- “당시 왜 그렇게 판단했는지” 설명

권장 규칙:
- old row update 금지
- `supersedes_*` 또는 lineage 필드로 연결
- `current_best_*` 포인터는 별도로 관리

즉, durable history와 current pointer를 분리한다.

---

## 11. idempotency key를 단계별로 다르게 둔다

재시도와 재처리가 많은 시스템에서는 idempotency가 없으면 중복 알림과 중복 비용이 발생한다.

권장 예시:

- normalization:
  - `norm:{source_message_id}:{revision_no}:{normalizer_version}`
- enrich:
  - `enrich:{artifact_id}:{profile}:{snapshot_input_hash}`
- bundle:
  - `bundle:{candidate_group_id}:{bundle_policy_version}:{supporting_set_hash}`
- judge:
  - `judge:{bundle_id}:{prompt_version}:{model}:{effort}`
- notify:
  - `notify:{analysis_id}:{delivery_policy_version}:{chat_id}:{template_version}`

이 키는 Redis lock뿐 아니라 **Postgres unique guard**에도 반영하는 편이 맞다.

---

## 12. dead-letter는 stage별로 분리한다

dead-letter queue를 하나로 합치면 운영자가 어떤 오류를 먼저 봐야 하는지 판단하기 어렵다.

권장 분리:
- `dlq_normalization`
- `dlq_enrich_github`
- `dlq_enrich_x`
- `dlq_bundle`
- `dlq_judge`
- `dlq_notify`

그리고 DLQ entry에는 최소 아래가 있어야 한다.

- 마지막 error code
- 마지막 raw error snippet
- retry count
- first_failed_at
- last_failed_at
- next_manual_action
- replay hint

핵심은 DLQ가 “무덤”이 아니라 **운영 큐레이션 대상**이어야 한다는 점이다.

---

## 13. Redis는 잃어도 되고, Postgres는 잃으면 안 된다

2단계와 연결되는 복구 원칙이다.

### Redis가 맡는 것
- lock
- in-flight queue
- debounce
- short-lived counters

### Postgres가 맡는 것
- source
- revisions
- artifacts
- snapshots
- bundles
- judge outputs
- analyses
- notifications
- replay requests
- job attempts
- state transitions

즉, Redis를 날려도 아래가 가능해야 한다.

- stale in-progress job 회수
- pending candidate 재큐잉
- notify 미완료분 복구
- replay 요청 재적재

**Redis 복구 = rebuild**  
**Postgres 복구 = restore**

---

## 14. 장애 유형별 복구 플레이북을 지금 잠근다

### A. worker 프로세스 죽음
처리:
- lease timeout 초과한 in-progress job 탐지
- `job_attempt` 종료 상태를 `abandoned`
- retry budget 남았으면 재큐잉
- stage별 poison detection 적용

### B. VPS 재부팅
처리:
1. Postgres 기동
2. Redis 기동
3. TDLib state mount 확인
4. collector 단일 인스턴스 기동
5. active chat gap scan 실행
6. durable pending jobs 기준으로 큐 재구성
7. notifier backlog 재개

### C. GitHub/X/OpenAI 장애
처리:
- stage별 circuit breaker open
- fresh work는 `deferred` 상태로 축적
- live HIGH 후보라도 evidence 부족이면 judge 승급 금지
- outage 종료 후 batch replay / deferred replay 실행

### D. schema/prompt/policy 변경
처리:
- 기존 분석은 immutable
- 새로운 `judge_output` / `analysis` 생성
- old/new diff 저장
- 운영 알림 없이 shadow replay 가능

### E. Telegram flood control
처리:
- `retry_after` 존중
- notification resend queue로 이동
- repeated flood면 digest로 우회 가능하되, `inspect_now` 기본 정책은 유지

---

## 15. 대량 재판정은 live path와 분리한다

6단계에서 hot path와 escalation path를 이미 잠갔다.  
8단계에서는 거기에 **offline replay path**를 추가해야 한다.

권장 구분:
- live path: 일반 Responses API, low concurrency
- offline replay path: 별도 worker pool, 별도 budget, 별도 queue

OpenAI는 Batch API에 별도 rate-limit pool을 두고 있으며, standard per-model rate limits와 분리된다고 문서화한다. batch 1건은 최대 50,000 requests, input file 200MB까지 허용된다. 따라서 prompt/policy 변경 후 과거 후보를 대량 재판정할 때는 live path를 막는 대신 **Batch 또는 별도 offline judge queue**를 쓰는 편이 맞다.

핵심은 이것이다.

> **live path latency와 replay throughput을 같은 파이프에서 해결하려 하면 둘 다 망가진다.**

---

## 16. 버전 축은 4개를 동시에 기록해야 한다

과거 분석을 비교하려면 최소 아래 버전들이 다 찍혀야 한다.

- `normalizer_version`
- `enrich_profile_version`
- `prompt_version`
- `policy_version`
- 필요 시 `template_version`

예를 들어 false positive가 나왔을 때 원인은 여러 가지일 수 있다.

- 트리거가 과했다
- GitHub snapshot이 얕았다
- prompt가 과대평가했다
- policy threshold가 낮았다
- 알림 템플릿이 과장되게 렌더링했다

버전 축을 안 찍으면 이걸 분리해서 못 본다.

---

## 17. 개인정보/민감정보 관점의 저장 경계

이 시스템은 public channel 위주지만, 저장 경계는 미리 정해야 한다.

원칙:
- Telegram 원문은 저장
- 외부 API raw response는 최소화
- binary media는 기본 저장 안 함
- text excerpt는 분석 필요량만 저장
- secrets와 user-controlled content를 같은 로그 라인에 두지 않음

특히 structured logs에 아래는 금지다.

- API key
- bearer token
- full raw HTTP headers
- TDLib auth material

---

## 18. 운영 대시보드는 최소 4개면 충분하다

초반부터 Grafana를 거창하게 만들 필요는 없지만, 화면 기준은 명확해야 한다.

### Dashboard A. Live pipeline
- collector freshness
- queue depth
- stage latency
- success/fail rate
- current dead-letter

### Dashboard B. External dependencies
- GitHub 403/429
- X 429 / partial error rate
- OpenAI 429/503
- Telegram notify flood control
- circuit breaker state

### Dashboard C. Product quality
- inspect_now / later / skip 추이
- channel별 candidate yield
- reroot 비율
- replay 후 verdict 변화율

### Dashboard D. Cost and token
- OpenAI input/output/reasoning tokens
- cached input tokens
- model mix
- escalation ratio
- offline replay 비용

---

## 19. 이번 단계에서 반드시 피해야 할 반패턴

1. 로그만 있으면 replay 가능하다고 착각  
2. Redis 큐를 durable source처럼 취급  
3. dead-letter를 stage 구분 없이 하나로 합침  
4. 알림 실패와 judge 실패를 같은 오류 축으로 처리  
5. replay가 기존 analysis row를 덮어씀  
6. prompt/policy 버전을 저장하지 않음  
7. external API 429를 generic retry로만 처리  
8. OpenAI dashboard를 audit source처럼 사용  
9. 운영 경보를 사용자 알림 채널로 보냄  
10. live path와 offline replay path를 합침

특히 2, 5, 8, 10은 구조를 크게 망가뜨린다.

---

## 20. 8단계 완료 기준

아래가 확정되면 8단계는 끝이다.

- `pipeline_run`, `job_attempt`, `state_transition`, `dead_letter_entry`, `replay_request` 스키마 확정
- correlation ID 규칙 확정
- structured log 공통 필드 확정
- service/pipeline/product metrics 목록 확정
- SLO v1 확정
- 운영자 경보 규칙 확정
- stage별 backoff / circuit breaker 정책 확정
- replay 종류와 overwrite 금지 규칙 확정
- Redis rebuild / Postgres restore 원칙 확정
- 장애 유형별 복구 플레이북 확정
- live path / offline replay path 분리 확정
- 버전 축 기록 규칙 확정

---

## 이번 단계의 한 줄 결론

**8단계는 이 봇을 “작동하는 코드”에서 “운영 가능한 시스템”으로 바꾸는 단계이며, 모든 객체를 durable state와 correlation ID로 추적하고, replay는 새 버전 생성 방식으로만 수행하며, Redis는 재구성 가능해야 하고 Postgres만이 복구 기준점이 되어야 한다.**


---

## Source file: `09_stage9_quality_tuning_eval_framework.md`

# 9단계: 품질 튜닝과 골든셋/평가 체계 설계

이번 단계는 앞선 0~8단계를 실제로 **안정적으로 개선할 수 있게 만드는 제어 계층**이다.

핵심 목표는 하나다.

> 이 시스템이 시간이 지나도 더 좋아지도록 만들되, 좋아진 척만 하고 실제로는 `inspect_now` 품질이 무너지는 상황을 막는 것.

이번 단계는 단순한 “모델 성능 평가”가 아니다. 이 프로젝트는 이미 다단계 파이프라인으로 설계되어 있으므로, 품질 평가도 **파이프라인 전체**를 기준으로 잡아야 한다.

---

## 이번 단계에서 유지해야 하는 전제

이 문서는 앞선 고정 결정을 그대로 전제로 한다.

- **0단계**: `SourceMessage → Artifact → CandidateGroup → Analysis → Notification`
- **1단계**: 계정/권한/키 분리
- **2단계**: `PostgreSQL = system of record`, `Redis = queue/lock`, 저동시성 런타임
- **3단계**: Telegram collector는 원문 보존 경계
- **4단계**: deterministic trigger / normalization / proposal
- **5단계**: external enricher가 `EvidenceBundle` 생성
- **6단계**: LLM은 `judge_output_v1`만 만들고, 최종 `analysis_v1`은 policy engine이 확정
- **7단계**: notifier는 presentation 계층이며 verdict를 재해석하지 않음
- **8단계**: replay는 overwrite가 아니라 새 run / 새 snapshot / 새 analysis를 생성

즉, 9단계의 평가는 **“모델이 잘했나”**가 아니라,

- 정규화가 제대로 되었는가
- enrich가 충분한 증거를 모았는가
- judge가 일관되게 냉정한 평가를 했는가
- policy가 올바르게 `inspect_now / later / skip`을 확정했는가
- notifier가 그 결과를 왜곡 없이 전달했는가

를 전부 본다.

---

## 이번 단계에서 고정할 핵심 결정

| 구분 | 고정 결정 |
|---|---|
| 품질 기준 단위 | **모델 단위가 아니라 파이프라인 단위** |
| 정본 데이터 | **snapshot 기반 eval fixture** |
| 핵심 세트 | **goldset + slices + shadow replay set** |
| 평가 계층 | **Tier 0~Tier 5 다층 평가** |
| release gate | **prompt/policy/schema 변경 전 필수 통과** |
| feedback 원천 | **offline human triage 우선, 실시간 유저 인터랙션은 선택 사항** |
| 재판정 정책 | **새 version 생성만 허용, overwrite 금지** |
| 개선 대상 분리 | **normalization / enrichment / judge / policy / delivery를 따로 튜닝** |
| 주요 KPI | **`inspect_now` 정밀도, reroot 정확도, false positive 억제, delivery 유용성** |
| baseline 관리 | **버전별 baseline 저장 + 회귀 비교** |

이 단계의 핵심은 **“좋아 보이는 프롬프트를 하나 찾는 것”이 아니라, 어떤 변경이 실제 품질 개선인지 증명하는 체계**를 만드는 것이다.

---

## 왜 이 단계가 필요한가

이 시스템은 단일 모델 호출이 아니다.

```text
Telegram SourceMessage
  ↓
Trigger / Normalization
  ↓
Artifact / CandidateGroup proposal
  ↓
External Enrichment
  ↓
EvidenceBundle
  ↓
LLM Judge
  ↓
Policy Engine
  ↓
Telegram Delivery
```

따라서 품질 저하 원인도 여러 군데에서 발생한다.

- GitHub canonicalization이 잘못되면 엉뚱한 repo를 분석한다.
- X → GitHub reroot가 틀리면 아이디어 글을 repo 평가처럼 다룬다.
- enrichment가 얕으면 좋은 repo도 `weak_evidence`가 된다.
- judge prompt가 흔들리면 허세 repo가 `inspect_now`로 올라온다.
- policy threshold가 과격하면 좋은 후보도 `skip`된다.
- notifier가 과하게 요약하면 핵심 리스크가 사라진다.

즉, **품질 개선은 prompt engineering만으로 해결되지 않는다.**

---

## 품질 체계의 핵심 개체

9단계에서는 아래 개체를 새로 고정하는 편이 맞다.

### 1) `eval_case`
한 개의 평가 사례.

핵심 필드 예시:
- `eval_case_id`
- `case_type`
- `source_fixture_refs`
- `expected_primary_artifact`
- `allowed_verdicts`
- `required_reason_codes`
- `forbidden_reason_codes`
- `required_limitations`
- `slice_tags`
- `notes`

### 2) `eval_slice`
비슷한 실패 모드를 묶는 분류 단위.

예:
- `github_strong_tool`
- `github_wrapper_hype`
- `x_idea_with_repo`
- `text_only_workflow`
- `multi_link_split`
- `reroot_required`
- `ai_noise`
- `broken_or_partial`

### 3) `eval_run`
특정 버전 조합으로 실행된 평가 단위.

핵심 필드 예시:
- `eval_run_id`
- `prompt_version`
- `policy_version`
- `normalization_version`
- `bundle_profile_version`
- `grader_version`
- `started_at`
- `completed_at`
- `result_summary_json`

### 4) `grader_result`
개별 평가 기준의 결과.

예:
- `primary_artifact_match`
- `reroot_correct`
- `verdict_allowed`
- `reason_code_present`
- `skeptical_take_present`
- `delivery_render_valid`

### 5) `release_decision`
실제 배포 승인을 기록하는 객체.

포함 항목 예시:
- `candidate_version_set`
- `baseline_version_set`
- `gate_results`
- `decision`
- `approved_by`
- `approved_at`
- `rollback_target`

---

## 정답 데이터는 live URL이 아니라 snapshot fixture여야 한다

이건 구조적으로 매우 중요하다.

9단계의 평가 입력은 live URL이 아니라 **앞선 단계에서 이미 저장된 snapshot fixture**여야 한다.

### 이유

1. GitHub/X 원문은 시간이 지나면 바뀐다.
2. 삭제, edit, force-push, archive 변경 때문에 동일 URL이 동일 의미를 보장하지 않는다.
3. 8단계에서 replay를 새 version으로 관리하기로 했기 때문에, 평가도 동일한 snapshot 위에서 재현 가능해야 한다.

따라서 eval fixture는 아래 중 하나를 기준으로 구성한다.

- `source_message_revision` snapshot
- `artifact_snapshot`
- `candidate_evidence_bundle`
- `analysis_v1` baseline

핵심 원칙은 이것이다.

> **평가 입력은 재현 가능한 frozen state여야 한다.**

live URL을 바로 평가 입력으로 쓰면, 오늘 통과한 테스트가 내일 같은 코드로 실패하는 일이 생긴다.

---

## 골든셋은 “정답 하나”보다 “허용 범위”가 중요하다

0단계에서 이미 verdict는 완전한 점수 정답보다 **허용 범위**로 두는 것이 낫다고 잠갔다. 9단계는 그것을 평가 체계로 구체화하는 단계다.

### 권장 라벨 방식

#### verdict
- `allowed_verdicts = {inspect_now}`
- `allowed_verdicts = {later}`
- `allowed_verdicts = {later, skip}`

#### primary artifact
- exact expected primary
- 또는 allowed set

#### reason codes
- 반드시 포함되어야 하는 code
- 포함되면 안 되는 code

#### score band
숫자 정답이 아니라 밴드로 둔다.

예:
- `novelty >= 65`
- `hype_penalty <= 40`
- `confidence >= 60`

이렇게 해야 모델/프롬프트가 조금 바뀌어도, **의미는 유지하면서 점수 노이즈를 허용**할 수 있다.

---

## 골든셋 구성은 기능별이 아니라 실패 모드별로 짜야 한다

초기 권장 구성은 아래처럼 잡는 편이 좋다.

| slice | 목적 | 권장 수량 |
|---|---|---:|
| `github_strong_tool` | 진짜 봐야 하는 repo를 HIGH로 올리는지 확인 | 20 |
| `github_wrapper_hype` | wrapper/hype repo를 과대평가하지 않는지 확인 | 20 |
| `x_idea_with_repo` | X 설명글에서 repo anchor를 제대로 찾는지 확인 | 15 |
| `text_only_workflow` | text-only idea를 놓치지 않는지 확인 | 15 |
| `multi_link_split` | 메시지 하나에서 후보를 올바르게 분리하는지 확인 | 10 |
| `reroot_required` | X/article → GitHub reroot가 맞는지 확인 | 10 |
| `ai_noise` | `ai` 단독 노이즈를 suppress하는지 확인 | 20 |
| `broken_or_partial` | partial evidence에서 과잉 확신하지 않는지 확인 | 10 |
| `stale_but_useful` | 오래됐지만 유의미한 도구를 자동 배제하지 않는지 확인 | 10 |
| `unsupported_media_only` | unsupported 상태를 정확히 표시하는지 확인 | 10 |

초기에는 총 **120~140건** 정도면 충분하다.

핵심은 “좋은 사례 많이 넣기”가 아니라 **실패 모드를 강제로 커버**하는 것이다.

---

## 평가 계층은 최소 6단계로 나눠야 한다

하나의 end-to-end accuracy 숫자로는 이 시스템을 운영할 수 없다.

### Tier 0. Deterministic unit tests
대상:
- URL extraction
- GitHub/X canonicalization
- short URL expansion rules
- artifact type classification
- candidate split / grouping
- reroot eligibility rules
- policy threshold functions
- notification render length / field presence

특징:
- LLM 없음
- 가장 빨라야 함
- CI에서 항상 실행

이 계층이 약하면 이후 eval은 전부 의미가 없다.

### Tier 1. Component fixture tests
대상:
- enricher profile output
- `artifact_snapshot` completeness
- `EvidenceBundle` assembly
- validator / policy-engine behavior

예:
- GitHub shallow profile이 README, manifest, CI/tests/examples signal을 빠짐없이 bundle에 반영하는가
- X snapshot이 edit-window 상태를 limitation으로 남기는가

### Tier 2. Judge evals
대상:
- `CandidateEvidenceBundle → judge_output_v1`
- `judge_output_v1 → analysis_v1`

핵심 평가:
- verdict allowed set 준수
- reason code 적합성
- skeptical take 품질
- comparables 존재 여부
- confidence calibration

### Tier 3. Delivery evals
대상:
- `analysis_v1 → notification_render_v1`

핵심 평가:
- 첫 3줄 안에 verdict 노출 여부
- 핵심 링크 포함 여부
- 리스크/냉정평가 누락 여부
- Telegram text length / entity validity

### Tier 4. Shadow replay
대상:
- 과거 실제 raw backlog
- 운영 데이터 일부 샘플

용도:
- prompt/policy 변경 전후 비교
- 비용/latency/precision 회귀 확인
- 새 변경이 real-world slice에서 깨지는지 점검

### Tier 5. Production feedback loop
대상:
- 실제 전송된 알림
- 운영자 수동 라벨
- false positive / false negative triage

핵심:
- 운영 실패를 새 eval case로 승격

즉, **프로덕션 실패는 버그 리포트가 아니라 다음 골든셋 케이스의 원천**이어야 한다.

---

## 평가 축은 “정답률” 하나로 끝내면 안 된다

이 프로젝트에서 최소한 아래 품질 축은 분리해야 한다.

### 1) `inspect_now` precision
의미:
- HIGH로 보낸 것 중 실제로 볼 가치가 있었는가

이 프로젝트의 최우선 KPI다.

### 2) false negative regret
의미:
- `later/skip`로 보냈지만 사실 HIGH였던 비율

precision-first 시스템이라도 완전히 무시하면 안 된다.

### 3) reroot accuracy
의미:
- X/article에서 GitHub 본체로 reroot한 결정이 맞았는가

4~5단계 구조의 핵심 품질 축이다.

### 4) evidence sufficiency
의미:
- 충분한 증거 없이 과한 결론을 내렸는가

### 5) hype suppression
의미:
- 허세 repo / 포장형 글을 잘 눌렀는가

### 6) delivery usefulness
의미:
- 알림만 읽고도 열어볼지 말지 판단 가능한가

### 7) latency / cost budget compliance
의미:
- 품질 개선이 실시간성/비용을 깨지 않았는가

즉, 이 시스템은 accuracy competition이 아니라 **운영 가치 최적화 문제**다.

---

## grader는 세 종류로 나누는 편이 맞다

### 1) rule-based grader
가장 중요하다.

예:
- schema valid
- enum valid
- skeptical take 존재
- required links 존재
- delivery length within bound
- forbidden reason code 없음
- policy override 일관성

장점:
- deterministic
- 빠름
- 운영 설명 가능성 높음

### 2) reference-based grader
사람 라벨 또는 허용 규칙과 직접 비교한다.

예:
- allowed verdict 내인지
- expected primary artifact와 일치하는지
- reroot 허용 조건을 만족하는지

### 3) LLM-assisted grader
보조 수단으로만 쓴다.

용도 예시:
- `skeptical_take_ko`가 실제로 냉정 요약인지
- comparables가 말이 되는 비교인지
- notification이 의미를 왜곡했는지

하지만 release gate의 핵심 기준은 **rule/reference grader**여야 한다.

즉, LLM grader는 인간 검토 비용을 줄이는 보조 장치지, 정답 그 자체가 아니다.

---

## release gate는 두 단계로 나눠야 한다

초기에는 baseline이 약하므로 바로 정교한 회귀 문턱을 걸 수 없다. 따라서 release gate를 두 단계로 나누는 편이 맞다.

### 1) bootstrap gate
초기 2~3회 릴리스용.

예:
- schema failure = 0
- delivery render failure = 0
- critical slice에서 forbidden verdict = 0
- `ai_noise` slice false positive 매우 낮음
- `github_strong_tool` slice에서 치명적 누락 없음

### 2) steady-state regression gate
baseline이 쌓인 뒤부터 적용.

예:
- `inspect_now` precision이 baseline 대비 하락 금지
- `ai_noise` false positive 악화 금지
- reroot accuracy 악화 금지
- latency / cost budget 초과 금지
- policy override rate 급증 금지

핵심은 이것이다.

> **초기에는 절대적 sanity gate, 이후에는 상대적 regression gate.**

---

## 품질 개선 루프는 항상 “실패 모드 단위”로 돌아야 한다

좋은 튜닝 루프는 아래 순서를 따른다.

1. 실패 수집
2. 실패 분류
3. 어느 계층 문제인지 판정
4. 가장 좁은 계층만 수정
5. 해당 slice 먼저 재평가
6. 전체 goldset 재평가
7. shadow replay
8. release gate 통과 시 승격

### 실패 분류 예시

#### A. normalization 문제
예:
- GitHub issue 링크를 repo 본체로 못 연결함
- `ai` 잡담을 candidate로 올림

조치:
- 4단계 규칙 수정
- judge prompt 수정 금지

#### B. enrichment 문제
예:
- repo 구조 증거가 부족해 좋은 repo를 LOW로 봄

조치:
- 5단계 profile 보강
- judge upgrade로 덮지 않음

#### C. judge 문제
예:
- evidence는 충분한데 허세 repo를 과대평가함

조치:
- 6단계 prompt / grader / escalation 수정

#### D. policy 문제
예:
- judge는 맞는데 threshold가 너무 낮아 HIGH 과다 발생

조치:
- `verdict_policy_v1` 계열 수정

#### E. delivery 문제
예:
- analysis는 맞는데 텔레그램 메시지가 길거나 핵심 리스크가 안 보임

조치:
- 7단계 render template 수정

즉, **문제 계층을 잘못 고치면 개선이 아니라 구조 붕괴가 된다.**

---

## offline replay와 shadow replay를 구분해야 한다

8단계에서 replay는 새 version 생성 방식으로 잠갔다. 9단계에서는 그것을 품질 평가에 연결한다.

### offline replay
목적:
- 특정 slice나 goldset을 빠르게 재평가
- prompt/policy 실험
- 릴리스 전 회귀 테스트

입력:
- frozen fixture

특징:
- deterministic
- 반복 가능
- 비교가 쉬움

### shadow replay
목적:
- 실제 과거 backlog에 대해 새 버전이 어떻게 행동하는지 보기

입력:
- 과거 `SourceMessage` / `EvidenceBundle` 샘플

특징:
- 실전 분포 반영
- 예기치 않은 edge case 발견
- 비용이 큼

두 경로를 섞지 않는 편이 좋다.

---

## 운영 피드백 수집은 v1에서 수동 라벨 우선

7단계에서 notifier는 아직 outbound-only다. 따라서 품질 루프가 텔레그램 인터랙션에 의존하면 구조가 깨진다.

그래서 v1의 운영 피드백은 아래처럼 두는 것이 맞다.

- 별도 triage 시트 또는 내부 admin UI
- 후보별 수동 라벨
  - `useful_now`
  - `useful_later`
  - `hype`
  - `duplicate`
  - `wrong_primary`
  - `insufficient_evidence`
  - `bad_summary`

나중에 7단계 확장으로 inbound 제어를 붙이면, Telegram feedback는 **보조 수집 채널**로 붙이면 된다. 하지만 지금은 core dependency로 두지 않는다.

---

## 주요 KPI는 지금부터 정의해야 한다

권장 KPI는 아래처럼 잡는 편이 맞다.

### 핵심 KPI
- `inspect_now_precision`
- `inspect_now_regret`
- `reroot_accuracy`
- `ai_noise_false_positive_rate`
- `notification_usefulness_score`

### 보조 KPI
- `bundle_partial_rate`
- `schema_failure_rate`
- `policy_override_rate`
- `judge_escalation_rate`
- `avg_cost_per_candidate`
- `p95_end_to_end_latency`

핵심 포인트는 **비용과 latency도 품질 지표의 일부**라는 점이다. 너무 비싸거나 너무 느리면, 실제 운영 가치가 무너진다.

---

## OpenAI 도구는 어디까지 쓰고 어디까지 안 쓸지

이 프로젝트는 OpenAI 도구를 쓰되, 소스 오브 트루스는 내부에 둬야 한다.

### 적극 활용할 것
- **Evals / Datasets**: prompt·grader 실험, judge 회귀 테스트
- **Batch API**: shadow replay, 대량 오프라인 재판정
- **Prompt optimizer**: 후보 prompt 생성 보조

### 내부가 정본이어야 하는 것
- goldset fixture
- verdict policy
- release gate
- pipeline-level score aggregation
- 운영 승인 기록

즉, OpenAI 도구는 **실험 가속기**로 쓰되, 제품 품질 기준은 우리 시스템 안에 남겨야 한다.

---

## 이번 단계에서 반드시 피해야 할 반패턴

1. accuracy 하나만 보고 릴리스 결정
2. live URL을 eval fixture로 직접 사용
3. prompt만 바꾸고 enrichment/normalization 문제를 무시
4. goldset을 좋은 사례 위주로만 구성
5. LLM grader를 release gate의 유일한 기준으로 사용
6. replay 결과를 기존 analysis에 overwrite
7. `inspect_now` precision 대신 전체 recall을 우선 KPI로 둠
8. 운영 피드백 없이 오프라인 예제만으로 튜닝
9. prompt 변경과 policy 변경을 한 번에 섞어서 원인 분리 불가 상태 만들기
10. notifier 품질을 judge 품질 평가에 섞어버리기

특히 2, 5, 6, 9는 구조를 크게 망가뜨린다.

---

## 9단계 완료 기준

아래가 고정되면 9단계는 끝이다.

- `eval_case`, `eval_slice`, `eval_run`, `grader_result`, `release_decision` 스키마 초안 확정
- goldset 구성 원칙 확정
- slice taxonomy 확정
- Tier 0~Tier 5 평가 계층 확정
- grader 체계 확정
- bootstrap gate / steady-state gate 분리 확정
- KPI 정의 확정
- offline replay / shadow replay 분리 확정
- human triage 루프 확정
- change-class별 튜닝 원칙 확정

---

## 이번 단계의 한 줄 결론

**9단계는 이 봇을 “한 번 잘 동작하는 시스템”에서 “변경해도 무너지지 않는 시스템”으로 바꾸는 단계이며, 핵심은 모델 평가가 아니라 snapshot 기반 goldset·slice·replay·release gate를 통해 파이프라인 전체를 통제하는 것이다.**

---

## 참고 문서

- OpenAI Evals guide: https://developers.openai.com/api/docs/guides/evals
- OpenAI Prompt optimizer: https://developers.openai.com/api/docs/guides/prompt-optimizer
- OpenAI Batch API guide: https://developers.openai.com/api/docs/guides/batch
- OpenAI blog - Testing Agent Skills Systematically with Evals: https://developers.openai.com/blog/eval-skills


---

## Source file: `10_stage10_rollout_cutover_governance.md`

# 10단계: 최종 롤아웃, 컷오버, 운영 거버넌스 설계

## 단계 목적

10단계의 목적은 단순 배포가 아니다.

> **0~9단계에서 잠근 구조를 실제 운영으로 전환하는 순서, 컷오버 기준, 롤백 기준, 운영 권한, 변경 통제, 최소 구현 우선순위를 최종 확정하는 것**이다.

이 단계가 필요한 이유는 명확하다.

앞 단계들까지는 `설계가 맞는가`를 잠갔다. 하지만 실제 운영에서는 아래 문제가 별도로 생긴다.

- 무엇부터 켤 것인가
- 어떤 기능은 아직 비활성로 둘 것인가
- 어느 시점에 live ingest를 허용할 것인가
- false positive가 나도 계속 운영할지 중단할지
- prompt / policy / threshold를 누가 어떻게 바꿀 수 있는가
- 장애가 나면 무엇을 끄고 어디까지 되돌릴 것인가

즉, 10단계는 **기술 설계의 마지막 조각**이 아니라 **운영 전환의 첫 번째 조각**이다.

---

## 지금까지 잠근 구조를 다시 한 번 압축한다

10단계는 앞선 고정 결정을 절대 깨면 안 된다.

### 0단계
- 제품은 **precision-first** 필터다.
- `SourceMessage → Artifact → CandidateGroup → Analysis → Notification` 구조를 유지한다.
- negative-first summary를 유지한다.

### 1단계
- Telegram reader 계정과 Telegram notifier bot은 분리한다.
- OpenAI는 기존 바이낸스 봇과 **프로젝트/키를 분리**한다.

### 2단계
- 단일 VPS 1대에서 시작한다.
- `PostgreSQL = system of record`
- `Redis = queue/lock`
- prod는 **단일 live collector**만 둔다.

### 3단계
- collector는 원문 보존만 담당한다.
- `source_message` / `source_message_revision` append-only 구조를 유지한다.

### 4단계
- 정규화는 deterministic하다.
- `ai`는 보조 신호가 있을 때만 실질 candidate로 승격한다.
- candidate는 provisional하며 reroot 가능하다.

### 5단계
- enrich는 비-LLM 증거 수집 계층이다.
- GitHub/X/Web snapshot과 `EvidenceBundle`을 만든다.

### 6단계
- LLM은 `judge_output_v1`만 생성한다.
- 최종 `verdict`와 `delivery_decision`은 deterministic policy engine이 계산한다.

### 7단계
- notifier는 presentation 계층이다.
- `analysis_v1`을 재해석하지 않는다.

### 8단계
- replay는 overwrite가 아니라 **새 run / 새 snapshot / 새 analysis**를 만든다.
- 운영 경보와 사용자 알림은 분리한다.

### 9단계
- 품질은 `goldset + slice + replay + release gate`로 검증한다.
- production failure는 새 eval case로 승격한다.

따라서 10단계는 이 모든 결정을 **운영 순서로 묶는 단계**다.

---

## 이번 단계에서 고정할 핵심 결론

| 구분 | 고정 결정 |
|---|---|
| 롤아웃 방식 | **6단계 점진 롤아웃** |
| 최초 운영 모드 | **shadow mode → silent delivery → live delivery** |
| go-live 기준 | **품질 게이트 + 운영 게이트 동시 충족** |
| 기본 알림 정책 | `inspect_now` 즉시 전송, `later`는 silent, `skip` suppress |
| 변경 권한 | **schema / policy / prompt / threshold / channel registry 분리 승인** |
| 배포 전략 | **blue-green이 아니라 single-prod + feature flags + replay validation** |
| 롤백 전략 | **코드 롤백 + feature flag 비활성화 + delivery stop + replay 복구** |
| 초기 기능 범위 | **GitHub + X + text_idea 우선**, 확장 기능은 비활성 |
| 운영 기준 | **explainable, replayable, rate-limited, reversible** |
| v1 성공 정의 | **사용자가 실제로 열어보는 HIGH 비율이 높고, spam으로 인식되지 않는 것** |

---

## 10-1. 최종 아키텍처를 운영 기준으로 다시 묶는다

운영 상태에서의 최종 구조는 아래와 같이 고정한다.

```text
Telegram channels
  ↓
collector-telegram
  ↓
source_message / revisions
  ↓
router-normalizer
  ↓
artifact registry + candidate proposals
  ↓
external enrichers
  ↓
artifact snapshots + evidence bundles
  ↓
judge-openai
  ↓
judge_output_v1
  ↓
analysis-validator + policy-engine
  ↓
analysis_v1
  ↓
notification planner / renderer / notifier
  ↓
Telegram delivery
```

이 구조에서 중요한 것은 **forward-only 파이프라인이 아니라, 각 단계가 durable boundary를 가진다**는 점이다.

즉,
- collector가 실패해도 raw message는 남고
- enrich가 실패해도 candidate proposal은 남고
- judge가 실패해도 bundle은 남고
- notifier가 실패해도 analysis는 남는다.

이 경계를 깨면 운영 중 재처리와 부분 복구가 불가능해진다.

---

## 10-2. 롤아웃은 6단계로 제한한다

처음부터 full live로 켜면 품질과 운영을 동시에 망칠 확률이 높다.  
그래서 롤아웃은 아래 6단계로 고정하는 편이 맞다.

### Phase 0. Offline fixture validation

상태:
- live ingest 없음
- 저장된 fixture / frozen snapshot만 사용

목적:
- schema
- normalization
- bundle assembly
- judge output
- policy reconciliation
- delivery rendering

을 end-to-end로 검증

종료 조건:
- 9단계 release gate 최소 충족
- 치명적 schema mismatch 없음
- notification render 파손 없음

### Phase 1. Live ingest, no delivery

상태:
- prod collector ON
- 실제 채널 읽음
- notifier OFF

목적:
- Telegram 수집 안정성 확인
- backfill / gap scan / dedupe / reroot 정상 여부 확인
- queue backlog / API latency / error rates 확인

종료 조건:
- collector 누락률이 허용 범위 내
- 중복/누락 사고 없음
- GitHub/X/OpenAI rate-limit 제어 안정

### Phase 2. Shadow analysis

상태:
- live ingest ON
- enrich / judge / analysis ON
- 사용자 알림 OFF
- 운영 로그/대시보드만 확인

목적:
- 실제 채널 데이터에서 false positive 패턴 확인
- `inspect_now/later/skip` 분포 확인
- `skeptical_take`, `comparables`, `reason_codes` 품질 확인

종료 조건:
- shadow 결과가 골든셋 기대와 크게 어긋나지 않음
- `inspect_now` 비율이 과도하게 높지 않음
- low-evidence HIGH가 거의 없음

### Phase 3. Silent delivery

상태:
- `inspect_now`와 `later`를 관리자 전용 텔레그램 채팅으로 보냄
- `later`는 notification off
- 운영자만 확인

목적:
- 실제 수신 UX 검증
- 메시지 길이/링크/정렬/중복 체감 검증
- `later`가 너무 많아 피로를 주는지 확인

종료 조건:
- 메시지 템플릿 수정 포인트가 안정화됨
- 중복 알림이 운영 가능한 수준
- 네가 실제로 “열어볼 가치 있다”고 느끼는 HIGH 비율이 충분함

### Phase 4. Restricted live delivery

상태:
- 실운영과 동일한 전달 정책 적용
- 단, tracked_chat 일부만 활성
- 채널별 override 보수적으로 적용

목적:
- 채널별 품질 차이 확인
- noisy channel과 high-signal channel 구분
- threshold/channel override 최종 조정

종료 조건:
- noisy channel에 대한 suppress 정책 확보
- 전체 daily volume이 예산/피로 한도 안에 있음

### Phase 5. Full v1 go-live

상태:
- 모든 목표 채널 활성
- full delivery policy 적용
- release gate 및 운영 gate 동시 적용

목적:
- v1 정식 운영

종료 조건:
- 없음. 이후는 steady-state 운영으로 전환

---

## 10-3. go-live 기준은 “품질 + 운영” 동시 충족이다

go-live를 quality only로 보면 안 된다.  
반대로 uptime only로 봐도 안 된다.

### 품질 게이트

최소 조건 예시:
- 골든셋 회귀 테스트 통과
- `inspect_now` precision이 내부 기준 이상
- `skip`이 과도하게 줄지 않음
- 필수 reason code 누락률 낮음
- low-evidence HIGH 거의 없음

### 운영 게이트

최소 조건 예시:
- collector gap scan 정상
- Redis 날림 후 재구성 가능
- PostgreSQL 백업/복구 검증 완료
- notifier flood-control 처리 검증 완료
- OpenAI daily hard cap 정상 동작
- GitHub/X rate-limit backoff 정상 동작

### UX 게이트

최소 조건 예시:
- 알림 1건 읽는 데 10초 내 핵심 판단 가능
- 링크 2개 이상이 안정적으로 붙음
- overly long message 비율 낮음
- `later`가 silent라도 아예 무의미하지 않음

즉, **세 게이트를 동시에 만족할 때만** full go-live가 가능하다.

---

## 10-4. 운영 모드는 feature flag로 제어한다

단일 prod에서 가는 이상, 기능 토글이 매우 중요하다.

권장 feature flag:

- `ENABLE_LIVE_COLLECTOR`
- `ENABLE_BACKFILL_ONBOARDING`
- `ENABLE_GAP_SCAN`
- `ENABLE_GITHUB_ENRICH`
- `ENABLE_X_ENRICH`
- `ENABLE_WEB_ENRICH`
- `ENABLE_TEXT_IDEA`
- `ENABLE_MODEL_ESCALATION`
- `ENABLE_LATER_DELIVERY`
- `ENABLE_NOTIFICATION_SEND`
- `ENABLE_SILENT_LATER`
- `ENABLE_CHANNEL_OVERRIDES`
- `ENABLE_REPLAY_TO_PROD_DB` (기본 false)

핵심은 아래 두 가지다.

1. **코드 배포 없이 행동을 바꿀 수 있어야 한다.**
2. **문제 발생 시 전체 시스템이 아니라 특정 단계만 끌 수 있어야 한다.**

예를 들어 X API가 불안정하면 `ENABLE_X_ENRICH=false`로 내리고, GitHub/text_idea만 운영 가능해야 한다.

---

## 10-5. 변경 통제(change governance)를 고정한다

이 단계에서 바꾸는 권한이 섞이면 품질이 급격히 흔들린다.  
그래서 변경 대상을 다섯 종류로 쪼개는 편이 맞다.

### 1) schema 변경
대상:
- `analysis_schema_v1`
- `judge_output_v1`
- bundle schema

특징:
- 가장 위험함
- replay/eval/render/storage 전부 건드림

원칙:
- patch 수정 금지
- `v2`로 새 버전 생성
- 배포 전 full replay 필수

### 2) policy 변경
대상:
- score threshold
- verdict mapping
- delivery mapping
- reroot 허용 조건

특징:
- 결과 분포를 크게 바꿈

원칙:
- `policy_version` 상승
- 최소 shadow replay 필수

### 3) prompt 변경
대상:
- judge instruction
- score rubric wording
- profile guidance

특징:
- 판단 문체/점수 분포를 바꿈

원칙:
- `prompt_version` 상승
- 골든셋 replay 필수

### 4) channel/threshold override 변경
대상:
- 특정 채널 suppress/boost
- `ai` 보조 신호 기준 조정

특징:
- 운영 잡음에 직접 영향

원칙:
- config 변경으로 처리
- 코드 배포와 분리

### 5) template/render 변경
대상:
- 텔레그램 메시지 템플릿
- inline button 구성

특징:
- 분석 품질은 안 바꾸지만 사용자 체감은 크게 바꿈

원칙:
- delivery replay로 검증

즉, **무엇이 바뀌는지에 따라 승인/검증 경로를 분리**해야 한다.

---

## 10-6. 롤백 전략은 단계별로 달라야 한다

롤백을 “이전 커밋으로 되돌리기”만으로 생각하면 부족하다.  
이 시스템은 단계별 상태가 다르므로 롤백도 계층별이어야 한다.

### A. notifier 문제
증상:
- flood control
- 템플릿 파손
- 중복 전송

조치:
- `ENABLE_NOTIFICATION_SEND=false`
- analysis는 계속 생성
- notify queue만 정지
- 나중에 delivery replay 가능

### B. judge 문제
증상:
- malformed structured output
- LOW/HIGH 분포 급변
- skeptical summary 품질 붕괴

조치:
- prompt rollback 또는 model escalation off
- 새 prompt_version으로 shadow replay
- 필요 시 직전 stable prompt_version으로 복귀

### C. enrich 문제
증상:
- GitHub/X API 장애
- snapshot partial 폭증

조치:
- 해당 enricher flag off
- 다른 source만 계속 운영
- unavailable reason을 명시적으로 남김

### D. normalization 문제
증상:
- candidate 과다 생성
- reroot 이상
- `ai` 과탐지

조치:
- router config rollback
- proposal 생성 중지 후 source_message는 계속 저장
- replay로 재산출

### E. collector 문제
증상:
- Telegram live 수집 누락
- session 손상
- backfill/gap scan 충돌

조치:
- collector 정지
- TDLib state 복구
- recent history reconciliation 실행
- downstream은 이미 저장된 raw 기준으로 유지 가능

즉, **문제가 난 단계만 멈추고, 아래/위 단계는 가능한 한 계속 살려두는 것이 원칙**이다.

---

## 10-7. 최소 구현 우선순위를 최종 확정한다

이 단계에서 “무조건 다 넣고 시작”을 막아야 한다.  
초기 v1에서 꼭 필요한 것과 나중으로 미룰 것을 분리한다.

### 반드시 포함

- Telegram collector
- bootstrap backfill + gap scan
- deterministic normalization
- GitHub enricher
- X enricher
- text_idea local enrich
- OpenAI judge (`gpt-5.4-mini` 기본)
- deterministic policy engine
- Telegram notifier
- structured logs / metrics / replay
- goldset + release gate

### 초기에는 옵션

- `gpt-5.4` escalation
- web article enrich 확장
- channel-specific override 고도화
- digest delivery
- inbound commands (`/pause`, `/why`)
- Batch 기반 대규모 shadow replay 자동화

### v1에서는 제외 권장

- OCR 중심 이미지 판독
- repo 전체 clone 기본 전략
- 브라우저 자동화 중심 web scraping
- 다중 LLM vendor 동시 운영
- multi-node / Kubernetes
- 사용자용 dashboard

이 우선순위가 중요한 이유는 단순하다.  
**가치 핵심은 “좋은 필터”이지 “큰 시스템”이 아니기 때문**이다.

---

## 10-8. 채널 운영 정책도 지금 고정한다

20~30개 채널을 전부 동일하게 다루면 품질이 흔들릴 가능성이 높다.  
그래서 tracked chat도 운영 등급을 두는 편이 좋다.

### Tier A: high-signal channels
특징:
- GitHub/X 중심
- 개발 도구/워크플로우 비중 높음

정책:
- medium trigger도 candidate 허용 가능
- `ai` 보조 신호 threshold 완화 가능

### Tier B: mixed channels
특징:
- 개발/뉴스/잡담 혼합

정책:
- 기본 threshold 유지
- `ai` 단독은 거의 suppress

### Tier C: noisy channels
특징:
- 광고/큐레이션/반복 repost 많음

정책:
- strong link 위주만 후보화
- text_idea 기본 비활성 가능

이걸 두면 4단계 normalization 규칙을 안 깨고도 **운영 잡음을 조정**할 수 있다.

---

## 10-9. steady-state 운영 루틴을 고정한다

정식 운영 후에는 개발보다 **운영 루틴**이 더 중요해진다.

### Daily
- collector heartbeat 확인
- backlog / 429 / partial snapshot 비율 확인
- HIGH 알림 품질 샘플 검토
- OpenAI hard cap / usage 확인

### Weekly
- false positive / false negative 사례 정리
- channel tier/override 조정
- prompt/policy 변경 필요성 검토
- noisy pattern을 새 suppression rule 후보로 정리

### Biweekly or release cycle
- 골든셋 업데이트
- shadow replay
- stable vs candidate prompt/policy 비교
- release gate 통과 시만 반영

### Monthly
- key rotation / secret audit
- backup restore drill
- TDLib state/DB 용량 점검
- tracked chat 목록 청소

이 루틴을 명시해야 운영이 사람 기억력에 의존하지 않는다.

---

## 10-10. 최종 성공 기준도 지금 잠근다

이 프로젝트의 성공은 “작동한다”가 아니다.  
정량과 정성을 같이 봐야 한다.

### 정량 기준 예시
- 하루 HIGH 건수가 과도하지 않음
- HIGH 중 실제 열람/유지 비율이 높음
- duplicate notification 비율 낮음
- low-evidence HIGH 비율 매우 낮음
- replay/recovery 성공률 높음

### 정성 기준 예시
- 네가 HIGH를 “spam”으로 느끼지 않음
- later가 조용하지만 나중에 훑을 가치가 있음
- skeptical summary가 실제 판단에 도움됨
- 기존 도구 대비 설명이 허세 억제에 유효함

결국 이 봇의 성공은 **더 많이 보내는 것**이 아니라 **볼 만한 것만 보내는 것**이다.

---

## 10-11. 최종 launch checklist

아래 체크리스트를 모두 통과해야 v1 go-live로 본다.

### 인프라
- [ ] prod VPS 자동 재기동 확인
- [ ] PostgreSQL backup/restore 검증
- [ ] Redis flush 후 job 재구성 검증
- [ ] TDLib state 복구 절차 검증

### 수집
- [ ] tracked chat allowlist 확정
- [ ] bootstrap backfill 정상
- [ ] gap scan 정상
- [ ] edit/delete/tombstone 처리 검증

### 정규화
- [ ] GitHub/X canonicalization 정상
- [ ] short URL expansion 정상
- [ ] text_idea 후보화 정상
- [ ] suppress reason code 기록 정상

### enrich
- [ ] GitHub shallow profile 정상
- [ ] X lookup/profile 정상
- [ ] reroot 규칙 정상
- [ ] EvidenceBundle size cap 정상

### judge
- [ ] Structured Outputs strict schema 정상
- [ ] validator/policy reconciliation 정상
- [ ] mini/default path 정상
- [ ] escalation path 정상 또는 비활성 명시

### delivery
- [ ] inspect_now 즉시 전송 정상
- [ ] later silent 전송 정상
- [ ] skip suppress 정상
- [ ] duplicate send 방지 정상

### 운영
- [ ] alerting 채널 분리
- [ ] daily hard cap 동작
- [ ] release gate 통과
- [ ] rollback runbook 문서화

---

## 이번 단계에서 반드시 피해야 할 마지막 실수

1. 0~9단계 구조를 무시하고 “일단 다 켜보자” 식으로 운영 시작
2. quality gate 없이 live delivery 시작
3. prompt와 policy와 threshold를 동시에 바꿔 원인 추적 불가능하게 만듦
4. replay를 overwrite 방식으로 돌림
5. notifier 문제를 collector/judge까지 멈추는 전체 장애로 확대
6. `ai` 과탐지 문제를 LLM prompt만으로 해결하려 듦
7. 운영 지표 없이 subjective impression만으로 threshold를 조정
8. full go-live 전에 silent delivery 구간을 생략

특히 2, 3, 4는 마지막 단계에서 전체 구조를 가장 쉽게 무너뜨린다.

---

## 10단계 완료 기준

아래가 모두 문서로 확정되면 10단계는 끝이다.

- 6단계 롤아웃 계획
- go-live 품질/운영/UX 게이트
- feature flag 목록
- 변경 통제 규칙
- 단계별 롤백 전략
- 최소 구현 우선순위
- 채널 운영 등급 정책
- steady-state 운영 루틴
- final launch checklist
- success criteria

---

## 전체 프로젝트의 최종 한 줄 결론

**이 봇은 “텔레그램 채널에서 개발 도구/아이디어 신호를 감지해, deterministic normalization과 비-LLM 증거 수집을 거친 뒤, OpenAI judge가 구조화된 냉정 평가를 생성하고, deterministic policy engine이 최종 verdict와 delivery를 확정해 운영자에게 전달하는 precision-first filtering system”으로 고정한다.**

그리고 실제 운영 전환은 **offline validation → live ingest → shadow analysis → silent delivery → restricted rollout → full go-live** 순서로만 진행한다.
