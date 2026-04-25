# 00 foundations stage0 stage1 bundle v0 1
이 문서는 프로젝트 소스 파일 수 제한(40개)을 피하기 위한 **통합 번들**이다.
구조 변경 문서가 아니며, 아래 원본 문서를 **순서와 내용을 보존한 채** 묶는다.

## 포함 원본 파일
- `00_overview_10_steps.md`
- `00_stage0_product_contract.md`
- `01_stage1_accounts_permissions_keys.md`

## 통합 원칙
- 아키텍처 불변식은 유지한다.
- 원본 파일명은 아래 섹션 제목으로 보존한다.
- 충돌 시 해석 우선순위는 기존과 동일하게 정본 단계 문서 → 실행 계약 → migration 정본 → 구현 초안이다.


---

## Source file: `00_overview_10_steps.md`

# 텔레그램 GitHub/X 캐치 봇 구축 로드맵 (10단계 개요)

## 목적
20~30개 텔레그램 채널에서 GitHub, AI, vibe coding 관련 신호를 감지하고, 각 후보를 냉정하게 1차 심사한 뒤 텔레그램으로 요약 전송하는 시스템을 설계한다.

---

## 전체 구조 요약

```text
[Telegram channels]
   ↓
[Collector]
 - 공개/타인 채널: TDLib/MTProto 기반 읽기 계정
 - 내가 운영하는 채널: 필요 시 Bot API
   ↓
[Trigger + Normalizer]
 - 키워드/URL 감지
 - URL 정규화
 - 중복 제거
   ↓
[Enricher]
 - GitHub API
 - X API
 - 일반 링크 본문 추출
   ↓
[LLM Judge]
 - 1차: GPT-5.4 mini
 - 2차 승급: GPT-5.4
   ↓
[Scorer + Verdict]
 - inspect_now / later / skip
   ↓
[Telegram Notifier]
 - 사용자에게 요약 전송
```

---

## 0단계. 성공 기준 고정
- 무엇을 감지할지, 무엇을 분석할지, 어떤 형식으로 판정할지 제품 계약을 고정한다.
- JSON 스키마, verdict 규칙, 알림 템플릿, 골든셋 구조를 먼저 확정한다.

## 1단계. 계정, 권한, 키 분리
- Telegram 수집 계정, Telegram 알림용 봇, OpenAI 프로젝트/서비스 계정, GitHub/X 자격증명을 분리한다.
- 기존 바이낸스 봇과 OpenAI 프로젝트 및 키를 분리해 운영 혼선을 막는다.

## 2단계. VPS와 런타임 준비
- 상시 실행 VPS, PostgreSQL, Redis(또는 큐 대체), 로그/백업/재시작 정책을 준비한다.
- collector, enricher, analyzer, notifier 프로세스를 분리한다.

## 3단계. Telegram 수집기 구현
- 20~30개 채널을 등록하고, 새 글/수정 글을 안정적으로 적재한다.
- `(chat_id, message_id)` 기준 중복을 막고 edit 이벤트도 추적한다.

## 4단계. 트리거와 정규화
- 키워드/링크 감지, 축약 URL 확장, GitHub/X canonicalization, 중복 묶기를 구현한다.
- 같은 repo/post가 여러 채널에서 재공유돼도 1개의 분석 후보로 묶는다.

## 5단계. 외부 증거 수집기
### 5-1. GitHub
- README, manifest, lockfile, tests, CI, examples, 엔트리 파일을 수집한다.
- 전체 clone 대신 tree/contents/archive 기반으로 필요한 파일만 샘플링한다.

### 5-2. X
- 링크에서 post ID를 추출하고, 공식 API로 본문/작성자/맥락을 가져온다.
- text-only 아이디어 글도 분석 대상에 포함한다.

## 6단계. LLM 전 1차 필터
- 휴리스틱으로 노이즈를 먼저 제거하고, 이미 분석한 동일 SHA/post는 재사용한다.
- LLM에는 raw 전체가 아니라 evidence pack만 전달한다.

## 7단계. LLM 판정기
- 기본은 `gpt-5.4-mini`, 애매하거나 고가치 후보만 `gpt-5.4`로 승급한다.
- 출력은 구조화 JSON으로 강제하고, 부정 우선 평가를 필수화한다.

## 8단계. 텔레그램 알림 포맷
- 판정이 먼저 보이게 하고, 한줄 요약/냉정 평가/기존 도구 대비/추천 행동을 짧게 보낸다.
- 너무 긴 분석은 본문과 상세 후속 메시지로 분리한다.

## 9단계. 로그, 모니터링, 재현성
- 채널별 수집 건수, trigger hit rate, enrich 성공률, LLM 호출 수, 토큰 사용량, false positive를 추적한다.
- 특정 message_id나 candidate를 재처리할 수 있어야 한다.

## 10단계. 품질 튜닝과 확장
- 골든셋 기반 회귀 테스트를 만들고, 프롬프트/정책 변경 시 성능 퇴행을 막는다.
- 채널별 규칙, feedback loop, 관리자 명령, 다이제스트 전송 등을 확장한다.

---

## MVP 순서
1. Telegram 수집기
2. URL/키워드 정규화
3. GitHub/X 증거 수집기
4. evidence pack 생성
5. GPT-5.4-mini 판정
6. Telegram 알림

---

## 초기에 넣지 않는 것이 좋은 것
- 전체 repo clone
- OCR 중심 파이프라인
- 멀티 LLM 벤더 동시 운영
- Local Telegram Bot API server
- 과도한 브라우저 자동화

---

## 최종 권장안
- 수집: TDLib/MTProto + 전용 읽기 계정
- 알림: Telegram Bot API
- 소스 해석: GitHub API + X API + 일반 URL 추출
- 1차 판정: GPT-5.4 mini
- 2차 승급: GPT-5.4
- 운영 분리: OpenAI 별도 프로젝트 + 별도 서비스 계정 + 별도 키 + 앱 내부 hard cap
- 인프라: VPS 1대


---

## Source file: `00_stage0_product_contract.md`

# 0단계: 제품 계약 잠금 (Product Contract Lock)

## 단계 목적
0단계는 단순한 개요 정리가 아니라, 이후 1~9단계에서 다시 흔들리지 않도록 **제품 계약을 잠그는 단계**다.

핵심 문장:

> 20~30개 텔레그램 채널에서 개발 도구/아이디어 신호를 감지하고, 각 후보를 “지금 볼 가치가 있는가” 기준으로 냉정하게 1차 심사해, 한국어 요약으로 빠르게 전달하는 시스템.

---

## 제품 목표
1. 신호를 놓치지 않고 감지한다.
2. 좋아 보이는 이유보다 별로일 수 있는 이유를 먼저 말한다.
3. 기존 도구 대비 진짜 차별점이 있는지 본다.
4. 사용자가 열어볼 가치가 있는 항목만 빠르게 걸러낸다.

## 비목표
- 실제 코드 실행 기반 검증 도구가 아님
- 벤치마크 재현 시스템이 아님
- 보안 감사 도구가 아님
- 모든 링크를 완전 이해하는 범용 리서치 에이전트가 아님
- 장문 리포트 작성기가 아님
- “좋은 글 추천기”가 아니라 “쓸만한 것만 거르는 필터”임

---

## 운영 우선순위
1. `inspect_now` 정밀도(precision)
2. 알림 속도(latency)
3. 중복 억제(deduplication)
4. 커버리지(recall)

이 시스템은 **recall-first가 아니라 precision-first**다.

---

## 분석 단위 모델

### SourceMessage
텔레그램 원문 1개.
- channel_id
- message_id
- posted_at
- text/caption
- entities
- forwarded 여부
- edit 여부

### Artifact
메시지에서 추출된 분석 대상 조각.
- GitHub repo
- GitHub subpath
- X post
- 일반 article
- text-only idea

### CandidateGroup
실제로 한 번 평가할 단위.
- primary artifact 1개
- supporting artifacts 0개 이상

예: Telegram 글 안에 GitHub repo + X 설명글이 함께 있으면
- primary = github_repo
- supporting = [x_post]

### Analysis
후보 1개에 대한 구조화된 판정 결과.

### Notification
사람에게 보내는 최종 텔레그램 알림.

> 중요한 설계 원칙: **메시지 1개 = 후보 1개**로 가정하지 않는다. 메시지 1개에서 후보가 0~N개 나올 수 있다.

---

## 감지 대상 v1

### 직접 트리거
- 키워드: `github`, `ai`, `vibe coding`, `vibe-coding`
- URL: `github.com`, `x.com`, `twitter.com`, `t.co`
- Telegram entity/previews 상 링크 감지

### 간접 트리거
- text-only vibe coding 아이디어 글
- 개발 workflow/도구 패턴 설명 글
- 코드가 없더라도 구체적인 개발 아이디어가 있는 글

## 제외 대상 v1
- 단순 뉴스 전달
- 행사 홍보
- 채용 공고
- 제휴/광고성 링크
- 일반적인 AI 잡담
- 개발 도구와 무관한 정치/연예/밈 글

### 애매한 케이스 처리
- 이미지-only: v1에서는 `unsupported_media_only` 또는 `low_evidence`
- 오래된 repo 재공유: freshness와 intrinsic usefulness를 분리해서 본다
- 링크가 깨진 경우: `source_unavailable` 상태를 명시적으로 허용한다

---

## verdict와 delivery를 분리

### 분석 verdict
- `inspect_now`
- `later`
- `skip`

### 전달 decision
- `send_now`
- `send_digest`
- `suppress`

판정과 전송 정책은 같은 것이 아니다.

---

## 평가 루브릭
모든 점수는 0~100.

### 공통 필수 점수
- `novelty`
- `practical_usefulness`
- `evidence_strength`
- `hype_penalty`
- `confidence`

### GitHub primary일 때
- `code_quality`
- `maintenance_signal`

### X/text primary일 때
- `specificity`
- `reproducibility_signal`

### 점수 해석 밴드
- 0~20: 매우 약함
- 21~40: 약함
- 41~60: 혼합
- 61~80: 꽤 강함
- 81~100: 매우 강함

---

## verdict 규칙 v1

### inspect_now
모두 만족:
- `practical_usefulness >= 70`
- `evidence_strength >= 50`
- `confidence >= 60`
- `hype_penalty < 70`

그리고 추가로 하나 만족:
- GitHub primary: `code_quality >= 65`
- X/text primary: `specificity >= 60`

### later
- `practical_usefulness >= 45`
- `evidence_strength >= 30`
- `confidence >= 35`
- 단, inspect_now는 아님

### skip
- 자료가 너무 빈약함
- 허세가 지나치게 강함
- 실용성이 낮음
- 광고/재포장/잡담 느낌이 강함
- 판단할 만한 증거가 거의 없음

### 보정 규칙
- `evidence_strength < 50`이면 inspect_now 금지
- 허세가 높은데 품질/구체성이 낮으면 거의 skip
- 오래됐다고 자동 skip 금지

---

## LLM 출력 계약: JSON 스키마 우선
사용자에게 보내는 텔레그램 문장은 LLM 자유문장이 아니라 **구조화 JSON을 렌더링해서 생성**한다.

### 권장 스키마 핵심 필드
- `schema_version`
- `policy_version`
- `prompt_version`
- `candidate_group_id`
- `source_message_refs`
- `primary_artifact`
- `supporting_artifacts`
- `trigger_reason_codes`
- `headline`
- `summary_one_line_ko`
- `skeptical_take_ko`
- `why_it_might_matter_ko`
- `comparables`
- `scores`
- `verdict`
- `delivery_decision`
- `reason_codes`
- `red_flags_ko`
- `evidence_limitations_ko`
- `recommended_action_ko`
- `freshness_note_ko`

### 절대 빼지 말아야 할 필드
- `primary_artifact`
- `skeptical_take_ko`
- `comparables`
- `reason_codes`
- `evidence_limitations_ko`

---

## 사용자 알림 템플릿 v1

```text
[HIGH|MID|LOW] [GitHub|X|Idea]
제목: ...
판정: inspect_now / later / skip

한줄 요약:
...

냉정 평가:
- ...
- ...

기존 도구 대비:
- ...

리스크:
- ...

추천 행동:
- ...

원문:
- Telegram
- Primary link
```

### 고정 규칙
- 첫 3줄 안에 판정 표시
- `냉정 평가` 무조건 포함
- GitHub는 `기존 도구 대비` 필수
- 링크 최소 2개
- 한 알림은 한 candidate group만 다룸

---

## 리팩토링 방지 설계 원칙
1. Raw / Normalized / Analysis 3층 분리
2. 스키마 버전과 정책 버전 분리
3. verdict와 delivery 분리
4. 메시지 1개에서 후보 0~N개 허용
5. `skeptical_take`는 필수 필드
6. `evidence_limitations`는 항상 채움
7. 임계값과 키워드는 config로 분리

---

## 골든셋 설계
초기 60~80건 권장.

- 좋은 GitHub repo 15
- 허세 GitHub repo 15
- 좋은 X 아이디어 글 10
- 잡담형 X 글 10
- 혼합 링크 케이스 10
- 엣지 케이스 10

### 라벨링 방식
정답 점수 대신 **허용 verdict 범위**를 기록한다.

예:
- 케이스 A: `{inspect_now}`
- 케이스 B: `{later, skip}`
- 케이스 C: `wrapper_risk` reason code 필수

---

## 0단계 완료 조건
- 제품 목표 문장 1개
- 비목표 목록
- 운영 우선순위
- 개체 모델
- artifact type enum
- verdict enum
- delivery enum
- 점수 항목 정의
- verdict 규칙
- JSON 스키마
- 사용자 알림 템플릿
- 골든셋 구조


---

## Source file: `01_stage1_accounts_permissions_keys.md`

# 1단계: 계정, 권한, 키 분리 설계

## 단계 목적
1단계의 목표는 기능 구현보다 먼저 **권한 경계와 사고 반경(blast radius)을 고정**하는 것이다.

이 단계가 제대로 설계되면 다음 문제가 크게 줄어든다.
- 기존 바이낸스 OpenAI 봇과 비용/운영 혼선
- 텔레그램 수집 계정과 알림 봇의 책임 충돌
- GitHub/X 자격증명 누출 시 피해 확산
- dev/prod 혼용으로 인한 재현 불가 문제
- 나중에 키 교체, 토큰 회전, 서비스 분리 시 대규모 리팩토링

---

## 설계 원칙
1. **기능별로 계정을 분리**한다.
2. **사람 계정과 머신 계정을 분리**한다.
3. **수집 권한과 전송 권한을 분리**한다.
4. **장기 비밀과 단기 토큰을 분리**한다.
5. **dev와 prod를 최소한 OpenAI 기준으로는 분리**한다.
6. **런타임에는 필요한 비밀만 노출**한다.
7. **키 회전이 설계에 포함**되어야 한다.
8. **분석 정책과 인증 수단을 분리**한다.

---

## 권장 계정/자격증명 인벤토리

### 1) Telegram 수집용 읽기 계정 (사용자 계정)
용도:
- 공개 텔레그램 채널 20~30개를 읽기 위한 계정
- TDLib/MTProto 로그인 전용

보관 자격증명:
- 전화번호
- Telegram 2FA 비밀번호(있다면)
- `api_id`
- `api_hash`
- TDLib 세션 파일

주의:
- 메인 개인 계정을 쓰지 않는다.
- 이 계정은 “읽기 전용 수집기” 역할만 맡는다.
- dev 환경은 가능하면 이 계정에 직접 붙지 말고 저장된 fixture/raw message를 재생한다.

### 2) Telegram 알림용 Bot 계정
용도:
- 분석 결과를 사용자에게 전송
- 초기에는 outbound 전송 위주
- 이후 `/pause`, `/resume`, `/why` 같은 제어 명령을 받을 수 있음

보관 자격증명:
- `TELEGRAM_BOT_TOKEN`

주의:
- 수집 계정과 같은 역할을 맡기지 않는다.
- 채널 수집과 사용자 알림은 같은 Telegram 생태계 안에 있어도 인증 경계가 다르다.

### 3) OpenAI 전용 프로젝트 (prod)
용도:
- 이 봇만의 사용량/모델/키/권한 관리
- 기존 바이낸스 봇과 운영 분리

보관 자격증명:
- 프로젝트 전용 서비스 계정 API 키

주의:
- 기존 OpenAI 키 재사용 금지
- 프로젝트 예산 알림은 hard cap이 아니므로 앱 내부 hard cap 필요

### 4) OpenAI 전용 프로젝트 (dev)
용도:
- 프롬프트 실험, 골든셋 회귀 테스트, 정책 조정

보관 자격증명:
- dev 서비스 계정 API 키

주의:
- prod 키와 절대 혼용 금지
- dev는 더 낮은 budget, 더 좁은 모델 허용 범위

### 5) GitHub 읽기 자격증명
권장:
- **GitHub App**

보관 자격증명:
- `GITHUB_APP_ID`
- `GITHUB_INSTALLATION_ID`
- `GITHUB_PRIVATE_KEY`

설명:
- 긴 수명의 통합에는 PAT보다 GitHub App이 맞다.
- 설치 토큰은 1시간 만료이므로 런타임은 이를 필요 시 발급해 쓴다.
- 필요한 권한은 최소화하고, 읽기 중심으로 제한한다.

보조 대안:
- 초기 개발 중에는 fine-grained PAT를 짧게 쓸 수 있지만, 운영 본체는 GitHub App이 낫다.

### 6) X API 읽기 자격증명
권장:
- 전용 X Developer App 1개
- app-only Bearer Token 사용

보관 자격증명:
- `X_BEARER_TOKEN`
- 필요 시 오프라인 보관: `X_API_KEY`, `X_API_SECRET`

설명:
- 이 봇은 공개 글 조회가 핵심이므로 user-context보다 app-only가 단순하다.
- runtime에는 가급적 bearer token만 둔다.

---

## 왜 이렇게 분리하는가

### Telegram
- 공개 채널 수집은 Bot API보다 TDLib/Telegram API 사용자 인증이 더 자연스럽다.
- 반면 최종 전송은 Bot API가 단순하다.
- 따라서 **수집과 전송을 분리**해야 이후 리팩토링이 줄어든다.

### OpenAI
- 같은 조직 안에서도 프로젝트별로 서비스 계정, API 키, 모델 사용 범위, 사용량 추적을 분리할 수 있다.
- 바이낸스 봇과 catch-bot은 **같은 조직이어도 다른 프로젝트**로 나누는 것이 맞다.
- 예산 기능은 soft alert이므로 앱 내부에서 hard stop을 추가해야 한다.

### GitHub
- 장기 통합에는 GitHub App이 권장된다.
- 인증 요청이 더 높은 rate limit을 제공한다.
- 설치 토큰은 짧게 살고 private key는 장기 비밀이므로, 런타임 설계도 이 구조를 따라야 한다.

### X
- public read 목적이라면 bearer token 기반 app-only 인증이 가장 단순하다.
- 런타임에 user token을 둘 필요가 없다.

---

## 권장 권한 매트릭스

| 구성요소 | 역할 | 가져야 할 것 | 가지면 안 되는 것 |
|---|---|---|---|
| Telegram reader account | 채널 읽기 | `api_id`, `api_hash`, 세션 | Bot token, OpenAI key |
| Telegram notifier bot | 사용자 알림 전송 | Bot token | Telegram user session |
| Analyzer service | LLM 호출/판정 | OpenAI prod key | Telegram login, GitHub private key 원문(가능하면 직접 접근 최소화) |
| GitHub fetcher | repo 메타/파일 수집 | GitHub App private key 또는 발급된 installation token | OpenAI key |
| X fetcher | X post 조회 | X bearer token | OpenAI key |
| Admin/dev tooling | 테스트/재처리 | dev 키와 제한된 운영 도구 | prod 수집기 세션 직접 수정 권한 |

핵심은 **한 프로세스가 모든 비밀을 다 알지 못하게 하는 것**이다.

---

## OpenAI 설계 상세

### 프로젝트 구조
- `telegram-catch-bot-prod`
- `telegram-catch-bot-dev`

### 서비스 계정 권장 구조
- `svc-catch-bot-prod-analyzer`
- `svc-catch-bot-dev-analyzer`

### 키 정책
- 프로젝트별로 키를 따로 생성
- 가능하면 `Restricted` 권한 사용
- 사용 모델은 prod/dev 각각 최소 집합만 허용

### 모델 허용 범위 예시
prod:
- `gpt-5.4-mini`
- `gpt-5.4`

dev:
- `gpt-5.4-mini`만 우선 허용
- 필요 시 실험용으로만 일시적 확장

### 앱 내부 hard guard 예시
- 하루 분석 건수 상한
- 하루 input token 상한
- 승급(`mini -> full`) 비율 상한
- 초과 시 `later` 큐로만 적재하고 즉시 분석 중단

> 이유: OpenAI 프로젝트 budget은 soft alert이며 요청을 자동 차단하지 않는다.

---

## Telegram 설계 상세

### 수집 계정
- 전용 전화번호로 생성
- 2FA 활성화
- 복구 이메일 등록
- 프로필/이름도 수집 전용 계정으로 식별 가능하게 설정

### `api_id` / `api_hash`
- 이 값은 사용자 인증용이므로 봇 토큰과 다르다.
- 세션 파일과 함께 관리되며, TDLib 로그인에 필요하다.

### 세션 저장 원칙
- TDLib 세션 파일은 `.env`에 넣지 않는다.
- `/var/lib/<app>/tdlib/` 같은 전용 디렉터리에 보관
- 권한은 서비스 계정 사용자만 읽게 제한
- 백업에서 평문 유출되지 않도록 주의

### notifier bot
- 초기에는 1:1 DM 전송 또는 소규모 관리 채팅방 전송
- 추후 제어 명령을 받을 수 있으므로 채팅 ID allowlist를 둔다.

---

## GitHub 설계 상세

### 우선안: GitHub App
필수 저장:
- App ID
- Installation ID
- Private key

런타임 흐름:
1. private key로 JWT 생성
2. installation token 발급
3. installation token으로 API 호출
4. 만료 시 재발급

### 최소 권한 원칙
권장 시작 권한:
- Repository metadata: Read-only
- Contents: Read-only

선택 권한:
- Pull requests / Issues는 초기에 없어도 됨
- Webhooks는 이 단계에서 필수 아님

### 예외 전략
- public README/contents 조회는 무인증으로도 가능한 endpoint가 있다.
- 따라서 GitHub 장애 시에는 일부 public-only fallback 모드를 둘 수 있다.

---

## X 설계 상세

### 권장 인증 방식
- app-only bearer token

### 런타임 저장 원칙
- 운영 컨테이너에는 가급적 `X_BEARER_TOKEN`만 주입
- `API_KEY/API_SECRET`은 토큰 재발급 자동화가 필요하지 않다면 오프라인 저장

### 이유
- 공개 포스트 읽기가 핵심이므로 user-context가 필요 없다.
- 런타임 비밀 개수를 줄이면 사고 반경이 작아진다.

---

## secret 저장 방식 권장

### 권장안
- 운영 서버에는 **실행 시 주입되는 secret**만 둔다.
- 코드 저장소에는 평문 키를 절대 넣지 않는다.
- 단일 VPS라면 다음 셋 중 하나로 시작하면 충분하다.
  1. Docker secrets
  2. `sops + age` 암호화 파일
  3. 외부 secret manager

### 환경 변수 네이밍 예시
```bash
APP_ENV=prod
OPENAI_PROJECT=telegram-catch-bot-prod
OPENAI_API_KEY=...
TELEGRAM_API_ID=...
TELEGRAM_API_HASH=...
TELEGRAM_BOT_TOKEN=...
GITHUB_APP_ID=...
GITHUB_INSTALLATION_ID=...
GITHUB_PRIVATE_KEY_PATH=/run/secrets/github_app_private_key
X_BEARER_TOKEN=...
TDLIB_STATE_DIR=/var/lib/catchbot/tdlib
```

---

## dev / prod 분리 기준

### 반드시 분리
- OpenAI 프로젝트와 키
- notifier bot 또는 최소한 대상 chat_id
- 데이터베이스
- 로그/metrics

### 가능하면 분리
- GitHub/X 자격증명
- Telegram reader 계정

### 굳이 초기에 분리하지 않아도 되는 것
- VPS 자체
- 도메인
- reverse proxy

### 중요한 운영 원칙
개발 환경이 실시간 채널을 다시 읽는 구조는 피한다.
- dev는 fixture 재생
- prod만 live ingest

이 원칙이 없으면 테스트가 운영 데이터를 어지럽히고, duplicate 분석과 재현 불가 문제가 생긴다.

---

## 회전(rotation)과 복구 설계

| 자격증명 | 회전 원칙 | 비고 |
|---|---|---|
| OpenAI API key | 정기 회전 + 유출 시 즉시 폐기 | 키는 한 번만 표시될 수 있으므로 안전 저장 필수 |
| Telegram bot token | 유출 시 즉시 BotFather에서 재발급 | 알림 채널 allowlist 유지 |
| Telegram user session | 계정 문제 시 재로그인 | 세션 파일은 백업과 별도 관리 |
| GitHub App private key | 정기 회전 | installation token은 단기 토큰이므로 저장 가치 낮음 |
| X bearer token | 유출 시 재발급 | 런타임에는 최소 비밀만 유지 |

복구 문서는 최소한 아래를 가져야 한다.
- 어떤 키를 어디서 재발급하는지
- 교체 후 어떤 서비스 재시작이 필요한지
- 어떤 대체 모드로 일시 운영 가능한지

---

## 초기에 자주 하는 실수

### 1. OpenAI 키를 기존 봇과 공유
문제:
- 비용 추적 혼선
- rate limit 충돌 원인 분석 어려움
- 예산 통제 실패

### 2. Telegram 개인 메인 계정을 수집기에 사용
문제:
- 계정 리스크가 개인 사용에 전이됨
- 세션 관리가 불편해짐

### 3. GitHub PAT를 장기 운영 키로 박아두기
문제:
- 개인 계정 의존이 커짐
- 교체/감사/권한 설명이 어려움

### 4. dev와 prod가 같은 chat_id, 같은 DB 사용
문제:
- 테스트 알림과 실운영 알림이 섞임
- 재현 불가

### 5. 모든 프로세스에 모든 키를 주입
문제:
- 하나 뚫리면 전부 노출
- 역할 분리가 무의미해짐

---

## 1단계 완료 조건
아래가 확정되면 1단계 완료다.

- Telegram reader 전용 계정 확보
- Telegram notifier bot 생성
- OpenAI prod/dev 프로젝트 생성
- OpenAI 서비스 계정과 키 생성
- GitHub App 생성 및 최소 권한 정의
- X Developer App 생성 및 bearer token 확보
- secret 주입 방식 결정
- dev/prod 경계 문서화
- 회전/폐기/복구 절차 문서화
- 각 프로세스별 필요한 secret 목록 확정

---

## 이번 단계의 최종 권장안
- **Telegram**: reader account 1개 + notifier bot 1개
- **OpenAI**: 기존 바이낸스 봇과 다른 프로젝트 2개(dev/prod) + 서비스 계정 분리
- **GitHub**: GitHub App 우선, read-only 최소 권한
- **X**: 전용 app-only bearer token
- **Secrets**: 평문 `.env` 장기 운영 금지, secret 주입 방식 명시
- **운영 원칙**: prod만 live ingest, dev는 fixture 중심
