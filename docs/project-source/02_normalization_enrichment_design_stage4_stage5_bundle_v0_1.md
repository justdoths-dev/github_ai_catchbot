# 02 normalization enrichment design stage4 stage5 bundle v0 1
이 문서는 프로젝트 소스 파일 수 제한(40개)을 피하기 위한 **통합 번들**이다.
구조 변경 문서가 아니며, 아래 원본 문서를 **순서와 내용을 보존한 채** 묶는다.

## 포함 원본 파일
- `04_stage4_trigger_normalization.md`
- `05_stage5_external_enrichers.md`

## 통합 원칙
- 아키텍처 불변식은 유지한다.
- 원본 파일명은 아래 섹션 제목으로 보존한다.
- 충돌 시 해석 우선순위는 기존과 동일하게 정본 단계 문서 → 실행 계약 → migration 정본 → 구현 초안이다.


---

## Source file: `04_stage4_trigger_normalization.md`

# 4단계: 트리거와 정규화 설계

## 단계 목적

이 단계의 역할은 명확하다.

> **Telegram 원문을, 분석 가능한 후보 집합으로 바꾸는 결정적 경계**다.

여기서 가장 중요한 것은 **구조를 안 깨는 것**이다.  
3단계 collector가 이미 원문을 안정적으로 저장하고 있으므로, 4단계는 그 위에서 **후보를 뽑고 표준화**만 해야 한다. 아직 GitHub/X에 깊게 들어가지도 않고, LLM도 쓰지 않는다.

## 이번 단계에서 고정할 핵심 결정

이번 단계의 결론은 아래처럼 잠그는 것이 맞다.

- 담당 서비스는 **`router-normalizer` 1개**
- 입력은 **`SourceMessageEnvelope v1`**
- 출력은 **`artifact upsert` + `candidate_group proposal` + `suppression trace`**
- 처리 방식은 **완전 결정적(deterministic), 비-LLM**
- 외부 네트워크는 **짧은 URL 확장까지만 제한 허용**
- 트리거는 **2단계 구조**로 설계
- 결과 후보는 **provisional**로 만들고, 이후 enrichment에서 **reroot 가능**하게 둔다

이 중에서 제일 중요한 두 가지는 아래다.

첫째, **메시지를 저장하는 것**과 **분석 후보를 만드는 것**을 분리한다.  
둘째, **지금 primary라고 정한 artifact가 나중에 바뀔 수 있게 허용**한다.

두 번째가 특히 중요하다. 실제로는 이런 케이스가 많다.

- 텔레그램에는 X 링크만 있음
- X 본문을 열어보니 GitHub repo가 본체
- 결국 분석의 중심은 X가 아니라 GitHub여야 함

이걸 허용하지 않으면 5단계 이후 구조가 딱딱하게 굳는다.

## 전체 구조 안에서 4단계의 위치

4단계는 아래 경계로 이해하면 된다.

```text
SourceMessage
  ↓
router-normalizer
  ↓
Artifact / CandidateGroup proposal
  ↓
GitHub/X/Web enrich
  ↓
LLM Analysis
```

즉, 4단계는 **원문을 후보화(candidate-ization)** 하는 단계다.

여기서 절대 하면 안 되는 것이 있다.

- 최종 판정 만들기
- GitHub 품질 평가하기
- X 글의 유용성 점수 매기기
- LLM 호출하기

그건 전부 뒤 단계 책임이다.

## “반응”을 두 종류로 나눠야 한다

사용자 요구를 그대로 구현하면 `github`, `ai`, `vibe-coding`, GitHub/X 링크에 반응해야 한다.  
그런데 0단계에서 이미 **precision-first**를 잠갔다. 이 둘은 그냥 붙이면 충돌한다. 특히 `ai`가 문제다.

그래서 4단계에서는 “반응”을 두 종류로 나눠야 한다.

### 1차 반응: `signal_detected`
아래 중 하나라도 있으면 “신호가 있다”고 본다.

- GitHub 링크
- X 링크
- `github`
- `vibe coding`
- `vibe-coding`
- `ai`

### 2차 반응: `candidate_eligible`
실제로 후보 그룹을 만드는 기준이다.

- 강한 링크 신호가 있음
- 또는 vibe coding / dev workflow 문맥이 충분함
- 또는 `ai`가 있어도 개발 맥락 보조 신호가 같이 있음

이 구조로 가면 사용자 요구인 “ai에도 반응”을 만족하면서도, `ai`라는 단어만 있는 일반 잡담까지 전부 분석 후보로 올리는 문제를 피할 수 있다.

즉, **모든 메시지는 저장된다. 하지만 모든 메시지가 후보는 아니다.**

## 4단계 파이프라인은 9단계로 자르는 편이 맞다

`router-normalizer` 내부를 한 덩어리 함수로 만들면 이후 수정이 어렵다.  
그래서 내부 파이프라인도 명시적으로 나누는 편이 좋다.

1. `load_projection`
2. `text_normalize`
3. `extract_surfaces`
4. `extract_urls`
5. `resolve_short_urls`
6. `classify_links`
7. `canonicalize_artifacts`
8. `evaluate_trigger_rules`
9. `propose_candidate_groups`

핵심은 **각 단계를 재실행 가능하게 만드는 것**이다.  
예를 들어 나중에 URL canonicalizer 규칙만 바꾸고 싶으면 7단계부터 재처리할 수 있어야 한다.

## 텍스트 정규화는 별도 surface에서만 한다

원문은 3단계 collector가 이미 보존하고 있다.  
4단계에서는 절대 원문을 덮어쓰지 않는다. 대신 분석용 surface를 따로 만든다.

권장 surface는 최소 이 정도다.

- `raw_text_surface`
- `keyword_scan_surface`
- `hash_surface`
- `display_surface`

여기서 각각의 목적이 다르다.

- `keyword_scan_surface`: 키워드 탐지
- `hash_surface`: text-only 아이디어 dedupe
- `display_surface`: 후단 디버깅과 설명
- `raw_text_surface`: 원문 보존

권장 정규화는 다음 정도면 충분하다.

- Unicode `NFKC`
- zero-width 문자 제거
- 줄바꿈 정규화
- keyword scan용 lowercase 사본
- hash용 whitespace collapse 사본

이 구조를 안 잡으면 나중에 “탐지는 되고 dedupe는 안 되는” 상태가 자주 생긴다.

## URL 추출은 entity 우선으로 가야 한다

Telegram에서 링크를 regex만으로 뽑으면 나중에 반드시 빠지는 케이스가 나온다.  
Telegram은 링크와 포맷된 텍스트를 message entity로 표현하므로, hidden text URL까지 잡으려면 **entity 우선**으로 가야 한다.

권장 순서는 이렇다.

1. **entity 기반 추출**
2. **preview 기반 추출**
3. **regex fallback**

즉,

- `text_url`
- explicit URL entity
- link preview URL/title/description
- 마지막으로 일반 URL regex

이렇게 3중으로 가야 한다.

그리고 저장도 그냥 URL 문자열 하나로 끝내면 안 된다. 최소 아래는 남겨야 한다.

- `observed_url`
- `source_kind` (`entity`, `preview`, `regex`)
- `normalized_url`
- `resolved_url`
- `canonical_url`
- `classification`

이게 있어야 나중에 “왜 이 링크를 GitHub repo로 봤는지” 설명할 수 있다.

## 짧은 URL 확장은 허용하되, 크롤러로 변질시키지 않는다

`t.co` 같은 short URL은 확장하지 않으면 X/GitHub로 제대로 분류하기 어렵다.  
그래서 4단계에서 아주 제한된 네트워크 접근은 허용하는 편이 맞다.

다만 이건 **crawler**가 아니다.  
짧은 URL을 canonicalization 가능한 최종 URL로 바꾸는 좁은 작업일 뿐이다.

권장 규칙:

- shortener allowlist만 허용
- redirect hop 제한
- HEAD 우선, 필요 시 짧은 GET fallback
- JS 렌더링 금지
- 낮은 timeout
- 실패해도 전체 메시지 처리를 막지 않음

즉, 여기서 페이지 본문을 읽기 시작하면 안 된다.  
그건 5단계 이후 enrich 영역이다.

## Artifact type은 지금부터 넓게 잡아야 한다

v1에서 바로 다 쓰지 않더라도 enum은 미리 넓게 잡는 편이 좋다.

권장 artifact type:

- `github_repo`
- `github_subpath`
- `github_gist`
- `github_repo_page`
- `x_post`
- `web_article`
- `text_idea`
- `unknown_link`
- `short_url_unresolved`

이걸 미리 나눠두는 이유는 간단하다.

- GitHub 링크가 항상 repo root는 아니다.
- gist는 repo와 다르다.
- issue / pull / release도 repo와는 성격이 다르다.
- 링크가 없는 idea post도 후보가 될 수 있다.
- short URL 실패를 예외가 아니라 상태로 남겨야 한다.

## GitHub canonicalization은 generic URL 처리로 끝내면 안 된다

GitHub는 5단계에서 fetch 전략이 달라진다.  
repo root인지, subpath인지, gist인지, issue/pull/release인지에 따라 후단이 다르게 움직여야 한다.

권장 규칙은 이렇다.

### repo root
`https://github.com/{owner}/{repo}`  
→ `artifact_type = github_repo`  
→ `canonical_id = github:repo:{owner}/{repo}`

### subpath
`.../tree/{ref}/{path...}`  
`.../blob/{ref}/{path...}`  
→ `artifact_type = github_subpath`

### gist
`https://gist.github.com/.../{gist_id}`  
→ `artifact_type = github_gist`

### repo page
`.../issues/{n}`  
`.../pull/{n}`  
`.../releases/tag/{tag}`  
→ `artifact_type = github_repo_page`

중요한 규칙 하나를 추가로 잠가야 한다.

**GitHub URL에서 owner/repo를 파싱할 수 있으면 repo artifact도 같이 만든다.**

예를 들어 issue 링크만 있어도,

- supporting artifact = `github_repo_page`
- inferred primary anchor = `github_repo`

이렇게 잡는 편이 맞다.  
왜냐하면 이 프로젝트의 평가 중심은 대체로 “repo/tool 자체”이기 때문이다.

## X canonicalization은 post ID 중심으로 간다

X는 후단 fetch가 **post ID 중심**으로 움직이는 게 맞다.

그래서 4단계에서 중요한 것은 X 글을 “예쁘게 해석”하는 것이 아니라, **안정적으로 post ID를 뽑아 canonical id를 만드는 것**이다.

권장 규칙:

- 허용 host: `x.com`, `twitter.com`, `mobile.twitter.com`, 확장된 `t.co`
- URL 패턴:
  - `/{user}/status/{id}`
  - `/i/web/status/{id}`
- canonical id:
  - `x:post:{id}`

여기서 `screen_name`은 observation으로만 저장하고, canonical id에는 넣지 않는 편이 낫다.

그리고 아주 중요한 규칙을 하나 더 잠가야 한다.

**X link만 있는 메시지는 일단 `x_post`를 primary로 잡되, enrichment 후 GitHub repo가 본체로 확인되면 primary를 GitHub로 reroot할 수 있게 한다.**

이 규칙이 없으면 “설명은 X, 본체는 GitHub” 케이스를 자연스럽게 처리할 수 없다.

## 일반 article도 supporting artifact로 살려둔다

GitHub/X가 아닌 일반 URL도 그냥 버리면 안 된다.  
어떤 포스트는 블로그 글이 핵심이고, 거기서 repo나 workflow 아이디어가 설명된다.

그래서 일반 URL은 기본적으로 다음처럼 두는 편이 맞다.

- `artifact_type = web_article`
- `canonical_id = web:{normalized_host}:{stable_path_hash}`

다만 주의할 점이 있다.

일반 article은 **항상 primary가 되는 것이 아니라**, 더 강한 anchor가 있으면 supporting으로 내려가야 한다.

예를 들어

- Telegram 글 안에 GitHub repo + 블로그 설명 링크가 같이 있으면
- primary는 GitHub
- article은 supporting

이렇게 두는 편이 전체 구조와 맞다.

## text-only 포스트를 위한 `text_idea`는 반드시 넣어야 한다

이 프로젝트는 repo 수집기만이 아니다.  
사용자가 이미 “vibe coding 관련 아이디어 글도 배제하지 말라”고 명시했다.

그래서 링크가 없더라도 아래 조건이면 `text_idea`를 만들어야 한다.

- 외부 강한 링크 없음
- dev/workflow/tool 관련 텍스트 신호가 충분함
- 단순 감상문이 아니라 절차/아이디어/도구 제안 성격이 있음

다만 여기서는 과한 dedupe가 더 위험하다.  
text-only 아이디어는 표현이 조금만 바뀌어도 전혀 다른 글처럼 보일 수 있다.

그래서 v1에서는 보수적으로 가는 편이 맞다.

- 기본은 `message-scoped text_idea`
- 이후 반복 관측이 쌓이면 후단에서 유사도 병합 고려

즉, `text_idea`는 **전역 완벽 dedupe보다 누락 방지 우선**이다.

## 트리거 강도는 3단계면 충분하다

권장 강도 체계:

### strong
- GitHub 링크
- X 링크
- `vibe coding`
- `vibe-coding`
- `github` + 개발 문맥

### medium
- `ai` + 개발 문맥
- tool/workflow/prototype 성격의 text-only 포스트
- preview metadata까지 포함하면 개발 맥락이 분명한 경우

### weak
- `ai` 단독
- vague hype 문구만 있음
- 일반 뉴스 제목 수준

운영 규칙은 이렇게 두는 편이 좋다.

- `strong`은 거의 항상 candidate 생성
- `medium`은 suppressor가 없으면 candidate 생성
- `weak`는 trace만 남기고 기본 suppress

이 규칙이 0단계의 precision-first와 가장 잘 맞는다.

## `ai` 처리는 별도 보조 신호가 있어야 한다

이 부분은 리팩토링 방지 관점에서 특히 중요하다.

`ai`는 사용자 요구상 감시 키워드다.  
하지만 `ai`를 hard trigger로 바로 candidate 생성하게 만들면, 나중에 거의 반드시 알림 품질이 무너진다.

그래서 `ai`는 이렇게 처리하는 게 맞다.

- `ai`가 보이면 **signal_detected**
- 하지만 candidate 생성에는 **dev-context 보조 신호**가 하나 이상 필요
- 다만 특정 채널이 원래 개발 도구 중심이라면 channel override로 threshold를 낮출 수 있음

즉, `ai`는 “완전 배제”도 아니고 “무조건 후보화”도 아니다.  
**보조 문맥이 붙어야 candidate로 승격**한다.

## CandidateGroup proposal은 provisional 상태로 만든다

이 단계의 산출물은 최종 CandidateGroup이 아니라 **proposal**이어야 한다.  
그 이유는 enrichment 이후 primary가 바뀔 수 있기 때문이다.

권장 우선순위는 이렇다.

1. `github_repo`
2. `github_gist`
3. `x_post`
4. `web_article`
5. `text_idea`
6. `unknown_link`

그리고 grouping 규칙은 아래처럼 두는 것이 맞다.

### GitHub만 있음
- primary = GitHub
- supporting = subpath / repo page / article

### X만 있음
- primary = X
- supporting = article 등

### GitHub + X 같이 있음
- 기본 primary = GitHub
- supporting = X

### 여러 GitHub repo 같이 있음
- repo별로 candidate group 분리

### 링크 없고 text-only idea만 있음
- primary = `text_idea`

핵심은 이것이다.

**한 메시지에서 여러 candidate group이 나와도 된다.**  
이걸 허용하지 않으면 multi-link 메시지에서 구조가 반드시 깨진다.

## Dedupe는 2층으로 나눠야 한다

이 단계에서 dedupe를 너무 세게 걸면 놓치는 게 늘고, 너무 약하게 걸면 같은 repo가 계속 후보로 생성된다.

그래서 두 층으로 나누는 편이 낫다.

### 1층: artifact dedupe
- `artifact.canonical_id` unique
- 같은 repo / 같은 X post는 하나의 artifact로 수렴

### 2층: proposal dedupe
- 같은 source message가 같은 artifact를 반복 관찰해도 proposal은 한 번만 생성
- 여러 source message가 같은 artifact를 참조하면 observation만 추가
- 실제 새 analysis job을 만들지 여부는 뒤 단계 정책에서 결정

즉, 4단계는 **artifact 통합**에 집중하고, **분석 중복 억제**는 뒤 단계로 넘기는 편이 구조적으로 맞다.

## 이 단계에서 반드시 남겨야 하는 테이블

실제 스키마는 나중에 더 구체화하더라도, 개념적으로는 아래 네 층을 꼭 유지하는 편이 좋다.

- `normalization_runs`
- `artifact_registry`
- `artifact_observations`
- `candidate_group_proposals`
- `candidate_group_members`

이 구성이 좋은 이유는 간단하다.

- 무엇을 봤는지
- 무엇으로 분류했는지
- 왜 후보가 됐는지
- 왜 후보가 안 됐는지

를 전부 복기할 수 있기 때문이다.

## 이번 단계에서 절대 하면 안 되는 것

아래는 지금 막아야 한다.

1. regex만으로 URL 추출
2. `ai`를 hard trigger로 즉시 candidate 생성
3. GitHub subpath를 generic URL로만 저장
4. X post를 screen name 기반 문자열로만 저장
5. 한 메시지에서 첫 링크 하나만 사용
6. suppress된 메시지를 그냥 버림
7. 정규화 단계에서 일반 웹 본문 크롤링 시작
8. LLM을 정규화 단계에 투입
9. text-only vibe post 배제
10. provisional primary reroot 금지

특히 2, 7, 10은 이후 구조를 크게 흔든다.

## 4단계 완료 기준

아래가 고정되면 4단계는 끝이다.

- `router-normalizer` 입력/출력 계약 확정
- URL extraction 우선순위 확정
- short-url expansion 범위 확정
- artifact type enum 확정
- GitHub/X canonicalization 규칙 확정
- `text_idea` 규칙 확정
- 2단계 trigger 구조 확정
- suppression reason code 확정
- candidate grouping / reroot 정책 확정
- artifact / observation / proposal 스키마 확정

## 한 줄 결론

**4단계는 Telegram 원문에서 링크와 텍스트 신호를 결정적으로 추출해 `Artifact`와 `CandidateGroup proposal`로 바꾸는 경계이며, 이 단계가 안정적이어야 이후 GitHub/X enrich와 LLM 분석이 구조적으로 흔들리지 않는다.**


---

## Source file: `05_stage5_external_enrichers.md`

# 5단계: 외부 증거 수집기(enricher) 설계

이번 단계는 앞선 구조를 그대로 유지한 채, **`Artifact / CandidateGroup proposal`을 실제 분석 가능한 `EvidenceBundle`로 바꾸는 경계**를 설계하는 단계다.

이 단계에서도 이전 단계에서 잠근 구조를 깨지 않는다.

- **0단계**: `SourceMessage → Artifact → CandidateGroup → Analysis → Notification`
- **1단계**: 계정/권한/키 분리
- **2단계**: `PostgreSQL = system of record`, `Redis = queue/lock`, `prod 단일 live collector`
- **3단계**: collector는 Telegram 원문 보존만 담당
- **4단계**: router-normalizer는 결정적으로 `Artifact`와 `CandidateGroup proposal`만 생성

따라서 5단계의 원칙은 명확하다.

> **외부 증거 수집기는 판단기가 아니다.**  
> **LLM을 호출하지 않고, 구조화된 증거만 모은다.**  
> **후단 LLM이 냉정한 판정을 내릴 수 있도록 provenance가 보존된 evidence pack을 만든다.**

---

## 5단계에서 고정할 핵심 결정

| 구분 | 고정 결정 |
|---|---|
| 책임 경계 | enrichers는 `Artifact`를 외부 소스에서 보강하고 `EvidenceBundle`을 만든다 |
| 비책임 | 최종 verdict 계산, 사용자 메시지 작성, LLM 호출 |
| 서비스 분리 | `gh-enricher`, `x-enricher`, `web-enricher`, `evidence-assembler` |
| 입력 | `candidate_group_proposal` + `artifact_registry` |
| 출력 | `artifact_snapshot` + `candidate_evidence_bundle` + `reroot_event` |
| 네트워크 정책 | GitHub/X는 **공식 API 우선**, 일반 웹은 제한적 fetch만 허용 |
| 저장 정책 | raw response는 별도 저장 가능하되, **분석용 snapshot은 append-only** |
| primary 변경 | 4단계 proposal의 primary는 **이 단계에서 reroot 가능** |
| 재귀 깊이 | **depth budget**를 둬서 링크 추적이 무한 증식하지 않게 함 |
| 실패 모델 | partial enrichment 허용, `low_evidence`를 명시적 상태로 유지 |

여기서 핵심은 두 가지다.

1. **정규화 규칙과 enrich 규칙을 재구현하지 않는다.**  
   enrichers가 새 링크를 발견해도, 그것을 다시 canonicalize하는 로직은 **4단계 canonicalization 규칙을 재사용**해야 한다.

2. **evidence는 snapshot으로 남기고, candidate는 lineage로 추적한다.**  
   그래야 나중에 “왜 X가 아니라 GitHub를 primary로 바꿨는지” 복기 가능하다.

---

## 전체 구조 안에서 5단계의 위치

```text
SourceMessage
  ↓
router-normalizer
  ↓
Artifact / CandidateGroup proposal
  ↓
external enrichers
  ├─ gh-enricher
  ├─ x-enricher
  ├─ web-enricher
  └─ evidence-assembler
  ↓
EvidenceBundle
  ↓
LLM Analysis
```

즉, 5단계는 **후보를 외부 증거로 고정(anchor)** 하는 단계다.

---

## 5단계의 공통 인터페이스를 먼저 잠근다

구조가 무너지지 않게 하려면 source별 fetcher보다 먼저 **공통 계약**을 잠가야 한다.

### 공통 입력: `ArtifactEnrichmentJob`

권장 형태:

```json
{
  "schema_version": "artifact_enrichment_job_v1",
  "job_id": "...",
  "candidate_group_id": "...",
  "artifact_id": "...",
  "artifact_type": "github_repo",
  "priority": "normal",
  "refresh_mode": "standard",
  "depth_budget": 1,
  "requested_at": "ISO-8601"
}
```

핵심 필드는 아래다.

- `artifact_id`: 4단계 canonical artifact 참조
- `artifact_type`: source-specific fetcher 라우팅 기준
- `depth_budget`: 무한 확장 방지
- `refresh_mode`: `standard / refresh_if_stale / force_refresh`

### 공통 출력: `ArtifactSnapshot`

권장 공통 필드:

```json
{
  "snapshot_schema_version": "artifact_snapshot_v1",
  "artifact_id": "...",
  "provider": "github",
  "snapshot_type": "github_repo",
  "status": "ready",
  "fetched_at": "ISO-8601",
  "content_anchor": "...",
  "auth_mode": "app_installation",
  "normalized_projection": {},
  "raw_payload_ref": "blob://...",
  "evidence_limitations": [],
  "fetch_anomalies": []
}
```

여기서 중요한 건 `content_anchor`다.

- GitHub repo는 **commit SHA / ref SHA**에 anchored
- X post는 **post ID + edit state**에 anchored
- web article은 **final URL + content hash / last-modified / etag**에 anchored

이걸 안 잡으면 같은 artifact를 언제 다시 분석해야 하는지 기준이 흐려진다.

### 공통 묶음: `CandidateEvidenceBundle`

LLM은 raw snapshot 여러 개를 직접 보면 안 된다.  
LLM 입력은 `EvidenceBundle`이라는 **정리된 중간 형식**으로 제한하는 편이 맞다.

권장 필드:

- `candidate_group_id`
- `initial_primary_artifact_id`
- `current_primary_artifact_id`
- `supporting_artifact_ids`
- `bundle_version`
- `reroot_count`
- `source_lineage`
- `primary_summary`
- `supporting_summaries`
- `discovered_links_summary`
- `evidence_limitations`
- `ready_for_analysis`
- `token_budget_profile`

이 구조를 잠가두면, 나중에 LLM 프롬프트를 바꿔도 enrich 계층은 거의 안 건드려도 된다.

---

## source별 enrichers를 분리해야 하는 이유

GitHub, X, 일반 웹은 **데이터 접근 방식도 다르고, 오류 모델도 다르고, freshness 모델도 다르다.**  
그래서 `one fetcher to rule them all` 식으로 합치면 결국 복잡도만 커진다.

권장 분리는 아래와 같다.

- `gh-enricher`: repo / subpath / repo page / gist 계열
- `x-enricher`: post / referenced post / media / author context
- `web-enricher`: 일반 article / landing page / preview page
- `evidence-assembler`: source별 snapshot을 묶어 candidate 중심 evidence로 조립

중요한 점은 이 네 서비스 중 **candidate를 직접 verdict로 바꾸는 서비스는 없다는 것**이다.

---

## GitHub enricher 설계

GitHub는 이 프로젝트에서 가장 중요한 소스다.  
하지만 여기서 흔한 실수가 두 가지다.

1. repo 전체를 무조건 clone하려는 것  
2. README만 보고 결론 내리려는 것

둘 다 구조를 망친다.

### GitHub enricher의 역할

`gh-enricher`의 목표는 아래 네 가지다.

- repo의 **정체성** 확보
- repo의 **구조(shape)** 확보
- repo의 **핵심 파일 샘플** 확보
- repo의 **운영 신호** 확보

즉, 목적은 “모든 파일 수집”이 아니라 **냉정한 1차 판정에 필요한 증거를 최소비용으로 모으는 것**이다.

### 인증과 degraded mode

GitHub App installation access token은 REST/GraphQL 요청에 사용할 수 있고, **1시간 후 만료**된다. GitHub는 설치 토큰을 매 요청에 직접 오래 보관하기보다, 필요 시 재발급하거나 SDK가 재생성하도록 쓰는 구조를 전제로 설명한다. 또한 많은 REST 엔드포인트는 public 리소스에 한해 **무인증 호출도 가능**하다. ([GitHub App installation auth](#official-source-notes), [GitHub contents/tree endpoints](#official-source-notes))

따라서 운영 원칙은 아래처럼 잠그는 편이 맞다.

- 기본: `auth_mode = app_installation`
- 인증 장애 시: public repo에 한해 `auth_mode = anonymous_degraded`
- private 전제 동작 금지
- installation token 자체는 장기 저장 금지

### GitHub fetch 전략은 3계층으로 나눈다

#### 1계층: identity/meta
가장 먼저 확보할 것:

- `owner/repo`
- default branch
- archived / fork / template 여부
- description
- license
- topics
- homepage
- pushed_at
- watchers/stars/forks/open issues 같은 **약한 운영 신호**

여기서 중요한 것은 **인기 지표는 보조 자료일 뿐**이라는 점이다.  
수집은 하되, 후단 LLM이 과대평가하지 않도록 약한 신호로 표시한다.

#### 2계층: tree/shape
그다음은 repo 구조를 파악한다.

GitHub contents API는 디렉터리당 **1,000 파일 상한**이 있고, 더 큰 재귀 조회가 필요하면 Trees API를 써야 한다. Trees API의 recursive 응답은 **최대 100,000 entries / 7 MB** 제한이 있으며, 응답에 `truncated=true`가 뜨면 비재귀로 서브트리를 나눠 가져와야 한다. 또한 contents API의 `download_url`은 **만료되며 1회성 사용을 전제**한다. ([GitHub repository contents](#official-source-notes), [Git trees API](#official-source-notes))

그래서 `gh-enricher`는 다음 규칙으로 고정한다.

- 작은 디렉터리: contents API 우선
- 큰 저장소/재귀 필요: Trees API 우선
- `download_url`은 영구 저장 금지
- `truncated=true`면 부분 서브트리 fetch로 전환

#### 3계층: evidence file sampling
그다음에야 파일 샘플링을 한다.

우선순위 예시:

1. `README*`
2. manifest
   - `package.json`
   - `pyproject.toml`
   - `requirements*.txt`
   - `Cargo.toml`
   - `go.mod`
   - `pom.xml`
   - `build.gradle*`
3. lockfile
4. CI
   - `.github/workflows/*`
5. tests
6. examples / demo
7. docs
8. 엔트리포인트 후보
9. 설정 파일
10. release metadata

핵심은 **역할 기반 샘플링**이다.  
확장자 기반 전체 긁기가 아니다.

### archive fallback은 “예외적 내용 샘플링” 용도다

GitHub는 branch, tag, commit 기준으로 tarball/zipball 아카이브를 다운로드할 수 있고, 이 아카이브는 **전체 저장소 이력은 포함하지 않는 스냅샷**이다. REST archive endpoint는 **302 redirect**를 반환하고, private repo 링크는 **5분 내 만료**될 수 있다. GitHub는 아카이브 재현성이 중요하면 branch/tag보다 **commit ID 기준**을 권장한다. ([Source code archives](#official-source-notes), [GitHub archive REST endpoints](#official-source-notes))

따라서 archive 사용 규칙은 아래처럼 잠그는 편이 맞다.

- 기본 전략은 아님
- contents/tree만으로 파일 샘플링이 비효율적일 때만 사용
- `ref`는 가능하면 commit SHA로 고정
- redirect URL 영구 저장 금지
- tarball/zip 전체를 DB에 저장하지 않고 필요한 파일만 추출

즉, archive는 **clone 대체재가 아니라 샘플링 fallback**이다.

### release / maintenance evidence도 수집하되, 부차 신호로 둔다

GitHub releases endpoint는 release 목록과 함께 `tarball_url`, `zipball_url`, asset의 `browser_download_url`, `download_count` 같은 정보를 제공한다. 그래서 배포 흔적이나 릴리즈 discipline을 수집하는 데는 유용하다. ([GitHub releases API](#official-source-notes))

다만 이걸 구조적으로는 아래처럼 다뤄야 한다.

- `release_count_recent`
- `latest_release_published_at`
- `has_release_assets`
- `release_asset_download_count_topk`
- `has_prerelease_pattern`

이 값들은 **품질의 직접 증거가 아니라 운영 성숙도 보조 신호**다.

### repo page / subpath / gist 처리

4단계에서 이미 아래 artifact type을 분리해뒀다.

- `github_repo`
- `github_subpath`
- `github_repo_page`
- `github_gist`

5단계에서는 이걸 다시 합치지 않는다.

#### `github_repo`
- 완전한 repo evidence 수집
- 가장 강한 primary 후보

#### `github_subpath`
- subpath 자체도 snapshot으로 남김
- 동시에 상위 repo snapshot 연결
- subpath에 집중된 evidence도 별도 유지

#### `github_repo_page`
- issue / pull / release / discussion 같은 페이지 evidence 수집
- 그러나 candidate primary는 기본적으로 repo anchor를 유지

#### `github_gist`
- gist 설명, 파일 목록, 언어 혼합, truncated 여부, 샘플 파일 내용만 얇게 수집
- repo와 동일한 무게를 주지 않음

### GitHub snapshot에 반드시 있어야 할 필드

권장 최소 필드:

- `repo_full_name`
- `default_branch`
- `resolved_ref`
- `content_anchor_commit_sha`
- `repo_flags` (`archived`, `fork`, `template` 등)
- `license_spdx`
- `topics`
- `readme_excerpt`
- `detected_build_systems`
- `detected_languages`
- `key_paths`
- `test_paths`
- `ci_paths`
- `examples_paths`
- `docs_paths`
- `release_summary`
- `sampled_files`
- `fetch_anomalies`
- `evidence_limitations`

여기서 `sampled_files`는 실제 파일 본문 전체를 들고 다니는 게 아니라, 다음처럼 잘라 보관하는 편이 낫다.

- path
- role
- size
- excerpt
- hash
- raw_blob_ref

### GitHub에서 발견한 새 링크는 같은 canonicalizer를 써야 한다

README나 docs 안에 X/GitHub/블로그 링크가 있을 수 있다. 하지만 `gh-enricher`가 독자적으로 새 artifact를 만들기 시작하면 4단계와 규칙이 갈라진다.

그래서 규칙은 아래처럼 잠근다.

- enricher는 `discovered_url_observation`만 emit
- canonicalization은 **4단계 규칙을 재사용**
- 새 artifact 생성은 공유 canonicalization 계층에서 수행
- candidate mutation은 `evidence-assembler`만 수행

이 규칙이 전체 구조를 가장 잘 지켜준다.

---

## X enricher 설계

X는 이 프로젝트에서 “코드가 아니라 아이디어/워크플로우”를 붙잡는 소스다.  
따라서 X enricher의 목적은 단순 텍스트 fetch가 아니라 **아이디어의 구체성, 맥락, 링크, 증거 밀도**를 높이는 것이다.

### 공식 API 우선 원칙

X의 `Get Posts by IDs` endpoint는 **최대 100개 ID**를 한 번에 조회할 수 있고, post ID는 큰 정수 처리 문제를 피하기 위해 **문자열(string)** 로 반환된다. `tweet.fields`로 `conversation_id`, `entities`, `referenced_tweets`, `public_metrics` 등 필요한 필드를 요청할 수 있고, `expansions`로 `author_id`, `attachments.media_keys`, `referenced_tweets.id` 등을 확장할 수 있다. X의 Data Dictionary도 Post 객체의 기본 필드가 `id`, `text`, `edit_history_tweet_ids`이며 추가 필드는 `tweet.fields`와 `expansions`로 확장한다고 설명한다. ([X Get Posts by IDs](#official-source-notes), [X Data Dictionary](#official-source-notes))

따라서 X 쪽 원칙은 이렇게 잠그는 편이 맞다.

- URL HTML scraping 금지
- post ID 기반 official lookup 우선
- `tweet.fields`, `user.fields`, `media.fields`, `expansions`를 명시적으로 요청
- post ID는 정수형이 아니라 문자열로 저장

### X fetch depth를 제한해야 한다

X는 thread/quote/reply를 무한히 따라가면 금방 crawler가 된다.  
이 프로젝트는 그게 목적이 아니다.

권장 depth budget:

- 기본: `1`
- 허용:
  - 본문 post
  - 직접 referenced_tweets
  - 필요 시 author
  - media summary
- 금지:
  - conversation 전체 무한 확장
  - 사용자 timeline 탐색
  - 검색 기반 주변 글 확장

즉, X enrich는 **해당 링크가 가리키는 글의 직접 맥락만 보강**한다.

### X에서 수집해야 하는 최소 증거

권장 최소 필드:

- `post_id`
- `author_id`
- `author_handle`
- `created_at`
- `conversation_id`
- `text`
- `entities.urls`
- `referenced_tweets`
- `public_metrics`
- `media_summary`
- `note_tweet`
- `lang`
- `possibly_sensitive`

여기서 `media_summary`는 이미지/영상의 존재와 alt text, preview 정도만 구조화하면 충분하다.  
OCR나 비전 분석까지 바로 들어가면 구조가 과해진다.

### X의 중요한 역할: GitHub/X/article 링크 발견

X post 본문에는 GitHub 링크나 article 링크가 숨어 있을 수 있다.  
이 경우 X는 본체가 아니라 **설명 계층**일 가능성이 높다.

그래서 X enricher는 아래를 해야 한다.

- `entities.urls` 추출
- 확장된 최종 URL 확보
- `discovered_url_observation` emit
- canonicalization 재사용
- GitHub repo가 확인되면 `reroot_candidate_suggestion` 생성

즉, X는 종종 **primary를 재선정하게 만드는 supporting source**다.

### X snapshot에 반드시 있어야 할 필드

권장 최소 필드:

- `post_id`
- `content_anchor_post_version`
- `author_summary`
- `text_full`
- `text_excerpt`
- `conversation_id`
- `referenced_post_ids`
- `discovered_links`
- `media_summary`
- `metrics_summary`
- `evidence_limitations`
- `fetch_anomalies`

여기서 `content_anchor_post_version`은 최소한 다음 정보를 포함하는 편이 맞다.

- `post_id`
- 마지막 `edit_history_tweet_id`
- `fetched_at`

이렇게 해야 수정된 post와 예전 snapshot을 구분할 수 있다.

### X에서 특히 피해야 할 반패턴

- 사용자 전체 profile을 뒤지기
- thread 전체를 무제한 복원하려 하기
- 이미지 OCR를 기본 경로로 넣기
- X 글 하나를 독립 제품처럼 과대평가하기
- GitHub link discovery를 X fetcher 내부 규칙으로만 처리하기

핵심은 **X는 아이디어와 문맥 증거를 붙이는 소스**라는 점이다.

---

## Web enricher 설계

일반 article은 GitHub/X보다 덜 구조화돼 있지만, 실제로는 꽤 중요하다.  
어떤 글은 블로그 본문이 핵심이고, GitHub repo는 supporting인 경우도 있다.

다만 일반 웹은 위험도 높다.

- 레이아웃이 제각각
- canonical link가 엉뚱할 수 있음
- preview와 본문이 다를 수 있음
- JS 렌더링을 요구하는 사이트가 있음

그래서 `web-enricher`는 **보수적으로 얇게** 설계하는 편이 맞다.

### web-enricher의 책임

- final URL 결정
- canonical URL 후보 추출
- title / description / og metadata 확보
- main text excerpt 확보
- outbound links 추출
- site identity 확보

### web-enricher의 비책임

- headless browser로 full rendering
- 로그인 세션 활용
- paywall 우회
- OCR 중심 처리
- 무제한 링크 재귀

### fetch 정책

권장 원칙:

- GET 기반 제한 fetch
- 낮은 timeout
- body size cap
- content-type allowlist (`text/html`, 일부 text/*)
- no JS rendering in v1
- no cookie jar shared with user browser
- redirect hop cap

즉, 일반 웹은 “얇게 읽고 링크를 뽑는 도구”이지, 브라우저 자동화가 아니다.

### article snapshot에 필요한 최소 필드

- `final_url`
- `canonical_url_candidate`
- `site_name`
- `title`
- `description`
- `author`
- `published_at`
- `content_hash`
- `main_text_excerpt`
- `outbound_links`
- `fetch_anomalies`
- `evidence_limitations`

### 일반 article도 reroot를 일으킬 수 있다

예를 들어,

- Telegram 글에는 블로그 링크만 있음
- article 본문에 GitHub repo 링크가 있음
- 실제 평가 대상은 article이 아니라 repo임

이 경우 web-enricher는 다음만 한다.

- outbound GitHub/X 링크 발견
- canonicalization 재사용
- `reroot_candidate_suggestion` emit

최종 primary 변경은 `evidence-assembler`가 한다.

### Web fetch에서 특히 피해야 할 반패턴

- readability 실패 시 headless browser를 기본 fallback로 두는 것
- article을 통째로 DB에 장기 저장하는 것
- 외부 JS/iframe까지 따라가는 것
- article 자체를 LLM에 바로 넣는 것

일반 웹은 **요약 가능한 evidence surface**만 확보하면 충분하다.

---

## 새 링크 발견(discovered links)은 공통 이벤트로 통합한다

이 부분이 전체 구조의 접착제다.

GitHub README, X post, 일반 article 모두 **새 링크를 발견할 수 있다.**  
그런데 각 enricher가 자기 방식대로 artifact를 만들어버리면 canonicalization 규칙이 찢어진다.

그래서 공통 이벤트를 두는 편이 맞다.

### `DiscoveredUrlObservation`

권장 형태:

```json
{
  "schema_version": "discovered_url_observation_v1",
  "parent_candidate_group_id": "...",
  "parent_artifact_id": "...",
  "observed_url": "...",
  "context_path": "x_post.entities.urls[0]",
  "discovery_reason": "embedded_link",
  "depth_remaining": 0
}
```

처리 규칙:

1. 공유 canonicalizer 사용
2. 동일 canonical artifact면 observation만 추가
3. 새 artifact면 supporting artifact로 우선 연결
4. reroot suggestion은 별도 이벤트로 분리

이렇게 해야 4단계 규칙과 5단계가 유기적으로 연결된다.

---

## evidence-assembler 설계

source별 snapshot을 모았다고 바로 LLM에 보내면 안 된다.  
`evidence-assembler`가 후보 중심으로 재구성해야 한다.

### assembler의 역할

- candidate group 단위로 snapshot 모으기
- supporting artifact 정리
- reroot 여부 판단
- 중복 supporting artifact 제거
- evidence limitations 통합
- token budget profile 생성
- `ready_for_analysis` 판정

### assembler만 primary를 바꿀 수 있게 해야 한다

이건 구조적으로 중요하다.

- `gh-enricher`는 GitHub evidence를 만든다
- `x-enricher`는 X evidence를 만든다
- `web-enricher`는 article evidence를 만든다
- **primary 교체는 assembler만 한다**

그래야 side effect가 한 군데에만 모인다.

### reroot 규칙 v1

대표 규칙은 아래처럼 잠그는 편이 좋다.

#### X → GitHub reroot
아래를 모두 만족하면 reroot 허용:

- current primary = `x_post`
- discovered canonical GitHub repo 존재
- GitHub enrichment status = `ready` or `partial_ready`
- GitHub evidence가 X보다 구조적으로 더 강함

#### Web → GitHub reroot
아래를 만족하면 허용:

- current primary = `web_article`
- article이 특정 GitHub repo를 핵심 본체로 지시
- GitHub snapshot 확보됨

#### GitHub subpath / repo_page → repo reroot
기본적으로 허용:

- 상위 repo snapshot 준비됨
- subpath/page는 supporting으로 유지

#### text_idea는 언제 유지하나
- 외부 강한 artifact가 끝내 발견되지 않으면 유지
- 발견된 외부 artifact가 약하거나 실패하면 유지 가능

즉, reroot는 “항상 GitHub로 바꾸기”가 아니라 **더 강한 분석 anchor가 확인될 때만** 일어난다.

### bundle readiness 규칙

후단 LLM으로 보내기 전에 assembler는 최소 기준을 체크해야 한다.

권장 기준:

- primary snapshot 존재
- primary status가 `ready` 또는 `partial_ready`
- evidence limitations 기록됨
- fetch anomalies 구조화됨
- token budget profile 생성됨

즉, evidence가 약해도 bundle은 만들어질 수 있다.  
하지만 약한 이유를 명시해야 한다.

### token budget profile도 이 단계에서 만든다

LLM 입력 budget은 enrich 단계에서 미리 계산하는 편이 좋다.

권장 profile:

- `small`: GitHub README + manifest + tree 요약 / X 본문 + immediate links
- `medium`: small + tests/CI/examples 일부 / X referenced post 일부
- `large`: medium + release summary + article excerpts

이 profile을 assembler가 만들면, 6단계 LLM 판정기는 “무엇을 더 자를지”보다 “어떻게 판정할지”에 집중할 수 있다.

---

## 상태 모델은 partial success를 기본 전제로 둔다

외부 API는 항상 불완전하다.  
그래서 5단계는 성공/실패 이분법으로 설계하면 안 된다.

권장 상태:

- `pending`
- `fetching`
- `ready`
- `partial_ready`
- `failed_transient`
- `failed_permanent`
- `rate_limited`
- `access_denied`
- `unsupported`
- `low_evidence`

이 중에서 중요한 것은 `partial_ready`와 `low_evidence`다.

- README는 읽었지만 tree가 잘리지 않았다 → `partial_ready`
- X 본문은 읽었지만 media/context가 부족하다 → `partial_ready`
- 링크가 죽어서 article 본문을 못 읽었다 → `low_evidence`

이 구조가 있어야 6단계 LLM이 “증거 한계”를 honest하게 반영할 수 있다.

---

## freshness와 재수집 규칙도 이번 단계에서 잠근다

enricher는 캐시와 재수집 기준이 없으면 곧 비용과 혼란이 폭증한다.

### GitHub
- 강한 anchor: `resolved_ref_sha`
- default branch 기준 artifact면 head SHA 바뀌면 새 snapshot
- 같은 SHA면 기존 snapshot 재사용 우선

### X
- 강한 anchor: `post_id + latest_edit_history_id`
- 같으면 재사용 우선
- referenced posts만 부분 refresh 가능

### Web
- 강한 anchor: `final_url + content_hash`
- `etag`/`last-modified`가 있으면 활용
- 동일 hash면 재분석 생략 가능

핵심은 **artifact ID와 content anchor를 분리**하는 것이다.

- artifact ID = “무엇을 가리키는가”
- content anchor = “그 대상의 어떤 버전을 봤는가”

이 둘을 섞으면 캐시가 곧 꼬인다.

---

## source별 큐와 동시성은 보수적으로 유지한다

2단계의 런타임 원칙과 연결하면, 5단계에서도 concurrency는 낮게 두는 편이 맞다.

권장 시작값:

- `q_enrich_github`: concurrency 1
- `q_enrich_x`: concurrency 1
- `q_enrich_web`: concurrency 1~2
- `q_assemble_evidence`: concurrency 1~2

GitHub는 공식 best practice에서 `retry-after`, `x-ratelimit-reset`, exponential backoff를 따르라고 하고, redirect도 일반적으로 따라야 한다고 설명한다. 따라서 GitHub fetcher는 빠른 병렬 crawler가 아니라 **저동시성, 재시도 통제형 worker**가 맞다. ([GitHub REST best practices](#official-source-notes))

OpenAI는 아직 이 단계에서 호출하지 않지만, 6단계에서 rate limit 관리가 필요하므로 evidence 단계에서부터 **bundle 수를 불필요하게 폭증시키지 않는 것**이 중요하다.

---

## 테이블/저장 구조도 이번 단계에서 고정하는 편이 좋다

권장 개념 테이블은 아래다.

- `artifact_enrichment_runs`
- `artifact_snapshots`
- `artifact_snapshot_github_repo`
- `artifact_snapshot_x_post`
- `artifact_snapshot_web_article`
- `discovered_url_observations`
- `candidate_reroot_events`
- `candidate_evidence_bundles`
- `candidate_evidence_members`

중요한 원칙:

- source별 상세 필드는 source-specific snapshot table에 둠
- 공통 상태/버전/anchor는 `artifact_snapshots`에 둠
- reroot는 별도 lineage 이벤트로 남김
- bundle은 overwrite 가능하되, 이전 version을 남길 수 있게 versioning 필드 유지

---

## 이번 단계에서 반드시 피해야 할 반패턴

아래는 지금 막아야 한다.

1. enrich 단계에서 바로 LLM 호출
2. GitHub repo를 기본적으로 clone
3. X HTML scraping 우선
4. 일반 웹에 headless browser를 기본 장착
5. enrichers가 자기 방식대로 새 artifact 생성
6. reroot를 각 fetcher가 직접 수행
7. partial failure를 exception으로만 처리
8. artifact ID와 content anchor를 섞어 저장
9. release/stars 같은 약한 신호를 강한 품질 신호처럼 다룸
10. bundle 없이 raw payload를 그대로 LLM에 넣음

특히 **5번, 6번, 10번**은 구조를 강하게 무너뜨린다.

---

## 5단계 완료 기준

아래가 확정되면 5단계는 끝이다.

- `ArtifactEnrichmentJob` 스키마 확정
- `ArtifactSnapshot` 공통 스키마 확정
- GitHub/X/Web source별 snapshot 스키마 확정
- `DiscoveredUrlObservation` 규칙 확정
- reroot 책임을 `evidence-assembler`로 단일화
- `CandidateEvidenceBundle` 스키마 확정
- source별 상태 모델 확정
- freshness/content-anchor 정책 확정
- source별 큐와 동시성 기준 확정
- partial_ready / low_evidence 처리 규칙 확정

이번 단계의 한 줄 결론은 이거다.

> **5단계는 `Artifact`를 공식 API와 제한적 웹 수집으로 보강해, provenance와 evidence limitation이 보존된 `CandidateEvidenceBundle`로 조립하는 단계다. 이 단계가 안정적이어야 6단계 LLM 판정기는 크롤링이 아니라 판단에만 집중할 수 있다.**

---

## Official source notes

- GitHub Docs — Authenticating as a GitHub App installation
- GitHub Docs — REST API endpoints for repository contents
- GitHub Docs — REST API endpoints for Git trees
- GitHub Docs — Best practices for using the REST API
- GitHub Docs — Downloading source code archives
- GitHub Docs — REST API endpoints for releases
- X Developer Platform — Get Posts by IDs
- X Developer Platform — Data Dictionary
