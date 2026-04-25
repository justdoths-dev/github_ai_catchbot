# 01 runtime collector design stage2 stage3 bundle v0 1
이 문서는 프로젝트 소스 파일 수 제한(40개)을 피하기 위한 **통합 번들**이다.
구조 변경 문서가 아니며, 아래 원본 문서를 **순서와 내용을 보존한 채** 묶는다.

## 포함 원본 파일
- `02_stage2_vps_runtime.md`
- `03_stage3_telegram_collector.md`

## 통합 원칙
- 아키텍처 불변식은 유지한다.
- 원본 파일명은 아래 섹션 제목으로 보존한다.
- 충돌 시 해석 우선순위는 기존과 동일하게 정본 단계 문서 → 실행 계약 → migration 정본 → 구현 초안이다.


---

## Source file: `02_stage2_vps_runtime.md`

# 2단계: VPS와 런타임 구조 설계

## 단계 목적

2단계의 목적은 이 봇을 **항상 켜져 있고, 중복 없이, 장애가 나도 복구 가능한 런타임**으로 고정하는 것이다.

여기서 잠그는 것은 기능이 아니라 **운영 골격**이다.

- 어디서 돌릴 것인가
- 어떤 프로세스로 나눌 것인가
- 무엇을 영구 저장하고 무엇은 임시로 둘 것인가
- 장애가 나면 어디서부터 다시 시작할 것인가
- 나중에 부하가 늘어나도 무엇을 안 뜯어고치고 확장할 수 있는가

이 단계가 약하면 이후 단계에서 collector, analyzer, notifier를 구현할수록 기술 부채가 쌓인다.

---

## 이번 단계에서 고정할 핵심 결정

| 구분 | 결정 |
|---|---|
| 배포 형태 | **단일 VPS 1대**에서 시작 |
| 런타임 방식 | **Docker Compose 기반 다중 서비스** |
| 시스템 오브 레코드 | **PostgreSQL** |
| 작업 큐/락/버스트 흡수 | **Redis** |
| Telegram 상태 저장 | **TDLib 전용 영구 디렉터리** |
| 파일 캐시 | 로컬 디스크 기반 **blob cache** |
| 운영 모드 | **prod만 live ingest**, dev는 replay 중심 |
| 수집기 수 | **Telegram collector는 prod에서 1개만** |
| 알림 수신 | 초기에는 **outbound-only bot** |
| 외부 공개 포트 | 초기에는 **없음** |

---

## 왜 단일 VPS로 시작하는가

이 프로젝트의 핵심 수집기는 TDLib 기반 Telegram reader다. TDLib는 네트워킹, 로컬 저장, 데이터 일관성을 자체적으로 담당하는 비동기 클라이언트다. Telegram의 업데이트는 인증된 사용자의 **마지막 활성 연결**로 전달되므로, 같은 계정으로 여러 live collector를 동시에 띄우는 구조는 처음부터 피하는 편이 맞다. 즉, 이 시스템은 초반부터 서버리스/크론보다 **상시 실행 단일 런타임**이 훨씬 자연스럽다.

따라서 v1은 과하게 분산하지 말고 **한 대의 VPS에서 분리된 서비스들**로 운용하는 것이 가장 안전하다.

---

## VPS 목표 사양

초기 권장치는 아래 정도면 충분하다.

- 2 vCPU
- 4 GB RAM
- 40 GB SSD
- Ubuntu LTS
- 일일 백업 가능 스토리지

이 수치는 제품 규격이 아니라 **초기 운영 추정치**다. 저장 공간은 코드보다도 아래 항목 때문에 먼저 사용된다.

- TDLib 상태 디렉터리
- PostgreSQL 데이터
- 로그
- GitHub/X 원문 캐시
- 분석 결과 JSON

여기서 중요한 것은 CPU보다 **지속 디스크와 복구성**이다.

---

## 권장 런타임 철학

이 단계에서 시스템 원칙은 아래 6개로 잠근다.

1. **Always-on**  
   수집기는 상시 실행한다.

2. **Durable first**  
   원문과 분석 결과는 DB에 남기고, 큐는 재구성 가능해야 한다.

3. **Single writer for Telegram ingest**  
   Telegram live collector는 하나만 둔다.

4. **Event-driven, not request-chain**  
   `수집 → 정규화 → enrich → 분석 → 알림`을 동기 함수 체인으로 묶지 않는다.

5. **Idempotent workers**  
   같은 job이 두 번 들어와도 결과가 망가지지 않게 설계한다.

6. **Replaceable boundaries**  
   나중에 worker를 다른 서버로 빼더라도 DB/큐 계약은 유지한다.

---

## 참조 아키텍처

```text
                    ┌──────────────────────────────┐
                    │            VPS               │
                    │                              │
                    │  ┌────────────────────────┐  │
Telegram TDLib ───▶ │  │ collector-telegram     │  │
                    │  └──────────┬─────────────┘  │
                    │             ▼                │
                    │  ┌────────────────────────┐  │
                    │  │ router-normalizer      │  │
                    │  └──────┬────────┬────────┘  │
                    │         ▼        ▼           │
                    │  ┌──────────┐ ┌──────────┐   │
                    │  │ gh-fetch  │ │ x-fetch  │   │
                    │  └────┬─────┘ └────┬─────┘   │
                    │       ▼            ▼         │
                    │   ┌──────────────────────┐   │
                    │   │ evidence assembler   │   │
                    │   └─────────┬────────────┘   │
                    │             ▼                │
                    │   ┌──────────────────────┐   │
                    │   │ analyzer-openai      │   │
                    │   └─────────┬────────────┘   │
                    │             ▼                │
                    │   ┌──────────────────────┐   │
                    │   │ notifier-telegram    │───┼──▶ Telegram Bot API
                    │   └──────────────────────┘   │
                    │                              │
                    │  PostgreSQL  Redis  Blob    │
                    └──────────────────────────────┘
```

핵심은 **동기식 일자형 프로그램이 아니라, 상태 기반 파이프라인**으로 자르는 것이다.

---

## 서비스 분리 계약

### 1) `collector-telegram`
역할:
- TDLib 로그인 세션 유지
- 새 메시지/수정 메시지 수집
- raw source message 저장
- 중복 방지 키 생성

규칙:
- prod에서 단 1개만 실행
- 이 서비스는 OpenAI 키를 몰라야 함
- 실패 시 재기동 가능해야 함

### 2) `router-normalizer`
역할:
- 키워드/URL 추출
- canonical URL 생성
- candidate group 생성
- 이미 분석한 대상과 중복 여부 확인

규칙:
- 메시지 1개를 후보 0~N개로 분해 가능해야 함
- 원문을 수정하지 않음

### 3) `gh-fetch`
역할:
- GitHub repo metadata, README, tree, 핵심 파일 수집
- 캐시/조건부 요청 처리

규칙:
- GitHub 요청은 낮은 동시성
- 큐 기반 직렬화 우선
- 논리 식별자만 저장하고 임시 download URL은 저장하지 않음

### 4) `x-fetch`
역할:
- X post text / author / media metadata 수집
- thread 또는 quote context 일부 확보

규칙:
- post ID 기준 캐시
- bearer token 외 다른 비밀 금지

### 5) `evidence-assembler`
역할:
- raw 증거를 LLM 입력용 evidence pack으로 조립
- 토큰 예산 계산
- low-evidence 플래그 부여

규칙:
- 원문과 요약을 섞어 저장하지 않음
- evidence pack은 재현 가능해야 함

### 6) `analyzer-openai`
역할:
- `gpt-5.4-mini` 기본 판정
- 필요 시 `gpt-5.4` 승급
- 구조화 JSON 결과 생성

규칙:
- 낮은 동시성
- rate limit/backoff 내장
- verdict와 delivery 분리

### 7) `notifier-telegram`
역할:
- 분석 JSON을 텔레그램 메시지로 렌더링
- send_now / digest / suppress 처리

규칙:
- analyzer와 직접 결합 금지
- 알림 실패 시 재시도 큐로 이동

### 8) `maintenance`
역할:
- stale job 감시
- 캐시 정리
- 백업 트리거
- 헬스 리포트 생성

규칙:
- hot path에 끼어들지 않음

---

## 데이터 평면과 제어 평면을 분리

초기부터 아래 둘을 분리하는 것이 좋다.

### 데이터 평면
실제 분석 파이프라인.

- source messages
- artifacts
- candidate groups
- evidence packs
- analysis outputs
- notifications

### 제어 평면
운영과 튜닝용 메타 기능.

- 큐 일시정지
- 재처리
- 스로틀 조정
- 채널 mute/unmute
- 비용 상한 초과 시 승급 중단

이걸 초기에 분리해두면 나중에 `/pause`, `/force analyze` 같은 명령을 붙여도 hot path를 건드리지 않는다.

---

## 저장소 역할 분리

### PostgreSQL = 시스템 오브 레코드
반드시 들어가야 하는 것:
- Telegram raw message
- 정규화 결과
- artifact canonical record
- candidate group
- analysis JSON
- notification log
- processing status
- retry history

원칙:
- 진실의 원천은 PostgreSQL 하나
- 재처리는 DB 상태만으로 재구성 가능해야 함

### Redis = 임시 실행 계층
사용 용도:
- job queue
- dedupe lock
- short-lived cache
- rate limiter counters

원칙:
- Redis가 날아가도 데이터 손실로 간주하지 않음
- Redis만 믿고 원문을 보관하지 않음

### TDLib state dir = 특수 영속 상태
역할:
- Telegram 세션
- 로컬 상태
- 재로그인 비용 절감

원칙:
- 일반 로그 디렉터리와 분리
- 백업 대상에 포함
- 권한 최소화

### Blob cache = 큰 원문 임시 저장
역할:
- GitHub raw files
- 일시적 archive 추출물
- X/media metadata snapshot
- evidence pack snapshot

원칙:
- TTL 기반 정리
- DB에는 경로와 해시만 기록
- 영구 보존이 필요한 것은 DB 또는 별도 보관소로 승격

---

## 큐 설계는 처음부터 분리한다

v1에서도 큐를 최소 6개로 자르는 편이 낫다.

- `q_ingest_normalize`
- `q_enrich_github`
- `q_enrich_x`
- `q_assemble_evidence`
- `q_analyze`
- `q_notify`
- `q_replay` (선택)

왜 이렇게 자르냐면, 부하와 실패 원인이 각기 다르기 때문이다.

- Telegram 수집 지연
- GitHub API rate limit
- X API 실패
- OpenAI 429/5xx
- Telegram Bot API 전송 실패

이걸 하나의 큐로 섞으면 장애 원인과 지연 원인을 분리하기 어렵다.

---

## 동시성 정책을 지금 잠근다

외부 서비스 특성을 감안하면, 이 봇은 처음부터 높은 병렬성이 필요하지 않다.

### 권장 초기 동시성
- `collector-telegram`: **1**
- `router-normalizer`: **1~2**
- `gh-fetch`: **1**
- `x-fetch`: **1**
- `evidence-assembler`: **1~2**
- `analyzer-openai`: **1**
- `notifier-telegram`: **1**

이 보수적 설정이 좋은 이유:
- GitHub는 동시성 폭주보다 직렬 큐를 권장한다.
- OpenAI는 rate limit이 짧은 시간 구간으로 양자화되어 적용될 수 있어 burst에 취약하다.
- Telegram collector는 구조상 단일 인스턴스가 맞다.

이 시스템의 병목은 보통 CPU가 아니라 **외부 API, rate limit, 토큰 비용, 중복 처리**다.

---

## GitHub fetcher 런타임 규칙

GitHub 쪽은 런타임에서 특히 아래를 잠가야 한다.

1. **직렬 또는 저동시성 요청**
2. **ETag / Last-Modified 기반 조건부 GET**
3. **download_url 장기 저장 금지**
4. **directory 1000개 초과 가능성 고려**
5. **큰 repo는 tree 기반 샘플링**

즉, fetcher는 처음부터 “repo 전체를 긁는 프로그램”이 아니라 **증거 수집기**로 설계해야 한다.

---

## OpenAI analyzer 런타임 규칙

OpenAI는 프로젝트별 rate limit, 모델 허용 범위, 예산 알림을 분리할 수 있지만, budget은 hard cap이 아니다. 또 rate limit은 짧은 구간으로 양자화될 수 있어 짧은 burst로도 429가 날 수 있다. 따라서 analyzer는 반드시 아래 규칙을 가져야 한다.

- low concurrency
- exponential backoff
- retry ceiling
- 일일 hard cap
- `mini → full` 승급 비율 상한
- 초과 시 `later` 큐로 강등

즉, OpenAI 운영 안정성은 “더 큰 인스턴스”가 아니라 **더 좋은 큐 정책**으로 해결한다.

---

## Telegram notifier 운영 방식

초기 notifier는 **outbound-only**로 두는 것이 가장 단순하다. 나중에 제어 명령을 붙이려면 Bot API의 `getUpdates` 또는 `setWebhook`를 선택하면 되는데, 둘은 상호배타적이다. webhook을 쓰면 공개 HTTPS 엔드포인트와 지원 포트 구성이 필요하고, `getUpdates`는 서버에 최대 24시간 pending updates가 유지된다.

따라서 v1은 다음처럼 잠그는 편이 좋다.

- 지금: outbound-only
- 다음: 관리 명령이 필요하면 `getUpdates`
- 더 나중: 외부 제어가 복잡해지면 webhook + reverse proxy

이 순서가 운영 복잡도를 가장 낮춘다.

---

## 프로세스 간 계약 원칙

### 1. 모든 작업은 job id를 가진다
예:
- `job_ingest_...`
- `job_enrich_gh_...`
- `job_analyze_...`

### 2. 모든 단계는 상태 전이를 기록한다
예:
- `pending`
- `running`
- `succeeded`
- `failed_retryable`
- `failed_terminal`
- `suppressed`

### 3. 모든 worker는 idempotent 해야 한다
동일 job 재실행 시:
- 중복 알림 금지
- 동일 analysis 덮어쓰기 규칙 명확
- side effect는 한 번만

### 4. 외부 API 응답 원문 일부를 남긴다
완전 전체 raw dump는 과하지만, 디버깅 재현을 위해 핵심 응답 snapshot은 남기는 편이 좋다.

---

## 폴더/볼륨 구조 권장안

```text
/srv/catchbot/
  compose/
  app/
  configs/

/var/lib/catchbot/
  postgres/
  redis/
  tdlib/
  blob-cache/
  backups/

/var/log/catchbot/
  app/
  maintenance/

/run/secrets/
  openai_api_key
  telegram_bot_token
  github_app_private_key
  x_bearer_token
```

원칙:
- 코드와 데이터 분리
- 세션과 로그 분리
- 비밀과 설정 분리
- 백업 경로를 별도 디렉터리로 고정

---

## Docker Compose를 쓰는 이유

이 단계에서는 Kubernetes가 아니라 Docker Compose가 맞다.

이유:
- 서비스 경계는 분리되지만 운영 복잡도는 낮음
- 볼륨, 재시작 정책, 의존성 순서 관리가 쉬움
- 나중에 서비스 단위로 다른 호스트에 옮기기 쉬움

권장 원칙:
- 서비스별 컨테이너 분리
- `restart: always` 또는 이에 준하는 정책
- healthcheck 필수
- stdout/stderr JSON 로그
- 호스트 네트워킹 지양
- 컨테이너는 non-root 권장

---

## 네트워크 설계

### 초기 상태
- 공개 인바운드 포트: 없음
- 아웃바운드만 허용
  - Telegram
  - OpenAI
  - GitHub
  - X

이 구조가 좋은 이유는 초기 공격면이 매우 작기 때문이다.

### 추후 webhook 도입 시
그때만 아래를 추가한다.
- reverse proxy (Caddy 또는 Nginx)
- HTTPS
- webhook secret token 검증
- 내부 notifier/admin endpoint

즉, **지금부터 공개 웹서버를 열 필요가 없다.**

---

## 로그와 메트릭 설계

초기부터 구조화 로그를 강제하는 편이 낫다.

필수 로그 필드:
- `ts`
- `service`
- `env`
- `job_id`
- `candidate_group_id`
- `source_message_id`
- `artifact_id`
- `event`
- `status`
- `duration_ms`
- `error_code`

필수 메트릭:
- ingest lag
- queue depth by queue
- enrich success rate
- analysis latency
- OpenAI 429 count
- GitHub 403/429 count
- duplicate suppression count
- notify success/failure count

운영상 중요한 것은 “에러가 났다”가 아니라 **어느 큐에서, 어느 외부 API 때문에, 얼마나 쌓였는가**다.

---

## 백업과 복구 원칙

### 백업해야 하는 것
- PostgreSQL
- TDLib state dir
- configs
- secrets source 자체는 별도 안전 저장소에서 관리

### 굳이 백업하지 않아도 되는 것
- Redis 전체 덤프
- 만료 가능한 blob cache
- 재생성 가능한 임시 파일

### 복구 목표
- 서버 1대가 날아가도
- PostgreSQL 복원
- TDLib 세션 복원
- compose up
- 미완료 job 재큐잉

이 흐름으로 다시 살아나야 한다.

---

## 장애 시나리오별 설계 포인트

### 시나리오 1: Telegram collector 다운
대응:
- system restart
- TDLib state dir 유지
- source message 중복 삽입 방지

### 시나리오 2: GitHub rate limit
대응:
- 큐 적체 허용
- fetch 동시성 유지 또는 더 낮춤
- 조건부 GET 적극 사용

### 시나리오 3: OpenAI 429/5xx
대응:
- exponential backoff
- 재시도 상한
- low-priority queue 강등
- 승급 중단

### 시나리오 4: Telegram Bot API 전송 실패
대응:
- notify 재시도 큐
- 동일 메시지 재전송 dedupe key

### 시나리오 5: Redis 재시작
대응:
- DB 기준으로 미완료 job 재구성
- Redis를 신뢰 저장소로 쓰지 않음

---

## dev / prod 분리 원칙

### prod
- live Telegram ingest 허용
- 실제 알림 전송
- 실제 OpenAI prod key 사용

### dev
- live ingest 금지
- fixture replay 또는 저장된 raw message 재처리
- dev bot 또는 별도 chat_id 사용
- 비용 한도 훨씬 낮게 설정

이 원칙을 지금 안 잠그면 나중에 dev가 실운영 채널을 건드리는 사고가 난다.

---

## 이후 확장 경로까지 고려한 설계

v1은 단일 VPS지만, 확장 경로는 처음부터 열어두는 게 좋다.

### 1차 확장
- analyzer만 별도 호스트로 분리
- Postgres/Redis는 유지

### 2차 확장
- fetchers를 별도 호스트로 분리
- notifier는 본체에 유지

### 3차 확장
- managed DB/queue 도입 검토
- blob cache를 외부 object storage로 이전

중요한 점은, 이런 확장이 **서비스 경계는 유지한 채 배치만 바꾸는 수준**이어야 한다는 것이다.

---

## 지금 반드시 피해야 할 설계

1. **모든 단계를 한 프로세스에서 동기 실행**
2. **Redis만 시스템 오브 레코드로 사용**
3. **Telegram collector 두 개 이상 동시 실행**
4. **dev 환경을 live 채널에 직접 연결**
5. **분석 결과 생성과 알림 전송을 하나의 함수로 결합**
6. **download_url, 임시 redirect URL을 DB에 영구 저장**
7. **전체 repo clone을 기본 전략으로 채택**

이 7개는 나중에 거의 반드시 리팩토링 비용으로 돌아온다.

---

## 2단계 완료 조건

아래가 확정되면 2단계는 끝이다.

- 단일 VPS 전략 확정
- 서비스 분리 목록 확정
- DB/Redis/TDLib/blob 역할 확정
- 큐 종류와 기본 동시성 확정
- dev/prod 경계 확정
- 로그/메트릭 최소 항목 확정
- 백업/복구 원칙 확정
- 공개 인바운드 포트 없음 원칙 확정
- 미래 확장 경로 문서화

---

## 이번 단계의 권장 최종안

**초기 운영 골격은 아래 한 줄로 요약된다.**

`단일 VPS + Docker Compose + PostgreSQL(진실의 원천) + Redis(큐/락) + TDLib 영구 상태 + 단일 Telegram collector + 저동시성 fetch/analyze workers`

이 구조면 v1은 충분히 안정적으로 굴릴 수 있고, 나중에 부하가 늘어도 전체를 뜯지 않고 worker만 분리해 확장할 수 있다.


---

## Source file: `03_stage3_telegram_collector.md`

# 3단계: Telegram 수집기 설계

## 이번 단계에서 잠그는 결론

| 구분 | 고정 결정 |
|---|---|
| 수집 엔진 | **TDLib 기반 단일 live collector 1개** |
| 수집 범위 | **등록된 채널의 모든 새 글/수정/삭제 이벤트를 저장** |
| collector 책임 | **수집·버전화·삭제표시·재조정(reconcile)까지만 수행** |
| collector 비책임 | **키워드 판정, GitHub/X 분석, LLM 호출 금지** |
| 출력 경계 | **PostgreSQL에 원문/버전 저장 + outbox event 발행** |
| 메시지 단위 | **Telegram 원문 1개 = SourceMessage 1개** |
| 논리 포스트 단위 | **album/media 묶음은 logical_post_key로 후단에서 병합 가능** |
| 운영 규칙 | **prod만 live ingest, dev는 replay만** |

핵심은 단순하다. 이 단계의 collector는 **판단기**가 아니라 **증거 보존기**다.  
0단계에서 고정한 `SourceMessage -> Artifact -> CandidateGroup -> Analysis` 구조를 깨지 않기 위해, collector는 Telegram 원문을 가능한 손실 없이 보존하고 후단 파이프라인에 넘기는 데 집중한다.

---

## 1. 왜 collector를 이렇게 좁게 정의해야 하는가

이전 단계에서 이미 아래 원칙이 잠겨 있다.

- **precision-first**
- **negative-first summary**
- **artifact-centric**
- **single live collector**
- **PostgreSQL = system of record**

따라서 Telegram collector가 키워드 판정이나 “이거 중요해 보인다” 같은 의미 해석까지 맡기 시작하면, 이후 4단계 트리거/정규화와 7단계 LLM 판정기의 경계가 무너진다. 그 순간부터 수정 비용이 커진다.

이번 단계의 collector는 아래 세 가지에만 책임을 진다.

1. **채널 글을 빠짐없이 받는다**
2. **원문과 수정 이력을 보존한다**
3. **후단이 안정적으로 재처리할 수 있게 이벤트를 내보낸다**

---

## 2. collector의 기술적 기반

TDLib는 Telegram용 완전한 클라이언트 라이브러리이고, 네트워킹·로컬 저장·데이터 일관성 처리를 자체적으로 담당한다. Telegram 공식 문서도 TDLib 사용에서 **incoming updates의 올바른 처리**가 중요하다고 설명한다. 또한 authorized user의 업데이트는 **마지막 활성 연결(last active connection)** 으로 전달된다. 그래서 이 프로젝트는 stage 2에서 정한 대로 **단일 live collector 1개** 구조를 유지해야 한다. ([core.telegram.org](https://core.telegram.org/tdlib/getting-started), [core.telegram.org](https://core.telegram.org/api/updates))

즉, collector 설계의 첫 원칙은 다음이다.

> **같은 reader 계정으로 두 개 이상의 실시간 collector를 띄우지 않는다.**

이 원칙을 깨면 메시지 누락보다 더 나쁜 상태, 즉 **수신 위치가 흔들리는 불안정한 수집기**가 된다.

---

## 3. collector의 책임 범위

### collector가 반드시 하는 일

- 채널 레지스트리 기반으로 추적 대상 chat 식별
- 새 메시지 수집
- 수정 이벤트 반영
- 삭제 이벤트 반영
- 재시작/장애 후 최근 히스토리 재조정
- immutable version 저장
- outbox event 생성

### collector가 하면 안 되는 일

- 키워드 조건식으로 저장 여부를 결정
- GitHub/X 링크를 타고 들어가 분석
- LLM 호출
- 최종 알림 포맷 작성
- 사용자 메시지 중요도 판정

이 경계를 지키면 4단계의 trigger/normalizer가 자유롭게 바뀌어도 collector는 거의 안 흔들린다.

---

## 4. TDLib 초기화와 인증 상태 머신

TDLib는 `authorizationStateWaitTdlibParameters` 상태에서 초기화 파라미터를 받고, 이후 `authorizationStateWaitPhoneNumber`, `authorizationStateWaitCode`, `authorizationStateWaitPassword`, `authorizationStateReady` 같은 상태를 거친다. `setTdlibParameters`에는 persistent database 경로, files 경로, database encryption key, 그리고 `use_file_database`, `use_chat_info_database`, `use_message_database` 같은 캐시 유지 옵션이 있다. 공식 문서상 `use_message_database=true`는 채팅과 메시지 캐시를 재시작 후에도 유지하고, `use_chat_info_database=true`는 users/basic groups/supergroups/channels 캐시를 유지하며, 이는 `use_file_database`를 함의한다. ([core.telegram.org](https://core.telegram.org/tdlib/docs/classtd_1_1td__api_1_1set_tdlib_parameters.html), [core.telegram.org](https://core.telegram.org/tdlib/docs/classtd_1_1td__api_1_1_authorization_state.html))

### 초기화 고정값

- `use_test_dc = false`
- `use_file_database = true`
- `use_chat_info_database = true`
- `use_message_database = true`
- `use_secret_chats = false`
- `database_directory = /var/lib/catchbot/tdlib/db`
- `files_directory = /var/lib/catchbot/tdlib/files`
- `database_encryption_key = 별도 secret`

### 인증 운영 정책

- 최초 로그인만 수동 수행
- 운영 중 code/password 재입력이 필요한 상태로 내려가면 collector를 **degraded**로 표시
- 자동으로 사람 계정의 인증 절차를 우회하려 하지 않음
- reader 계정은 **이 collector 외 다른 Telegram 클라이언트에서 사용 금지**

이렇게 해야 “로그인 한 번 꼬이면 업데이트가 다른 세션으로 빠지는” 구조적 문제를 줄일 수 있다.

---

## 5. 채널 레지스트리 설계

collector가 먼저 알아야 하는 것은 “무슨 글이 왔냐”가 아니라 **“어떤 채널을 추적 중이냐”** 다.

권장 테이블은 아래다.

## `telegram_channel_registry`

| 필드 | 설명 |
|---|---|
| `registry_id` | 내부 식별자 |
| `source_kind` | `public_username` / `invite_link` / `chat_id` |
| `source_value` | `@handle` 또는 invite link |
| `desired_state` | `active` / `paused` / `removed` |
| `access_state` | `unresolved` / `joined` / `join_requested` / `forbidden` / `not_found` / `left` |
| `chat_id` | 최종 anchor. username 변경 대비용 |
| `username_snapshot` | 최근 공개 username |
| `title_snapshot` | 최근 채널명 |
| `chat_type` | channel / supergroup 등 |
| `last_resolved_at` | 마지막 식별 시각 |
| `last_join_attempt_at` | 최근 가입 시도 |
| `last_history_sync_at` | 최근 backfill 시각 |
| `last_seen_message_id` | collector 기준 최종 반영 message_id |
| `last_seen_message_date` | collector 기준 최종 반영 시각 |
| `priority_weight` | 후단 정책용 |
| `notes` | 운영 메모 |

### 왜 `chat_id`를 anchor로 잡는가

공개 username은 바뀔 수 있다. 따라서 레지스트리의 진짜 anchor는 `chat_id`여야 하고, `username_snapshot`은 참고값이어야 한다. 이 원칙을 안 잠그면 나중에 채널 이름이 바뀔 때 데이터 연결이 끊긴다.

---

## 6. 채널 온보딩 플로우

### public channel

TDLib의 `searchPublicChat`은 username으로 public chat을 찾는다. 문서상 현재 public일 수 있는 대상은 private chat, supergroup, channel이다. 이후 `joinChat`은 현재 사용자를 해당 chat의 멤버로 추가하며, 경우에 따라 `INVITE_REQUEST_SENT` 에러를 돌려줄 수 있다. ([core.telegram.org](https://core.telegram.org/tdlib/docs/classtd_1_1td__api_1_1search_public_chat.html), [core.telegram.org](https://core.telegram.org/tdlib/docs/classtd_1_1td__api_1_1join_chat.html))

### invite link

`joinChatByInviteLink`는 invite link로 현재 사용자를 chat에 추가하며, 여기서도 `INVITE_REQUEST_SENT`만 남고 실제 가입이 보류될 수 있다. ([core.telegram.org](https://core.telegram.org/tdlib/docs/classtd_1_1td__api_1_1join_chat_by_invite_link.html))

### 온보딩 상태 머신

- `unresolved`
- `resolved_not_joined`
- `join_attempted`
- `join_requested`
- `joined_active`
- `paused`
- `access_lost`
- `removed`

### 운영 규칙

- **등록 시 public username을 받더라도 최종 저장 anchor는 chat_id**
- `join_requested`는 실패가 아니라 **대기 상태**로 취급
- 승인 전 채널은 live tracking 대상에서 제외
- `access_lost`가 되면 collector는 계속 재시도하지 않고 운영자 확인 플래그를 세움

---

## 7. 수집 단위와 출력 단위

0단계에서 정의한 구조를 collector 쪽으로 내리면 아래와 같다.

### 수집 단위

- **Telegram 원문 1개 = SourceMessage 1개**

### 후단 병합 가능 단위

- **logical_post_key**
  - 기본값: `tg:{chat_id}:{message_id}`
  - album이면: `tg:{chat_id}:album:{media_album_id}`

TDLib `message` 객체에는 `media_album_id`가 있고, 0이 아니면 그 메시지가 album에 속한다. 공식 문서상 audios/documents/photos/videos만 album으로 묶일 수 있다. 또한 `message`는 `is_channel_post`, `author_signature`, `forward_info`, `content` 같은 중요한 필드를 가진다. ([core.telegram.org](https://core.telegram.org/tdlib/docs/classtd_1_1td__api_1_1message.html))

이 설계를 지금 잠그는 이유는 명확하다.

> **SourceMessage는 Telegram 원문과 1:1로 유지하되, album 같은 “하나의 논리 포스트”는 후단에서 병합할 수 있게 logical key를 따로 둔다.**

이렇게 해야 message-level 보존성과 post-level 해석을 동시에 확보할 수 있다.

---

## 8. collector가 저장해야 할 핵심 테이블

## `telegram_raw_updates`

옵션이 아니라 사실상 필요하다.

| 필드 | 설명 |
|---|---|
| `update_seq` | 내부 증가 시퀀스 |
| `received_at` | collector 수신 시각 |
| `update_type` | TDLib update 타입 |
| `chat_id` | 가능하면 파싱해서 저장 |
| `message_id` | 가능하면 파싱해서 저장 |
| `payload_json` | 원본 JSON |
| `applied_at` | DB 반영 시각 |
| `apply_status` | `pending/applied/failed` |
| `error_text` | 실패 이유 |

이 테이블은 영구 보관까지는 필요 없지만, **재현·디버깅·리플레이**를 위해 최소 보관 기간이 필요하다.

권장 보관 기간:
- prod: 14~30일
- dev/replay: 필요 시 더 길게

## `source_messages`

현재 상태를 나타내는 canonical row다.

| 필드 | 설명 |
|---|---|
| `platform` | `telegram` 고정 |
| `chat_id` | 채팅 식별자 |
| `message_id` | 메시지 식별자 |
| `logical_post_key` | album 병합 대비 |
| `is_channel_post` | 채널 포스트 여부 |
| `posted_at` | 최초 발행 시각 |
| `edited_at` | 마지막 수정 시각 |
| `deleted_at` | 삭제 시각 |
| `delete_kind` | `none/permanent/cache_only` |
| `current_version_no` | 최신 버전 번호 |
| `message_link` | 가능하면 생성한 t.me 링크 |
| `author_signature` | 채널 서명 |
| `forward_info_json` | 원문 출처 정보 |
| `content_type` | text/photo/video/document/... |
| `text_body` | 순수 텍스트 본문 |
| `caption_text` | 캡션 |
| `text_surface` | 후단 검색용 결합 텍스트 |
| `entities_json` | text entities 원본 |
| `url_surface_json` | 추출 URL 목록 |
| `raw_message_json` | 최신 message 객체 원본 |
| `first_seen_at` | collector 최초 관측 시각 |
| `last_seen_at` | collector 마지막 관측 시각 |

## `source_message_versions`

수정 이력은 immutable로 쌓는다.

| 필드 | 설명 |
|---|---|
| `chat_id`, `message_id` | 원문 식별 |
| `version_no` | 1부터 증가 |
| `version_reason` | `new/edit/content_change/reconcile/delete_marker` |
| `observed_at` | collector가 본 시각 |
| `telegram_edit_date` | TDLib edit_date |
| `text_surface` | 그 시점의 텍스트 |
| `entities_json` | 그 시점 엔티티 |
| `raw_message_json` | 그 시점 스냅샷 |
| `content_hash` | 변화 비교용 |

## `event_outbox`

collector와 후단을 튼튼하게 분리하려면 **transactional outbox**가 필요하다.

| 필드 | 설명 |
|---|---|
| `event_id` | UUID |
| `event_type` | `source_message.created/edited/deleted/reconciled` |
| `aggregate_key` | `tg:{chat_id}:{message_id}` |
| `payload_json` | 후단용 최소 payload |
| `published_at` | Redis publish 시각 |
| `status` | `pending/published/failed` |

이걸 지금 넣는 이유는 중요하다. DB 저장 성공 후 Redis enqueue 실패가 나도 **유실 없이 재발행**할 수 있기 때문이다.

---

## 9. update 처리 매트릭스

TDLib는 update를 통해 데이터 변경을 알려준다. 공식 문서상 `updateNewMessage`는 새 메시지 수신을 뜻하고, `updateMessageEdited`는 편집 발생을 뜻하며 **실제 내용 변화는 별도의 `updateMessageContent`** 로 온다. `updateDeleteMessages`는 삭제된 메시지 목록과 함께 `is_permanent`, `from_cache` 상태를 제공한다. ([core.telegram.org](https://core.telegram.org/tdlib/docs/classtd_1_1td__api_1_1update_new_message.html), [core.telegram.org](https://core.telegram.org/tdlib/docs/classtd_1_1td__api_1_1update_message_edited.html), [core.telegram.org](https://core.telegram.org/tdlib/docs/classtd_1_1td__api_1_1update_message_content.html), [core.telegram.org](https://core.telegram.org/tdlib/docs/classtd_1_1td__api_1_1update_delete_messages.html))

### 최소 처리 대상 update

| update | 해야 하는 일 |
|---|---|
| `updateNewMessage` | 신규 SourceMessage 생성 |
| `updateMessageEdited` | edit metadata 반영, pending content sync 표시 |
| `updateMessageContent` | 새 버전 생성 |
| `updateDeleteMessages` | soft delete marker 기록 |
| `updateChatLastMessage` | gap 가능성 감지, reconcile 스케줄 |

### 왜 `updateMessageEdited`와 `updateMessageContent`를 분리 저장해야 하나

공식 문서가 아예 그렇게 분리해서 온다고 못 박고 있다. 따라서 collector는 다음처럼 처리해야 한다.

1. `updateMessageEdited` 도착
   - `edited_at` 반영
   - message를 `pending_edit_sync=true`로 잠깐 표시
2. `updateMessageContent` 도착
   - 실제 내용 버전 생성
   - `pending_edit_sync=false`

이 순서를 무시하고 “수정 이벤트가 왔으니 본문도 같이 왔겠지”로 가정하면, 수정 이력 누락이나 잘못된 overwrite가 발생한다.

---

## 10. `updateChatLastMessage` 때문에 reconcile이 필수다

이 단계에서 가장 중요한 방어 설계는 이것이다.

TDLib의 `updateChatLastMessage` 문서에는 **last message가 unknown인 동안에는 `updateNewMessage` 없이도 새 메시지가 chat에 추가될 수 있다**고 적혀 있다. 즉, live updates만 믿고 “new message update만 오면 충분하다”고 설계하면 누락 가능성이 남는다. ([core.telegram.org](https://core.telegram.org/tdlib/docs/classtd_1_1td__api_1_1update_chat_last_message.html))

그래서 collector는 **반드시 reconcile 루프**를 가져야 한다.

### reconcile 규칙 v1

- startup 직후 active 채널마다 최근 `N=30~50`개 backfill
- 주기적 reconcile: 5~15분 간격
- collector 재시작 후 즉시 최근 `N`개 재조회
- `updateChatLastMessage.last_message = null` 감지 시 해당 chat 우선 reconcile
- 채널별 `last_seen_message_id/date`와 대조해 미반영 메시지 보정

---

## 11. backfill 전략

TDLib의 `getChatHistory`는 메시지를 **message_id 내림차순**으로 반환하고, `from_message_id=0`이면 마지막 메시지부터 가져오며, `limit`은 100 이하이고 `only_local=true`일 때는 네트워크 없이 로컬에 있는 메시지만 반환한다. ([core.telegram.org](https://core.telegram.org/tdlib/docs/classtd_1_1td__api_1_1get_chat_history.html))

이를 collector에서는 두 단계로 사용한다.

### A. warm backfill

목적: 재시작 직후 빠른 상태 복원

- `only_local = true`
- `from_message_id = 0`
- `limit = 30`

장점:
- 네트워크 의존도 낮음
- TDLib 캐시 활용

### B. authoritative reconcile

목적: 실제 누락 보정

- `only_local = false`
- `from_message_id = 0`
- `limit = 30~100`
- 저장된 `last_seen_message_id` 이후 미반영분 보강

### 중요한 원칙

- startup backfill과 live update를 **중복 허용 + idempotent upsert** 구조로 처리
- 동일 메시지가 live로도 오고 history로도 오면 version 중복 생성 금지
- message_id gap만으로 손실이라 단정하지 않음

---

## 12. 메시지 내용 추출 규칙

collector는 “해석”이 아니라 “보존”을 해야 한다. 따라서 내용 추출도 **손실 최소화**가 기준이다.

### 저장 원칙

- 원본 `raw_message_json` 저장
- 사람이 보기 쉬운 `text_surface`도 별도 저장
- 엔티티/URL은 원본 구조와 파생 결과 둘 다 저장

### 권장 추출 필드

- `text_body`
- `caption_text`
- `text_surface = body + caption`
- `entities_json`
- `url_surface_json`
- `content_type`
- `author_signature`
- `forward_info_json`
- `media_album_id`

Telegram은 styled text를 message entities로 표현한다. 따라서 URL 추출은 정규식보다 **entities 기반 우선**, 정규식 기반 후순위가 맞다. ([core.telegram.org](https://core.telegram.org/api/entities))

### URL 추출 우선순위

1. entity 기반 URL
2. explicit text URL
3. caption 내 URL
4. regex fallback

정규식을 1순위로 두면, Telegram 특유의 엔티티 정보 손실이 발생한다.

---

## 13. message link와 logical key

TDLib의 `getMessageLink`는 조건이 맞으면 메시지에 대한 HTTPS 링크를 오프라인으로 생성할 수 있고, `for_album=true`를 주면 whole album 링크를 만들 수 있다. `messageLink`는 supergroup/channel/topic 메시지의 HTTPS 링크를 나타낸다. ([core.telegram.org](https://core.telegram.org/tdlib/docs/classtd_1_1td__api_1_1get_message_link.html), [core.telegram.org](https://core.telegram.org/tdlib/docs/classtd_1_1td__api_1_1message_link.html))

### collector 권장 규칙

- 가능하면 `message_link`를 저장
- album이면 `for_album=true` 링크도 저장 시도
- 링크 생성 실패는 오류가 아니라 `null` 상태로 허용

이걸 지금 해두면 후단 알림에서 Telegram 원문 링크를 쉽게 붙일 수 있다.

---

## 14. 삭제 처리 원칙

`updateDeleteMessages`에는 `is_permanent`와 `from_cache`가 함께 온다. 즉, 삭제는 하나가 아니다. **영구 삭제**와 **캐시에서만 사라짐**을 구분해야 한다. ([core.telegram.org](https://core.telegram.org/tdlib/docs/classtd_1_1td__api_1_1update_delete_messages.html))

### 고정 규칙

- source row를 hard delete 하지 않는다
- `delete_kind = permanent` 또는 `cache_only`로 표시
- 삭제 이벤트도 version/event로 남긴다
- 후단 재처리에서 삭제된 메시지를 어떻게 볼지는 별도 정책으로 남긴다

이 원칙을 안 지키면 “잠깐 접근 불가였던 메시지”와 “정말 삭제된 메시지”가 뒤섞인다.

---

## 15. collector의 필터링 경계

이 단계에서 매우 중요한 결정 하나를 고정한다.

> **collector는 등록된 채널의 모든 메시지를 저장한다. 키워드에 맞는 것만 저장하지 않는다.**

이유는 세 가지다.

1. 20~30개 채널 규모에서는 전체 저장 비용이 감당 가능하다.
2. 4단계 trigger 규칙이 바뀌어도 과거 메시지를 다시 평가할 수 있다.
3. false negative를 뒤늦게 줄이려 할 때 collector를 다시 만들 필요가 없다.

즉, keyword match는 **저장 조건이 아니라 후단 라우팅 조건**이어야 한다.

---

## 16. idempotency와 순서 보장

collector는 절대 “한 번만 온다”는 가정을 하면 안 된다.

### 고정 규칙

- `(chat_id, message_id)` unique key
- `(chat_id, message_id, version_no)` unique key
- `content_hash` 같으면 불필요한 새 version 생성 금지
- outbox event는 `aggregate_key + semantic_event` 수준으로 dedupe

### 왜 필요한가

- live update와 history reconcile이 같은 메시지를 중복 제공할 수 있음
- edit 이벤트와 content 이벤트가 다른 시점에 올 수 있음
- restart 후 최근 메시지를 다시 읽어도 시스템이 안정적으로 noop 처리해야 함

---

## 17. collector health와 장애 탐지

collector에서 봐야 할 지표는 일반적인 CPU/RAM보다 더 구체적이어야 한다.

### 필수 지표

- `tdlib_authorization_state`
- `updates_received_total{type}`
- `source_messages_created_total`
- `source_messages_edited_total`
- `source_messages_deleted_total`
- `reconcile_runs_total`
- `reconcile_gap_fills_total`
- `outbox_pending_count`
- `tracked_channels_active`
- `last_update_received_at`
- `last_successful_history_sync_at{chat}`

### 알람 조건

- 10분 이상 update 수신 0 + 채널 전체 silence 아님
- authorization state가 `ready` 아님
- outbox pending backlog 급증
- 특정 채널 `last_successful_history_sync_at` 지연
- reader account가 다른 곳에서 활성화되어 update 흐름이 끊긴 정황

마지막 항목은 특히 중요하다. 업데이트가 last active connection으로 가기 때문이다.

---

## 18. collector와 후단의 접점

collector는 DB에 저장하고 outbox를 남기면 역할이 끝난다. 후단 `router-normalizer`가 다음을 맡는다.

- 키워드/URL 탐지
- Artifact 생성
- CandidateGroup 구성
- GitHub/X enrich job 발행

### collector가 후단에 넘기는 최소 payload

```json
{
  "event_type": "source_message.created",
  "platform": "telegram",
  "chat_id": 123,
  "message_id": 456,
  "current_version_no": 1,
  "logical_post_key": "tg:123:456",
  "posted_at": "2026-04-12T10:20:00Z",
  "edited_at": null,
  "deleted_at": null
}
```

핵심은 payload를 얇게 두고, 후단이 필요하면 PostgreSQL에서 canonical row를 다시 읽게 하는 것이다.

---

## 19. 지금 넣지 않는 기능

초기 collector에 바로 넣지 않는 것이 좋은 것들도 있다.

- discussion thread/comment 수집
- OCR 기반 이미지 텍스트 수집
- story 수집
- reaction 수집
- view count 변화 추적
- 모든 미디어 파일 다운로드
- linked supergroup까지 자동 확장 수집

이걸 안 넣는 이유는 단순하다. collector의 목적은 **채널 글 감시**이지, Telegram 전체 리서치 클라이언트가 아니기 때문이다.

---

## 20. 이번 단계에서 반드시 피해야 할 설계

1. **keyword 맞는 메시지만 DB에 저장**
2. **edit를 in-place overwrite만 하고 version 미보존**
3. **delete를 hard delete 처리**
4. **live update만 믿고 reconcile 생략**
5. **album을 message 1개로 뭉개 저장**
6. **동일 reader 계정으로 live collector 2개 실행**
7. **Redis enqueue 성공을 저장 성공의 전제로 둠**

이 일곱 개는 거의 확실하게 나중에 리팩토링 비용으로 돌아온다.

---

## 21. 3단계 완료 기준

아래가 확정되면 3단계는 끝이다.

- `telegram_channel_registry` 스키마 확정
- `telegram_raw_updates` / `source_messages` / `source_message_versions` / `event_outbox` 스키마 확정
- TDLib 초기화 파라미터 확정
- 인증 운영 절차 확정
- 온보딩 상태 머신 확정
- update 처리 매트릭스 확정
- reconcile 정책 확정
- idempotency 규칙 확정
- collector와 후단 경계 확정

---

## 이번 단계의 한 줄 잠금

**`TDLib 단일 live collector가 등록 채널의 모든 원문을 버전/삭제 이력 포함하여 PostgreSQL에 보존하고, transactional outbox로 후단에 넘기며, live updates만 믿지 않고 getChatHistory 기반 reconcile을 주기적으로 수행한다.`**
