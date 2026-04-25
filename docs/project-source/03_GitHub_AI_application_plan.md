# GitHub AI 적용 설계

작성 기준: 2026-04-14 스냅샷  
기준 소스:

- 업로드된 GitHub AI 프로젝트 소스 문서 34개
  - 단계 설계 정본
  - 실행 계약 / migration 정본
  - collector / outbox-relay / router-normalizer / gh-enricher / x-enricher 구현 초안
- 외부 저장소
  - `NousResearch/hermes-agent`
  - `subinium/CrowClaw`
  - `subinium/awesome-agent-frameworks`
  - `seojoonkim/memkraft`
  - `seojoonkim/prompt-guard`
  - `seojoonkim/agentlinter`
  - `seojoonkim/txxt`

---

## 1. 문서 목적

이 문서는 현재 GitHub AI 프로젝트에 외부 자산을 어디까지 적용할 수 있는지, 그리고 어디부터는 오히려 구조를 망가뜨리는지 정리한 설계서다.

중요한 전제는 하나다.

**GitHub AI는 더 이상 빈 도화지가 아니다.**

이미 다음이 잠겨 있다.

- 제품 계약
- 실행 계약
- migration 설계
- collector-telegram 내부 구현 초안
- outbox-relay 초안
- router-normalizer 초안 + integration hardening
- gh-enricher 초안
- x-enricher 초안
- 다음 구현 순서: `web-enricher → evidence-assembler`

따라서 이 저장소에는 "새 agent framework를 가져와 전체를 갈아엎는 방식"이 아니라,
**현재 파이프라인 경계를 보존한 상태에서 필요한 기능만 삽입**하는 방식이 맞다.

---

## 2. 현재 GitHub AI를 어떻게 봐야 하는가

이 프로젝트는 일반적인 채팅 에이전트나 범용 코파일럿이 아니다.

### 제품 관점
- 20~30개 텔레그램 채널에서 개발 도구/아이디어 신호를 감지
- GitHub / X / text idea를 분석 후보화
- "지금 열어볼 가치가 있는가"를 precision-first로 선별
- 한국어 요약과 skeptical take를 전달

### 아키텍처 관점
- `SourceMessage -> Artifact -> CandidateGroup -> EvidenceBundle -> JudgeOutput -> Analysis -> Notification`
- collector / normalizer / enricher / judge / policy / notifier가 분리
- PostgreSQL이 durable truth
- Redis는 얇은 queue / lock / short-lived execution state
- verdict와 delivery가 분리
- LLM은 `judge_output_v1`까지만 생성하고, 최종 verdict/delivery는 deterministic policy가 확정

즉, 이 프로젝트는 **agent loop** 보다 **contract-driven pipeline** 에 가깝다.

---

## 3. 최종 결론

### 강하게 적용 권장
1. **Prompt Guard**
2. **AgentLinter**

### 제한적으로 적용 권장
1. **MemKraft** — runtime이 아니라 `design/eval/ops memory` sidecar
2. **Hermes/CrowClaw의 skill/playbook 패턴**
3. **CrowClaw checkpoint/replay 개념 차용**

### 참고 전용
1. **awesome-agent-frameworks**

### 적용 제외
1. **Hermes/CrowClaw 전체 runtime**
2. **auto-skill extraction / self-learning loop**
3. **MemKraft를 production pipeline memory로 직접 삽입**
4. **`TerminalClaw`, `txxt` 직접 재사용**

---

## 4. 적용 판단표

| 외부 자산 | 적용 위치 | 기대 효과 | 현실성 | 판정 |
|---|---|---:|---:|---|
| Prompt Guard | `web-enricher`, `x-enricher`, `judge-openai` 직전 | injection / secret / malicious text 차단 | 매우 높음 | 적용 |
| AgentLinter | `AGENTS.md`, `contracts/`, `prompts/`, `policies/` | instruction drift 감소 | 매우 높음 | 적용 |
| MemKraft | `ops-memory/`, eval 기록, postmortem | 운영 기억 축적 | 높음 | sidecar 적용 |
| Hermes skill 패턴 | judge profile / skill docs | prompt 관리성 향상 | 높음 | 부분 적용 |
| CrowClaw skill 패턴 | profile handbook / preset docs | 판단 규칙 문서화 | 높음 | 부분 적용 |
| CrowClaw checkpoint 개념 | replay tooling, state debug | 디버깅 용어/구조 정교화 | 중간 | 개념 차용 |
| Hermes runtime | 전체 agent loop | 현재 파이프라인과 중복 | 낮음 | 제외 |
| CrowClaw runtime | gateway/MCP/scheduler 전체 | 구조 중복 + TS 운영 비용 | 낮음 | 제외 |
| self-learning loop | prompt/skill 자동 진화 | precision-first 제품과 충돌 | 낮음 | 제외 |
| MemKraft runtime memory | live bundle retrieval | 파이프라인 책임 혼선 | 낮음 | 제외 |
| awesome-agent-frameworks | 실행 코드 | 참고 가치만 큼 | 낮음 | 참고 전용 |
| TerminalClaw / txxt | 직접 코드 재사용 | 실질 시너지 작음 | 낮음 | 제외 |

---

## 5. 왜 Prompt Guard는 맞고, Hermes/CrowClaw runtime은 안 맞는가

### Prompt Guard가 맞는 이유
GitHub AI는 외부 비신뢰 텍스트가 매우 많다.

- 텔레그램 원문
- GitHub README / docs / issue / release text
- X post
- 웹 article excerpt
- 앞으로 붙을 `web-enricher` 결과

이 텍스트들은 judge에 들어가기 전에 **공격 표면**이 된다.

그래서 guardrail은 아래 두 지점에 넣는 것이 가장 효율적이다.

1. **증거 수집 직후**  
   외부 텍스트를 snapshot / normalized_projection으로 저장하기 직전
2. **LLM 호출 직전**  
   EvidenceBundle에서 실제 model context를 만들기 직전

### Hermes/CrowClaw runtime이 안 맞는 이유
현재 프로젝트는 이미 다음이 있다.

- collector
- outbox relay
- deterministic normalizer
- source별 enricher
- evidence assembler
- judge / validator / policy / notifier
- replay / observability 설계

여기서 Hermes/CrowClaw 전체 runtime을 넣으면 생기는 문제는 명확하다.

- 기존 서비스 경계가 흐려진다.
- queue / retry / scheduler / gateway가 중복된다.
- thin Redis payload 철학과 충돌할 가능성이 크다.
- "agent가 알아서 하도록" 바뀌면서 precision-first 시스템이 흔들린다.

---

## 6. 권장 적용안 A — Prompt Guard

### A-1. 어디에 넣을 것인가

#### 1) `web-enricher`
웹 본문은 가장 큰 prompt injection 표면이다.

- HTML에서 긁어온 본문
- instruction-like 문장
- “ignore previous instructions” 류 패턴
- credential / cookie / token 유도 패턴

#### 2) `x-enricher`
X 글은 짧지만 공격 문구가 섞이기 쉽다.

- quoted text
- referenced posts
- thread 문맥 일부

#### 3) `judge-openai` 직전
최종 EvidenceBundle에서 실제 model context를 만들기 전에 한 번 더 검사해야 한다.

### A-2. 넣지 말아야 할 곳
- collector raw update journal
- deterministic normalizer core
- event_outbox routing

이유:
- 원문 보존 경계를 건드리면 안 된다.
- 판단 이전의 deterministic 단계는 최대한 순수하게 유지해야 한다.

### A-3. 권장 출력
- `prompt_risk_level`
- `injection_signals`
- `secret_exfil_signals`
- `requires_quarantine`
- `sanitized_excerpt`

### A-4. 권장 동작
- 원문 overwrite 금지
- raw는 보존
- model context에는 sanitized view 사용
- 고위험 bundle은 `judge_openai`로 바로 보내지 않고 quarantine 또는 suppress 후보로 보냄

---

## 7. 권장 적용안 B — AgentLinter

이 프로젝트는 문서가 곧 시스템이다.

- 단계 문서
- 실행 계약
- prompt
- policy
- rollout rule
- feature flag

이 많은 문서가 길어질수록, 규칙이 파일 중간에 묻혀 버리거나 서로 충돌할 가능성이 커진다.

### AgentLinter가 특히 잘 맞는 부분
- `AGENTS.md`를 도입하는 순간
- `prompts/` 아래 judge profile 문서
- `policies/` 아래 verdict/delivery 규칙
- `README` 및 operator handbook

### 권장 구조

```text
repo/
  AGENTS.md
  contracts/
  prompts/
  policies/
  docs/
  .github/workflows/agent_lint.yml
```

### 기대 효과
- critical rule 위치 문제를 조기에 감지
- 중복/충돌 지침을 줄임
- 불필요하게 긴 instruction 파일을 정리
- prompt/policy 변경 시 누락 규칙을 줄임

---

## 8. 권장 적용안 C — MemKraft는 `ops/eval memory`로만 사용

MemKraft를 production path에 넣는 것은 맞지 않다.  
대신 아래 같은 운영 기억 저장소에는 매우 잘 맞는다.

- false positive 사례
- false negative 사례
- reroot 실패 사례
- prompt 변경 이유
- policy threshold 조정 이유
- release gate 실패 기록
- operator postmortem

### 권장 디렉터리

```text
ops-memory/
  cases/
  regressions/
  reroot/
  prompt-decisions/
  policy-decisions/
  release-gates/
```

### 이 방식이 좋은 이유
- production data plane과 분리된다.
- 장기적인 운영 판단 근거를 남길 수 있다.
- 평가 실패를 같은 실수로 반복하지 않게 한다.

### 중요한 제한
- `CandidateGroup`, `Artifact`, `Analysis`의 system of record는 여전히 PostgreSQL이다.
- MemKraft는 **운영 기억**이지 **실행 truth**가 아니다.

---

## 9. 권장 적용안 D — Hermes/CrowClaw에서 가져올 것은 `skill discipline`이지 runtime이 아니다

현재 GitHub AI는 contract-driven pipeline이므로, 가져올 수 있는 것은 아래 정도다.

### 가져와도 되는 것
- Markdown skill 분리
- profile별 prompt handbook
- reusable skeptical scoring checklist
- checkpoint/replay 용어와 UX 아이디어
- MCP preset 아이디어(미래 확장용)

### 가져오면 안 되는 것
- full agent loop
- auto skill synthesis
- self-improving closed loop
- gateway / scheduler / tool runtime 전체

### 권장 폴더
```text
prompts/
  judge_profiles/
    github_primary.md
    x_primary.md
    text_idea.md
  skills/
    skeptical_take.md
    comparables.md
    evidence_limitations.md
    anti_hype_rules.md
```

이 구조는 현재 `judge_output_v1` / `analysis_v1` 경계와도 잘 맞는다.

---

## 10. 적용 제외 항목과 이유

### 10-1. Hermes/CrowClaw 전체 runtime
제외 이유:
- 이미 잠긴 파이프라인 구조와 충돌
- 중복 orchestration 발생
- 운영 설명 가능성 악화

### 10-2. auto-skill extraction / self-learning loop
제외 이유:
- 이 제품은 recall보다 precision이 더 중요하다.
- 자동 학습은 false positive를 빠르게 증폭시킬 위험이 있다.
- eval harness와 release gate가 충분히 닫히기 전에는 금지하는 편이 맞다.

### 10-3. MemKraft runtime memory
제외 이유:
- live candidate 판단 경로를 무겁게 만든다.
- PostgreSQL 중심 데이터모델과 역할이 겹친다.

### 10-4. `awesome-agent-frameworks`
제외 이유:
- 구현 자산이 아니라 decision guide에 가깝다.
- 아키텍처 비교 참고자료 이상으로 쓰는 것은 과하다.

### 10-5. `TerminalClaw`, `txxt`
제외 이유:
- 현재 저장소와 직접적인 구조 시너지가 작다.
- GitHub AI가 필요한 것은 terminal UI나 web scaffold가 아니라 **evidence pipeline hardening**이다.

---

## 11. 현재 구현 순서에 맞춘 3개 스프린트 제안

### Sprint 1
`web-enricher` / `evidence-assembler` 마무리  
+ judge 직전 Prompt Guard preflight 설계

### Sprint 2
`AGENTS.md` + `prompts/` + `policies/` 정리  
+ AgentLinter CI 추가

### Sprint 3
`ops-memory/` 생성  
+ MemKraft로 false positive / reroot / release gate 기록 축적

---

## 12. 한 줄 결론

GitHub AI는 **Hermes나 CrowClaw로 다시 짤 프로젝트가 아니라**,  
**Prompt Guard와 AgentLinter로 더 단단하게 만들고, MemKraft로 운영 기억을 보강해야 하는 계약 기반 파이프라인**이다.
