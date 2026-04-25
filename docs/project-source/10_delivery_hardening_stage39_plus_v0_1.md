# 10 delivery hardening stage39 plus v0 1

이 문서는 프로젝트 소스 파일 수 제한(40개)을 피하기 위한 **통합 번들**이다.
구조 변경 문서가 아니며, 아래 원본 문서를 **순서와 내용을 보존한 채** 묶는다.

## 포함 원본 파일
- `39_notifier_telegram_integration_hardening_v0_1.md`
- `40_end_to_end_delivery_acceptance_and_compose_hardening_v0_1.md`
- `41_delivery_retry_promotion_and_replay_hardening_v0_1.md`
- `42_delivery_operations_observability_and_dead_letter_hardening_v0_1.md`
- `43_delivery_gate_runner_and_batch_recovery_code_draft_v0_1.md`
- `44_delivery_gate_and_recovery_acceptance_hardening_v0_1.md`


## 통합 원칙
- 아키텍처 불변식은 유지한다.
- 원본 파일명은 아래 섹션 제목으로 보존한다.
- 충돌 시 해석 우선순위는 기존과 동일하게 **최신 README → 정본 단계 번들(`00~04`) → 구현 번들(`05~10`) → advisory design note** 이다.
- 이번 통합은 **standalone 39~44 delivery hardening 문서를 preferred upload set 기준에서 bundle 1개로 흡수** 하기 위한 것이다.
  - 내용은 합치되, 구조는 바꾸지 않는다.
- `03_GitHub_AI_application_plan.md`는 이 번들에 포함하지 않는다.
  - 이유: 적용 검토용 advisory 문서이며, phase/ordering authority가 아니기 때문이다.

---


## Source file: `39_notifier_telegram_integration_hardening_v0_1.md`

# 39단계: `notifier-telegram` integration hardening v0.1

## 0. 문서 목적

이 문서는 `38_notifier_telegram_skeleton_and_code_draft_v0_1.md`를 뒤엎는 문서가 아니다.  
목적은 **이미 잠긴 delivery skeleton을 유지한 채, 첫 운영 hardening 층만 좁게 닫는 것**이다.

이번 단계에서 닫는 것은 정확히 아래 열 가지다.

1. `notification.plan.created.v1` **plan-intent rehydration**
2. `notification_plans` **concretization idempotency**
3. **duplicate delivery guard**
4. **send-vs-edit decision hardening**
5. `material_change_hash`와 `render_hash`의 역할 분리
6. Telegram transport의 **retryable vs terminal classification**
7. **flood-control / backoff** 처리
8. `notification_renders` / `notification_delivery_records` / `state_transitions` 일관성
9. **dry-run / replay-safe** 동작
10. notifier 전용 **compose/runtime acceptance assumptions**

핵심 전제는 그대로 유지한다.

- `notifier-telegram`은 **policy-engine이 아니다**.
- `notifier-telegram`은 **judge가 아니다**.
- `notifier-telegram`은 **verdict / delivery_decision을 재계산하지 않는다**.
- `notifier-telegram`은 **presentation / delivery boundary** 로만 동작한다.
- durable truth는 여전히 **PostgreSQL** 이고, Redis는 **short-lived queue state** 다.

---

## 1. 왜 지금 이 단계가 정확한 다음 단계인가

현재 authoritative README 기준 최신 상태는 stage 38 delivery skeleton까지 닫힌 상태이고,  
그 다음 안전한 작업은 두 가지뿐이었다.

1. `39_notifier_telegram_integration_hardening_v0_1.md`
2. end-to-end delivery acceptance / compose wiring / rollout flag hardening

즉, 지금 collector / normalizer / enricher / judge / policy를 다시 여는 것은 순서상 후퇴다.  
지금 닫아야 하는 것은 **delivery skeleton의 첫 운영 하드닝 층** 이다.

---

## 2. 현재 38단계 초안의 운영상 빈틈

38단계 초안은 skeleton으로서는 충분하지만, 운영 경계로 바로 쓰기에는 아래 공백이 남아 있다.

### 2-1. plan-intent는 들어오지만, concretization idempotency가 약하다

현재 초안은 아래 둘을 동시에 충분히 닫지 못한다.

- `notification_plan_id` 기준 재실행
- `(analysis_id, target_chat_id, material_change_hash)` 기준 재실행

즉, 같은 intent가 중복 소비되면 plan row / render row / delivery row가 흔들릴 여지가 있다.

### 2-2. duplicate delivery guard가 약하다

현재 초안은 최근 subject를 하나만 보고 edit/noop를 판단하는 방향인데, 아래 경우를 구분하지 못한다.

- 같은 `material_change_hash` 재도착
- 같은 `render_hash` 재도착
- 이전 전송이 이미 성공했고 새 intent는 사실상 동일한 경우
- 이전 전송은 실패했지만 render는 동일한 경우

### 2-3. send-vs-edit 기준이 너무 거칠다

현재 초안은 “같은 subject + 최근 메시지 + material hash 변경”이면 edit 쪽으로 기우는 구조인데, 아래는 분리해야 한다.

- `later -> inspect_now` 우선순위 상승
- primary artifact canonical subject 변경
- reroot 결과로 headline / 링크 구조가 바뀐 경우
- render profile 변경
- 이미 너무 오래된 메시지
- 기존 message가 존재하지만 edit가 의미상 부적절한 경우

### 2-4. transport 분류가 지나치게 단순하다

모든 4xx를 terminal, 5xx만 retryable로 두면 Telegram 운영 현실과 안 맞는다.

- `429 Too Many Requests` 는 retryable 이다.
- `Retry-After` 가 있으면 send_after backoff로 연결해야 한다.
- `message is not modified` 는 실패라기보다 logical no-op에 가깝다.
- `chat not found`, `bot was blocked`, `message can't be edited` 는 terminal 성격이 강하다.

### 2-5. dry-run / replay-safe가 없다

현재 초안은 dev/replay에서 실수로 실제 Telegram 전송을 건드릴 방어막이 약하다.  
이 상태로는 replay나 acceptance test가 운영 채팅에 side-effect를 낼 수 있다.

### 2-6. 상태 전이가 stage 7 정본과 완전히 맞물리지 않는다

stage 7은 아래 상태를 권장했다.

- `planned`
- `rendered`
- `queued`
- `sent`
- `edited`
- `suppressed`
- `failed_retryable`
- `failed_terminal`

그런데 38 초안은 `queued` 의미가 약하고, no-op / dry-run / retry scheduling과의 관계도 정리되지 않았다.

---

## 3. 이번 단계에서 드러나는 충돌과 최소-change 해석

## 3-1. 충돌 A — retry/backoff를 넣고 싶지만 notifier ownership에는 `job_attempts`가 없다

execution contracts의 notifier ownership은 아래로 잠겨 있다.

- `notification_plans`
- `notification_renders`
- `notification_delivery_records`
- `state_transitions`
- `event_outbox`

즉, notifier가 `job_attempts.retry_after_at`를 직접 조작하는 식으로 hardening하면 현재 ownership을 넓히게 된다.

### 최소-change 해석 A

이번 v0.1 hardening에서는 **retry scheduling은 `notification_plans.send_after`를 재사용** 한다.

- retryable 실패 발생
- notifier는 `notification_plans.status = failed_retryable`
- 동시에 `notification_plans.send_after = next_retry_at` 로 밀어둔다
- 이후 notifier worker는 `send_after > now()` 인 plan을 transport 대상으로 취급하지 않는다

즉, **새 컬럼이나 새 ownership 없이 기존 schema로 retry/backoff를 흡수** 한다.

---

## 3-2. 충돌 B — dry-run/replay-safe를 넣고 싶지만 `notification_status_enum`에는 `dry_run`이 없다

현재 enum에는 `dry_run` 상태가 없다.

### 최소-change 해석 B

이번 v0.1에서는 dry-run transport 결과를 아래처럼 해석한다.

- `notification_plans.status = suppressed`
- `notification_delivery_records.delivery_status = suppressed`
- `telegram_response_json` 안에 `{"dry_run": true, ...}` 메타데이터 기록
- `state_transitions.reason_code = dry_run_skip_transport`

즉, **새 enum을 만들지 않고, “실제 전송을 하지 않은 의도적 suppress” 로 기록** 한다.

이 방식의 장점:

- replay-safe 보장
- schema 변경 없음
- 운영자가 dry-run 흔적을 식별 가능

---

## 3-3. 충돌 C — `notification.plan.created.v1` 이름은 plan-created인데 실제로는 plan-intent bridge다

37단계는 ownership을 지키기 위해 `notification.plan.created.v1`를 plan-intent event로 썼다.  
이 이름은 이미 굳었고 queue routing도 잠겨 있다.

### 최소-change 해석 C

이 단계에서는 이름을 바꾸지 않는다.

- event 이름은 유지
- notifier가 이를 소비해 **실제 `notification_plans` row를 concretize**
- durable row가 이미 있으면 **event를 idempotent rehydrate signal** 로만 취급

즉, **event 이름은 historical artifact로 수용하고, 실제 semantics는 notifier에서 닫는다**.

---

## 3-4. 충돌 D — `material_change_hash`는 policy-engine이 만들었고, `render_hash`는 notifier가 만든다

둘의 역할을 섞으면 아래 문제가 생긴다.

- notifier가 semantic change를 다시 판단하게 됨
- presentation-only 변경과 semantic 변경이 뒤섞임
- replay 시 동일 의미지만 렌더 공백 차이로 edit/send가 흔들림

### 최소-change 해석 D

두 해시는 역할을 분리해 유지한다.

- `material_change_hash`
  - policy-engine가 만든 **semantic change fingerprint**
  - notifier는 **재계산하지 않고 소비만** 한다
- `render_hash`
  - notifier가 만든 **exact payload fingerprint**
  - 같은 plan 내 exact render dedupe에만 사용

즉, **semantic dedupe는 material_change_hash**, **exact render dedupe는 render_hash** 로 고정한다.

---

## 4. hardening 범위와 제외 범위

### 포함

- plan concretization idempotency
- same-material no-op guard
- exact-render no-op guard
- send-after gate
- edit-vs-new decision hardening
- retryable/terminal 분류
- flood wait parsing + backoff scheduling
- dry-run / replay-safe mode
- state transition consistency
- notifier acceptance assumptions

### 제외

- digest planner 활성화
- inbound command plane
- notifier가 final policy를 override하는 기능
- Prompt Guard lifecycle 추가
- MemKraft runtime retrieval
- Redis claim/distributed lease 재설계
- delivery enum / schema 대규모 변경

즉, 이번 문서는 **notifier hot path를 다시 설계하지 않고 운영 gap만 닫는다.**

---

## 5. 대상 파일 트리

```text
src/services/notifier_telegram/
  config.py          # updated
  models.py          # updated
  telegram_client.py # updated
  repositories.py    # updated
  service.py         # updated
  worker.py          # tiny update

tests/
  unit/
    services/
      notifier_telegram/
        test_plan_concretization_idempotency.py   # new
        test_duplicate_delivery_guard.py          # new
        test_send_vs_edit_rules.py                # new
        test_retryable_terminal_classification.py # new
        test_send_after_gate.py                   # new
        test_dry_run_path.py                      # new
  component/
    services/
      notifier_telegram/
        test_existing_material_noop.py           # new
        test_retryable_failure_reschedules_send_after.py  # new
        test_429_retry_after_backoff.py          # new
        test_replay_safe_dry_run_no_transport.py # new
        test_transition_sequence_consistency.py  # new
```

`entity_builder.py`, `keyboard_builder.py`, `renderer.py`, `main.py`는 이번 턴에서 구조 변경 없이 재사용 가능하다.

---

## 6. 이번 단계에서 고정할 구현 규칙

## 6-1. intent rehydration과 plan concretization

### 입력 원칙

Redis payload는 계속 thin message다.  
consumer는 반드시 `trigger_event_id`로 `event_outbox`를 다시 조회한다.

### concretization 순서

1. `notification_plan_id`로 기존 row 조회
2. 있으면 **그 row를 재사용**
3. 없으면 `(analysis_id, target_chat_id, material_change_hash)` 기준으로 기존 row 조회
4. 있으면 **그 row를 재사용**
5. 둘 다 없을 때만 새 `notification_plans` insert

### 상태 기본값

새 concretization row는 아래로 시작한다.

- `status = planned`
- `send_after = payload.send_after`

즉, **event 재도착이 plan row 중복 생성으로 이어지지 않도록** 한다.

---

## 6-2. `send_after` gate

notifier는 transport 전에 반드시 아래를 확인한다.

- `send_after is null` 이거나 `send_after <= now()` 인 경우만 send/edit 후보
- `send_after > now()` 이면 **no transport**
- 이 경우 `notification_plans.status`는 유지하거나 `queued` 로 둘 수 있지만, v0.1에서는 **상태 변경 없이 skip** 한다

왜 이렇게 두는가:

- retry scheduling이 `send_after`로 들어오기 때문
- notifier가 자체 sleep/backoff 루프를 길게 들고 있지 않아도 되기 때문
- replay/dry-run에서도 deterministic하게 같은 판단을 내릴 수 있기 때문

---

## 6-3. duplicate delivery guard

### guard 1 — same material, already delivered

아래를 모두 만족하면 **no-op** 이다.

- 같은 `dedupe_subject_key`
- 같은 `target_chat_id`
- 최근 successful delivery 존재
- 기존 plan의 `material_change_hash == incoming material_change_hash`

이 경우:

- 새 render는 optional
- 새 Telegram transport 없음
- 새 delivery record는 남길 수 있지만, v0.1에서는 **`state_transitions`에 `notification_duplicate_noop`** 만 남기는 보수 경로를 기본으로 둔다

### guard 2 — same render within same plan

같은 `notification_plan_id` 아래에 같은 `render_hash`가 이미 있으면:

- 새 `notification_renders` insert 없음
- render generation은 logical no-op

즉, **semantic duplicate와 exact render duplicate를 분리해서 막는다.**

---

## 6-4. send-vs-edit-vs-noop 결정 규칙

### `noop`

아래면 무조건 noop:

- same material already delivered
- `send_after > now()`
- digest path인데 runtime disabled
- dry-run인데 transport 금지

### `edit`

아래를 모두 만족할 때만 edit 허용:

- same `dedupe_subject_key`
- same `target_chat_id`
- recent successful delivery 존재
- 기존 message id 존재
- 기존 `material_change_hash != incoming material_change_hash`
- **urgency escalation이 없음**
- **primary canonical subject가 동일**
- edit window 안쪽

### `send`

아래 중 하나면 새 send 강제:

- 기존 message 없음
- 기존 successful delivery 없음
- `later -> inspect_now` 또는 urgency `normal_silent -> high`
- primary canonical URL 변경
- render profile 변경
- 기존 message가 edit window 밖
- terminal edit restriction(`message can't be edited`) 이력 존재

즉, **edit는 예외적 최적화이고, send가 기본** 이다.

---

## 6-5. `material_change_hash`와 `render_hash` 역할 분리

### `material_change_hash`
판단 대상:

- send vs noop
- edit vs send
- same subject semantic update 여부

### `render_hash`
판단 대상:

- 같은 plan 내부 exact render 중복
- 동일 render 재insert 방지

### 금지

- notifier가 `material_change_hash`를 재계산하는 것
- `render_hash`만 보고 semantic no-op로 해석하는 것

즉, **semantic은 upstream truth를 믿고, rendering only는 notifier에서 처리** 한다.

---

## 6-6. transport 전/후 상태 일관성

### 권장 상태 전이

```text
planned
  -> rendered
  -> queued
  -> sent | edited | suppressed | failed_retryable | failed_terminal
```

### v0.1 규칙

1. plan concretized 후 `planned`
2. render row 성공 후 `rendered`
3. transport 직전 `queued`
4. 결과에 따라 final status 기록

### no-op / dry-run

- same-material noop: `state_transitions`만 남기고 final status 변경 없음 또는 `edited` 금지
- dry-run: final status `suppressed`, reason `dry_run_skip_transport`

즉, **`edited`는 실제 edit transport 성공에만 사용** 한다.

---

## 6-7. retryable vs terminal Telegram transport classification

### retryable

다음은 retryable 로 분류한다.

- HTTP 429
- HTTP 5xx
- timeout / connect error / reset
- Telegram flood-control (`retry_after` 존재)
- temporary upstream bad gateway / gateway timeout

### terminal

다음은 terminal 로 분류한다.

- invalid chat id
- bot blocked by user
- bot removed from chat / insufficient rights
- malformed entity / message text invalid
- message to edit not found
- message can’t be edited

### special-case no-op

아래는 transport failure가 아니라 logical no-op 로 본다.

- `message is not modified`

이 경우:

- final status를 `edited`로 위장하지 않는다
- `state_transitions.reason_code = telegram_edit_not_modified_noop`
- delivery record는 optional이지만, v0.1에서는 **`edited` 대신 no transport transition만 남기는 보수 경로** 를 권장한다

---

## 6-8. flood-control / backoff 처리

### 기본 규칙

- Telegram 응답에 `retry_after`가 있으면 최우선 사용
- 없으면 notifier deterministic backoff를 계산

### 권장 backoff

- first retryable failure: +30초
- repeated retryable failure: `min(5m, 30s * 2^(n-1))`
- Telegram `retry_after`가 있으면 그 값 우선

### durable 반영

retryable 실패 시:

- `notification_plans.status = failed_retryable`
- `notification_plans.send_after = next_retry_at`
- `notification_delivery_records` append
- `notification.delivery.result.v1` emit
- `state_transitions.reason_code`에 transport class 기록

즉, **worker가 잠들지 않고 durable state로 retry를 넘긴다.**

---

## 6-9. dry-run / replay-safe 동작

### 새 config

- `NOTIFIER_TELEGRAM_DRY_RUN=true|false`
- `NOTIFIER_TELEGRAM_ALLOW_EDITS=true|false`

### 규칙

- `APP_ENV != prod` 이고 `NOTIFIER_TELEGRAM_DRY_RUN=true` 이면 실제 Telegram transport 금지
- replay/manual acceptance 환경에서는 기본 dry-run 권장
- dry-run 에서는
  - plan concretization 수행
  - render append 수행
  - transport는 수행하지 않음
  - delivery record는 `suppressed` + `{"dry_run": true}` 로 남김

### replay-safe 추가 규칙

- dry-run/replay에서는 절대 기존 live Telegram message edit 금지
- `DeliveryAction.edit`는 dry-run 에서 강제로 noop 처리

즉, **replay가 historical live notification을 훼손하지 않게 한다.**

---

## 6-10. notifier acceptance assumptions

이번 hardening에서 compose/runtime acceptance 전제는 아래로 고정한다.

1. `q.notification.send`는 여전히 outbox-relay가 발행한다.
2. notifier worker는 thin payload만 읽고 PostgreSQL rehydrate를 한다.
3. `send_after` future row는 transport 안 한다.
4. dev/replay는 dry-run 기본값을 권장한다.
5. prod는 bot token만 가진다. collector/user session과 secret을 섞지 않는다.
6. retryable transport는 `failed_retryable + send_after` 로 남기고 worker 장기 sleep을 피한다.
7. maintenance 또는 동일 notifier polling이 due retry row를 다시 집행할 수 있어야 한다.

---

## 7. 코드 초안

## 7-1. `src/services/notifier_telegram/config.py` (updated)

```python
from __future__ import annotations

import os
from dataclasses import dataclass


class NotifierTelegramConfigurationError(ValueError):
    pass


@dataclass(slots=True, frozen=True)
class NotifierTelegramConfig:
    app_env: str
    database_url: str
    redis_url: str

    queue_name: str
    consumer_group: str
    consumer_name: str
    batch_size: int
    block_ms: int

    telegram_bot_token: str
    telegram_api_base_url: str
    request_timeout_sec: float

    max_message_chars: int
    edit_window_minutes: int
    enable_digest_runtime: bool
    dry_run: bool
    allow_edits: bool
    log_level: str

    @classmethod
    def from_env(cls) -> "NotifierTelegramConfig":
        def _read(name: str, default: str = "") -> str:
            return os.getenv(name, default).strip()

        cfg = cls(
            app_env=_read("APP_ENV", "dev").lower(),
            database_url=_read("DATABASE_URL"),
            redis_url=_read("REDIS_URL"),
            queue_name=_read("NOTIFIER_TELEGRAM_QUEUE_NAME", "q.notification.send"),
            consumer_group=_read("NOTIFIER_TELEGRAM_CONSUMER_GROUP", "notifier-telegram"),
            consumer_name=_read("NOTIFIER_TELEGRAM_CONSUMER_NAME", "notifier-telegram-1"),
            batch_size=int(_read("NOTIFIER_TELEGRAM_BATCH_SIZE", "20")),
            block_ms=int(_read("NOTIFIER_TELEGRAM_BLOCK_MS", "5000")),
            telegram_bot_token=_read("TELEGRAM_BOT_TOKEN"),
            telegram_api_base_url=_read("TELEGRAM_API_BASE_URL", "https://api.telegram.org"),
            request_timeout_sec=float(_read("NOTIFIER_TELEGRAM_REQUEST_TIMEOUT_SEC", "20")),
            max_message_chars=int(_read("NOTIFIER_TELEGRAM_MAX_MESSAGE_CHARS", "3800")),
            edit_window_minutes=int(_read("NOTIFIER_TELEGRAM_EDIT_WINDOW_MINUTES", "180")),
            enable_digest_runtime=_read("ENABLE_DIGEST_RUNTIME", "false").lower() == "true",
            dry_run=_read("NOTIFIER_TELEGRAM_DRY_RUN", "false").lower() == "true",
            allow_edits=_read("NOTIFIER_TELEGRAM_ALLOW_EDITS", "true").lower() == "true",
            log_level=_read("LOG_LEVEL", "INFO").upper(),
        )
        cfg.validate()
        return cfg

    def validate(self) -> None:
        if not self.database_url:
            raise NotifierTelegramConfigurationError("DATABASE_URL is required")
        if not self.redis_url:
            raise NotifierTelegramConfigurationError("REDIS_URL is required")
        if not self.queue_name:
            raise NotifierTelegramConfigurationError("NOTIFIER_TELEGRAM_QUEUE_NAME must not be empty")
        if not self.consumer_group:
            raise NotifierTelegramConfigurationError("NOTIFIER_TELEGRAM_CONSUMER_GROUP must not be empty")
        if not self.consumer_name:
            raise NotifierTelegramConfigurationError("NOTIFIER_TELEGRAM_CONSUMER_NAME must not be empty")
        if self.batch_size <= 0 or self.batch_size > 100:
            raise NotifierTelegramConfigurationError("NOTIFIER_TELEGRAM_BATCH_SIZE must be between 1 and 100")
        if self.block_ms <= 0:
            raise NotifierTelegramConfigurationError("NOTIFIER_TELEGRAM_BLOCK_MS must be > 0")
        if not self.telegram_bot_token:
            raise NotifierTelegramConfigurationError("TELEGRAM_BOT_TOKEN is required")
        if self.request_timeout_sec <= 0:
            raise NotifierTelegramConfigurationError("NOTIFIER_TELEGRAM_REQUEST_TIMEOUT_SEC must be > 0")
        if self.max_message_chars <= 0 or self.max_message_chars > 4096:
            raise NotifierTelegramConfigurationError("NOTIFIER_TELEGRAM_MAX_MESSAGE_CHARS must be between 1 and 4096")
        if self.edit_window_minutes <= 0:
            raise NotifierTelegramConfigurationError("NOTIFIER_TELEGRAM_EDIT_WINDOW_MINUTES must be > 0")
```

---

## 7-2. `src/services/notifier_telegram/models.py` (updated)

```python
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Literal


DeliveryStatus = Literal[
    "planned",
    "rendered",
    "queued",
    "sent",
    "edited",
    "suppressed",
    "failed_retryable",
    "failed_terminal",
]


@dataclass(slots=True, frozen=True)
class NotificationIntentJob:
    trigger_event_id: str
    event_type: str
    notification_plan_id: str
    analysis_id: str
    candidate_group_id: str
    delivery_decision: str
    urgency_profile: str
    target_chat_id: int
    target_thread_id: int | None
    render_profile: str | None
    dedupe_subject_key: str
    material_change_hash: str
    send_after: datetime | None
    suppress_reason_code: str | None


@dataclass(slots=True, frozen=True)
class ExistingRecentDelivery:
    notification_plan_id: str
    telegram_message_id: int | None
    telegram_chat_id: int | None
    material_change_hash: str
    primary_canonical_url: str | None
    urgency_profile: str | None
    render_profile: str | None
    created_at: datetime


@dataclass(slots=True, frozen=True)
class DeliveryAction:
    mode: Literal["send", "edit", "noop"]
    existing_message_id: int | None = None
    reason_code: str | None = None


@dataclass(slots=True, frozen=True)
class DeliveryResult:
    delivery_status: str
    telegram_chat_id: int | None
    telegram_message_id: int | None
    attempt_count: int
    transport_error_code: str | None = None
    transport_error_class: str | None = None
    telegram_response_json: dict[str, Any] | None = None
    retry_after_seconds: int | None = None
    edited: bool = False
```

---

## 7-3. `src/services/notifier_telegram/telegram_client.py` (updated)

```python
from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import httpx


class TelegramTransportRetryableError(Exception):
    def __init__(self, message: str, *, retry_after_seconds: int | None = None) -> None:
        super().__init__(message)
        self.retry_after_seconds = retry_after_seconds


class TelegramTransportTerminalError(Exception):
    pass


class TelegramTransportNoopError(Exception):
    pass


@dataclass(slots=True, frozen=True)
class TelegramCallResult:
    response_json: dict[str, Any]


class TelegramBotClient:
    def __init__(self, *, base_url: str, bot_token: str, timeout_sec: float) -> None:
        self._base_url = base_url.rstrip("/")
        self._bot_token = bot_token
        self._timeout_sec = timeout_sec

    async def send_message(self, *, chat_id: int, text: str, entities: list[dict], reply_markup: dict | None, disable_notification: bool, link_preview_options: dict | None, message_thread_id: int | None) -> dict:
        payload = {
            "chat_id": chat_id,
            "text": text,
            "entities": entities,
            "reply_markup": reply_markup,
            "disable_notification": disable_notification,
            "link_preview_options": link_preview_options,
        }
        if message_thread_id is not None:
            payload["message_thread_id"] = message_thread_id
        return await self._post("sendMessage", payload)

    async def edit_message_text(self, *, chat_id: int, message_id: int, text: str, entities: list[dict], reply_markup: dict | None, link_preview_options: dict | None) -> dict:
        payload = {
            "chat_id": chat_id,
            "message_id": message_id,
            "text": text,
            "entities": entities,
            "reply_markup": reply_markup,
            "link_preview_options": link_preview_options,
        }
        return await self._post("editMessageText", payload)

    async def _post(self, method: str, payload: dict) -> dict:
        url = f"{self._base_url}/bot{self._bot_token}/{method}"
        try:
            async with httpx.AsyncClient(timeout=self._timeout_sec) as client:
                response = await client.post(url, json=payload)
        except (httpx.TimeoutException, httpx.ConnectError, httpx.ReadError) as exc:
            raise TelegramTransportRetryableError(str(exc)) from exc

        data = None
        try:
            data = response.json()
        except Exception:
            data = None

        retry_after = None
        if isinstance(data, dict):
            params = data.get("parameters") or {}
            if isinstance(params, dict) and params.get("retry_after") is not None:
                try:
                    retry_after = int(params.get("retry_after"))
                except Exception:
                    retry_after = None

        if response.status_code == 429:
            raise TelegramTransportRetryableError(response.text, retry_after_seconds=retry_after)
        if response.status_code >= 500:
            raise TelegramTransportRetryableError(response.text, retry_after_seconds=retry_after)
        if response.status_code >= 400:
            description = str((data or {}).get("description") if isinstance(data, dict) else response.text)
            if "Too Many Requests" in description:
                raise TelegramTransportRetryableError(description, retry_after_seconds=retry_after)
            if "message is not modified" in description:
                raise TelegramTransportNoopError(description)
            raise TelegramTransportTerminalError(description)

        if not isinstance(data, dict) or not data.get("ok", False):
            description = str((data or {}).get("description") if isinstance(data, dict) else "telegram request failed")
            if "Too Many Requests" in description:
                raise TelegramTransportRetryableError(description, retry_after_seconds=retry_after)
            if "message is not modified" in description:
                raise TelegramTransportNoopError(description)
            if any(token in description.lower() for token in ["timed out", "timeout", "temporar"]):
                raise TelegramTransportRetryableError(description, retry_after_seconds=retry_after)
            raise TelegramTransportTerminalError(description)

        return data
```

---

## 7-4. `src/services/notifier_telegram/repositories.py` (updated)

```python
from __future__ import annotations

import json
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Any

import sqlalchemy as sa
from sqlalchemy.ext.asyncio import AsyncSession

from .models import ExistingRecentDelivery, NotificationIntentJob, NotificationPlanDraft, NotificationRenderDraft


def _jsonb_dumps(value: Any) -> str | None:
    if value is None:
        return None
    return json.dumps(value, ensure_ascii=False)


class NotifierTelegramRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    @asynccontextmanager
    async def transaction(self):
        if self._session.in_transaction():
            yield self._session
            return
        async with self._session.begin():
            yield self._session

    async def load_intent_job(self, trigger_event_id: str) -> NotificationIntentJob | None:
        result = await self._session.execute(
            sa.text(
                """
                SELECT event_id, event_type, payload_json
                FROM event_outbox
                WHERE event_id = CAST(:event_id AS uuid)
                """
            ),
            {"event_id": trigger_event_id},
        )
        row = result.mappings().first()
        if row is None or str(row["event_type"]) != "notification.plan.created.v1":
            return None
        payload = row["payload_json"] or {}
        send_after = None
        if payload.get("send_after"):
            send_after = datetime.fromisoformat(str(payload["send_after"]).replace("Z", "+00:00"))
        return NotificationIntentJob(
            trigger_event_id=str(row["event_id"]),
            event_type=str(row["event_type"]),
            notification_plan_id=str(payload["notification_plan_id"]),
            analysis_id=str(payload["analysis_id"]),
            candidate_group_id=str(payload["candidate_group_id"]),
            delivery_decision=str(payload["delivery_decision"]),
            urgency_profile=str(payload["urgency_profile"]),
            target_chat_id=int(payload["target_chat_id"]),
            target_thread_id=int(payload["target_thread_id"]) if payload.get("target_thread_id") else None,
            render_profile=str(payload.get("render_profile")) if payload.get("render_profile") else None,
            dedupe_subject_key=str(payload["dedupe_subject_key"]),
            material_change_hash=str(payload["material_change_hash"]),
            send_after=send_after,
            suppress_reason_code=str(payload.get("suppress_reason_code")) if payload.get("suppress_reason_code") else None,
        )

    async def load_notification_plan(self, notification_plan_id: str):
        result = await self._session.execute(
            sa.text(
                """
                SELECT *
                FROM notification_plans
                WHERE notification_plan_id = CAST(:notification_plan_id AS uuid)
                """
            ),
            {"notification_plan_id": notification_plan_id},
        )
        return result.mappings().first()

    async def load_existing_plan_by_material(self, *, analysis_id: str, target_chat_id: int, material_change_hash: str):
        result = await self._session.execute(
            sa.text(
                """
                SELECT *
                FROM notification_plans
                WHERE analysis_id = CAST(:analysis_id AS uuid)
                  AND target_chat_id = :target_chat_id
                  AND material_change_hash = :material_change_hash
                LIMIT 1
                """
            ),
            {
                "analysis_id": analysis_id,
                "target_chat_id": target_chat_id,
                "material_change_hash": material_change_hash,
            },
        )
        return result.mappings().first()

    async def insert_notification_plan(self, draft: NotificationPlanDraft) -> None:
        await self._session.execute(
            sa.text(
                """
                INSERT INTO notification_plans (
                    notification_plan_id,
                    analysis_id,
                    candidate_group_id,
                    delivery_decision,
                    urgency_profile,
                    target_chat_id,
                    target_thread_id,
                    render_profile,
                    dedupe_subject_key,
                    material_change_hash,
                    send_after,
                    suppress_reason_code,
                    status,
                    created_at
                ) VALUES (
                    CAST(:notification_plan_id AS uuid),
                    CAST(:analysis_id AS uuid),
                    CAST(:candidate_group_id AS uuid),
                    CAST(:delivery_decision AS delivery_decision_enum),
                    CAST(:urgency_profile AS urgency_profile_enum),
                    :target_chat_id,
                    :target_thread_id,
                    :render_profile,
                    :dedupe_subject_key,
                    :material_change_hash,
                    :send_after,
                    :suppress_reason_code,
                    CAST(:status AS notification_status_enum),
                    now()
                )
                ON CONFLICT (notification_plan_id) DO NOTHING
                """
            ),
            {
                "notification_plan_id": draft.notification_plan_id,
                "analysis_id": draft.analysis_id,
                "candidate_group_id": draft.candidate_group_id,
                "delivery_decision": draft.delivery_decision,
                "urgency_profile": draft.urgency_profile,
                "target_chat_id": draft.target_chat_id,
                "target_thread_id": draft.target_thread_id,
                "render_profile": draft.render_profile,
                "dedupe_subject_key": draft.dedupe_subject_key,
                "material_change_hash": draft.material_change_hash,
                "send_after": draft.send_after,
                "suppress_reason_code": draft.suppress_reason_code,
                "status": draft.status,
            },
        )

    async def load_render_by_hash(self, *, notification_plan_id: str, render_hash: str):
        result = await self._session.execute(
            sa.text(
                """
                SELECT *
                FROM notification_renders
                WHERE notification_plan_id = CAST(:notification_plan_id AS uuid)
                  AND render_hash = :render_hash
                LIMIT 1
                """
            ),
            {"notification_plan_id": notification_plan_id, "render_hash": render_hash},
        )
        return result.mappings().first()

    async def insert_notification_render(self, draft: NotificationRenderDraft) -> str | None:
        result = await self._session.execute(
            sa.text(
                """
                INSERT INTO notification_renders (
                    notification_render_id,
                    notification_plan_id,
                    message_text,
                    entities_json,
                    link_preview_options_json,
                    reply_markup_json,
                    disable_notification,
                    protect_content,
                    parse_strategy,
                    render_hash,
                    created_at
                ) VALUES (
                    gen_random_uuid(),
                    CAST(:notification_plan_id AS uuid),
                    :message_text,
                    CAST(:entities_json AS jsonb),
                    CAST(:link_preview_options_json AS jsonb),
                    CAST(:reply_markup_json AS jsonb),
                    :disable_notification,
                    :protect_content,
                    :parse_strategy,
                    :render_hash,
                    now()
                )
                ON CONFLICT (notification_plan_id, render_hash) DO NOTHING
                RETURNING notification_render_id
                """
            ),
            {
                "notification_plan_id": draft.notification_plan_id,
                "message_text": draft.message_text,
                "entities_json": _jsonb_dumps(draft.entities_json),
                "link_preview_options_json": _jsonb_dumps(draft.link_preview_options_json),
                "reply_markup_json": _jsonb_dumps(draft.reply_markup_json),
                "disable_notification": draft.disable_notification,
                "protect_content": draft.protect_content,
                "parse_strategy": draft.parse_strategy,
                "render_hash": draft.render_hash,
            },
        )
        value = result.scalar_one_or_none()
        return str(value) if value else None

    async def load_recent_delivery_for_subject(self, *, dedupe_subject_key: str, target_chat_id: int, within_minutes: int):
        result = await self._session.execute(
            sa.text(
                """
                SELECT np.notification_plan_id,
                       ndr.telegram_message_id,
                       ndr.telegram_chat_id,
                       np.material_change_hash,
                       np.created_at,
                       ar.canonical_url AS primary_canonical_url,
                       np.urgency_profile,
                       np.render_profile
                FROM notification_plans np
                JOIN notification_delivery_records ndr ON ndr.notification_plan_id = np.notification_plan_id
                LEFT JOIN analyses a ON a.analysis_id = np.analysis_id
                LEFT JOIN candidate_group_proposals cgp ON cgp.candidate_group_id = np.candidate_group_id
                LEFT JOIN artifact_registry ar ON ar.artifact_id = cgp.current_primary_artifact_id
                WHERE np.dedupe_subject_key = :dedupe_subject_key
                  AND np.target_chat_id = :target_chat_id
                  AND ndr.delivery_status IN ('sent', 'edited')
                  AND np.created_at >= (now() - make_interval(mins => :within_minutes))
                ORDER BY np.created_at DESC
                LIMIT 1
                """
            ),
            {
                "dedupe_subject_key": dedupe_subject_key,
                "target_chat_id": target_chat_id,
                "within_minutes": within_minutes,
            },
        )
        row = result.mappings().first()
        if row is None:
            return None
        return ExistingRecentDelivery(
            notification_plan_id=str(row["notification_plan_id"]),
            telegram_message_id=int(row["telegram_message_id"]) if row["telegram_message_id"] else None,
            telegram_chat_id=int(row["telegram_chat_id"]) if row["telegram_chat_id"] else None,
            material_change_hash=str(row["material_change_hash"]),
            primary_canonical_url=str(row["primary_canonical_url"]) if row["primary_canonical_url"] else None,
            urgency_profile=str(row["urgency_profile"]) if row["urgency_profile"] else None,
            render_profile=str(row["render_profile"]) if row["render_profile"] else None,
            created_at=row["created_at"],
        )

    async def update_plan_status(self, *, notification_plan_id: str, status: str) -> None:
        await self._session.execute(
            sa.text(
                """
                UPDATE notification_plans
                SET status = CAST(:status AS notification_status_enum)
                WHERE notification_plan_id = CAST(:notification_plan_id AS uuid)
                """
            ),
            {"notification_plan_id": notification_plan_id, "status": status},
        )

    async def reschedule_retry(self, *, notification_plan_id: str, next_retry_at: datetime) -> None:
        await self._session.execute(
            sa.text(
                """
                UPDATE notification_plans
                SET status = 'failed_retryable'::notification_status_enum,
                    send_after = :next_retry_at
                WHERE notification_plan_id = CAST(:notification_plan_id AS uuid)
                """
            ),
            {"notification_plan_id": notification_plan_id, "next_retry_at": next_retry_at},
        )
```

---

## 7-5. `src/services/notifier_telegram/service.py` (updated)

```python
from __future__ import annotations

from datetime import datetime, timedelta, timezone
import logging

from .models import DeliveryAction, DeliveryResult, NotificationPlanDraft
from .renderer import NotificationRenderer, RenderInput
from .telegram_client import (
    TelegramBotClient,
    TelegramTransportNoopError,
    TelegramTransportRetryableError,
    TelegramTransportTerminalError,
)


class NotifierTelegramService:
    def __init__(
        self,
        config,
        *,
        repository,
        renderer: NotificationRenderer,
        telegram_client: TelegramBotClient,
        logger: logging.Logger | None = None,
    ) -> None:
        self._config = config
        self._repository = repository
        self._renderer = renderer
        self._telegram_client = telegram_client
        self._logger = logger or logging.getLogger(__name__)

    async def rehydrate_intent(self, trigger_event_id: str):
        return await self._repository.load_intent_job(trigger_event_id)

    async def handle_intent(self, intent) -> None:
        analysis = await self._repository.load_analysis(intent.analysis_id)
        if analysis is None:
            return
        if analysis.candidate_group_id != intent.candidate_group_id:
            return
        if analysis.delivery_decision == "suppress":
            return
        if intent.send_after is not None and intent.send_after > datetime.now(timezone.utc):
            return

        judge_output = await self._repository.load_judge_output_render_fields(analysis.judge_output_id)
        candidate = await self._repository.load_candidate_render_context(intent.candidate_group_id)
        if judge_output is None or candidate is None:
            return

        async with self._repository.transaction():
            existing_plan = await self._repository.load_notification_plan(intent.notification_plan_id)
            if existing_plan is None:
                existing_by_material = await self._repository.load_existing_plan_by_material(
                    analysis_id=intent.analysis_id,
                    target_chat_id=intent.target_chat_id,
                    material_change_hash=intent.material_change_hash,
                )
                if existing_by_material is None:
                    await self._repository.insert_notification_plan(
                        NotificationPlanDraft(
                            notification_plan_id=intent.notification_plan_id,
                            analysis_id=intent.analysis_id,
                            candidate_group_id=intent.candidate_group_id,
                            delivery_decision=intent.delivery_decision,
                            urgency_profile=intent.urgency_profile,
                            target_chat_id=intent.target_chat_id,
                            target_thread_id=intent.target_thread_id,
                            render_profile=intent.render_profile,
                            dedupe_subject_key=intent.dedupe_subject_key,
                            material_change_hash=intent.material_change_hash,
                            send_after=intent.send_after,
                            suppress_reason_code=intent.suppress_reason_code,
                            status="planned",
                        )
                    )
                else:
                    # same material already concretized; nothing new to deliver
                    return

        render = self._renderer.render(
            notification_plan_id=intent.notification_plan_id,
            payload=RenderInput(
                analysis=analysis,
                judge_output=judge_output,
                candidate=candidate,
            ),
        )

        existing_render = await self._repository.load_render_by_hash(
            notification_plan_id=intent.notification_plan_id,
            render_hash=render.render_hash,
        )
        if existing_render is None:
            async with self._repository.transaction():
                await self._repository.insert_notification_render(render)
                await self._repository.update_plan_status(
                    notification_plan_id=intent.notification_plan_id,
                    status="rendered",
                )

        action = await self._decide_delivery_action(intent=intent, candidate=candidate)
        result = await self._perform_delivery(intent=intent, render=render, action=action)

        async with self._repository.transaction():
            if result.delivery_status == "failed_retryable":
                next_retry_at = self._compute_next_retry_at(result.retry_after_seconds)
                await self._repository.reschedule_retry(
                    notification_plan_id=intent.notification_plan_id,
                    next_retry_at=next_retry_at,
                )
            else:
                await self._repository.update_plan_status(
                    notification_plan_id=intent.notification_plan_id,
                    status=result.delivery_status,
                )

            record_id = await self._repository.insert_delivery_record(
                notification_plan_id=intent.notification_plan_id,
                delivery_status=result.delivery_status,
                telegram_chat_id=result.telegram_chat_id,
                telegram_message_id=result.telegram_message_id,
                attempt_count=result.attempt_count,
                transport_error_code=result.transport_error_code,
                transport_error_class=result.transport_error_class,
                telegram_response_json=result.telegram_response_json,
            )
            await self._repository.insert_state_transition(
                object_type="notification_plan",
                object_id=intent.notification_plan_id,
                from_state="queued" if result.delivery_status in {"sent", "edited", "failed_retryable", "failed_terminal"} else "rendered",
                to_state=result.delivery_status,
                reason_code=action.reason_code or "telegram_delivery_result",
            )
            await self._repository.insert_delivery_result_outbox(
                notification_plan_id=intent.notification_plan_id,
                delivery_status=result.delivery_status,
                telegram_chat_id=result.telegram_chat_id,
                telegram_message_id=result.telegram_message_id,
                notification_delivery_record_id=record_id,
                attempt_count=result.attempt_count,
                transport_error_code=result.transport_error_code,
                transport_error_class=result.transport_error_class,
            )

    async def _decide_delivery_action(self, *, intent, candidate) -> DeliveryAction:
        if intent.delivery_decision == "send_digest" and not self._config.enable_digest_runtime:
            return DeliveryAction(mode="noop", reason_code="digest_runtime_disabled")

        if self._config.dry_run:
            return DeliveryAction(mode="noop", reason_code="dry_run_skip_transport")

        recent = await self._repository.load_recent_delivery_for_subject(
            dedupe_subject_key=intent.dedupe_subject_key,
            target_chat_id=intent.target_chat_id,
            within_minutes=self._config.edit_window_minutes,
        )
        if recent is None:
            return DeliveryAction(mode="send", reason_code="no_recent_delivery")

        if recent.material_change_hash == intent.material_change_hash:
            return DeliveryAction(mode="noop", reason_code="same_material_already_delivered")

        if not self._config.allow_edits:
            return DeliveryAction(mode="send", reason_code="edits_disabled")

        if recent.telegram_message_id is None:
            return DeliveryAction(mode="send", reason_code="missing_recent_message_id")

        if recent.primary_canonical_url != candidate.primary_canonical_url:
            return DeliveryAction(mode="send", reason_code="primary_subject_changed")

        if recent.urgency_profile != intent.urgency_profile:
            return DeliveryAction(mode="send", reason_code="urgency_changed")

        if recent.render_profile != intent.render_profile:
            return DeliveryAction(mode="send", reason_code="render_profile_changed")

        return DeliveryAction(
            mode="edit",
            existing_message_id=recent.telegram_message_id,
            reason_code="recent_same_subject_material_changed",
        )

    async def _perform_delivery(self, *, intent, render, action: DeliveryAction) -> DeliveryResult:
        if action.mode == "noop":
            status = "suppressed" if action.reason_code in {"dry_run_skip_transport", "digest_runtime_disabled"} else "suppressed"
            return DeliveryResult(
                delivery_status=status,
                telegram_chat_id=intent.target_chat_id,
                telegram_message_id=action.existing_message_id,
                attempt_count=0,
                telegram_response_json={"noop": True, "reason_code": action.reason_code, "dry_run": self._config.dry_run},
            )

        async with self._repository.transaction():
            await self._repository.update_plan_status(
                notification_plan_id=intent.notification_plan_id,
                status="queued",
            )

        try:
            if action.mode == "edit" and action.existing_message_id is not None:
                response = await self._telegram_client.edit_message_text(
                    chat_id=intent.target_chat_id,
                    message_id=action.existing_message_id,
                    text=render.message_text,
                    entities=render.entities_json,
                    reply_markup=render.reply_markup_json,
                    link_preview_options=render.link_preview_options_json,
                )
                return DeliveryResult(
                    delivery_status="edited",
                    telegram_chat_id=intent.target_chat_id,
                    telegram_message_id=action.existing_message_id,
                    attempt_count=1,
                    telegram_response_json=response,
                    edited=True,
                )

            response = await self._telegram_client.send_message(
                chat_id=intent.target_chat_id,
                text=render.message_text,
                entities=render.entities_json,
                reply_markup=render.reply_markup_json,
                disable_notification=render.disable_notification,
                link_preview_options=render.link_preview_options_json,
                message_thread_id=intent.target_thread_id,
            )
            message = response.get("result") or {}
            return DeliveryResult(
                delivery_status="sent",
                telegram_chat_id=int(message.get("chat", {}).get("id", intent.target_chat_id)),
                telegram_message_id=int(message.get("message_id")),
                attempt_count=1,
                telegram_response_json=response,
            )
        except TelegramTransportNoopError:
            return DeliveryResult(
                delivery_status="suppressed",
                telegram_chat_id=intent.target_chat_id,
                telegram_message_id=action.existing_message_id,
                attempt_count=1,
                telegram_response_json={"noop": True, "reason_code": "telegram_edit_not_modified_noop"},
            )
        except TelegramTransportRetryableError as exc:
            return DeliveryResult(
                delivery_status="failed_retryable",
                telegram_chat_id=intent.target_chat_id,
                telegram_message_id=action.existing_message_id,
                attempt_count=1,
                transport_error_code="telegram_retryable",
                transport_error_class=type(exc).__name__,
                retry_after_seconds=exc.retry_after_seconds,
            )
        except TelegramTransportTerminalError as exc:
            return DeliveryResult(
                delivery_status="failed_terminal",
                telegram_chat_id=intent.target_chat_id,
                telegram_message_id=action.existing_message_id,
                attempt_count=1,
                transport_error_code="telegram_terminal",
                transport_error_class=type(exc).__name__,
                telegram_response_json={"error": str(exc)},
            )

    @staticmethod
    def _compute_next_retry_at(retry_after_seconds: int | None) -> datetime:
        seconds = retry_after_seconds if retry_after_seconds is not None else 30
        return datetime.now(timezone.utc) + timedelta(seconds=max(1, seconds))
```

---

## 7-6. `src/services/notifier_telegram/worker.py` (tiny update)

```python
from __future__ import annotations

import asyncio
import logging


class NotifierTelegramWorker:
    def __init__(self, config, *, consumer, service, logger: logging.Logger | None = None) -> None:
        self._config = config
        self._consumer = consumer
        self._service = service
        self._logger = logger or logging.getLogger(__name__)
        self._stop_event = asyncio.Event()

    async def run_forever(self) -> None:
        await self._consumer.ensure_group()
        while not self._stop_event.is_set():
            batch = await self._consumer.read_batch()
            if not batch:
                await asyncio.sleep(0)
                continue
            for message in batch:
                trigger_event_id = message.fields.get("trigger_event_id")
                if not trigger_event_id:
                    await self._consumer.ack(message.message_id)
                    continue
                intent = await self._service.rehydrate_intent(trigger_event_id)
                if intent is not None:
                    await self._service.handle_intent(intent)
                await self._consumer.ack(message.message_id)

    async def stop(self) -> None:
        self._stop_event.set()
```

---

## 8. 테스트 초안 포인트

### `tests/unit/services/notifier_telegram/test_plan_concretization_idempotency.py`

검증:
- 같은 `notification_plan_id` 재도착 시 새 plan row가 생기지 않는지
- 같은 `(analysis_id, target_chat_id, material_change_hash)` 재도착 시 새 plan row가 생기지 않는지

### `tests/unit/services/notifier_telegram/test_duplicate_delivery_guard.py`

검증:
- same material already delivered면 transport 없이 noop 되는지
- same render hash면 새 render row가 생기지 않는지

### `tests/unit/services/notifier_telegram/test_send_vs_edit_rules.py`

검증:
- urgency 상승이면 edit가 아니라 send를 고르는지
- primary canonical URL 변경이면 send를 고르는지
- edits disabled면 무조건 send/noop인지

### `tests/unit/services/notifier_telegram/test_retryable_terminal_classification.py`

검증:
- 429 / retry_after는 retryable인지
- timeout/network는 retryable인지
- bot blocked / invalid chat / cannot edit는 terminal인지
- `message is not modified`는 noop으로 분기되는지

### `tests/unit/services/notifier_telegram/test_send_after_gate.py`

검증:
- `send_after > now()` 이면 transport를 건너뛰는지

### `tests/unit/services/notifier_telegram/test_dry_run_path.py`

검증:
- dry-run에서 render는 생성되지만 실제 transport는 호출되지 않는지
- delivery status가 `suppressed` 로 남는지

### `tests/component/services/notifier_telegram/test_existing_material_noop.py`

검증:
- same material delivered state에서 새 Telegram API 호출이 없는지

### `tests/component/services/notifier_telegram/test_retryable_failure_reschedules_send_after.py`

검증:
- retryable failure 후 `notification_plans.status = failed_retryable`
- `send_after`가 미래 시점으로 갱신되는지

### `tests/component/services/notifier_telegram/test_429_retry_after_backoff.py`

검증:
- Telegram `retry_after` 값을 우선 사용해 reschedule 하는지

### `tests/component/services/notifier_telegram/test_replay_safe_dry_run_no_transport.py`

검증:
- replay/dev dry-run에서 live message send/edit가 발생하지 않는지

### `tests/component/services/notifier_telegram/test_transition_sequence_consistency.py`

검증:
- `planned -> rendered -> queued -> sent/edited/failed_*` 흐름이 일관되게 남는지

---

## 9. compose / runtime acceptance checklist (notifier only)

다음 항목이 맞으면 notifier hardening v0.1은 acceptance 가능 상태로 본다.

1. `notification.plan.created.v1` 가 `q.notification.send` 로 정상 라우팅된다.
2. notifier는 thin Redis payload가 아니라 `event_outbox` 기준으로 intent를 재hydrate 한다.
3. 같은 intent 재소비 시 새 `notification_plans` row가 중복 생성되지 않는다.
4. same material delivery는 Telegram transport를 다시 치지 않는다.
5. retryable transport는 `send_after`로 재스케줄된다.
6. dry-run/replay-safe 모드에서 Telegram transport는 호출되지 않는다.
7. prod secret은 bot token만 사용하고 collector secret과 섞이지 않는다.
8. `notification_renders`, `notification_delivery_records`, `state_transitions`, `notification.delivery.result.v1` 사이 연결이 audit 가능하게 남는다.

---

## 10. 이번 단계가 구조를 지키는 이유

1. notifier는 여전히 `notification_*`, `state_transitions`, `event_outbox`만 직접 쓴다.  
   즉, service ownership을 넘지 않는다.

2. retry/backoff는 `send_after` 재사용으로 처리한다.  
   즉, schema/ownership 변경 없이 hardening 된다.

3. `material_change_hash`는 upstream truth로 유지하고, `render_hash`만 notifier 내부 exact dedupe에 쓴다.  
   즉, semantic 판단과 presentation dedupe가 분리된다.

4. dry-run/replay-safe를 넣되 새 enum/lifecycle을 만들지 않는다.  
   즉, contracts를 최소 변경으로 유지한다.

5. send/edit/noop를 더 보수적으로 나누고 urgency escalation에서는 new send를 강제한다.  
   즉, stage 7의 “single-shot send 기본, material edit only 예외” 원칙이 더 정확해진다.

---

## 11. 다음 단계

이 hardening이 닫히면 다음 안전한 구현 순서는 아래가 맞다.

1. `40_end_to_end_delivery_acceptance_and_compose_hardening_v0_1.md`
   - q.notification.send end-to-end wiring
   - feature flag defaults 점검
   - maintenance / replay handoff 점검
   - notifier failure rollback / retry promotion 점검
   - prod/dev dry-run 기본값 점검

2. 그 다음에 파일 수가 더 늘면
   - `10_delivery_hardening_stage39_plus_v0_1.md`
   같은 통합 번들로 묶는다.

즉, 다음 단계는 **새 구조 발명** 이 아니라 **delivery acceptance + compose hardening** 이다.

---

## 최종 한 줄 결론

이번 단계의 최소-change 정답은 **`notification.plan.created.v1` plan-intent를 idempotent하게 concretize하고, same-material duplicate를 no-op 처리하고, send-after 기반 retry/backoff와 dry-run/replay-safe를 추가하며, send-vs-edit 판단을 urgency/primary-subject/material-change 기준으로 보수적으로 강화해서, notifier delivery skeleton을 실제 운영 가능한 첫 hardening 상태로 닫는 것** 이다.



## Source file: `40_end_to_end_delivery_acceptance_and_compose_hardening_v0_1.md`

# 40단계: end-to-end delivery acceptance and compose hardening v0.1

## 0. 문서 목적

이 문서는 `39_notifier_telegram_integration_hardening_v0_1.md`를 뒤엎는 문서가 아니다.  
목적은 **이미 잠긴 notifier delivery hardening을 유지한 채, end-to-end delivery runtime/compose acceptance 층만 좁게 닫는 것**이다.

이번 단계에서 닫는 것은 정확히 아래 여덟 가지다.

1. `notification.plan.created.v1 -> q.notification.send -> notifier-telegram` **end-to-end wiring acceptance**
2. `send_after` 기반 **retry/backoff가 notifier 내부 상태에서 끝나지 않고 운영 경계까지 이어지는지** 고정
3. `ENABLE_NOTIFICATION_SEND`, `NOTIFIER_TELEGRAM_DRY_RUN`, `NOTIFIER_TELEGRAM_ALLOW_EDITS`의 **환경별 기본값 매트릭스** 고정
4. prod/dev/replay에서 **실제 Telegram transport가 언제 허용되고 언제 금지되는지** 고정
5. `notification.delivery.result.v1 -> q.maintenance` **handoff 의미** 고정
6. notifier failure 시 **rollback / recovery / delivery replay** 경계를 stage 8~10 정본과 연결
7. compose/runtime 기준 **운영 acceptance checklist** 고정
8. 다음 단계가 새 구조 발명이 아니라 **delivery replay / retry promotion hardening** 임을 명확히 고정

핵심 전제는 그대로 유지한다.

- `notifier-telegram`은 **policy-engine이 아니다**.
- `notifier-telegram`은 **judge가 아니다**.
- `notifier-telegram`은 **verdict / delivery_decision을 재계산하지 않는다**.
- durable truth는 여전히 **PostgreSQL** 이고, Redis는 **short-lived queue state** 다.
- delivery replay는 stage 8 정본대로 **NotificationPlan에서 다시 시작하는 좁은 replay** 다.

---

## 1. 왜 지금 이 단계가 정확한 다음 단계인가

현재 authoritative README v14 기준 최신 상태는 stage 39까지 닫힌 상태이고, 다음 안전한 작업은 명시적으로 아래 하나였다.

- `40_end_to_end_delivery_acceptance_and_compose_hardening_v0_1.md`

즉, 지금 collector / normalizer / enrichers / judge / policy를 다시 여는 것은 순서상 후퇴다.  
지금 닫아야 하는 것은 **delivery hardening 이후의 runtime acceptance** 다.

---

## 2. stage 39 이후에도 남아 있는 운영 공백

### 2-1. `send_after`는 notifier 내부 규칙으로는 닫혔지만, compose/runtime 기본값과 연결되지 않았다

39단계는 아래를 닫았다.

- retryable failure -> `notification_plans.send_after = next_retry_at`
- `send_after > now()` 이면 notifier transport skip
- dry-run / replay-safe transport skip

하지만 아직 아래는 고정되지 않았다.

- 어떤 서비스가 due retry를 다시 `q.notification.send`로 올릴 것인가
- prod/dev/replay에서 `send_after` 동작이 어떤 env 기본값 아래 검증되는가
- rollback 상황에서 retry promotion을 계속 둘 것인가 끌 것인가

즉, **상태는 닫혔지만 운영 wiring은 아직 닫히지 않았다.**

### 2-2. `ENABLE_NOTIFICATION_SEND=false` 의 의미가 운영 기준으로 고정되지 않았다

stage 10 정본은 notifier 문제 시 아래 rollback을 권장한다.

- `ENABLE_NOTIFICATION_SEND=false`
- analysis는 계속 생성
- 나중에 delivery replay 가능

그런데 이걸 현재 runtime에 그대로 매핑하면 해석이 둘로 갈린다.

1. notifier worker 자체를 정지
2. notifier는 consume/concretize는 계속하되 transport만 막기

이 문서에서는 **2번을 최소-change 정답** 으로 고정한다.

이유:

- `notification_plans` concretization이 notifier ownership 안에서 유지된다.
- queue 적체 없이 analysis / plan durable row를 남길 수 있다.
- recovery는 stage 8 delivery replay로 좁게 연결할 수 있다.

### 2-3. `notification.delivery.result.v1 -> q.maintenance` 의 의미가 acceptance 레벨로 닫히지 않았다

26단계 outbox-relay는 이미 `notification.delivery.result.v1 -> q.maintenance` 를 잠갔다.  
하지만 지금까지는 이것이 “있다” 수준이지, **무엇을 위해 쓰는지** 가 운영 acceptance로 닫히지 않았다.

이번 단계에서는 아래처럼 고정한다.

- notifier는 `notification.delivery.result.v1`를 **결과 통지 이벤트** 로만 발행한다.
- maintenance는 이 이벤트와 `notification_plans.status/send_after`를 읽어 **due retry promotion / replay support** 를 담당한다.
- notifier는 장기 sleep worker가 아니며, retry budget orchestration의 중심이 아니다.

즉, **notifier는 transport 경계, maintenance는 retry/replay orchestration 경계** 다.

### 2-4. prod/dev/replay 기본값이 충분히 보수적으로 잠겨 있지 않다

현재 stage 39만 보면 dry-run 플래그는 있지만, 아래가 운영 문서 수준으로 잠겨 있지 않다.

- dev 기본값
- replay/manual acceptance 기본값
- prod rollout 전 기본값
- prod rollback 시 복귀값

이 상태로 두면 “실수로 dev/replay에서 live chat을 건드리는” 사고가 남는다.

---

## 3. 이번 단계에서 드러나는 충돌과 최소-change 해석

## 3-1. 충돌 A — maintenance ownership에는 `notification_plans` 쓰기가 없다

execution contracts 기준 maintenance 직접 소유 durable write는 아래다.

- `pipeline_runs`
- `job_attempts`
- `dead_letter_entries`
- `replay_requests`
- `event_outbox`

즉, maintenance가 retry promotion을 위해 `notification_plans`를 직접 수정하면 ownership을 넓히게 된다.

### 최소-change 해석 A

이번 단계에서는 아래처럼 고정한다.

- retryable 실패 시 `notification_plans.send_after` 갱신은 **notifier가 직접 수행** 한다.
- maintenance는 `notification_plans`를 **읽기만** 한다.
- due row를 찾으면 maintenance는 `event_outbox`에 **새 `notification.plan.created.v1` retry-intent** 를 append 한다.
- notifier는 그 event를 다시 consume해 같은 `notification_plan_id` 기준으로 rehydrate 한다.

즉, maintenance는 **plan mutate가 아니라 retry re-dispatch only** 를 담당한다.

---

## 3-2. 충돌 B — rollback 중에도 notifier worker를 살릴 것인가

worker를 통째로 내리면 transport는 멈추지만 아래 문제가 생긴다.

- 새 `notification.plan.created.v1`가 durable `notification_plans`로 concretize 되지 않음
- 이후 delivery replay 출발점이 event_outbox에만 남음
- notifier ownership 경계가 흐려짐

### 최소-change 해석 B

- worker는 살아 있다.
- `ENABLE_NOTIFICATION_SEND=false` 일 때 notifier는 아래까지만 수행한다.
  1. plan-intent rehydrate
  2. plan concretization
  3. optional render append
  4. **Telegram transport skip**
  5. `suppressed` 성격의 durable 흔적 남김
- recovery는 explicit delivery replay 또는 maintenance 재promotion으로 처리한다.

즉, **transport만 끄고 ownership chain은 유지** 한다.

---

## 3-3. 충돌 C — dry-run 과 send-disabled rollback 은 비슷하지만 같은 것은 아니다

둘 다 실제 transport를 하지 않지만, 의미가 다르다.

- dry-run: dev/replay safety
- send-disabled: prod rollback / gated rollout

### 최소-change 해석 C

둘 다 새 enum은 만들지 않되, `reason_code` 와 `telegram_response_json` 메타데이터로 구분한다.

- dry-run skip
  - `reason_code = dry_run_skip_transport`
  - `telegram_response_json = {"dry_run": true, ...}`

- send-disabled skip
  - `reason_code = notification_send_flag_disabled`
  - `telegram_response_json = {"send_disabled": true, ...}`

즉, **같은 suppress 계열이지만 운영 의미는 reason metadata로 분리** 한다.

---

## 3-4. 충돌 D — delivery replay는 어디서부터 다시 시작할 것인가

stage 8 정본은 replay를 다섯 종류로 나누고, delivery replay 대상은 `NotificationPlan` 으로 잠갔다.

### 최소-change 해석 D

이번 단계에서는 delivery replay를 아래처럼 고정한다.

- root object = `notification_plan_id`
- 다시 도는 단계 = stage 7부터
- upstream `analysis`, `judge`, `bundle`은 다시 계산하지 않음
- 재출발 이벤트는 `notification.plan.created.v1` retry-intent 또는 explicit `replay.requested.v1 (delivery)` 다

즉, **delivery failure 복구 때문에 upstream pipeline을 다시 돌리지 않는다.**

---

## 4. 이번 단계에서 고정할 범위와 제외 범위

### 포함

- `q.notification.send` end-to-end acceptance
- `send_after` due-state handoff 규칙
- compose env default matrix
- prod/dev/replay transport gating
- maintenance retry promotion interpretation
- delivery replay entry boundary
- notifier rollback / recovery runbook
- acceptance checklist

### 제외

- 새 queue 추가
- notifier가 retry scheduler 전체를 흡수하는 구조
- digest runtime 활성화
- Prompt Guard lifecycle 추가
- MemKraft runtime retrieval
- replay engine 전체 구현
- stage 39+ bundle 생성

즉, 이번 문서는 **새 service나 새 lifecycle을 만드는 턴이 아니라, stage 39를 운영 가능한 compose/runtime 기준으로 닫는 턴** 이다.

---

## 5. 대상 파일 트리

```text
compose/
  docker-compose.yml                 # updated fragment
  env/
    notifier.prod.env.example       # new
    notifier.dev.env.example        # new
    notifier.replay.env.example     # new

ops/
  runbooks/
    notifier_rollback.md            # new
    delivery_replay.md              # new
    notifier_acceptance_checklist.md # new

tests/
  component/
    services/
      notifier_telegram/
        test_notification_send_flag_disabled_no_transport.py   # new
        test_due_retry_intent_re_emitted_by_maintenance.py    # new
        test_prod_vs_replay_env_defaults.py                   # new
        test_delivery_replay_starts_from_notification_plan.py # new

  integration/
    delivery/
      test_q_notification_send_end_to_end.py                  # new
      test_retryable_failure_to_maintenance_handoff.py        # new
      test_rollback_disable_send_preserves_plan_rows.py       # new
```

주의:

- 이번 단계는 notifier code skeleton을 재설계하지 않는다.
- compose/env/runbook/acceptance 가 중심이다.
- maintenance retry promotion 구현은 **event_outbox append + read-only scan** 수준으로만 고정한다.

---

## 6. compose / runtime hardening 규칙

## 6-1. 서비스 간 E2E 경로를 아래로 고정한다

```text
policy-engine
  -> event_outbox(notification.plan.created.v1)
  -> outbox-relay
  -> q.notification.send
  -> notifier-telegram
  -> notification_plans / notification_renders / notification_delivery_records / state_transitions
  -> event_outbox(notification.delivery.result.v1)
  -> outbox-relay
  -> q.maintenance
  -> maintenance
  -> due retry intent (notification.plan.created.v1 retry-intent) or replay.requested.v1(delivery)
```

핵심은 이것이다.

- `q.notification.send`는 **plan-intent consumption queue** 다.
- retryable failure 후의 재진입도 **같은 queue** 를 재사용한다.
- 새 retry queue를 만들지 않는다.

---

## 6-2. 환경별 기본값 매트릭스

### prod baseline (rollout 전 / rollback 후)

```env
APP_ENV=prod
ENABLE_NOTIFICATION_SEND=false
NOTIFIER_TELEGRAM_DRY_RUN=false
NOTIFIER_TELEGRAM_ALLOW_EDITS=true
ENABLE_LATER_DELIVERY=true
ENABLE_SILENT_LATER=true
ENABLE_REPLAY_TO_PROD_DB=false
MAINTENANCE_ENABLE_NOTIFICATION_RETRY_PROMOTION=false
```

설명:

- prod에서도 notifier worker는 뜬다.
- 하지만 transport는 막는다.
- retry promotion도 기본 off 다.
- 이 상태는 restricted/full rollout 직전 대기 상태 또는 rollback 직후 안정화 상태다.

### prod restricted/full delivery

```env
APP_ENV=prod
ENABLE_NOTIFICATION_SEND=true
NOTIFIER_TELEGRAM_DRY_RUN=false
NOTIFIER_TELEGRAM_ALLOW_EDITS=true
ENABLE_LATER_DELIVERY=true
ENABLE_SILENT_LATER=true
ENABLE_REPLAY_TO_PROD_DB=false
MAINTENANCE_ENABLE_NOTIFICATION_RETRY_PROMOTION=true
```

설명:

- 실제 Telegram transport 허용
- retryable failure 후 due retry promotion 허용
- edit 허용

### dev / test baseline

```env
APP_ENV=dev
ENABLE_NOTIFICATION_SEND=false
NOTIFIER_TELEGRAM_DRY_RUN=true
NOTIFIER_TELEGRAM_ALLOW_EDITS=false
ENABLE_LATER_DELIVERY=true
ENABLE_SILENT_LATER=true
ENABLE_REPLAY_TO_PROD_DB=false
MAINTENANCE_ENABLE_NOTIFICATION_RETRY_PROMOTION=false
```

설명:

- plan/render 흐름은 검증 가능
- 실제 send/edit는 금지
- historical live message 훼손 방지

### replay / manual acceptance baseline

```env
APP_ENV=replay
ENABLE_NOTIFICATION_SEND=false
NOTIFIER_TELEGRAM_DRY_RUN=true
NOTIFIER_TELEGRAM_ALLOW_EDITS=false
ENABLE_LATER_DELIVERY=true
ENABLE_SILENT_LATER=true
ENABLE_REPLAY_TO_PROD_DB=false
MAINTENANCE_ENABLE_NOTIFICATION_RETRY_PROMOTION=false
```

설명:

- delivery replay는 구조 검증용으로만 수행
- live transport는 금지
- explicit operator 승인 없이는 prod send로 넘어가지 않음

---

## 6-3. compose fragment 권장안

### `compose/docker-compose.yml` fragment

```yaml
services:
  notifier-telegram:
    restart: unless-stopped
    depends_on:
      - postgres
      - redis
    env_file:
      - ./env/base.env
      - ./env/notifier.${APP_ENV}.env
    environment:
      DATABASE_URL: ${DATABASE_URL}
      REDIS_URL: ${REDIS_URL}
      ENABLE_NOTIFICATION_SEND: ${ENABLE_NOTIFICATION_SEND:-false}
      ENABLE_LATER_DELIVERY: ${ENABLE_LATER_DELIVERY:-true}
      ENABLE_SILENT_LATER: ${ENABLE_SILENT_LATER:-true}
      NOTIFIER_TELEGRAM_DRY_RUN: ${NOTIFIER_TELEGRAM_DRY_RUN:-false}
      NOTIFIER_TELEGRAM_ALLOW_EDITS: ${NOTIFIER_TELEGRAM_ALLOW_EDITS:-true}
    command: ["python", "-m", "src.services.notifier_telegram.main"]

  maintenance:
    restart: unless-stopped
    depends_on:
      - postgres
      - redis
    env_file:
      - ./env/base.env
      - ./env/notifier.${APP_ENV}.env
    environment:
      DATABASE_URL: ${DATABASE_URL}
      REDIS_URL: ${REDIS_URL}
      MAINTENANCE_ENABLE_NOTIFICATION_RETRY_PROMOTION: ${MAINTENANCE_ENABLE_NOTIFICATION_RETRY_PROMOTION:-false}
      MAINTENANCE_NOTIFICATION_RETRY_BATCH_SIZE: ${MAINTENANCE_NOTIFICATION_RETRY_BATCH_SIZE:-100}
      MAINTENANCE_NOTIFICATION_RETRY_POLL_SEC: ${MAINTENANCE_NOTIFICATION_RETRY_POLL_SEC:-30}
    command: ["python", "-m", "src.services.maintenance.main"]
```

주의:

- 새 queue를 추가하지 않는다.
- notifier와 maintenance는 같은 env family를 공유하지만 secret ownership은 섞지 않는다.
- `ENABLE_NOTIFICATION_SEND=false` 라도 notifier service는 내리지 않는다.

---

## 6-4. `send_after`와 due retry promotion 규칙

### notifier 책임

retryable failure 시 notifier는 아래까지만 책임진다.

1. `notification_plans.status = failed_retryable`
2. `notification_plans.send_after = next_retry_at`
3. `notification_delivery_records` append
4. `state_transitions` append
5. `notification.delivery.result.v1` emit

### maintenance 책임

maintenance는 주기적으로 아래 due row를 읽는다.

```sql
SELECT notification_plan_id
FROM notification_plans
WHERE status = 'failed_retryable'::notification_status_enum
  AND send_after IS NOT NULL
  AND send_after <= now()
ORDER BY send_after ASC, created_at ASC
LIMIT :batch_size;
```

그리고 각 row에 대해 **새 retry-intent event** 를 append 한다.

권장 dedupe key:

```text
notify:retry-intent:{notification_plan_id}:{epoch_send_after_or_attempt_no}
```

payload 최소 필드:

```json
{
  "notification_plan_id": "...",
  "analysis_id": "...",
  "candidate_group_id": "...",
  "delivery_decision": "send_now",
  "urgency_profile": "high|normal_silent|digest|suppressed",
  "target_chat_id": 123,
  "target_thread_id": null,
  "render_profile": "single_alert_v1",
  "dedupe_subject_key": "...",
  "material_change_hash": "...",
  "send_after": null,
  "retry_reason": "due_retry_promotion"
}
```

중요:

- maintenance는 `notification_plans` row 자체를 덮어쓰지 않는다.
- notifier는 같은 `notification_plan_id`를 기준으로 rehydrate 한다.
- 다시 transport에 성공하면 새 delivery record가 append 된다.

---

## 6-5. `ENABLE_NOTIFICATION_SEND=false` rollback 규칙

이 flag가 false면 notifier는 아래처럼 동작한다.

1. event rehydrate
2. plan concretization
3. render append 가능
4. **Telegram transport 금지**
5. `notification_delivery_records.delivery_status = suppressed`
6. `state_transitions.reason_code = notification_send_flag_disabled`
7. `notification.delivery.result.v1` emit은 optional 이지만, v0.1에서는 **emit 허용** 을 권장한다

이 경로를 쓰는 이유:

- queue 적체를 막는다.
- plan durable row를 보존한다.
- 나중에 delivery replay로 복구할 수 있다.

즉, **rollback은 upstream pipeline 중단이 아니라 transport stop** 이다.

---

## 6-6. delivery replay entry 규칙

stage 8 정본에 맞춰 delivery replay는 아래처럼 고정한다.

### replay root

- `root_object_type = notification_plan`
- `root_object_id = notification_plan_id`
- `replay_type = delivery`

### replay가 다시 도는 범위

- notifier concretization 재검증
- render 재생성 가능
- Telegram transport 재시도

### replay가 다시 돌지 않는 범위

- `analysis`
- `judge_output`
- `bundle`
- `candidate`
- `artifact`

즉, **delivery recovery는 좁게 복구** 한다.

---

## 6-7. notifier failure rollback / recovery runbook 규칙

### A. notifier 중복 전송 / 템플릿 이상 / flood-control 폭증

즉시 조치:

```text
ENABLE_NOTIFICATION_SEND=false
MAINTENANCE_ENABLE_NOTIFICATION_RETRY_PROMOTION=false
```

그 다음:

1. notifier worker는 살아 있게 둔다
2. 새 plan durable row는 계속 만든다
3. 실제 Telegram transport는 막는다
4. 원인 수정 후
5. explicit delivery replay 또는 retry-intent 재promotion으로 복구한다

### B. Telegram API 장애 (429/5xx)

즉시 조치:

- send flag는 유지 가능
- notifier는 retryable 로만 남김
- maintenance promotion interval만 운영 조정 가능
- 필요 시 일시적으로 `MAINTENANCE_ENABLE_NOTIFICATION_RETRY_PROMOTION=false`

### C. dev/replay 실수 방지

항상 확인:

```text
ENABLE_NOTIFICATION_SEND=false
NOTIFIER_TELEGRAM_DRY_RUN=true
NOTIFIER_TELEGRAM_ALLOW_EDITS=false
```

즉, **dev/replay 안전성은 compose default에서 먼저 막는다.**

---

## 7. acceptance checklist

## 7-1. queue wiring acceptance

- [ ] `notification.plan.created.v1` 가 outbox-relay에서 `q.notification.send` 로 라우팅된다.
- [ ] notifier worker는 thin Redis payload만 읽고 `trigger_event_id` 기준으로 `event_outbox` rehydrate를 한다.
- [ ] `notification.delivery.result.v1` 가 outbox-relay에서 `q.maintenance` 로 라우팅된다.

## 7-2. notifier runtime acceptance

- [ ] `send_after > now()` 면 transport가 발생하지 않는다.
- [ ] same-material duplicate는 no-op 처리된다.
- [ ] send-vs-edit는 urgency 상승 / primary subject 변경 / edit window 규칙을 따른다.
- [ ] `message is not modified` 는 logical no-op 로 기록된다.

## 7-3. environment safety acceptance

- [ ] dev/test 기본값은 `ENABLE_NOTIFICATION_SEND=false`, `NOTIFIER_TELEGRAM_DRY_RUN=true` 다.
- [ ] replay/manual 기본값은 send/edit 둘 다 금지다.
- [ ] prod baseline은 send-disabled 상태로 기동 가능하다.
- [ ] restricted/full rollout 에서만 실제 transport가 허용된다.

## 7-4. retry / maintenance acceptance

- [ ] retryable failure 후 `notification_plans.send_after` 가 미래 시점으로 갱신된다.
- [ ] due retry row를 maintenance가 읽어 retry-intent event를 다시 발행할 수 있다.
- [ ] notifier는 same `notification_plan_id` 로 재hydrate 한다.
- [ ] rollback 중에는 retry promotion도 함께 끌 수 있다.

## 7-5. replay / rollback acceptance

- [ ] send-disabled rollback 시 analysis와 notification plan durable row는 계속 생성된다.
- [ ] rollback 후 recovery는 delivery replay로만 좁게 수행 가능하다.
- [ ] delivery replay는 upstream `analysis` / `judge` / `bundle` 을 다시 계산하지 않는다.

---

## 8. 테스트 초안 포인트

### `tests/integration/delivery/test_q_notification_send_end_to_end.py`

검증:
- `notification.plan.created.v1`
- `event_outbox`
- `outbox-relay`
- `q.notification.send`
- `notifier-telegram`
연결이 end-to-end 로 닫히는지

### `tests/component/services/notifier_telegram/test_notification_send_flag_disabled_no_transport.py`

검증:
- `ENABLE_NOTIFICATION_SEND=false`
- plan concretization은 수행
- Telegram transport는 수행하지 않음
- delivery record/state transition reason 이 `notification_send_flag_disabled` 로 남는지

### `tests/integration/delivery/test_retryable_failure_to_maintenance_handoff.py`

검증:
- retryable transport failure
- `send_after` future update
- `notification.delivery.result.v1 -> q.maintenance`
- maintenance due promotion 가능성 확인

### `tests/component/services/notifier_telegram/test_due_retry_intent_re_emitted_by_maintenance.py`

검증:
- maintenance가 due `failed_retryable` row를 읽음
- 새 retry-intent `notification.plan.created.v1` 를 append
- notifier가 같은 `notification_plan_id` 기준으로 다시 집행 가능한지

### `tests/integration/delivery/test_rollback_disable_send_preserves_plan_rows.py`

검증:
- prod-like env에서 send flag off
- analysis / plan / render durable row 유지
- transport만 빠지는지

### `tests/component/services/notifier_telegram/test_delivery_replay_starts_from_notification_plan.py`

검증:
- replay root = `notification_plan`
- upstream bundle/judge/analysis 재실행 없이 notifier만 다시 타는지

---

## 9. compose / runtime acceptance checklist (delivery only)

아래 항목이 맞으면 stage 40은 acceptance 가능 상태로 본다.

1. `q.notification.send` end-to-end wiring이 깨지지 않는다.
2. notifier는 worker stop 없이도 send-disabled rollback을 수행할 수 있다.
3. `send_after` 기반 retry 상태가 durable 하게 유지된다.
4. maintenance는 read-only scan + outbox append 방식으로 due retry promotion을 지원할 수 있다.
5. dev/replay는 dry-run defaults 때문에 live Telegram transport를 건드리지 않는다.
6. delivery replay는 stage 8 정본대로 NotificationPlan에서만 다시 시작한다.
7. stage 39의 notifier internal hardening과 ownership을 깨지 않는다.

---

## 10. 다음 단계

이번 acceptance 층이 닫히면 다음 남은 안전한 작업은 아래다.

1. `41_delivery_retry_promotion_and_replay_hardening_v0_1.md`
   - `q.maintenance` consumer contract 구체화
   - due retry intent emission code draft
   - explicit `replay.requested.v1 (delivery)` path hardening
   - send-disabled rollback 후 recovery runbook 구체화

2. 그 이후 누적되면
   - `10_delivery_hardening_stage39_plus_v0_1.md`
   같은 새 번들로 stage 39+ 문서를 묶는 것이 자연스럽다.

즉, 다음 단계도 여전히 **delivery recovery를 좁게 닫는 작업** 이다.

---

## 최종 한 줄 결론

이번 단계의 최소-change 정답은 **stage 39에서 닫은 notifier internal hardening 위에, `notification.plan.created.v1 -> q.notification.send -> notifier -> notification.delivery.result.v1 -> q.maintenance` end-to-end wiring과 `send_after`/dry-run/send-disabled/replay 기본값을 compose/runtime 수준으로 고정하고, rollback과 recovery를 NotificationPlan 기반 delivery replay로만 좁게 연결해 delivery layer를 실제 운영 acceptance 가능한 상태로 만드는 것** 이다.



## Source file: `41_delivery_retry_promotion_and_replay_hardening_v0_1.md`

# 41단계: `delivery` retry promotion and replay hardening v0.1

## 0. 문서 목적

이 문서는 `40_end_to_end_delivery_acceptance_and_compose_hardening_v0_1.md`를 뒤엎는 문서가 아니다.  
목적은 **이미 잠긴 delivery acceptance 경계를 유지한 채, `q.maintenance` / `q.replay` 기준의 retry promotion·explicit delivery replay·rollback recovery 경계만 좁게 닫는 것**이다.

이번 단계에서 닫는 것은 정확히 아래 여덟 가지다.

1. `q.maintenance`의 **delivery-result consumer contract** 고정
2. `notification.delivery.result.v1` 기준 **retry promotion 판정 경계** 고정
3. due row에 대한 **retry-intent emission code draft** 고정
4. `replay.requested.v1 (delivery)`의 **explicit replay dispatch path** 고정
5. `failed_retryable`와 `notification_send_flag_disabled` recovery를 **의도적으로 분리**
6. delivery line의 **retry ceiling / dead-letter boundary** 고정
7. `pipeline_runs` / `job_attempts` / `replay_requests` / `dead_letter_entries` 의 **maintenance 사용 방식** 고정
8. send-disabled rollback 이후 **recovery runbook** 을 explicit replay 중심으로 구체화

핵심 전제는 그대로 유지한다.

- `notifier-telegram`은 여전히 **transport boundary** 다.
- `maintenance`는 **retry / replay orchestration boundary** 다.
- durable truth는 여전히 **PostgreSQL** 이고, Redis는 **short-lived queue state** 다.
- delivery replay는 stage 8 정본대로 **`NotificationPlan`에서 다시 시작하는 좁은 replay** 다.
- upstream `analysis` / `judge` / `bundle` / `candidate` / `artifact` 는 delivery 복구 때문에 다시 계산하지 않는다.

---

## 1. 왜 지금 이 단계가 정확한 다음 단계인가

최신 README 기준 현재 구현 상태는 stage 40까지 닫힌 상태이고, 다음 안전한 순서는 명시적으로 아래 하나였다.

- `41_delivery_retry_promotion_and_replay_hardening_v0_1.md`

또한 stage 40은 다음을 이미 잠갔다.

- `notification.delivery.result.v1 -> q.maintenance`
- retryable 실패 시 `notification_plans.send_after = next_retry_at`
- delivery replay root = `notification_plan_id`
- maintenance는 `notification_plans`를 **읽기만** 하고, due row에 대해 **retry-intent outbox append** 만 수행

즉, 지금 collector / normalizer / enrichers / judge / policy를 다시 여는 것은 순서상 후퇴고,  
지금 닫아야 하는 것은 **delivery acceptance 이후의 실제 retry / replay orchestration 경계** 다.

---

## 2. 이번 단계에서 드러나는 충돌과 최소-change 해석

## 2-1. 충돌 A — maintenance ownership에는 `notification_plans` write가 없다

execution contracts 기준 maintenance 직접 소유 durable write는 아래로 잠겨 있다.

- `pipeline_runs`
- `job_attempts`
- `dead_letter_entries`
- `replay_requests`
- `event_outbox`

반면 `notification_plans` / `notification_renders` / `notification_delivery_records` / `state_transitions` 는 notifier ownership 이다.

### 최소-change 해석 A

maintenance는 아래만 수행한다.

1. `notification_plans` / `notification_delivery_records` 를 **read-only** 로 조회
2. due retry 조건이 맞으면 **새 `notification.plan.created.v1` retry-intent event** 를 append
3. ceiling 초과 시 **`dead_letter_entries`** 를 append
4. explicit replay 요청은 **`replay_requests.status`** 와 **`event_outbox`** 만 갱신

즉, **plan mutate는 notifier**, **retry re-dispatch / replay dispatch 는 maintenance** 로 역할을 유지한다.

---

## 2-2. 충돌 B — `q.maintenance` 와 `q.replay` 를 어떻게 나눌 것인가

stage 11 계약은 queue 를 아래처럼 이미 잠갔다.

- `q.maintenance`
- `q.replay`

stage 40은 `notification.delivery.result.v1 -> q.maintenance` 를 잠갔고, explicit replay 는 `replay.requested.v1 (delivery)` 로 열어뒀다.

### 최소-change 해석 B

이번 단계에서는 두 queue 를 그대로 살린다.

### `q.maintenance`
역할:
- `notification.delivery.result.v1` 소비
- retryable / terminal / no-op 결과 해석
- due retry promotion loop
- retry ceiling / DLQ 판정

### `q.replay`
역할:
- `replay.requested.v1` 소비
- 그 중 **`replay_type = delivery` + `root_object_type = notification_plan`** 만 처리
- explicit replay-intent dispatch

즉, **자동 재시도는 `q.maintenance`**, **운영자 명시 재생은 `q.replay`** 로 나눈다.

---

## 2-3. 충돌 C — send-disabled rollback 에서 suppress 된 row 를 auto retry 하면 안 된다

stage 40은 `ENABLE_NOTIFICATION_SEND=false` 일 때 notifier worker 를 내리지 않고, plan/render ownership chain 을 유지하되 **transport만 skip** 하도록 잠갔다.

이 경로의 결과는 보통 아래 의미다.

- `delivery_status = suppressed`
- `reason_code = notification_send_flag_disabled`

이 row 는 **retryable transport failure** 가 아니다.

### 최소-change 해석 C

- `failed_retryable` 만 auto retry promotion 대상이다.
- `notification_send_flag_disabled` 로 suppress 된 row 는 **explicit replay.requested.v1 (delivery)** 로만 복구한다.
- 즉, send-disabled rollback recovery 는 **운영자 승인 후 명시 replay** 를 요구한다.

이렇게 해야 rollback 상태에서 send가 자동으로 되살아나는 사고를 막을 수 있다.

---

## 2-4. 충돌 D — due retry promotion 은 중복 발행 위험이 있다

maintenance 는 `notification_plans` row 를 mutate 하지 않으므로, 동일한 due row 를 주기적으로 다시 볼 수 있다.

### 최소-change 해석 D

retry-intent dedupe key 는 **현재 attempt 상태를 반영한 안정 키** 로 고정한다.

권장 키:

```text
notify:retry-intent:{notification_plan_id}:{latest_attempt_count}:{send_after_epoch}
```

의미:
- 같은 실패 attempt 상태에서는 같은 dedupe key
- notifier 가 새 attempt 를 append 하면 `latest_attempt_count` 가 바뀜
- 그 후 새로운 due 시점에서만 다음 retry-intent 가 가능

즉, **plan mutate 없이도 outbox dedupe 로 중복 promotion 을 흡수** 한다.

---

## 2-5. 충돌 E — explicit delivery replay 를 prod 에 바로 허용할 것인가

stage 40 env matrix 는 기본적으로 아래를 권장했다.

- `ENABLE_REPLAY_TO_PROD_DB=false`

즉, prod 에서의 replay 는 기본 허용 상태가 아니다.

### 최소-change 해석 E

explicit delivery replay dispatch 는 아래 guard 를 둔다.

- `APP_ENV in {dev, replay, test}` → 허용
- `APP_ENV = prod` 이고 `ENABLE_REPLAY_TO_PROD_DB=true` → 허용
- 그 외 → `replay_requests.status = rejected_by_env_guard`

즉, **prod delivery replay 는 explicit opt-in 이 있어야만 dispatch** 된다.

---

## 3. 이번 단계에서 고정할 범위와 제외 범위

### 포함

- `q.maintenance` consumer contract
- due retry scan / promotion contract
- retry ceiling / dead-letter boundary
- `q.replay` delivery-only explicit replay contract
- `replay_requests` status transition 규칙
- send-disabled rollback 후 recovery runbook
- delivery retry / replay 중심 테스트 포인트

### 제외

- notifier 내부 render/send 로직 재설계
- digest runtime 활성화
- new queue 추가
- replay engine 전체 일반화
- source/enrich/judge/full_pipeline replay 구현
- stage 39+ bundle 생성
- 운영 dashboard 전체 설계

즉, 이번 문서는 **delivery line 안에서 maintenance / replay orchestration 경계만** 닫는다.

---

## 4. 대상 파일 트리

```text
src/services/maintenance/
  __init__.py
  config.py
  models.py
  repositories.py
  retry_policy.py
  delivery_result_worker.py
  due_retry_promoter.py
  replay_worker.py
  service.py
  main.py

tests/
  unit/
    services/
      maintenance/
        test_retry_policy.py
        test_retry_intent_dedupe_key.py
        test_delivery_replay_guard.py
        test_send_disabled_rows_not_auto_retried.py
  component/
    services/
      maintenance/
        test_failed_retryable_due_row_emits_retry_intent.py
        test_retry_ceiling_creates_dead_letter.py
        test_q_replay_delivery_request_dispatches_notification_plan_created.py
        test_non_delivery_replay_request_rejected.py
        test_prod_replay_guard_blocks_dispatch.py
  integration/
    delivery/
      test_q_maintenance_result_to_retry_intent_flow.py
      test_explicit_delivery_replay_preserves_upstream_objects.py
```

주의:

- maintenance 는 새로운 hot-path 의미 판단 계층이 아니다.
- notifier ownership 과 judge/policy ownership 을 넘지 않는다.
- `notification_plans` update 는 여전히 notifier 영역이다.

---

## 5. `q.maintenance` consumer contract

## 5-1. 허용 입력 이벤트

허용 입력은 아래 하나로 좁게 고정한다.

- `notification.delivery.result.v1`

Redis Streams 메시지는 여전히 thin payload 다.

```json
{
  "job_id": "<event_id>",
  "stage_name": "maintenance",
  "root_object_type": "notification_plan",
  "root_object_id": "<notification_plan_id>",
  "idempotency_key": "<dedupe_key>",
  "pipeline_run_id": "",
  "not_before": "",
  "trigger_event_id": "<event_id>"
}
```

즉, consumer 는 Redis 본문을 business source 처럼 쓰지 않고,  
반드시 `trigger_event_id` 로 `event_outbox` 를 다시 조회한다.

---

## 5-2. rehydrate 후 읽는 durable source

`notification.delivery.result.v1` 를 rehydrate 한 다음 maintenance 는 아래를 읽는다.

- `notification_plans`
- `notification_delivery_records` 최신 row
- 필요 시 `replay_requests`

읽는 이유:
- event payload 최소 필드는 너무 얇기 때문
- retry 여부는 **최신 plan 상태 + latest delivery attempt** 를 같이 봐야 하기 때문

중요:
- maintenance 는 이 row 들을 **갱신하지 않는다**
- write 는 `pipeline_runs`, `job_attempts`, `dead_letter_entries`, `replay_requests`, `event_outbox` 만 허용된다

---

## 5-3. 결과 상태별 처리 규칙

### A. `delivery_status in {sent, edited}`

처리:
- retry intent 없음
- dead-letter 없음
- pipeline/job attempt 는 `succeeded`
- 그냥 terminal-success 로 종료

### B. `delivery_status = suppressed`

처리:
- auto retry 없음
- reason 이 `dry_run_skip_transport` 이면 dev/replay safety 로 간주
- reason 이 `notification_send_flag_disabled` 이면 **explicit replay recovery 후보** 로만 간주
- pipeline/job attempt 는 `succeeded` 또는 `abandoned` 가 아니라 **logical_noop_success** 의미의 text reason 으로 정리

### C. `delivery_status = failed_terminal`

처리:
- auto retry 없음
- `dead_letter_entries` append 가능
- `next_manual_action` 예시:
  - `fix_chat_access_then_request_delivery_replay`
  - `fix_invalid_entity_or_template_then_request_delivery_replay`

### D. `delivery_status = failed_retryable`

처리:
- 최신 `notification_plans.status = failed_retryable` 와 `send_after` 를 확인
- `send_after > now()` 이면 **즉시 재발행 안 함**
- due scan loop 가 나중에 retry-intent 를 발행
- 단, ceiling 초과면 DLQ 로 보냄

즉, `q.maintenance` consumer 는 **즉시 sleep / retry 하는 worker 가 아니라**,  
**결과를 durable orchestration state 로 해석하는 worker** 다.

---

## 6. due retry promotion 계약

## 6-1. due row selection

due retry promotion loop 는 아래 row 만 읽는다.

```sql
SELECT np.notification_plan_id,
       np.analysis_id,
       np.candidate_group_id,
       np.delivery_decision,
       np.urgency_profile,
       np.target_chat_id,
       np.target_thread_id,
       np.render_profile,
       np.dedupe_subject_key,
       np.material_change_hash,
       np.send_after,
       dr.attempt_count,
       dr.transport_error_code,
       dr.transport_error_class
FROM notification_plans np
JOIN LATERAL (
  SELECT notification_delivery_record_id,
         attempt_count,
         transport_error_code,
         transport_error_class,
         delivery_status,
         created_at
  FROM notification_delivery_records
  WHERE notification_plan_id = np.notification_plan_id
  ORDER BY created_at DESC
  LIMIT 1
) dr ON TRUE
WHERE np.status = 'failed_retryable'::notification_status_enum
  AND np.send_after IS NOT NULL
  AND np.send_after <= now()
ORDER BY np.send_after ASC, np.created_at ASC
LIMIT :batch_size;
```

중요:
- `suppressed` row 는 여기에 들어오지 않는다.
- send-disabled rollback row 는 auto retry 대상이 아니다.

---

## 6-2. retry ceiling 규칙

권장 기본값:

```text
NOTIFICATION_RETRY_MAX_ATTEMPTS = 5
```

판정:
- `latest_attempt_count < max_attempts` → retry-intent 발행 가능
- `latest_attempt_count >= max_attempts` → retry-intent 금지 + dead-letter append

권장 dead-letter 내용:

- `stage_name = maintenance_delivery_retry`
- `queue_name = q.maintenance`
- `root_object_type = notification_plan`
- `root_object_id = notification_plan_id`
- `last_error_code = max_notification_retry_attempts_exceeded`
- `next_manual_action = request_delivery_replay_after_operator_fix`
- `replay_hint = delivery_replay_from_notification_plan`

즉, **무한 재시도 대신 bounded retry + explicit operator recovery** 로 고정한다.

---

## 6-3. retry-intent emission 규칙

due row 가 retry 가능하면 maintenance 는 **새 `notification.plan.created.v1` retry-intent** 를 append 한다.

권장 dedupe key:

```text
notify:retry-intent:{notification_plan_id}:{latest_attempt_count}:{send_after_epoch}
```

payload 최소 필드:

```json
{
  "notification_plan_id": "...",
  "analysis_id": "...",
  "candidate_group_id": "...",
  "delivery_decision": "send_now",
  "urgency_profile": "high|normal_silent|digest|suppressed",
  "target_chat_id": 123,
  "target_thread_id": null,
  "render_profile": "single_alert_v1",
  "dedupe_subject_key": "...",
  "material_change_hash": "...",
  "send_after": null,
  "retry_reason": "due_retry_promotion",
  "previous_attempt_count": 2
}
```

중요:
- notifier 는 같은 `notification_plan_id` 기준으로 rehydrate 한다.
- maintenance 는 `notification_plans` row 를 reset 하지 않는다.
- notifier 가 새 delivery attempt 를 append 하면 latest attempt count 가 바뀌고, 그때 다음 retry cycle 이 열릴 수 있다.

---

## 6-4. `notification.delivery.result.v1` 와 due scan 의 역할 분리

### result consumer
- 최신 결과를 읽고
- immediate retryable/terminal/no-op 의미를 해석하고
- job/pipeline/DLQ/logical terminal 을 기록

### due scan loop
- 실제 시간이 지난 `failed_retryable` row 를 읽고
- retry-intent event 를 다시 발행

즉, **이벤트 소비와 시간 기반 재발행은 분리** 한다.  
이렇게 해야 worker 가 장기 sleep 을 들고 있지 않아도 된다.

---

## 7. `q.replay` explicit delivery replay contract

## 7-1. 허용 입력 이벤트

허용 입력은 아래 하나다.

- `replay.requested.v1`

하지만 이번 단계에서 실제 처리 대상은 아래로 좁힌다.

- `replay_type = delivery`
- `root_object_type = notification_plan`

그 외 replay type 은 이 단계에서는 **unsupported** 로 본다.

---

## 7-2. explicit delivery replay 처리 규칙

### 허용 조건

1. `replay_request` row 존재
2. `replay_type = delivery`
3. `root_object_type = notification_plan`
4. 대상 `notification_plan` row 존재
5. env guard 통과

### dispatch 동작

조건을 만족하면 maintenance replay worker 는 아래를 수행한다.

1. `replay_requests.status = dispatched`
2. 새 `notification.plan.created.v1` **replay-intent** append
3. dedupe key 는 `replay_request_id` 기준으로 고정
4. notifier 가 동일 `notification_plan_id` 로 다시 진입
5. 성공적으로 outbox append 되면 `replay_requests.status = completed`

권장 dedupe key:

```text
notify:replay-intent:{replay_request_id}
```

payload 최소 필드:

```json
{
  "notification_plan_id": "...",
  "analysis_id": "...",
  "candidate_group_id": "...",
  "delivery_decision": "send_now",
  "urgency_profile": "high|normal_silent|digest|suppressed",
  "target_chat_id": 123,
  "target_thread_id": null,
  "render_profile": "single_alert_v1",
  "dedupe_subject_key": "...",
  "material_change_hash": "...",
  "send_after": null,
  "replay_reason": "explicit_delivery_replay",
  "replay_request_id": "..."
}
```

즉, explicit replay 도 결국 **같은 `notification.plan.created.v1` 브리지** 를 재사용한다.

---

## 7-3. explicit replay env guard

### 허용

- `APP_ENV in {dev, test, replay}`
- 또는 `APP_ENV = prod` 이고 `ENABLE_REPLAY_TO_PROD_DB=true`

### 거부

그 외는 아래처럼 처리한다.

- `replay_requests.status = rejected_by_env_guard`
- 필요 시 `dead_letter_entries` append
- `notification.plan.created.v1` emit 금지

즉, **prod delivery replay 는 operator opt-in 이 있어야만** 한다.

---

## 7-4. unsupported replay request 처리

아래는 이번 단계에서 unsupported 다.

- `replay_type != delivery`
- `root_object_type != notification_plan`

처리:
- `replay_requests.status = unsupported_in_stage41`
- 필요 시 `dead_letter_entries` append
- downstream emit 없음

이 문서는 delivery line 만 닫는 단계이므로, source/enrich/judge/full_pipeline replay 는 다음 턴 대상이다.

---

## 8. send-disabled rollback recovery runbook 규칙

## 8-1. rollback 중 상태

즉시 조치:

```text
ENABLE_NOTIFICATION_SEND=false
MAINTENANCE_ENABLE_NOTIFICATION_RETRY_PROMOTION=false
```

그 다음 의미는 아래로 고정한다.

- notifier worker 는 살아 있다.
- 새 plan / render durable row 는 계속 남는다.
- transport 는 skip 된다.
- `notification_send_flag_disabled` suppress row 가 누적될 수 있다.
- 이 row 는 **auto retry 대상이 아니다.**

---

## 8-2. recovery 절차

### A. retryable failure backlog 복구

조건:
- 기존 상태가 `failed_retryable`
- `send_after <= now()`

조치:
1. 원인 해결
2. `MAINTENANCE_ENABLE_NOTIFICATION_RETRY_PROMOTION=true`
3. due retry promotion loop 로 자연 복구 또는 one-shot promotion 실행

### B. send-disabled suppress backlog 복구

조건:
- `delivery_status = suppressed`
- `reason_code = notification_send_flag_disabled`

조치:
1. `ENABLE_NOTIFICATION_SEND=true`
2. 필요한 time window / subject / priority 범위를 선정
3. 각 `notification_plan_id` 에 대해 explicit `replay.requested.v1 (delivery)` 생성
4. `q.replay` 경유로 replay-intent dispatch

즉, **send-disabled suppress row 는 replay-request 를 명시적으로 올려야만 복구** 된다.

---

## 8-3. recovery selection query 예시

### send-disabled suppress row 조회

```sql
SELECT np.notification_plan_id,
       np.analysis_id,
       np.candidate_group_id,
       dr.created_at,
       dr.telegram_response_json
FROM notification_plans np
JOIN notification_delivery_records dr
  ON dr.notification_plan_id = np.notification_plan_id
WHERE dr.delivery_status = 'suppressed'::notification_status_enum
  AND dr.telegram_response_json ->> 'send_disabled' = 'true'
  AND dr.created_at >= :from_ts
  AND dr.created_at < :to_ts
ORDER BY dr.created_at ASC;
```

### explicit replay request 생성 예시

```sql
INSERT INTO replay_requests (
  replay_request_id,
  replay_type,
  root_object_type,
  root_object_id,
  requested_by,
  requested_at,
  status
) VALUES (
  gen_random_uuid(),
  'delivery'::replay_type_enum,
  'notification_plan',
  CAST(:notification_plan_id AS uuid),
  'operator_recovery',
  now(),
  'pending'
);
```

---

## 9. 코드 초안

## 9-1. `src/services/maintenance/__init__.py`

```python
from .config import MaintenanceConfig
from .service import MaintenanceService

__all__ = [
    "MaintenanceConfig",
    "MaintenanceService",
]
```

---

## 9-2. `src/services/maintenance/config.py`

```python
from __future__ import annotations

import os
from dataclasses import dataclass


class MaintenanceConfigurationError(ValueError):
    pass


@dataclass(slots=True, frozen=True)
class MaintenanceConfig:
    app_env: str
    database_url: str
    redis_url: str

    maintenance_queue_name: str
    maintenance_consumer_group: str
    maintenance_consumer_name: str

    replay_queue_name: str
    replay_consumer_group: str
    replay_consumer_name: str

    batch_size: int
    block_ms: int
    retry_scan_poll_sec: int
    notification_retry_max_attempts: int
    enable_notification_retry_promotion: bool
    enable_replay_to_prod_db: bool
    log_level: str

    @classmethod
    def from_env(cls) -> "MaintenanceConfig":
        def _read(name: str, default: str = "") -> str:
            return os.getenv(name, default).strip()

        cfg = cls(
            app_env=_read("APP_ENV", "dev").lower(),
            database_url=_read("DATABASE_URL"),
            redis_url=_read("REDIS_URL"),
            maintenance_queue_name=_read("MAINTENANCE_QUEUE_NAME", "q.maintenance"),
            maintenance_consumer_group=_read("MAINTENANCE_CONSUMER_GROUP", "maintenance"),
            maintenance_consumer_name=_read("MAINTENANCE_CONSUMER_NAME", "maintenance-1"),
            replay_queue_name=_read("REPLAY_QUEUE_NAME", "q.replay"),
            replay_consumer_group=_read("REPLAY_CONSUMER_GROUP", "maintenance-replay"),
            replay_consumer_name=_read("REPLAY_CONSUMER_NAME", "maintenance-replay-1"),
            batch_size=int(_read("MAINTENANCE_BATCH_SIZE", "50")),
            block_ms=int(_read("MAINTENANCE_BLOCK_MS", "5000")),
            retry_scan_poll_sec=int(_read("MAINTENANCE_NOTIFICATION_RETRY_POLL_SEC", "30")),
            notification_retry_max_attempts=int(_read("NOTIFICATION_RETRY_MAX_ATTEMPTS", "5")),
            enable_notification_retry_promotion=_read("MAINTENANCE_ENABLE_NOTIFICATION_RETRY_PROMOTION", "false").lower() == "true",
            enable_replay_to_prod_db=_read("ENABLE_REPLAY_TO_PROD_DB", "false").lower() == "true",
            log_level=_read("LOG_LEVEL", "INFO").upper(),
        )
        cfg.validate()
        return cfg

    def validate(self) -> None:
        if not self.database_url:
            raise MaintenanceConfigurationError("DATABASE_URL is required")
        if not self.redis_url:
            raise MaintenanceConfigurationError("REDIS_URL is required")
        if self.batch_size <= 0 or self.batch_size > 500:
            raise MaintenanceConfigurationError("MAINTENANCE_BATCH_SIZE must be between 1 and 500")
        if self.block_ms <= 0:
            raise MaintenanceConfigurationError("MAINTENANCE_BLOCK_MS must be > 0")
        if self.retry_scan_poll_sec <= 0:
            raise MaintenanceConfigurationError("MAINTENANCE_NOTIFICATION_RETRY_POLL_SEC must be > 0")
        if self.notification_retry_max_attempts <= 0:
            raise MaintenanceConfigurationError("NOTIFICATION_RETRY_MAX_ATTEMPTS must be > 0")
```

---

## 9-3. `src/services/maintenance/models.py`

```python
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Literal


@dataclass(slots=True, frozen=True)
class DeliveryResultJob:
    trigger_event_id: str
    event_type: str
    notification_plan_id: str
    delivery_status: str
    telegram_chat_id: int | None
    telegram_message_id: int | None


@dataclass(slots=True, frozen=True)
class ReplayRequestedJob:
    trigger_event_id: str
    replay_request_id: str
    replay_type: str
    root_object_type: str
    root_object_id: str


@dataclass(slots=True, frozen=True)
class NotificationPlanRecord:
    notification_plan_id: str
    analysis_id: str
    candidate_group_id: str
    delivery_decision: str
    urgency_profile: str
    target_chat_id: int
    target_thread_id: int | None
    render_profile: str
    dedupe_subject_key: str
    material_change_hash: str
    send_after: datetime | None
    status: str


@dataclass(slots=True, frozen=True)
class LatestDeliveryRecord:
    notification_delivery_record_id: str
    notification_plan_id: str
    delivery_status: str
    attempt_count: int
    transport_error_code: str | None
    transport_error_class: str | None
    created_at: datetime
    telegram_response_json: dict | None


@dataclass(slots=True, frozen=True)
class DueRetryCandidate:
    plan: NotificationPlanRecord
    latest_delivery: LatestDeliveryRecord


MaintenanceAction = Literal[
    "noop_success",
    "schedule_retry_later",
    "emit_retry_intent",
    "dead_letter_terminal",
    "dead_letter_retry_ceiling",
    "emit_explicit_replay_intent",
    "reject_replay_request",
]
```

---

## 9-4. `src/services/maintenance/retry_policy.py`

```python
from __future__ import annotations

from .models import DueRetryCandidate


class DeliveryRetryPolicy:
    def __init__(self, *, max_attempts: int) -> None:
        self._max_attempts = max_attempts

    def should_emit_retry_intent(self, candidate: DueRetryCandidate) -> bool:
        return candidate.latest_delivery.attempt_count < self._max_attempts

    def retry_intent_dedupe_key(self, candidate: DueRetryCandidate) -> str:
        send_after_epoch = int(candidate.plan.send_after.timestamp()) if candidate.plan.send_after else 0
        return (
            f"notify:retry-intent:{candidate.plan.notification_plan_id}:"
            f"{candidate.latest_delivery.attempt_count}:{send_after_epoch}"
        )

    def max_attempts_exceeded(self, candidate: DueRetryCandidate) -> bool:
        return candidate.latest_delivery.attempt_count >= self._max_attempts
```

---

## 9-5. `src/services/maintenance/repositories.py`

```python
from __future__ import annotations

import json
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Any, AsyncIterator

import sqlalchemy as sa
from sqlalchemy.ext.asyncio import AsyncSession

from .models import (
    DeliveryResultJob,
    DueRetryCandidate,
    LatestDeliveryRecord,
    NotificationPlanRecord,
    ReplayRequestedJob,
)


def _jsonb_dumps(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False)


class MaintenanceRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    @asynccontextmanager
    async def transaction(self) -> AsyncIterator[AsyncSession]:
        if self._session.in_transaction():
            yield self._session
            return
        async with self._session.begin():
            yield self._session

    async def load_delivery_result_job(self, trigger_event_id: str) -> DeliveryResultJob | None:
        result = await self._session.execute(
            sa.text(
                """
                SELECT event_type, payload_json
                FROM event_outbox
                WHERE event_id = CAST(:event_id AS uuid)
                """
            ),
            {"event_id": trigger_event_id},
        )
        row = result.mappings().first()
        if row is None or str(row["event_type"]) != "notification.delivery.result.v1":
            return None
        payload = row["payload_json"] or {}
        return DeliveryResultJob(
            trigger_event_id=trigger_event_id,
            event_type=str(row["event_type"]),
            notification_plan_id=str(payload["notification_plan_id"]),
            delivery_status=str(payload["delivery_status"]),
            telegram_chat_id=payload.get("telegram_chat_id"),
            telegram_message_id=payload.get("telegram_message_id"),
        )

    async def load_replay_requested_job(self, trigger_event_id: str) -> ReplayRequestedJob | None:
        result = await self._session.execute(
            sa.text(
                """
                SELECT event_type, payload_json
                FROM event_outbox
                WHERE event_id = CAST(:event_id AS uuid)
                """
            ),
            {"event_id": trigger_event_id},
        )
        row = result.mappings().first()
        if row is None or str(row["event_type"]) != "replay.requested.v1":
            return None
        payload = row["payload_json"] or {}
        return ReplayRequestedJob(
            trigger_event_id=trigger_event_id,
            replay_request_id=str(payload["replay_request_id"]),
            replay_type=str(payload["replay_type"]),
            root_object_type=str(payload["root_object_type"]),
            root_object_id=str(payload["root_object_id"]),
        )

    async def load_notification_plan(self, notification_plan_id: str) -> NotificationPlanRecord | None:
        result = await self._session.execute(
            sa.text(
                """
                SELECT notification_plan_id, analysis_id, candidate_group_id,
                       delivery_decision, urgency_profile, target_chat_id,
                       target_thread_id, render_profile, dedupe_subject_key,
                       material_change_hash, send_after, status
                FROM notification_plans
                WHERE notification_plan_id = CAST(:notification_plan_id AS uuid)
                """
            ),
            {"notification_plan_id": notification_plan_id},
        )
        row = result.mappings().first()
        if row is None:
            return None
        return NotificationPlanRecord(
            notification_plan_id=str(row["notification_plan_id"]),
            analysis_id=str(row["analysis_id"]),
            candidate_group_id=str(row["candidate_group_id"]),
            delivery_decision=str(row["delivery_decision"]),
            urgency_profile=str(row["urgency_profile"]),
            target_chat_id=int(row["target_chat_id"]),
            target_thread_id=row["target_thread_id"],
            render_profile=str(row["render_profile"]),
            dedupe_subject_key=str(row["dedupe_subject_key"]),
            material_change_hash=str(row["material_change_hash"]),
            send_after=row["send_after"],
            status=str(row["status"]),
        )

    async def load_latest_delivery_record(self, notification_plan_id: str) -> LatestDeliveryRecord | None:
        result = await self._session.execute(
            sa.text(
                """
                SELECT notification_delivery_record_id, notification_plan_id,
                       delivery_status, attempt_count,
                       transport_error_code, transport_error_class,
                       telegram_response_json, created_at
                FROM notification_delivery_records
                WHERE notification_plan_id = CAST(:notification_plan_id AS uuid)
                ORDER BY created_at DESC
                LIMIT 1
                """
            ),
            {"notification_plan_id": notification_plan_id},
        )
        row = result.mappings().first()
        if row is None:
            return None
        return LatestDeliveryRecord(
            notification_delivery_record_id=str(row["notification_delivery_record_id"]),
            notification_plan_id=str(row["notification_plan_id"]),
            delivery_status=str(row["delivery_status"]),
            attempt_count=int(row["attempt_count"]),
            transport_error_code=row["transport_error_code"],
            transport_error_class=row["transport_error_class"],
            telegram_response_json=row["telegram_response_json"],
            created_at=row["created_at"],
        )

    async def list_due_retry_candidates(self, *, limit: int) -> list[DueRetryCandidate]:
        result = await self._session.execute(
            sa.text(
                """
                SELECT np.notification_plan_id, np.analysis_id, np.candidate_group_id,
                       np.delivery_decision, np.urgency_profile, np.target_chat_id,
                       np.target_thread_id, np.render_profile, np.dedupe_subject_key,
                       np.material_change_hash, np.send_after, np.status,
                       dr.notification_delivery_record_id, dr.delivery_status, dr.attempt_count,
                       dr.transport_error_code, dr.transport_error_class,
                       dr.telegram_response_json, dr.created_at
                FROM notification_plans np
                JOIN LATERAL (
                  SELECT notification_delivery_record_id, delivery_status, attempt_count,
                         transport_error_code, transport_error_class,
                         telegram_response_json, created_at
                  FROM notification_delivery_records
                  WHERE notification_plan_id = np.notification_plan_id
                  ORDER BY created_at DESC
                  LIMIT 1
                ) dr ON TRUE
                WHERE np.status = 'failed_retryable'::notification_status_enum
                  AND np.send_after IS NOT NULL
                  AND np.send_after <= now()
                ORDER BY np.send_after ASC, np.created_at ASC
                LIMIT :limit
                """
            ),
            {"limit": limit},
        )
        out: list[DueRetryCandidate] = []
        for row in result.mappings().all():
            plan = NotificationPlanRecord(
                notification_plan_id=str(row["notification_plan_id"]),
                analysis_id=str(row["analysis_id"]),
                candidate_group_id=str(row["candidate_group_id"]),
                delivery_decision=str(row["delivery_decision"]),
                urgency_profile=str(row["urgency_profile"]),
                target_chat_id=int(row["target_chat_id"]),
                target_thread_id=row["target_thread_id"],
                render_profile=str(row["render_profile"]),
                dedupe_subject_key=str(row["dedupe_subject_key"]),
                material_change_hash=str(row["material_change_hash"]),
                send_after=row["send_after"],
                status=str(row["status"]),
            )
            latest = LatestDeliveryRecord(
                notification_delivery_record_id=str(row["notification_delivery_record_id"]),
                notification_plan_id=str(row["notification_plan_id"]),
                delivery_status=str(row["delivery_status"]),
                attempt_count=int(row["attempt_count"]),
                transport_error_code=row["transport_error_code"],
                transport_error_class=row["transport_error_class"],
                telegram_response_json=row["telegram_response_json"],
                created_at=row["created_at"],
            )
            out.append(DueRetryCandidate(plan=plan, latest_delivery=latest))
        return out

    async def insert_retry_intent_outbox(self, *, dedupe_key: str, payload: dict[str, Any]) -> None:
        await self._session.execute(
            sa.text(
                """
                INSERT INTO event_outbox (
                    event_id, event_type, aggregate_type, aggregate_id,
                    dedupe_key, payload_json, status, created_at
                ) VALUES (
                    gen_random_uuid(),
                    'notification.plan.created.v1',
                    'analysis',
                    CAST(:analysis_id AS uuid),
                    :dedupe_key,
                    CAST(:payload_json AS jsonb),
                    'pending'::outbox_status_enum,
                    now()
                )
                ON CONFLICT (dedupe_key) DO NOTHING
                """
            ),
            {
                "analysis_id": payload["analysis_id"],
                "dedupe_key": dedupe_key,
                "payload_json": _jsonb_dumps(payload),
            },
        )

    async def insert_dead_letter_if_absent(
        self,
        *,
        stage_name: str,
        queue_name: str,
        root_object_type: str,
        root_object_id: str,
        last_error_code: str,
        retry_count: int,
        next_manual_action: str,
        replay_hint: str,
    ) -> None:
        await self._session.execute(
            sa.text(
                """
                INSERT INTO dead_letter_entries (
                    dead_letter_entry_id,
                    stage_name,
                    queue_name,
                    root_object_type,
                    root_object_id,
                    last_error_code,
                    retry_count,
                    next_manual_action,
                    replay_hint,
                    first_failed_at,
                    last_failed_at
                )
                SELECT gen_random_uuid(), :stage_name, :queue_name,
                       :root_object_type, CAST(:root_object_id AS uuid),
                       :last_error_code, :retry_count,
                       :next_manual_action, :replay_hint,
                       now(), now()
                WHERE NOT EXISTS (
                  SELECT 1
                  FROM dead_letter_entries
                  WHERE stage_name = :stage_name
                    AND root_object_type = :root_object_type
                    AND root_object_id = CAST(:root_object_id AS uuid)
                    AND last_error_code = :last_error_code
                    AND retry_count = :retry_count
                )
                """
            ),
            {
                "stage_name": stage_name,
                "queue_name": queue_name,
                "root_object_type": root_object_type,
                "root_object_id": root_object_id,
                "last_error_code": last_error_code,
                "retry_count": retry_count,
                "next_manual_action": next_manual_action,
                "replay_hint": replay_hint,
            },
        )

    async def load_replay_request_row(self, replay_request_id: str) -> dict[str, Any] | None:
        result = await self._session.execute(
            sa.text(
                """
                SELECT replay_request_id, replay_type, root_object_type, root_object_id,
                       requested_by, requested_at, status
                FROM replay_requests
                WHERE replay_request_id = CAST(:replay_request_id AS uuid)
                """
            ),
            {"replay_request_id": replay_request_id},
        )
        row = result.mappings().first()
        return dict(row) if row else None

    async def update_replay_request_status(self, *, replay_request_id: str, status: str) -> None:
        await self._session.execute(
            sa.text(
                """
                UPDATE replay_requests
                SET status = :status
                WHERE replay_request_id = CAST(:replay_request_id AS uuid)
                """
            ),
            {"replay_request_id": replay_request_id, "status": status},
        )

    async def insert_pipeline_run(self, *, trigger_source: str, run_kind: str, root_object_type: str, root_object_id: str, terminal_status: str | None = None) -> None:
        await self._session.execute(
            sa.text(
                """
                INSERT INTO pipeline_runs (
                    pipeline_run_id, trigger_source, run_kind,
                    root_object_type, root_object_id,
                    started_at, finished_at, terminal_status
                ) VALUES (
                    gen_random_uuid(),
                    :trigger_source,
                    :run_kind,
                    :root_object_type,
                    CAST(:root_object_id AS uuid),
                    now(), now(), :terminal_status
                )
                """
            ),
            {
                "trigger_source": trigger_source,
                "run_kind": run_kind,
                "root_object_type": root_object_type,
                "root_object_id": root_object_id,
                "terminal_status": terminal_status,
            },
        )

    async def insert_job_attempt(self, *, stage_name: str, queue_name: str, root_object_type: str, root_object_id: str, attempt_status: str, error_code: str | None = None) -> None:
        await self._session.execute(
            sa.text(
                """
                INSERT INTO job_attempts (
                    job_attempt_id,
                    stage_name,
                    queue_name,
                    root_object_type,
                    root_object_id,
                    attempt_no,
                    lease_owner,
                    started_at,
                    finished_at,
                    attempt_status,
                    error_code,
                    retry_after_at
                ) VALUES (
                    gen_random_uuid(),
                    :stage_name,
                    :queue_name,
                    :root_object_type,
                    CAST(:root_object_id AS uuid),
                    1,
                    NULL,
                    now(),
                    now(),
                    CAST(:attempt_status AS job_attempt_status_enum),
                    :error_code,
                    NULL
                )
                """
            ),
            {
                "stage_name": stage_name,
                "queue_name": queue_name,
                "root_object_type": root_object_type,
                "root_object_id": root_object_id,
                "attempt_status": attempt_status,
                "error_code": error_code,
            },
        )
```

---

## 9-6. `src/services/maintenance/delivery_result_worker.py`

```python
from __future__ import annotations

from .models import DeliveryResultJob
from .repositories import MaintenanceRepository


class DeliveryResultWorker:
    def __init__(self, repository: MaintenanceRepository) -> None:
        self._repository = repository

    async def handle_trigger_event(self, trigger_event_id: str) -> None:
        job = await self._repository.load_delivery_result_job(trigger_event_id)
        if job is None:
            return

        plan = await self._repository.load_notification_plan(job.notification_plan_id)
        latest = await self._repository.load_latest_delivery_record(job.notification_plan_id)

        async with self._repository.transaction():
            await self._repository.insert_pipeline_run(
                trigger_source="q.maintenance",
                run_kind="maintenance_delivery_result",
                root_object_type="notification_plan",
                root_object_id=job.notification_plan_id,
                terminal_status=job.delivery_status,
            )

            if job.delivery_status in {"sent", "edited", "suppressed"}:
                await self._repository.insert_job_attempt(
                    stage_name="maintenance_delivery_result",
                    queue_name="q.maintenance",
                    root_object_type="notification_plan",
                    root_object_id=job.notification_plan_id,
                    attempt_status="succeeded",
                    error_code=None,
                )
                return

            if job.delivery_status == "failed_terminal":
                await self._repository.insert_job_attempt(
                    stage_name="maintenance_delivery_result",
                    queue_name="q.maintenance",
                    root_object_type="notification_plan",
                    root_object_id=job.notification_plan_id,
                    attempt_status="failed_terminal",
                    error_code=(latest.transport_error_code if latest else "terminal_delivery_failure"),
                )
                await self._repository.insert_dead_letter_if_absent(
                    stage_name="maintenance_delivery_result",
                    queue_name="q.maintenance",
                    root_object_type="notification_plan",
                    root_object_id=job.notification_plan_id,
                    last_error_code=(latest.transport_error_code if latest else "terminal_delivery_failure"),
                    retry_count=(latest.attempt_count if latest else 1),
                    next_manual_action="fix_delivery_terminal_condition_then_request_delivery_replay",
                    replay_hint="delivery_replay_from_notification_plan",
                )
                return

            if job.delivery_status == "failed_retryable":
                await self._repository.insert_job_attempt(
                    stage_name="maintenance_delivery_result",
                    queue_name="q.maintenance",
                    root_object_type="notification_plan",
                    root_object_id=job.notification_plan_id,
                    attempt_status="failed_retryable",
                    error_code=(latest.transport_error_code if latest else "retryable_delivery_failure"),
                )
                return
```

---

## 9-7. `src/services/maintenance/due_retry_promoter.py`

```python
from __future__ import annotations

from .models import DueRetryCandidate
from .repositories import MaintenanceRepository
from .retry_policy import DeliveryRetryPolicy


class DueRetryPromoter:
    def __init__(
        self,
        *,
        repository: MaintenanceRepository,
        retry_policy: DeliveryRetryPolicy,
        enabled: bool,
    ) -> None:
        self._repository = repository
        self._retry_policy = retry_policy
        self._enabled = enabled

    async def run_once(self, *, batch_size: int) -> int:
        if not self._enabled:
            return 0

        candidates = await self._repository.list_due_retry_candidates(limit=batch_size)
        emitted = 0
        for candidate in candidates:
            async with self._repository.transaction():
                await self._repository.insert_pipeline_run(
                    trigger_source="maintenance_due_retry_scan",
                    run_kind="maintenance_retry_promotion",
                    root_object_type="notification_plan",
                    root_object_id=candidate.plan.notification_plan_id,
                    terminal_status=None,
                )

                if self._retry_policy.max_attempts_exceeded(candidate):
                    await self._repository.insert_job_attempt(
                        stage_name="maintenance_retry_promotion",
                        queue_name="q.maintenance",
                        root_object_type="notification_plan",
                        root_object_id=candidate.plan.notification_plan_id,
                        attempt_status="failed_terminal",
                        error_code="max_notification_retry_attempts_exceeded",
                    )
                    await self._repository.insert_dead_letter_if_absent(
                        stage_name="maintenance_delivery_retry",
                        queue_name="q.maintenance",
                        root_object_type="notification_plan",
                        root_object_id=candidate.plan.notification_plan_id,
                        last_error_code="max_notification_retry_attempts_exceeded",
                        retry_count=candidate.latest_delivery.attempt_count,
                        next_manual_action="request_delivery_replay_after_operator_fix",
                        replay_hint="delivery_replay_from_notification_plan",
                    )
                    continue

                dedupe_key = self._retry_policy.retry_intent_dedupe_key(candidate)
                payload = {
                    "notification_plan_id": candidate.plan.notification_plan_id,
                    "analysis_id": candidate.plan.analysis_id,
                    "candidate_group_id": candidate.plan.candidate_group_id,
                    "delivery_decision": candidate.plan.delivery_decision,
                    "urgency_profile": candidate.plan.urgency_profile,
                    "target_chat_id": candidate.plan.target_chat_id,
                    "target_thread_id": candidate.plan.target_thread_id,
                    "render_profile": candidate.plan.render_profile,
                    "dedupe_subject_key": candidate.plan.dedupe_subject_key,
                    "material_change_hash": candidate.plan.material_change_hash,
                    "send_after": None,
                    "retry_reason": "due_retry_promotion",
                    "previous_attempt_count": candidate.latest_delivery.attempt_count,
                }
                await self._repository.insert_retry_intent_outbox(
                    dedupe_key=dedupe_key,
                    payload=payload,
                )
                await self._repository.insert_job_attempt(
                    stage_name="maintenance_retry_promotion",
                    queue_name="q.maintenance",
                    root_object_type="notification_plan",
                    root_object_id=candidate.plan.notification_plan_id,
                    attempt_status="succeeded",
                    error_code=None,
                )
                emitted += 1
        return emitted
```

---

## 9-8. `src/services/maintenance/replay_worker.py`

```python
from __future__ import annotations

from .repositories import MaintenanceRepository


class ReplayWorker:
    def __init__(
        self,
        *,
        repository: MaintenanceRepository,
        app_env: str,
        enable_replay_to_prod_db: bool,
    ) -> None:
        self._repository = repository
        self._app_env = app_env
        self._enable_replay_to_prod_db = enable_replay_to_prod_db

    async def handle_trigger_event(self, trigger_event_id: str) -> None:
        job = await self._repository.load_replay_requested_job(trigger_event_id)
        if job is None:
            return

        async with self._repository.transaction():
            await self._repository.insert_pipeline_run(
                trigger_source="q.replay",
                run_kind="maintenance_delivery_replay_dispatch",
                root_object_type=job.root_object_type,
                root_object_id=job.root_object_id,
                terminal_status=None,
            )

            if job.replay_type != "delivery" or job.root_object_type != "notification_plan":
                await self._repository.update_replay_request_status(
                    replay_request_id=job.replay_request_id,
                    status="unsupported_in_stage41",
                )
                await self._repository.insert_job_attempt(
                    stage_name="maintenance_delivery_replay_dispatch",
                    queue_name="q.replay",
                    root_object_type=job.root_object_type,
                    root_object_id=job.root_object_id,
                    attempt_status="failed_terminal",
                    error_code="unsupported_replay_request",
                )
                return

            if self._app_env == "prod" and not self._enable_replay_to_prod_db:
                await self._repository.update_replay_request_status(
                    replay_request_id=job.replay_request_id,
                    status="rejected_by_env_guard",
                )
                await self._repository.insert_job_attempt(
                    stage_name="maintenance_delivery_replay_dispatch",
                    queue_name="q.replay",
                    root_object_type=job.root_object_type,
                    root_object_id=job.root_object_id,
                    attempt_status="failed_terminal",
                    error_code="replay_to_prod_db_disabled",
                )
                return

            plan = await self._repository.load_notification_plan(job.root_object_id)
            if plan is None:
                await self._repository.update_replay_request_status(
                    replay_request_id=job.replay_request_id,
                    status="failed_terminal_missing_root",
                )
                await self._repository.insert_job_attempt(
                    stage_name="maintenance_delivery_replay_dispatch",
                    queue_name="q.replay",
                    root_object_type=job.root_object_type,
                    root_object_id=job.root_object_id,
                    attempt_status="failed_terminal",
                    error_code="missing_notification_plan",
                )
                return

            await self._repository.update_replay_request_status(
                replay_request_id=job.replay_request_id,
                status="dispatched",
            )
            await self._repository.insert_retry_intent_outbox(
                dedupe_key=f"notify:replay-intent:{job.replay_request_id}",
                payload={
                    "notification_plan_id": plan.notification_plan_id,
                    "analysis_id": plan.analysis_id,
                    "candidate_group_id": plan.candidate_group_id,
                    "delivery_decision": plan.delivery_decision,
                    "urgency_profile": plan.urgency_profile,
                    "target_chat_id": plan.target_chat_id,
                    "target_thread_id": plan.target_thread_id,
                    "render_profile": plan.render_profile,
                    "dedupe_subject_key": plan.dedupe_subject_key,
                    "material_change_hash": plan.material_change_hash,
                    "send_after": None,
                    "replay_reason": "explicit_delivery_replay",
                    "replay_request_id": job.replay_request_id,
                },
            )
            await self._repository.update_replay_request_status(
                replay_request_id=job.replay_request_id,
                status="completed",
            )
            await self._repository.insert_job_attempt(
                stage_name="maintenance_delivery_replay_dispatch",
                queue_name="q.replay",
                root_object_type=job.root_object_type,
                root_object_id=job.root_object_id,
                attempt_status="succeeded",
                error_code=None,
            )
```

---

## 9-9. `src/services/maintenance/service.py`

```python
from __future__ import annotations

import asyncio

from .config import MaintenanceConfig
from .delivery_result_worker import DeliveryResultWorker
from .due_retry_promoter import DueRetryPromoter
from .replay_worker import ReplayWorker
from .retry_policy import DeliveryRetryPolicy


class MaintenanceService:
    def __init__(
        self,
        config: MaintenanceConfig,
        *,
        delivery_result_worker: DeliveryResultWorker,
        replay_worker: ReplayWorker,
        due_retry_promoter: DueRetryPromoter,
    ) -> None:
        self._config = config
        self._delivery_result_worker = delivery_result_worker
        self._replay_worker = replay_worker
        self._due_retry_promoter = due_retry_promoter
        self._stop_event = asyncio.Event()

    async def run_due_retry_loop(self) -> None:
        while not self._stop_event.is_set():
            await self._due_retry_promoter.run_once(batch_size=self._config.batch_size)
            await asyncio.sleep(self._config.retry_scan_poll_sec)

    async def stop(self) -> None:
        self._stop_event.set()
```

---

## 10. 테스트 초안 포인트

### `tests/component/services/maintenance/test_failed_retryable_due_row_emits_retry_intent.py`

검증:
- `failed_retryable`
- `send_after <= now()`
- `latest_attempt_count < max`
- 새 `notification.plan.created.v1` retry-intent 가 append 되는지

### `tests/component/services/maintenance/test_retry_ceiling_creates_dead_letter.py`

검증:
- `latest_attempt_count >= NOTIFICATION_RETRY_MAX_ATTEMPTS`
- retry-intent 없음
- `dead_letter_entries` 생성
- `replay_hint = delivery_replay_from_notification_plan`

### `tests/unit/services/maintenance/test_send_disabled_rows_not_auto_retried.py`

검증:
- `delivery_status = suppressed`
- `telegram_response_json.send_disabled = true`
- due retry selection 에 들어오지 않는지

### `tests/component/services/maintenance/test_q_replay_delivery_request_dispatches_notification_plan_created.py`

검증:
- `replay.requested.v1`
- `replay_type = delivery`
- `root_object_type = notification_plan`
- 새 replay-intent `notification.plan.created.v1` append 되는지

### `tests/component/services/maintenance/test_non_delivery_replay_request_rejected.py`

검증:
- `replay_type != delivery` 또는 `root_object_type != notification_plan`
- `replay_requests.status = unsupported_in_stage41`
- downstream emit 없음

### `tests/component/services/maintenance/test_prod_replay_guard_blocks_dispatch.py`

검증:
- `APP_ENV=prod`
- `ENABLE_REPLAY_TO_PROD_DB=false`
- `replay_requests.status = rejected_by_env_guard`
- replay-intent 없음

### `tests/integration/delivery/test_explicit_delivery_replay_preserves_upstream_objects.py`

검증:
- replay root = `notification_plan`
- upstream `analysis` / `judge` / `bundle` 재실행 없음
- notifier 만 다시 타는지

---

## 11. 이번 단계가 구조를 지키는 이유

1. maintenance 는 `notification_plans` 를 직접 수정하지 않는다.  
   즉, notifier ownership 을 넘지 않는다.

2. due retry promotion 은 **read-only scan + outbox append** 로만 수행한다.  
   즉, stage 40 acceptance 를 코드 수준으로 내린다.

3. explicit delivery replay 도 결국 **같은 `notification.plan.created.v1` 브리지** 를 재사용한다.  
   즉, queue/runtime 구조를 새로 만들지 않는다.

4. send-disabled suppress row 와 failed_retryable row 를 분리한다.  
   즉, rollback recovery 가 자동 재전송으로 새지 않는다.

5. DLQ 는 bounded retry 초과분에만 사용한다.  
   즉, 무한 재시도와 침묵 실패 둘 다 피한다.

6. upstream `analysis` / `judge` / `bundle` 은 다시 계산하지 않는다.  
   즉, delivery recovery 때문에 상위 pipeline 을 오염시키지 않는다.

---

## 12. 다음 단계

이번 단계가 닫히면 다음 안전한 구현 순서는 아래가 맞다.

1. `42_delivery_operations_observability_and_dead_letter_hardening_v0_1.md`
   - delivery line 전용 operator alert / dashboard 최소 집합
   - `dead_letter_entries` triage / batch recovery 규칙
   - `pipeline_runs` / `job_attempts` 를 이용한 delivery SLO/lag 관측 기준
   - restricted/full rollout 전 delivery 운영 게이트 정교화

2. 그 이후 문서가 누적되면
   - `10_delivery_hardening_stage39_plus_v0_1.md`
   같은 새 번들로 stage 39+ 문서를 묶는 것이 자연스럽다.

즉, 다음 단계도 여전히 **delivery line 의 운영 hardening** 이다.

---

## 최종 한 줄 결론

이번 단계의 최소-change 정답은 **maintenance 가 `notification.delivery.result.v1` 와 due `failed_retryable` row 를 읽어 bounded retry-intent 를 `notification.plan.created.v1` 로 재발행하고, explicit `replay.requested.v1 (delivery)` 는 `notification_plan` root 에서만 받아 같은 브리지로 재진입시키며, send-disabled rollback recovery 는 auto retry 가 아니라 operator 승인형 delivery replay 로만 열어, delivery line 의 retry / replay / recovery 경계를 ownership 보존 상태로 닫는 것** 이다.



## Source file: `42_delivery_operations_observability_and_dead_letter_hardening_v0_1.md`

# 42단계: `delivery` operations observability and dead-letter hardening v0.1

## 0. 문서 목적

이 문서는 `41_delivery_retry_promotion_and_replay_hardening_v0_1.md`를 뒤엎는 문서가 아니다.  
목적은 **이미 잠긴 delivery retry/replay 경계를 유지한 채, delivery line 전용 운영 관측·DLQ triage·batch recovery·restricted/full rollout 게이트를 좁게 닫는 것**이다.

이번 단계에서 닫는 것은 정확히 아래 여덟 가지다.

1. delivery line 전용 **operator alert 최소 집합** 고정
2. `pipeline_runs` / `job_attempts` / `notification_delivery_records` / `dead_letter_entries` 기반의 **delivery SLO / lag / backlog 관측 규칙** 고정
3. `dead_letter_entries`를 delivery line에서 어떻게 분류하고, 무엇을 **auto-retry하지 않고 operator triage 대상으로 남길지** 고정
4. send-disabled suppress backlog, retry ceiling 초과 backlog, terminal delivery failure backlog에 대한 **batch recovery 규칙** 고정
5. restricted rollout / full rollout 직전의 **delivery 운영 게이트** 를 query/scorecard 기준으로 고정
6. runtime hot path가 아니라 ops/control plane에서 유지해야 하는 **SQL / dashboard / alert asset 패키지** 구조를 고정
7. maintenance / notifier ownership 을 넘지 않으면서도 **운영자가 어디를 보고 어떤 순서로 복구해야 하는지** 명확히 고정
8. 다음 단계가 새 구조 발명이 아니라 **gate runner / batch recovery tool code draft** 임을 명확히 고정

핵심 전제는 그대로 유지한다.

- `notifier-telegram`은 여전히 **presentation / delivery boundary** 다.
- `maintenance`는 여전히 **retry / replay orchestration boundary** 다.
- `notification_plans` / `notification_renders` / `notification_delivery_records` / `state_transitions` 직접 변경은 notifier ownership 이다.
- `dead_letter_entries` / `replay_requests` / `pipeline_runs` / `job_attempts` / `event_outbox` 는 maintenance/control-plane ownership 이다.
- delivery replay는 여전히 **`NotificationPlan` root 에서만** 다시 시작한다.
- upstream `analysis` / `judge` / `bundle` / `candidate` / `artifact` 는 delivery 장애 복구 때문에 다시 계산하지 않는다.

---

## 1. 왜 지금 이 단계가 정확한 다음 단계인가

최신 README 기준 현재 구현 상태는 stage 41까지 닫힌 상태이고, 다음 안전한 순서는 명시적으로 아래 하나였다.

- `42_delivery_operations_observability_and_dead_letter_hardening_v0_1.md`

또한 stage 41은 이미 아래를 잠갔다.

- `q.maintenance` 는 `notification.delivery.result.v1` 를 소비한다.
- due `failed_retryable` row 는 **retry-intent** 로만 재발행한다.
- send-disabled suppress row 는 **auto retry 대상이 아니다**.
- explicit delivery replay 는 **`notification_plan` root** 에서만 허용된다.
- maintenance 는 `notification_plans` 를 **읽기만** 하고 mutate 하지 않는다.

즉, 지금 collector / normalizer / enrichers / judge / policy / notifier 내부 hot path 를 다시 여는 것은 순서상 후퇴고,  
지금 닫아야 하는 것은 **delivery line 의 운영 관측과 DLQ/recovery 기준** 이다.

---

## 2. 이번 단계에서 드러나는 충돌과 최소-change 해석

## 2-1. 충돌 A — delivery observability를 강화하고 싶지만 스키마는 generic하다

현재 durable 운영 테이블은 delivery 전용이 아니라 전체 pipeline 공용이다.

- `pipeline_runs`
- `job_attempts`
- `dead_letter_entries`

반면 delivery-specific durable row는 notifier ownership 아래에 있다.

- `notification_plans`
- `notification_renders`
- `notification_delivery_records`
- `state_transitions`

즉, delivery line 운영 지표를 위해 새 테이블을 만들고 싶어질 수 있다.

### 최소-change 해석 A

이번 단계에서는 **새 delivery observability table을 추가하지 않는다.**

대신 아래처럼 해석한다.

1. `root_object_type = notification_plan` 을 delivery line root 로 고정한다.
2. `queue_name in {q.notification.send, q.maintenance, q.replay}` 와
   `stage_name` 관례로 delivery sub-stage 를 구분한다.
3. delivery dashboard / alert / gate 는 위 필터를 사용해 generic table 에서 계산한다.

즉, **generic schema 위에 delivery-specific query convention을 얹는 것**이 최소 변경이다.

---

## 2-2. 충돌 B — DLQ triage를 하고 싶지만 maintenance가 `notification_plans`를 고칠 수는 없다

stage 41에서 이미 notifier / maintenance ownership 경계가 잠겼다.

- notifier: `notification_*`, `state_transitions`, `event_outbox`
- maintenance: `pipeline_runs`, `job_attempts`, `dead_letter_entries`, `replay_requests`, `event_outbox`

따라서 DLQ batch recovery가 곧바로 `notification_plans.status` reset 으로 이어지면 ownership이 무너진다.

### 최소-change 해석 B

batch recovery는 아래 두 가지로만 고정한다.

1. **retry-intent event 재발행**
   - due `failed_retryable` row
   - 또는 operator가 명시적으로 선택한 retryable backlog

2. **explicit replay request 생성**
   - send-disabled suppress backlog
   - terminal failure backlog
   - retry ceiling 초과 backlog

즉, **plan row reset은 하지 않고, outbox/replay bridge만 다시 태운다.**

---

## 2-3. 충돌 C — rollout gate를 강화하고 싶지만 runtime hot path가 gate runner가 되면 안 된다

stage 10은 rollout / rollback / feature flag / gate 를 운영 통제로 봤다.  
반면 notifier / maintenance hot path 는 transport / retry / replay orchestration 계층이다.

즉, runtime worker가 스스로 restricted/full rollout 여부를 판단하고 행동을 바꾸기 시작하면 계층이 섞인다.

### 최소-change 해석 C

restricted/full rollout gate는 **runtime 로직이 아니라 ops scorecard** 로 둔다.

- SQL / dashboard / alert 로 현 상태를 계산
- operator 또는 gate-runner가 해석
- feature flag on/off 는 그 결과를 반영

즉, **gate는 판단 체계이지 transport worker 내부 if/else가 아니다.**

---

## 2-4. 충돌 D — 운영 경보를 notifier로 보내고 싶어질 수 있다

delivery line 운영 사고는 notifier와 관련이 있으므로, 가장 쉬운 길은 notifier가 자기 장애를 같은 텔레그램 채널로 보내는 것이다.

하지만 stage 8은 운영 경보와 사용자 알림 분리를 잠갔다.

### 최소-change 해석 D

이번 단계에서는 아래를 고정한다.

- delivery operator alert 는 **별도 ops 채널 / dashboard / pager** 로 간다.
- `notifier-telegram`은 사용자 triage 전달물만 담당한다.
- delivery line 지표는 notifier 본문에 섞지 않는다.

즉, **운영 경보는 delivery line의 소비자가 아니라 delivery line을 감시하는 별도 표면** 이어야 한다.

---

## 3. 이번 단계에서 고정할 범위와 제외 범위

### 포함

- delivery line correlation/filter convention
- operator alert 최소 집합
- dashboard 최소 집합
- SQL query pack 초안
- dead-letter taxonomy
- batch recovery rules
- rollout gate scorecard 기준
- 관련 테스트 포인트

### 제외

- notifier / maintenance hot path 재설계
- 새 durable table 추가
- auto-resolve DLQ workflow
- digest runtime 활성화
- user-facing dashboard
- stage 39+ bundle 생성
- source/enrich/judge/full pipeline observability 전체 일반화

즉, 이번 문서는 **delivery line 전용 운영 hardening** 만 닫는다.

---

## 4. 대상 파일 트리

```text
ops/
  delivery/
    alerts/
      delivery_minimal_alerts.yaml          # new
    dashboards/
      delivery_minimal_dashboard.md         # new
    runbooks/
      delivery_dead_letter_triage.md        # new
      delivery_batch_recovery.md            # new
    sql/
      delivery_observability_queries.sql    # new
      delivery_rollout_gate_queries.sql     # new

tests/
  unit/
    ops/
      delivery/
        test_delivery_gate_scorecard.md     # new doc-test style
        test_delivery_dead_letter_taxonomy.md # new doc-test style
  integration/
    delivery/
      test_delivery_slo_queries_match_fixture_expectations.py   # new
      test_dead_letter_batch_recovery_request_pack.py           # new
      test_restricted_rollout_gate_fails_on_delivery_dlq.py     # new
      test_full_rollout_gate_requires_zero_send_disabled_rows.py # new
```

주의:

- 이번 단계는 새 서비스를 만드는 턴이 아니다.
- `ops/` 자산과 query/scorecard 기준을 먼저 고정하는 턴이다.
- code draft는 다음 단계에서 gate runner / batch recovery tool 형태로 내리는 것이 맞다.

---

## 5. delivery observability model

## 5-1. delivery line root object

delivery line 의 운영 root 는 아래로 고정한다.

```text
root_object_type = notification_plan
root_object_id   = notification_plan_id
```

이유:
- stage 8 정본이 delivery replay root 를 `NotificationPlan` 으로 잠갔다.
- stage 40/41도 같은 root 에서 retry / replay / recovery 를 해석했다.
- upstream `analysis` / `judge` / `bundle` 을 delivery 장애 복구에 다시 태우지 않기 위해서다.

즉, delivery line 관측/복구/게이트도 **`notification_plan` 단위** 로 보는 것이 맞다.

---

## 5-2. stage / queue naming convention

generic 운영 테이블 위에서 delivery line 을 안정적으로 쿼리하려면 관례가 필요하다.

권장 관례는 아래다.

### `job_attempts.stage_name`

- `notify`
- `maintenance_delivery_result`
- `maintenance_delivery_due_retry`
- `replay_delivery`

### `job_attempts.queue_name`

- `q.notification.send`
- `q.maintenance`
- `q.replay`

### `pipeline_runs.run_kind`

- `live`
- `maintenance`
- `replay`
- `manual`

### `pipeline_runs.trigger_source`

- `notification_plan_created`
- `notification_delivery_result`
- `delivery_due_retry_promotion`
- `explicit_delivery_replay`

중요:
- 이건 새 enum 이 아니다.
- generic text column 에 대한 **운영 명명 규칙** 이다.
- stage 42 이후의 query/alert/gate 는 이 규칙을 전제로 한다.

---

## 5-3. delivery correlation 최소 세트

delivery line query 와 dashboard 에서 최소 아래 ID 를 항상 같이 본다.

- `notification_plan_id`
- `analysis_id`
- `candidate_group_id`
- `telegram_message_id` (있을 때)
- `pipeline_run_id` (있을 때)
- `job_attempt_id`

즉, delivery line 은 **plan → render → delivery_record → job_attempt / pipeline_run** 으로 복기 가능해야 한다.

---

## 5-4. delivery lag 해석 규칙

delivery lag 는 하나가 아니라 세 가지로 나눈다.

### A. plan-to-transport lag
정의:
- `notification_plans.created_at` → latest successful `sent_at/edited_at`

용도:
- notifier queue / transport 지연 확인

### B. due-retry promotion lag
정의:
- `notification_plans.send_after` → retry-intent emit 시각

용도:
- maintenance promotion loop 지연 확인

### C. end-to-end HIGH lag
정의:
- `source_messages.posted_at` → delivery success 시각

용도:
- stage 8/10 rollout gate와 연결되는 최종 운영 가치 확인

즉, alert 와 gate 는 이 세 lag 를 혼동하면 안 된다.

---

## 6. operator alert 최소 집합

이번 단계의 최소 alert set 은 아래로 고정한다.

## 6-1. Alert A — `q.notification.send` backlog age spike

의미:
- notifier consumption/transport 가 밀리고 있음

권장 조건:
- trailing 15분 기준
- oldest unsent `notification_plans` age > 5분
- 또는 HIGH only 기준 oldest age > 2분

권장 severity:
- warning → critical

권장 조치:
1. Telegram API 장애 / flood-control 확인
2. `ENABLE_NOTIFICATION_SEND` / dry-run 상태 확인
3. retry backlog 와 send_after future row 구분 확인

---

## 6-2. Alert B — retryable backlog due but not promoted

의미:
- maintenance due retry promotion loop 가 밀리거나 꺼져 있음

권장 조건:
- `failed_retryable` 이고 `send_after <= now()` 인 row 가 N개 이상
- 또는 oldest due retry lag > 2분

권장 severity:
- warning

권장 조치:
1. `MAINTENANCE_ENABLE_NOTIFICATION_RETRY_PROMOTION` 확인
2. `q.maintenance` worker health 확인
3. outbox append 실패 / dedupe conflict 확인

---

## 6-3. Alert C — delivery DLQ growth

의미:
- bounded retry 밖의 delivery failure 가 누적 중

권장 조건:
- `dead_letter_entries`에서 delivery-related stage 의 row count 증가
- oldest delivery DLQ age > 15분

권장 severity:
- warning → critical

권장 조치:
1. `last_error_code` / `next_manual_action` 기준 triage
2. template/render 문제인지 chat access 문제인지 분리
3. batch replay 대상과 close 대상 분리

---

## 6-4. Alert D — send-disabled suppress rows while rollout flag says send should be on

의미:
- runtime flag drift 또는 env misconfiguration

권장 조건:
- `ENABLE_NOTIFICATION_SEND=true`
- 그런데 latest `notification_delivery_records.telegram_response_json.send_disabled = true` rows 존재

권장 severity:
- critical

권장 조치:
1. compose/env drift 확인
2. notifier container env 확인
3. rollout gate 즉시 fail 처리

---

## 6-5. Alert E — prod replay guard rejects

의미:
- prod 에서 explicit replay 요청이 들어왔지만 env guard 때문에 막힘
- 운영자가 replay 허용을 기대했을 수 있음

권장 조건:
- `replay_requests.status = rejected_by_env_guard` row 증가

권장 severity:
- info → warning

권장 조치:
1. 의도된 차단인지 확인
2. 필요 시 `ENABLE_REPLAY_TO_PROD_DB=true` 임시 승인
3. replay 대상 범위 축소 후 재요청

---

## 6-6. Alert F — delivery success rate drop

의미:
- send/edit 성공률 하락

권장 조건:
- trailing 1h 기준
- `sent|edited` / total delivery attempts < 99%

권장 severity:
- warning

권장 조치:
1. 429/5xx 증가 여부 확인
2. terminal failure code 분포 확인
3. render bug / entity invalid 증가 여부 확인

---

## 7. dashboard 최소 집합

초기 dashboard 는 4개면 충분하다.

## 7-1. Dashboard A — Delivery Runtime

표시:
- `q.notification.send` backlog count
- oldest plan age
- due retry count
- oldest due retry lag
- `send_after > now()` future backlog count

목적:
- 현재 delivery line 이 실제로 밀리고 있는지 즉시 확인

---

## 7-2. Dashboard B — Delivery Outcome Mix

표시:
- `sent`
- `edited`
- `suppressed`
- `failed_retryable`
- `failed_terminal`
- `notification_duplicate_noop`
- `telegram_edit_not_modified_noop`

목적:
- transport 성공/실패/no-op 비율 확인
- 중복 억제나 edit-noop spike 감지

---

## 7-3. Dashboard C — Delivery DLQ / Recovery

표시:
- delivery-related `dead_letter_entries` count
- oldest DLQ age
- `last_error_code` top N
- `next_manual_action` top N
- replay request statuses
  - `requested`
  - `dispatched`
  - `completed`
  - `unsupported_in_stage41`
  - `rejected_by_env_guard`

목적:
- operator triage 와 recovery backlog 확인

---

## 7-4. Dashboard D — Rollout Gate Scorecard

표시:
- HIGH end-to-end p95 lag
- plan-to-transport p95 lag
- success rate
- due retry promotion lag
- delivery DLQ open count
- send-disabled suppress count
- prod replay guard reject count

목적:
- restricted/full rollout entry 여부를 숫자로 확인

---

## 8. SQL draft

아래 SQL 은 stage 42에서 고정하는 **운영 query contract** 다.  
실제 파일은 `ops/delivery/sql/` 아래로 내리는 것이 맞다.

## 8-1. 현재 unsent backlog

```sql
SELECT COUNT(*) AS unsent_plan_count,
       MIN(np.created_at) AS oldest_plan_created_at
FROM notification_plans np
WHERE np.status IN (
  'planned'::notification_status_enum,
  'rendered'::notification_status_enum,
  'queued'::notification_status_enum,
  'failed_retryable'::notification_status_enum
);
```

---

## 8-2. due retry backlog

```sql
SELECT COUNT(*) AS due_retry_count,
       MIN(np.send_after) AS oldest_due_send_after
FROM notification_plans np
WHERE np.status = 'failed_retryable'::notification_status_enum
  AND np.send_after IS NOT NULL
  AND np.send_after <= now();
```

---

## 8-3. trailing 1h delivery outcome mix

```sql
SELECT dr.delivery_status,
       COUNT(*) AS cnt
FROM notification_delivery_records dr
WHERE dr.created_at >= now() - interval '1 hour'
GROUP BY dr.delivery_status
ORDER BY cnt DESC;
```

---

## 8-4. trailing 1h transport error class mix

```sql
SELECT COALESCE(dr.transport_error_class, 'none') AS transport_error_class,
       COUNT(*) AS cnt
FROM notification_delivery_records dr
WHERE dr.created_at >= now() - interval '1 hour'
GROUP BY COALESCE(dr.transport_error_class, 'none')
ORDER BY cnt DESC;
```

---

## 8-5. HIGH plan-to-transport p95 lag

```sql
WITH delivered AS (
  SELECT np.notification_plan_id,
         np.created_at AS plan_created_at,
         COALESCE(dr.sent_at, dr.edited_at) AS delivered_at
  FROM notification_plans np
  JOIN LATERAL (
    SELECT delivery_status, sent_at, edited_at
    FROM notification_delivery_records
    WHERE notification_plan_id = np.notification_plan_id
      AND delivery_status IN (
        'sent'::notification_status_enum,
        'edited'::notification_status_enum
      )
    ORDER BY created_at DESC
    LIMIT 1
  ) dr ON TRUE
  WHERE np.urgency_profile = 'high'::urgency_profile_enum
)
SELECT percentile_cont(0.95)
       WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM (delivered_at - plan_created_at))) AS p95_plan_to_transport_sec
FROM delivered
WHERE delivered_at IS NOT NULL;
```

---

## 8-6. HIGH source-to-delivery p95 lag

```sql
WITH high_delivered AS (
  SELECT np.notification_plan_id,
         sm.posted_at AS source_posted_at,
         COALESCE(dr.sent_at, dr.edited_at) AS delivered_at
  FROM notification_plans np
  JOIN analyses a
    ON a.analysis_id = np.analysis_id
  JOIN candidate_group_proposals cgp
    ON cgp.candidate_group_id = np.candidate_group_id
  JOIN source_messages sm
    ON sm.source_message_id = cgp.source_message_id
  JOIN LATERAL (
    SELECT delivery_status, sent_at, edited_at
    FROM notification_delivery_records
    WHERE notification_plan_id = np.notification_plan_id
      AND delivery_status IN (
        'sent'::notification_status_enum,
        'edited'::notification_status_enum
      )
    ORDER BY created_at DESC
    LIMIT 1
  ) dr ON TRUE
  WHERE np.urgency_profile = 'high'::urgency_profile_enum
)
SELECT percentile_cont(0.95)
       WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM (delivered_at - source_posted_at))) AS p95_source_to_delivery_sec
FROM high_delivered
WHERE delivered_at IS NOT NULL;
```

---

## 8-7. delivery DLQ triage view

```sql
SELECT dle.stage_name,
       dle.queue_name,
       dle.root_object_id,
       dle.last_error_code,
       dle.retry_count,
       dle.first_failed_at,
       dle.last_failed_at,
       dle.next_manual_action,
       dle.replay_hint
FROM dead_letter_entries dle
WHERE dle.root_object_type = 'notification_plan'
ORDER BY dle.last_failed_at DESC;
```

---

## 8-8. send-disabled suppress backlog selection

```sql
SELECT np.notification_plan_id,
       np.analysis_id,
       np.candidate_group_id,
       dr.created_at,
       dr.telegram_response_json
FROM notification_plans np
JOIN notification_delivery_records dr
  ON dr.notification_plan_id = np.notification_plan_id
WHERE dr.delivery_status = 'suppressed'::notification_status_enum
  AND dr.telegram_response_json ->> 'send_disabled' = 'true'
ORDER BY dr.created_at ASC;
```

---

## 8-9. batch replay request insert skeleton

```sql
INSERT INTO replay_requests (
  replay_request_id,
  replay_type,
  root_object_type,
  root_object_id,
  requested_by,
  requested_at,
  status
)
SELECT gen_random_uuid(),
       'delivery'::replay_type_enum,
       'notification_plan',
       CAST(src.notification_plan_id AS uuid),
       :requested_by,
       now(),
       'requested'
FROM (
  SELECT unnest(CAST(:notification_plan_ids AS uuid[])) AS notification_plan_id
) src;
```

---

## 9. dead-letter taxonomy

이번 단계에서 delivery line DLQ taxonomy 를 아래처럼 고정한다.

## 9-1. `last_error_code`

- `max_notification_retry_attempts_exceeded`
- `notify_transport_terminal_chat_access`
- `notify_transport_terminal_edit_forbidden`
- `notify_render_invalid_payload`
- `delivery_replay_env_guard_rejected`
- `delivery_replay_unsupported_request`
- `maintenance_due_retry_emit_failed`

---

## 9-2. `next_manual_action`

- `request_explicit_delivery_replay`
- `fix_chat_access_then_delivery_replay`
- `disable_edits_then_delivery_replay`
- `fix_template_then_delivery_replay`
- `acknowledge_and_close_no_recovery`
- `fix_env_guard_then_retry_replay_request`

---

## 9-3. `replay_hint`

- `delivery_replay_from_notification_plan`

중요:
- delivery DLQ 는 자동 close 하지 않는다.
- replay hint 는 always-on freeform 이 아니라 위 한 값으로 좁게 유지한다.
- operator 가 다음 동작을 오해하지 않도록 vocabulary 를 줄인다.

---

## 10. batch recovery rules

## 10-1. auto retry 허용

아래만 auto retry 허용 대상이다.

- latest delivery status = `failed_retryable`
- `send_after <= now()`
- latest attempt count < retry ceiling
- `notification_send_flag_disabled` suppress 아님

즉, stage 41 규칙을 유지한다.

---

## 10-2. explicit replay required

아래는 explicit replay 로만 복구한다.

- `delivery_status = suppressed` + `send_disabled=true`
- `delivery_status = failed_terminal`
- retry ceiling 초과 DLQ row
- `replay_requests.status = rejected_by_env_guard`
- `replay_requests.status = unsupported_in_stage41`

---

## 10-3. batch recovery 우선순위

권장 우선순위:

1. send-disabled suppress backlog
2. retry ceiling exceeded backlog
3. terminal chat access fixed backlog
4. render/template fix backlog
5. env-guard rejected replay backlog

이유:
- send-disabled suppress 는 가장 “정상적인 운영 보류 상태” 이고, 원인 해결 후 복구가 가장 쉽다.
- render/template fix backlog 는 operator 확인이 더 많이 필요하다.
- env guard reject 는 운영 승인 체계가 먼저다.

---

## 11. restricted / full rollout delivery gates

## 11-1. restricted rollout entry gate

아래를 모두 만족해야 한다.

- `ENABLE_NOTIFICATION_SEND=true`
- `NOTIFIER_TELEGRAM_DRY_RUN=false`
- `MAINTENANCE_ENABLE_NOTIFICATION_RETRY_PROMOTION=true`
- trailing 1h delivery success rate >= 99%
- HIGH source-to-delivery p95 <= 120s
- due retry backlog oldest lag <= 120s
- open delivery DLQ count = 0 또는 operator 승인된 known issue only
- unexpected send-disabled suppress row = 0

---

## 11-2. full rollout entry gate

restricted rollout 기준을 만족한 상태에서 추가로 아래를 본다.

- trailing 24h delivery success rate stable
- retry ceiling exceeded new rows = 0 또는 매우 낮음
- delivery DLQ oldest age < 1h
- prod replay guard reject count = 0 (의도된 테스트 제외)
- duplicate/no-op spike 없음
- operator manual review 에서 message usefulness 문제 없음

---

## 11-3. gate fail 시 조치

### A. runtime/transport 문제
- `ENABLE_NOTIFICATION_SEND=false`
- `MAINTENANCE_ENABLE_NOTIFICATION_RETRY_PROMOTION=false`
- 원인 수정 후 explicit replay 또는 retry promotion 재개

### B. rollout gate만 fail
- transport는 유지 가능
- restricted scope 축소
- noisy channel 제외
- template/render 수정 후 재평가

즉, gate fail 이 항상 full stop 을 의미하지는 않는다.  
다만 **full rollout 진입은 금지** 한다.

---

## 12. 테스트 포인트

### `tests/integration/delivery/test_delivery_slo_queries_match_fixture_expectations.py`
검증:
- fixture 기준 p95/source-to-delivery 쿼리가 기대값 범위 안에 있는지

### `tests/integration/delivery/test_dead_letter_batch_recovery_request_pack.py`
검증:
- selected `notification_plan_id` 집합으로 replay_requests batch insert 가 정확히 만들어지는지

### `tests/integration/delivery/test_restricted_rollout_gate_fails_on_delivery_dlq.py`
검증:
- open delivery DLQ 존재 시 restricted gate 가 fail 하는지

### `tests/integration/delivery/test_full_rollout_gate_requires_zero_send_disabled_rows.py`
검증:
- unexpected send-disabled suppress row 가 있으면 full gate fail 인지

### `tests/unit/ops/delivery/test_delivery_dead_letter_taxonomy.md`
검증:
- `last_error_code / next_manual_action / replay_hint` vocabulary 가 stage 42 문서와 일치하는지

---

## 13. 이번 단계가 구조를 지키는 이유

1. 새 delivery observability table 을 만들지 않는다.  
   즉, 기존 generic 운영 스키마를 재사용한다.

2. maintenance 는 `notification_plans` 를 계속 read-only 로만 본다.  
   즉, notifier ownership 을 넘지 않는다.

3. DLQ batch recovery 도 결국 **retry-intent event 또는 replay request** 로만 다시 들어간다.  
   즉, same bridge를 재사용한다.

4. rollout gate 는 ops scorecard 이지 runtime policy 가 아니다.  
   즉, policy-engine / notifier responsibilities를 침범하지 않는다.

5. operator alert 는 사용자 알림 채널과 분리된다.  
   즉, notifier 가 자기 장애를 같은 triage 채널로 흘리지 않는다.

---

## 14. 다음 단계

이번 단계가 닫히면 다음 안전한 구현 순서는 아래가 맞다.

1. `43_delivery_gate_runner_and_batch_recovery_code_draft_v0_1.md`
   - stage 42 SQL/scorecard 를 실제 gate-runner / CLI / batch recovery tool 초안으로 내리기
   - restricted/full rollout 판정 로직의 code draft
   - selected `notification_plan_id` batch replay request 생성 utility
   - delivery ops asset 과 maintenance service wiring 경계 정리

2. 그 이후 문서가 누적되면
   - `10_delivery_hardening_stage39_plus_v0_1.md`
   같은 새 번들로 stage 39+ 문서를 묶는 것이 자연스럽다.

즉, 다음 단계도 여전히 **delivery line의 control-plane hardening** 이다.

---

## 최종 한 줄 결론

이번 단계의 최소-change 정답은 **delivery line 전용 operator alert / dashboard / SQL query pack / dead-letter taxonomy / batch recovery 규칙 / rollout gate scorecard를 generic 운영 스키마 위에 얹어, notifier와 maintenance의 ownership을 넘지 않은 상태로 delivery line의 운영 관측·triage·복구 기준을 닫는 것** 이다.



## Source file: `43_delivery_gate_runner_and_batch_recovery_code_draft_v0_1.md`

# 43단계: `delivery` gate runner and batch recovery code draft v0.1

## 0. 문서 목적

이 문서는 `42_delivery_operations_observability_and_dead_letter_hardening_v0_1.md`를 뒤엎는 문서가 아니다.  
목적은 **이미 잠긴 delivery line control-plane 경계를 유지한 채, stage 42에서 고정한 SQL/scorecard/batch-recovery 규칙을 실제 gate-runner / CLI / batch recovery tool 코드 초안으로 내리는 것**이다.

이번 단계에서 닫는 것은 정확히 아래 여섯 가지다.

1. restricted / full rollout **gate runner의 read-only code path** 고정
2. stage 42 scorecard를 **`DeliveryGateReportV1`** 로 계산하는 계약 고정
3. selected `notification_plan_id` 집합에 대해 **explicit replay request batch 생성 utility** 를 코드로 고정
4. selected due retryable plan 집합에 대해 **manual retry-intent emission utility** 를 코드로 고정
5. 위 두 control-plane 도구가 **notifier ownership을 침범하지 않도록** boundary를 고정
6. maintenance service와 delivery ops asset 사이의 **wiring point는 CLI / one-shot job이지 hot-path worker가 아님** 을 고정

핵심 전제는 그대로 유지한다.

- `notifier-telegram`은 여전히 **presentation / delivery boundary** 다.
- `maintenance`는 여전히 **retry / replay orchestration boundary** 다.
- `notification_plans` / `notification_renders` / `notification_delivery_records` / `state_transitions` 직접 변경은 notifier ownership 이다.
- gate runner / batch recovery tool 은 **ops/control plane asset** 이지, queue consumer의 신규 의미 계층이 아니다.
- delivery recovery 때문에 upstream `analysis` / `judge` / `bundle` / `candidate` / `artifact` 를 다시 계산하지 않는다.

---

## 1. 왜 지금 이 단계가 정확한 다음 단계인가

최신 README 기준 현재 구현 상태는 stage 42까지 닫힌 상태이고, 다음 안전한 순서는 명시적으로 아래 하나였다.

- `43_delivery_gate_runner_and_batch_recovery_code_draft_v0_1.md`

또한 stage 42는 아래를 이미 잠갔다.

- delivery gate 는 **runtime worker 내부 분기** 가 아니라 **ops scorecard** 로 본다.
- batch recovery 는 `notification_plans` reset 이 아니라 **retry-intent event 재발행** 또는 **explicit replay request 생성** 으로만 수행한다.
- delivery line observability 는 **generic 운영 스키마 + query convention** 위에서 계산한다.
- send-disabled suppress backlog, retry ceiling 초과 backlog, terminal failure backlog 는 모두 **operator가 선택한 복구 집합** 으로 다뤄야 한다.

즉, 지금 새 서비스를 설계하거나 notifier/maintenance hot path 를 다시 열어보는 것은 순서상 후퇴고,  
지금 닫아야 하는 것은 **stage 42의 운영 규칙을 실제 실행 가능한 code draft 로 내리는 단계** 다.

---

## 2. 이번 단계에서 드러나는 충돌과 최소-change 해석

## 2-1. 충돌 A — gate runner를 runtime worker로 만들면 계층이 섞인다

stage 42는 rollout gate를 **ops scorecard** 로 잠갔다.  
그런데 이를 편하게 구현하려고 `maintenance` worker 내부에서 restricted/full gate를 상시 계산하고 feature flag를 바꾸게 만들면 아래 문제가 생긴다.

- runtime hot path 가 운영 통제 계층을 흡수함
- feature flag 판단과 transport/retry orchestration이 섞임
- stage 10의 operator-governed rollout 철학과 충돌함

### 최소-change 해석 A

gate runner 는 **one-shot CLI / scheduled manual job** 으로만 둔다.

- long-running queue consumer가 아니다.
- feature flag를 직접 바꾸지 않는다.
- 현재 scorecard를 계산하고 `pass/fail/warn` 과 `recommended_flag_patch` 를 출력만 한다.

즉, **gate는 계산하고 권고만 하며, 적용은 operator가 한다.**

---

## 2-2. 충돌 B — batch recovery가 쉬워 보인다고 `notification_plans`를 직접 고치면 ownership이 무너진다

stage 41/42에서 이미 아래가 잠겨 있다.

- notifier: `notification_*`, `state_transitions`, `event_outbox`
- maintenance/control plane: `pipeline_runs`, `job_attempts`, `dead_letter_entries`, `replay_requests`, `event_outbox`

따라서 batch recovery tool 이 아래를 직접 하면 안 된다.

- `notification_plans.status` reset
- `notification_plans.send_after` 직접 변경
- `notification_delivery_records` 수정

### 최소-change 해석 B

batch recovery tool 은 아래 두 가지 write 만 수행한다.

1. **explicit replay request 생성**
   - `replay_requests` insert
   - downstream `q.replay` path 재사용

2. **manual retry-intent outbox append**
   - `event_outbox(notification.plan.created.v1)` insert
   - downstream `q.notification.send` path 재사용

즉, **plan row mutate 가 아니라 bridge 재진입** 만 수행한다.

---

## 2-3. 충돌 C — full rollout gate에는 operator judgement 항목이 있다

stage 42의 full rollout gate 에는 아래 같은 항목이 있다.

- duplicate/no-op spike 없음
- operator manual review 에서 message usefulness 문제 없음

이 둘은 완전히 machine-only hard gate 로 고정하기 어렵다.

### 최소-change 해석 C

gate runner 는 지표를 두 층으로 나눈다.

### hard blocking metrics
- success rate
- p95 lag
- due retry lag
- open delivery DLQ count
- unexpected send-disabled count
- replay guard reject count

### operator review required metrics
- duplicate/no-op ratio
- message usefulness review flag
- known issue override note

즉, **hard fail 은 코드로, 주관적 검토는 report field 로 남긴다.**

---

## 2-4. 충돌 D — selected `notification_plan_id` 집합이 서로 다른 복구 경로를 섞을 수 있다

운영자가 선택한 plan 집합에는 아래가 섞일 수 있다.

- `failed_retryable`
- `suppressed` + `send_disabled=true`
- `failed_terminal`
- retry ceiling 초과 DLQ row

이걸 한 API로 그대로 받으면 잘못된 복구 경로가 섞일 수 있다.

### 최소-change 해석 D

batch recovery CLI 는 **전략별 subcommand** 로 강제한다.

- `replay-selected`
- `retry-selected-due`

그리고 각 subcommand 는 selection validation 을 반드시 수행한다.

- `replay-selected`
  - send-disabled suppress / failed_terminal / retry-ceiling-exceeded 같은 explicit replay 대상만 허용
- `retry-selected-due`
  - latest delivery status = `failed_retryable`
  - `send_after <= now()`
  - attempt ceiling 미도달 인 plan 만 허용

즉, **복구 전략 혼합은 CLI 레벨에서 차단** 한다.

---

## 2-5. 충돌 E — gate runner가 feature flag를 자동 patch 하면 rollback/governance 경계가 흔들린다

stage 10은 rollout / rollback / change governance 를 operator 통제로 본다.  
따라서 gate runner 가 아래를 직접 해서는 안 된다.

- `.env` 수정
- compose override write
- flag toggle 자동 적용

### 최소-change 해석 E

gate runner 는 **권장 patch** 만 출력한다.

예:

```json
{
  "recommended_flag_patch": {
    "ENABLE_NOTIFICATION_SEND": true,
    "MAINTENANCE_ENABLE_NOTIFICATION_RETRY_PROMOTION": true,
    "NOTIFIER_TELEGRAM_DRY_RUN": false
  }
}
```

즉, **gate runner는 calculator + reporter 이고, applier 가 아니다.**

---

## 3. 이번 단계에서 고정할 범위와 제외 범위

### 포함

- delivery gate scorecard code draft
- restricted/full rollout decision logic code draft
- selected plan batch replay request utility
- selected due retryable plan manual retry-intent utility
- maintenance package 내 CLI wiring
- control-plane pipeline/job attempt correlation draft
- 관련 테스트 초안

### 제외

- notifier 내부 send/edit/render 로직 수정
- `notification_plans` 직접 mutate
- feature flag auto apply
- digest runtime 활성화
- user-facing dashboard 구현
- stage 39+ bundle 생성
- delivery governance UI

즉, 이번 문서는 **delivery control-plane execution draft** 만 닫는다.

---

## 4. 대상 파일 트리

```text
src/services/maintenance/
  __init__.py
  config.py                # updated
  models.py                # updated
  repositories.py          # updated
  retry_policy.py          # unchanged
  delivery_result_worker.py # unchanged
  due_retry_promoter.py    # unchanged
  replay_worker.py         # unchanged
  delivery_gate_runner.py  # new
  batch_recovery_tool.py   # new
  main.py                  # updated

tests/
  unit/
    services/
      maintenance/
        test_delivery_gate_runner.py          # new
        test_batch_recovery_validation.py     # new
        test_batch_recovery_replay_insert.py  # new
        test_batch_recovery_retry_intent.py   # new
  integration/
    delivery/
      test_restricted_gate_runner_fixture.py  # new
      test_full_gate_runner_warn_only_fields.py # new
      test_batch_replay_request_cli_flow.py   # new
      test_selected_due_retry_cli_flow.py     # new
```

주의:

- stage 41의 queue consumer worker 들은 유지한다.
- stage 43 신규 자산은 **one-shot control-plane tool** 로만 들어간다.
- `ops/delivery/sql/*` 는 여전히 stage 42의 source-of-truth query contract 이고, stage 43 코드는 그 SQL 해석을 재사용한다.

---

## 5. configuration 추가 규칙

## 5-1. `src/services/maintenance/config.py` 추가 필드

```text
DELIVERY_GATE_MIN_SUCCESS_RATE_1H=0.99
DELIVERY_GATE_MIN_SUCCESS_RATE_24H=0.99
DELIVERY_GATE_MAX_HIGH_SOURCE_TO_DELIVERY_P95_SEC=120
DELIVERY_GATE_MAX_PLAN_TO_TRANSPORT_P95_SEC=120
DELIVERY_GATE_MAX_DUE_RETRY_LAG_SEC=120
DELIVERY_GATE_MAX_OPEN_DLQ_COUNT=0
DELIVERY_GATE_MAX_SEND_DISABLED_COUNT=0
DELIVERY_GATE_MAX_REPLAY_GUARD_REJECT_COUNT=0
DELIVERY_GATE_REQUIRE_OPERATOR_REVIEW_FOR_FULL=true
```

설명:

- 1h / 24h success rate 는 stage 42 gate scorecard 기준을 그대로 code parameter 로 내린 것이다.
- duplicate/no-op spike 는 hard block 이 아니라 warning/report 영역으로 남긴다.
- full rollout 의 operator manual review 항목은 env 로 숨기지 않고 `require_operator_review_for_full` 로 명시한다.

---

## 5-2. CLI 실행 모드

`main.py` 는 최소 아래 subcommand 를 지원하는 것이 맞다.

```text
python -m src.services.maintenance.main worker
python -m src.services.maintenance.main delivery-gate --mode restricted --format json
python -m src.services.maintenance.main delivery-gate --mode full --format json
python -m src.services.maintenance.main batch-recovery replay-selected --plan-id <uuid> --requested-by ops
python -m src.services.maintenance.main batch-recovery retry-selected-due --plan-id <uuid> --requested-by ops
```

원칙:

- default 는 기존 worker runtime 유지
- gate / batch-recovery 는 explicit subcommand 로만 진입
- accidental live execution 을 막기 위해 subcommand 가 없으면 control-plane 동작을 하지 않는다.

---

## 6. gate runner contract

## 6-1. 입력

### 필수 입력
- `mode = restricted | full`
- 현재 env flags
- stage 42 scorecard query 결과

### 선택 입력
- `operator_review_passed: bool | None`
- `known_issue_override_codes: list[str]`

설명:

- restricted gate 는 machine-only hard block 위주다.
- full gate 는 operator review 항목이 있으므로 선택 입력을 받는다.

---

## 6-2. 출력 모델

```python
from dataclasses import dataclass, field
from typing import Literal

GateMode = Literal["restricted", "full"]
GateStatus = Literal["pass", "fail", "warn"]

@dataclass(slots=True, frozen=True)
class DeliveryGateMetric:
    metric_name: str
    observed_value: float | int | str | None
    threshold: float | int | str | None
    comparator: str
    passed: bool
    severity: str = "block"


@dataclass(slots=True, frozen=True)
class DeliveryGateReportV1:
    mode: GateMode
    gate_status: GateStatus
    blocking_reason_codes: list[str]
    warning_reason_codes: list[str]
    metrics: list[DeliveryGateMetric]
    operator_review_required: bool
    operator_review_passed: bool | None
    recommended_flag_patch: dict[str, object]
```

핵심:

- `gate_status = fail` 이면 rollout 진입 금지
- `gate_status = warn` 은 hard-fail 은 아니지만 operator 확인 필요
- `recommended_flag_patch` 는 출력만 하고 자동 적용하지 않는다.

---

## 6-3. restricted gate hard block 규칙

stage 42 기준을 코드로 내리면 아래가 맞다.

- `ENABLE_NOTIFICATION_SEND == true`
- `NOTIFIER_TELEGRAM_DRY_RUN == false`
- `MAINTENANCE_ENABLE_NOTIFICATION_RETRY_PROMOTION == true`
- trailing 1h delivery success rate >= `DELIVERY_GATE_MIN_SUCCESS_RATE_1H`
- HIGH source-to-delivery p95 <= `DELIVERY_GATE_MAX_HIGH_SOURCE_TO_DELIVERY_P95_SEC`
- due retry oldest lag <= `DELIVERY_GATE_MAX_DUE_RETRY_LAG_SEC`
- open delivery DLQ count <= `DELIVERY_GATE_MAX_OPEN_DLQ_COUNT`
- unexpected send-disabled suppress count <= `DELIVERY_GATE_MAX_SEND_DISABLED_COUNT`

실패 reason code 예시:

- `delivery_gate_flag_send_disabled`
- `delivery_gate_dry_run_enabled`
- `delivery_gate_retry_promotion_disabled`
- `delivery_gate_success_rate_below_threshold`
- `delivery_gate_high_e2e_p95_too_high`
- `delivery_gate_due_retry_lag_too_high`
- `delivery_gate_open_dlq_present`
- `delivery_gate_unexpected_send_disabled_rows_present`

---

## 6-4. full gate 해석 규칙

full gate 는 restricted gate 통과를 전제로 한다.

추가 hard block:

- trailing 24h success rate >= `DELIVERY_GATE_MIN_SUCCESS_RATE_24H`
- prod replay guard reject count <= `DELIVERY_GATE_MAX_REPLAY_GUARD_REJECT_COUNT`
- retry ceiling exceeded new rows == 0
- delivery DLQ oldest age < 3600s

warning only:

- duplicate/no-op spike
- operator manual review not yet supplied

즉, v0.1 code draft 에서는 **주관 항목을 hard-fail 로 과하게 자동화하지 않는다.**

---

## 6-5. 추천 flag patch 규칙

### restricted pass

```json
{
  "ENABLE_NOTIFICATION_SEND": true,
  "MAINTENANCE_ENABLE_NOTIFICATION_RETRY_PROMOTION": true,
  "NOTIFIER_TELEGRAM_DRY_RUN": false
}
```

### fail on runtime/transport issue

```json
{
  "ENABLE_NOTIFICATION_SEND": false,
  "MAINTENANCE_ENABLE_NOTIFICATION_RETRY_PROMOTION": false
}
```

### warn only

```json
{
  "ENABLE_NOTIFICATION_SEND": true,
  "MAINTENANCE_ENABLE_NOTIFICATION_RETRY_PROMOTION": true,
  "RESTRICTED_SCOPE_REVIEW_REQUIRED": true
}
```

주의:

- `RESTRICTED_SCOPE_REVIEW_REQUIRED` 는 실제 env key 라기보다 operator hint placeholder 다.
- 코드 초안에서는 output field 로만 둔다.

---

## 7. batch recovery contract

## 7-1. 공통 원칙

batch recovery tool 은 공통으로 아래를 따른다.

1. 입력 root 는 언제나 `notification_plan_id` 집합이다.
2. `notification_plans` / `notification_delivery_records` 를 읽어 latest status 를 검증한다.
3. write 는 아래 둘만 허용한다.
   - `replay_requests`
   - `event_outbox(notification.plan.created.v1)`
4. pipeline/job attempt row 는 control-plane audit 용으로 남긴다.

즉, **batch recovery 는 selection validator + bridge emitter** 다.

---

## 7-2. `replay-selected` 규칙

허용 대상:

- latest delivery status = `suppressed` and `send_disabled=true`
- latest delivery status = `failed_terminal`
- delivery-related DLQ open row 존재

금지 대상:

- latest delivery status = `failed_retryable` 이고 due retry 경로로 충분히 복구 가능한 row
- `requested` 상태의 기존 open replay_request 가 이미 있는 row

write:

- `replay_requests` batch insert
- `status = requested`
- downstream dispatch 는 기존 `q.replay` worker 가 처리

dedupe:

- replay request 는 row-level PK로 분리
- 동일 batch 재실행은 허용하되, 동일 plan에 `requested|dispatched` open row가 있으면 skip 또는 warn

---

## 7-3. `retry-selected-due` 규칙

허용 대상:

- latest delivery status = `failed_retryable`
- `notification_plans.send_after <= now()`
- latest attempt count < retry ceiling

금지 대상:

- `send_disabled=true` suppress
- `failed_terminal`
- ceiling exceeded row
- 아직 due 가 아닌 retryable row

write:

- `event_outbox(notification.plan.created.v1)` manual retry-intent insert

권장 dedupe key:

```text
notify:manual-retry-intent:{notification_plan_id}:{latest_attempt_count}:{recovery_batch_id}
```

payload 최소 필드:

```json
{
  "notification_plan_id": "...",
  "analysis_id": "...",
  "candidate_group_id": "...",
  "delivery_decision": "send_now",
  "urgency_profile": "high|normal_silent|digest|suppressed",
  "target_chat_id": 123,
  "target_thread_id": null,
  "render_profile": "single_alert_v1",
  "dedupe_subject_key": "...",
  "material_change_hash": "...",
  "send_after": null,
  "retry_reason": "manual_selected_due_retry",
  "recovery_batch_id": "...",
  "previous_attempt_count": 2
}
```

즉, **selected due retry 는 replay 가 아니라 notification.plan.created bridge 재발행** 이다.

---

## 7-4. recovery batch correlation

각 CLI invocation 마다 `recovery_batch_id` 를 생성해 아래에 공통으로 남긴다.

- `pipeline_runs.root_object_type = notification_plan_batch`
- `pipeline_runs.root_object_id = recovery_batch_id`
- `job_attempts.error_code` 또는 metadata snippet 에 batch id 포함
- replay payload / retry-intent payload 에도 batch id 포함

이렇게 해야 operator 가 “어느 batch run 이 어떤 plan 들을 다시 태웠는지” 복기할 수 있다.

---

## 8. 코드 초안

## 8-1. `src/services/maintenance/models.py` (updated)

```python
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Literal


GateMode = Literal["restricted", "full"]
GateStatus = Literal["pass", "fail", "warn"]
RecoveryMode = Literal["replay-selected", "retry-selected-due"]


@dataclass(slots=True, frozen=True)
class DeliveryGateMetric:
    metric_name: str
    observed_value: float | int | str | None
    threshold: float | int | str | None
    comparator: str
    passed: bool
    severity: str = "block"


@dataclass(slots=True, frozen=True)
class DeliveryGateReportV1:
    mode: GateMode
    gate_status: GateStatus
    blocking_reason_codes: list[str]
    warning_reason_codes: list[str]
    metrics: list[DeliveryGateMetric]
    operator_review_required: bool
    operator_review_passed: bool | None
    recommended_flag_patch: dict[str, object]


@dataclass(slots=True, frozen=True)
class DeliveryGateSnapshot:
    success_rate_1h: float | None
    success_rate_24h: float | None
    high_source_to_delivery_p95_sec: float | None
    plan_to_transport_p95_sec: float | None
    due_retry_oldest_lag_sec: float | None
    open_delivery_dlq_count: int
    oldest_delivery_dlq_age_sec: float | None
    unexpected_send_disabled_count: int
    replay_guard_reject_count_24h: int
    retry_ceiling_exceeded_count_24h: int
    duplicate_noop_ratio_1h: float | None


@dataclass(slots=True, frozen=True)
class SelectedPlanRecoveryRow:
    notification_plan_id: str
    analysis_id: str
    candidate_group_id: str
    delivery_status: str
    attempt_count: int | None
    send_after: datetime | None
    telegram_chat_id: int | None
    target_chat_id: int | None
    target_thread_id: int | None
    render_profile: str
    dedupe_subject_key: str
    material_change_hash: str
    urgency_profile: str
    delivery_decision: str
    send_disabled: bool = False
    has_open_replay_request: bool = False
    has_delivery_dlq: bool = False


@dataclass(slots=True, frozen=True)
class RecoveryBatchResult:
    recovery_batch_id: str
    recovery_mode: RecoveryMode
    selected_count: int
    accepted_count: int
    skipped_count: int
    emitted_count: int
    skipped_reason_codes: dict[str, int] = field(default_factory=dict)
```

---

## 8-2. `src/services/maintenance/config.py` (updated)

```python
from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(slots=True, frozen=True)
class MaintenanceConfig:
    app_env: str
    database_url: str
    redis_url: str

    enable_notification_retry_promotion: bool
    enable_replay_to_prod_db: bool

    delivery_gate_min_success_rate_1h: float
    delivery_gate_min_success_rate_24h: float
    delivery_gate_max_high_source_to_delivery_p95_sec: int
    delivery_gate_max_plan_to_transport_p95_sec: int
    delivery_gate_max_due_retry_lag_sec: int
    delivery_gate_max_open_dlq_count: int
    delivery_gate_max_send_disabled_count: int
    delivery_gate_max_replay_guard_reject_count: int
    delivery_gate_require_operator_review_for_full: bool

    @classmethod
    def from_env(cls) -> "MaintenanceConfig":
        def _read(name: str, default: str) -> str:
            return os.getenv(name, default).strip()

        return cls(
            app_env=_read("APP_ENV", "dev").lower(),
            database_url=_read("DATABASE_URL", ""),
            redis_url=_read("REDIS_URL", ""),
            enable_notification_retry_promotion=_read("MAINTENANCE_ENABLE_NOTIFICATION_RETRY_PROMOTION", "false").lower() == "true",
            enable_replay_to_prod_db=_read("ENABLE_REPLAY_TO_PROD_DB", "false").lower() == "true",
            delivery_gate_min_success_rate_1h=float(_read("DELIVERY_GATE_MIN_SUCCESS_RATE_1H", "0.99")),
            delivery_gate_min_success_rate_24h=float(_read("DELIVERY_GATE_MIN_SUCCESS_RATE_24H", "0.99")),
            delivery_gate_max_high_source_to_delivery_p95_sec=int(_read("DELIVERY_GATE_MAX_HIGH_SOURCE_TO_DELIVERY_P95_SEC", "120")),
            delivery_gate_max_plan_to_transport_p95_sec=int(_read("DELIVERY_GATE_MAX_PLAN_TO_TRANSPORT_P95_SEC", "120")),
            delivery_gate_max_due_retry_lag_sec=int(_read("DELIVERY_GATE_MAX_DUE_RETRY_LAG_SEC", "120")),
            delivery_gate_max_open_dlq_count=int(_read("DELIVERY_GATE_MAX_OPEN_DLQ_COUNT", "0")),
            delivery_gate_max_send_disabled_count=int(_read("DELIVERY_GATE_MAX_SEND_DISABLED_COUNT", "0")),
            delivery_gate_max_replay_guard_reject_count=int(_read("DELIVERY_GATE_MAX_REPLAY_GUARD_REJECT_COUNT", "0")),
            delivery_gate_require_operator_review_for_full=_read("DELIVERY_GATE_REQUIRE_OPERATOR_REVIEW_FOR_FULL", "true").lower() == "true",
        )
```

---

## 8-3. `src/services/maintenance/repositories.py` (updated)

```python
from __future__ import annotations

import json

import sqlalchemy as sa
from sqlalchemy.ext.asyncio import AsyncSession

from .models import DeliveryGateSnapshot, SelectedPlanRecoveryRow


class MaintenanceRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def load_delivery_gate_snapshot(self) -> DeliveryGateSnapshot:
        success_1h = await self._scalar(
            """
            SELECT CASE WHEN COUNT(*) = 0 THEN NULL
                        ELSE SUM(CASE WHEN delivery_status IN ('sent','edited') THEN 1 ELSE 0 END)::float / COUNT(*)::float
                   END
            FROM notification_delivery_records
            WHERE created_at >= now() - interval '1 hour'
            """
        )
        success_24h = await self._scalar(
            """
            SELECT CASE WHEN COUNT(*) = 0 THEN NULL
                        ELSE SUM(CASE WHEN delivery_status IN ('sent','edited') THEN 1 ELSE 0 END)::float / COUNT(*)::float
                   END
            FROM notification_delivery_records
            WHERE created_at >= now() - interval '24 hour'
            """
        )
        high_e2e_p95 = await self._scalar(
            """
            WITH high_delivered AS (
              SELECT np.notification_plan_id,
                     sm.posted_at AS source_posted_at,
                     COALESCE(dr.sent_at, dr.edited_at) AS delivered_at
              FROM notification_plans np
              JOIN candidate_group_proposals cgp
                ON cgp.candidate_group_id = np.candidate_group_id
              JOIN source_messages sm
                ON sm.source_message_id = cgp.source_message_id
              JOIN LATERAL (
                SELECT sent_at, edited_at
                FROM notification_delivery_records
                WHERE notification_plan_id = np.notification_plan_id
                  AND delivery_status IN ('sent'::notification_status_enum,'edited'::notification_status_enum)
                ORDER BY created_at DESC
                LIMIT 1
              ) dr ON TRUE
              WHERE np.urgency_profile = 'high'::urgency_profile_enum
            )
            SELECT percentile_cont(0.95)
                   WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM (delivered_at - source_posted_at)))
            FROM high_delivered
            WHERE delivered_at IS NOT NULL
            """
        )
        plan_to_transport_p95 = await self._scalar(
            """
            WITH delivered AS (
              SELECT np.notification_plan_id,
                     np.created_at AS plan_created_at,
                     COALESCE(dr.sent_at, dr.edited_at) AS delivered_at
              FROM notification_plans np
              JOIN LATERAL (
                SELECT sent_at, edited_at
                FROM notification_delivery_records
                WHERE notification_plan_id = np.notification_plan_id
                  AND delivery_status IN ('sent'::notification_status_enum,'edited'::notification_status_enum)
                ORDER BY created_at DESC
                LIMIT 1
              ) dr ON TRUE
            )
            SELECT percentile_cont(0.95)
                   WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM (delivered_at - plan_created_at)))
            FROM delivered
            WHERE delivered_at IS NOT NULL
            """
        )
        due_retry_oldest_lag_sec = await self._scalar(
            """
            SELECT EXTRACT(EPOCH FROM (now() - MIN(send_after)))
            FROM notification_plans
            WHERE status = 'failed_retryable'::notification_status_enum
              AND send_after IS NOT NULL
              AND send_after <= now()
            """
        )
        open_delivery_dlq_count = int((await self._scalar(
            """
            SELECT COUNT(*)
            FROM dead_letter_entries
            WHERE root_object_type = 'notification_plan'
            """
        )) or 0)
        oldest_delivery_dlq_age_sec = await self._scalar(
            """
            SELECT EXTRACT(EPOCH FROM (now() - MIN(last_failed_at)))
            FROM dead_letter_entries
            WHERE root_object_type = 'notification_plan'
            """
        )
        unexpected_send_disabled_count = int((await self._scalar(
            """
            SELECT COUNT(*)
            FROM notification_delivery_records
            WHERE created_at >= now() - interval '1 hour'
              AND delivery_status = 'suppressed'::notification_status_enum
              AND telegram_response_json ->> 'send_disabled' = 'true'
            """
        )) or 0)
        replay_guard_reject_count_24h = int((await self._scalar(
            """
            SELECT COUNT(*)
            FROM replay_requests
            WHERE requested_at >= now() - interval '24 hour'
              AND status = 'rejected_by_env_guard'
            """
        )) or 0)
        retry_ceiling_exceeded_count_24h = int((await self._scalar(
            """
            SELECT COUNT(*)
            FROM dead_letter_entries
            WHERE root_object_type = 'notification_plan'
              AND last_error_code = 'max_notification_retry_attempts_exceeded'
              AND last_failed_at >= now() - interval '24 hour'
            """
        )) or 0)
        duplicate_noop_ratio_1h = await self._scalar(
            """
            WITH totals AS (
              SELECT COUNT(*)::float AS total_cnt,
                     SUM(CASE WHEN reason_code IN ('notification_duplicate_noop','telegram_edit_not_modified_noop') THEN 1 ELSE 0 END)::float AS noop_cnt
              FROM state_transitions
              WHERE object_type = 'notification_plan'
                AND created_at >= now() - interval '1 hour'
            )
            SELECT CASE WHEN total_cnt = 0 THEN NULL ELSE noop_cnt / total_cnt END
            FROM totals
            """
        )
        return DeliveryGateSnapshot(
            success_rate_1h=success_1h,
            success_rate_24h=success_24h,
            high_source_to_delivery_p95_sec=high_e2e_p95,
            plan_to_transport_p95_sec=plan_to_transport_p95,
            due_retry_oldest_lag_sec=due_retry_oldest_lag_sec,
            open_delivery_dlq_count=open_delivery_dlq_count,
            oldest_delivery_dlq_age_sec=oldest_delivery_dlq_age_sec,
            unexpected_send_disabled_count=unexpected_send_disabled_count,
            replay_guard_reject_count_24h=replay_guard_reject_count_24h,
            retry_ceiling_exceeded_count_24h=retry_ceiling_exceeded_count_24h,
            duplicate_noop_ratio_1h=duplicate_noop_ratio_1h,
        )

    async def load_selected_recovery_rows(self, notification_plan_ids: list[str]) -> list[SelectedPlanRecoveryRow]:
        if not notification_plan_ids:
            return []
        result = await self._session.execute(
            sa.text(
                """
                SELECT np.notification_plan_id,
                       np.analysis_id,
                       np.candidate_group_id,
                       np.send_after,
                       np.target_chat_id,
                       np.target_thread_id,
                       np.render_profile,
                       np.dedupe_subject_key,
                       np.material_change_hash,
                       np.urgency_profile,
                       np.delivery_decision,
                       dr.telegram_chat_id,
                       dr.delivery_status,
                       dr.attempt_count,
                       COALESCE((dr.telegram_response_json ->> 'send_disabled') = 'true', false) AS send_disabled,
                       EXISTS (
                         SELECT 1 FROM replay_requests rr
                         WHERE rr.root_object_type = 'notification_plan'
                           AND rr.root_object_id = np.notification_plan_id
                           AND rr.status IN ('requested', 'dispatched')
                       ) AS has_open_replay_request,
                       EXISTS (
                         SELECT 1 FROM dead_letter_entries dle
                         WHERE dle.root_object_type = 'notification_plan'
                           AND dle.root_object_id = np.notification_plan_id
                       ) AS has_delivery_dlq
                FROM notification_plans np
                JOIN LATERAL (
                  SELECT delivery_status, attempt_count, telegram_chat_id, telegram_response_json, created_at
                  FROM notification_delivery_records
                  WHERE notification_plan_id = np.notification_plan_id
                  ORDER BY created_at DESC
                  LIMIT 1
                ) dr ON TRUE
                WHERE np.notification_plan_id = ANY(CAST(:plan_ids AS uuid[]))
                ORDER BY np.notification_plan_id
                """
            ),
            {"plan_ids": notification_plan_ids},
        )
        return [
            SelectedPlanRecoveryRow(
                notification_plan_id=str(row["notification_plan_id"]),
                analysis_id=str(row["analysis_id"]),
                candidate_group_id=str(row["candidate_group_id"]),
                delivery_status=str(row["delivery_status"]),
                attempt_count=int(row["attempt_count"]) if row["attempt_count"] is not None else None,
                send_after=row["send_after"],
                telegram_chat_id=int(row["telegram_chat_id"]) if row["telegram_chat_id"] is not None else None,
                target_chat_id=int(row["target_chat_id"]),
                target_thread_id=int(row["target_thread_id"]) if row["target_thread_id"] is not None else None,
                render_profile=str(row["render_profile"]),
                dedupe_subject_key=str(row["dedupe_subject_key"]),
                material_change_hash=str(row["material_change_hash"]),
                urgency_profile=str(row["urgency_profile"]),
                delivery_decision=str(row["delivery_decision"]),
                send_disabled=bool(row["send_disabled"]),
                has_open_replay_request=bool(row["has_open_replay_request"]),
                has_delivery_dlq=bool(row["has_delivery_dlq"]),
            )
            for row in result.mappings().all()
        ]

    async def insert_replay_requests_for_selected_plans(self, *, plan_ids: list[str], requested_by: str) -> int:
        if not plan_ids:
            return 0
        result = await self._session.execute(
            sa.text(
                """
                INSERT INTO replay_requests (
                  replay_request_id,
                  replay_type,
                  root_object_type,
                  root_object_id,
                  requested_by,
                  requested_at,
                  status
                )
                SELECT gen_random_uuid(),
                       'delivery'::replay_type_enum,
                       'notification_plan',
                       CAST(src.notification_plan_id AS uuid),
                       :requested_by,
                       now(),
                       'requested'
                FROM (
                  SELECT unnest(CAST(:plan_ids AS uuid[])) AS notification_plan_id
                ) src
                RETURNING replay_request_id
                """
            ),
            {"plan_ids": plan_ids, "requested_by": requested_by},
        )
        return len(result.fetchall())

    async def insert_manual_retry_intent_outbox(self, *, row: SelectedPlanRecoveryRow, recovery_batch_id: str) -> None:
        payload = {
            "notification_plan_id": row.notification_plan_id,
            "analysis_id": row.analysis_id,
            "candidate_group_id": row.candidate_group_id,
            "delivery_decision": row.delivery_decision,
            "urgency_profile": row.urgency_profile,
            "target_chat_id": row.target_chat_id,
            "target_thread_id": row.target_thread_id,
            "render_profile": row.render_profile,
            "dedupe_subject_key": row.dedupe_subject_key,
            "material_change_hash": row.material_change_hash,
            "send_after": None,
            "retry_reason": "manual_selected_due_retry",
            "recovery_batch_id": recovery_batch_id,
            "previous_attempt_count": row.attempt_count or 0,
        }
        await self._session.execute(
            sa.text(
                """
                INSERT INTO event_outbox (
                  event_id,
                  event_type,
                  aggregate_type,
                  aggregate_id,
                  dedupe_key,
                  payload_json,
                  status,
                  created_at
                ) VALUES (
                  gen_random_uuid(),
                  'notification.plan.created.v1',
                  'notification_plan',
                  CAST(:notification_plan_id AS uuid),
                  :dedupe_key,
                  CAST(:payload_json AS jsonb),
                  'pending'::outbox_status_enum,
                  now()
                )
                ON CONFLICT (dedupe_key) DO NOTHING
                """
            ),
            {
                "notification_plan_id": row.notification_plan_id,
                "dedupe_key": f"notify:manual-retry-intent:{row.notification_plan_id}:{row.attempt_count or 0}:{recovery_batch_id}",
                "payload_json": json.dumps(payload, ensure_ascii=False),
            },
        )
```

구현 메모:

- 실제 코드에서는 repository helper `_jsonb_dumps()` 또는 동등한 serializer 로 바인딩하는 편이 맞다.
- 문서에서는 핵심 SQL shape 와 dedupe contract 를 고정하는 것이 목적이다.

---

## 8-4. `src/services/maintenance/delivery_gate_runner.py` (new)

```python
from __future__ import annotations

from .models import DeliveryGateMetric, DeliveryGateReportV1, DeliveryGateSnapshot, GateMode


class DeliveryGateRunner:
    def __init__(self, config, *, repository) -> None:
        self._config = config
        self._repository = repository

    async def run(
        self,
        *,
        mode: GateMode,
        operator_review_passed: bool | None = None,
    ) -> DeliveryGateReportV1:
        snap: DeliveryGateSnapshot = await self._repository.load_delivery_gate_snapshot()
        metrics: list[DeliveryGateMetric] = []
        blocking_reason_codes: list[str] = []
        warning_reason_codes: list[str] = []

        def add_metric(name, observed, threshold, comparator, passed, severity="block", reason_code=None):
            metrics.append(
                DeliveryGateMetric(
                    metric_name=name,
                    observed_value=observed,
                    threshold=threshold,
                    comparator=comparator,
                    passed=passed,
                    severity=severity,
                )
            )
            if not passed and reason_code:
                if severity == "block":
                    blocking_reason_codes.append(reason_code)
                else:
                    warning_reason_codes.append(reason_code)

        add_metric(
            "success_rate_1h",
            snap.success_rate_1h,
            self._config.delivery_gate_min_success_rate_1h,
            ">=",
            (snap.success_rate_1h or 0.0) >= self._config.delivery_gate_min_success_rate_1h,
            reason_code="delivery_gate_success_rate_below_threshold",
        )
        add_metric(
            "high_source_to_delivery_p95_sec",
            snap.high_source_to_delivery_p95_sec,
            self._config.delivery_gate_max_high_source_to_delivery_p95_sec,
            "<=",
            (snap.high_source_to_delivery_p95_sec or 0.0) <= self._config.delivery_gate_max_high_source_to_delivery_p95_sec,
            reason_code="delivery_gate_high_e2e_p95_too_high",
        )
        add_metric(
            "due_retry_oldest_lag_sec",
            snap.due_retry_oldest_lag_sec,
            self._config.delivery_gate_max_due_retry_lag_sec,
            "<=",
            (snap.due_retry_oldest_lag_sec or 0.0) <= self._config.delivery_gate_max_due_retry_lag_sec,
            reason_code="delivery_gate_due_retry_lag_too_high",
        )
        add_metric(
            "open_delivery_dlq_count",
            snap.open_delivery_dlq_count,
            self._config.delivery_gate_max_open_dlq_count,
            "<=",
            snap.open_delivery_dlq_count <= self._config.delivery_gate_max_open_dlq_count,
            reason_code="delivery_gate_open_dlq_present",
        )
        add_metric(
            "unexpected_send_disabled_count",
            snap.unexpected_send_disabled_count,
            self._config.delivery_gate_max_send_disabled_count,
            "<=",
            snap.unexpected_send_disabled_count <= self._config.delivery_gate_max_send_disabled_count,
            reason_code="delivery_gate_unexpected_send_disabled_rows_present",
        )

        if mode == "full":
            add_metric(
                "success_rate_24h",
                snap.success_rate_24h,
                self._config.delivery_gate_min_success_rate_24h,
                ">=",
                (snap.success_rate_24h or 0.0) >= self._config.delivery_gate_min_success_rate_24h,
                reason_code="delivery_gate_24h_success_rate_below_threshold",
            )
            add_metric(
                "replay_guard_reject_count_24h",
                snap.replay_guard_reject_count_24h,
                self._config.delivery_gate_max_replay_guard_reject_count,
                "<=",
                snap.replay_guard_reject_count_24h <= self._config.delivery_gate_max_replay_guard_reject_count,
                reason_code="delivery_gate_prod_replay_guard_rejects_present",
            )
            add_metric(
                "retry_ceiling_exceeded_count_24h",
                snap.retry_ceiling_exceeded_count_24h,
                0,
                "<=",
                snap.retry_ceiling_exceeded_count_24h <= 0,
                reason_code="delivery_gate_retry_ceiling_exceeded_rows_present",
            )
            add_metric(
                "duplicate_noop_ratio_1h",
                snap.duplicate_noop_ratio_1h,
                None,
                "warn-only",
                True,
                severity="warn",
            )
            if self._config.delivery_gate_require_operator_review_for_full and operator_review_passed is not True:
                warning_reason_codes.append("delivery_gate_operator_review_required")

        gate_status = "pass"
        if blocking_reason_codes:
            gate_status = "fail"
        elif warning_reason_codes:
            gate_status = "warn"

        recommended_flag_patch = {
            "ENABLE_NOTIFICATION_SEND": gate_status != "fail",
            "MAINTENANCE_ENABLE_NOTIFICATION_RETRY_PROMOTION": gate_status != "fail",
            "NOTIFIER_TELEGRAM_DRY_RUN": False,
        }
        if gate_status == "fail":
            recommended_flag_patch = {
                "ENABLE_NOTIFICATION_SEND": False,
                "MAINTENANCE_ENABLE_NOTIFICATION_RETRY_PROMOTION": False,
            }

        return DeliveryGateReportV1(
            mode=mode,
            gate_status=gate_status,
            blocking_reason_codes=blocking_reason_codes,
            warning_reason_codes=warning_reason_codes,
            metrics=metrics,
            operator_review_required=(mode == "full" and self._config.delivery_gate_require_operator_review_for_full),
            operator_review_passed=operator_review_passed,
            recommended_flag_patch=recommended_flag_patch,
        )
```

---

## 8-5. `src/services/maintenance/batch_recovery_tool.py` (new)

```python
from __future__ import annotations

import uuid

from .models import RecoveryBatchResult, RecoveryMode, SelectedPlanRecoveryRow


class DeliveryBatchRecoveryTool:
    def __init__(self, *, repository) -> None:
        self._repository = repository

    async def replay_selected(self, *, plan_ids: list[str], requested_by: str) -> RecoveryBatchResult:
        rows = await self._repository.load_selected_recovery_rows(plan_ids)
        accepted: list[str] = []
        skipped: dict[str, int] = {}
        for row in rows:
            reason = self._validate_replay_row(row)
            if reason is None:
                accepted.append(row.notification_plan_id)
            else:
                skipped[reason] = skipped.get(reason, 0) + 1

        emitted = await self._repository.insert_replay_requests_for_selected_plans(
            plan_ids=accepted,
            requested_by=requested_by,
        )
        return RecoveryBatchResult(
            recovery_batch_id=str(uuid.uuid4()),
            recovery_mode="replay-selected",
            selected_count=len(plan_ids),
            accepted_count=len(accepted),
            skipped_count=len(plan_ids) - len(accepted),
            emitted_count=emitted,
            skipped_reason_codes=skipped,
        )

    async def retry_selected_due(self, *, plan_ids: list[str], requested_by: str) -> RecoveryBatchResult:
        rows = await self._repository.load_selected_recovery_rows(plan_ids)
        recovery_batch_id = str(uuid.uuid4())
        accepted_rows: list[SelectedPlanRecoveryRow] = []
        skipped: dict[str, int] = {}
        for row in rows:
            reason = self._validate_due_retry_row(row)
            if reason is None:
                accepted_rows.append(row)
            else:
                skipped[reason] = skipped.get(reason, 0) + 1

        emitted = 0
        for row in accepted_rows:
            await self._repository.insert_manual_retry_intent_outbox(
                row=row,
                recovery_batch_id=recovery_batch_id,
            )
            emitted += 1

        return RecoveryBatchResult(
            recovery_batch_id=recovery_batch_id,
            recovery_mode="retry-selected-due",
            selected_count=len(plan_ids),
            accepted_count=len(accepted_rows),
            skipped_count=len(plan_ids) - len(accepted_rows),
            emitted_count=emitted,
            skipped_reason_codes=skipped,
        )

    def _validate_replay_row(self, row: SelectedPlanRecoveryRow) -> str | None:
        if row.has_open_replay_request:
            return "open_replay_request_exists"
        if row.send_disabled and row.delivery_status == "suppressed":
            return None
        if row.delivery_status == "failed_terminal":
            return None
        if row.has_delivery_dlq:
            return None
        return "replay_not_allowed_for_status"

    def _validate_due_retry_row(self, row: SelectedPlanRecoveryRow) -> str | None:
        from datetime import datetime, timezone

        if row.delivery_status != "failed_retryable":
            return "status_is_not_failed_retryable"
        if row.send_disabled:
            return "send_disabled_rows_require_replay"
        if row.send_after is None:
            return "send_after_missing"
        if row.send_after > datetime.now(timezone.utc):
            return "send_after_not_due_yet"
        if row.has_open_replay_request:
            return "open_replay_request_exists"
        return None
```

---

## 8-6. `src/services/maintenance/main.py` (updated)

```python
from __future__ import annotations

import argparse
import asyncio
import json
from dataclasses import asdict

from .batch_recovery_tool import DeliveryBatchRecoveryTool
from .config import MaintenanceConfig
from .delivery_gate_runner import DeliveryGateRunner
from .repositories import MaintenanceRepository


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="maintenance")
    sub = parser.add_subparsers(dest="command", required=True)

    worker = sub.add_parser("worker")
    worker.set_defaults(command="worker")

    gate = sub.add_parser("delivery-gate")
    gate.add_argument("--mode", choices=["restricted", "full"], required=True)
    gate.add_argument("--format", choices=["json", "text"], default="json")
    gate.add_argument("--operator-review-passed", choices=["true", "false"], default=None)

    recovery = sub.add_parser("batch-recovery")
    recovery_sub = recovery.add_subparsers(dest="recovery_mode", required=True)

    replay = recovery_sub.add_parser("replay-selected")
    replay.add_argument("--plan-id", action="append", required=True)
    replay.add_argument("--requested-by", required=True)

    retry = recovery_sub.add_parser("retry-selected-due")
    retry.add_argument("--plan-id", action="append", required=True)
    retry.add_argument("--requested-by", required=True)

    return parser


async def run_delivery_gate(args, repo) -> int:
    cfg = MaintenanceConfig.from_env()
    runner = DeliveryGateRunner(cfg, repository=repo)
    op_review = None
    if args.operator_review_passed == "true":
        op_review = True
    elif args.operator_review_passed == "false":
        op_review = False
    report = await runner.run(mode=args.mode, operator_review_passed=op_review)
    print(json.dumps(asdict(report), ensure_ascii=False, default=str, indent=2))
    return 0 if report.gate_status == "pass" else 2


async def run_batch_recovery(args, repo) -> int:
    tool = DeliveryBatchRecoveryTool(repository=repo)
    if args.recovery_mode == "replay-selected":
        result = await tool.replay_selected(plan_ids=args.plan_id, requested_by=args.requested_by)
    else:
        result = await tool.retry_selected_due(plan_ids=args.plan_id, requested_by=args.requested_by)
    print(json.dumps(asdict(result), ensure_ascii=False, default=str, indent=2))
    return 0
```

구현 메모:

- 실제 `worker` subcommand 는 기존 maintenance runtime 을 호출하면 된다.
- stage 43의 핵심은 **gate / batch recovery subcommand 추가** 다.

---

## 9. 테스트 초안 포인트

### `tests/unit/services/maintenance/test_delivery_gate_runner.py`
검증:
- restricted gate fail 조건들이 reason code 로 안정적으로 나오는지
- full gate 에서 operator review 미제공이 warning-only 인지

### `tests/unit/services/maintenance/test_batch_recovery_validation.py`
검증:
- `replay-selected` 와 `retry-selected-due` 가 서로 다른 status를 정확히 reject 하는지

### `tests/unit/services/maintenance/test_batch_recovery_replay_insert.py`
검증:
- selected plan ids 로 replay_requests batch insert 가 만들어지는지
- open replay request 존재 시 skip 되는지

### `tests/unit/services/maintenance/test_batch_recovery_retry_intent.py`
검증:
- due retryable selected rows 에 대해 manual retry-intent dedupe key 가 안정적인지

### `tests/integration/delivery/test_restricted_gate_runner_fixture.py`
검증:
- fixture scorecard 결과가 restricted gate pass/fail 기대값과 일치하는지

### `tests/integration/delivery/test_full_gate_runner_warn_only_fields.py`
검증:
- duplicate/no-op ratio, operator review required 가 warn field 로만 남는지

### `tests/integration/delivery/test_batch_replay_request_cli_flow.py`
검증:
- `batch-recovery replay-selected` 가 `replay_requests` row 를 생성하는지

### `tests/integration/delivery/test_selected_due_retry_cli_flow.py`
검증:
- `batch-recovery retry-selected-due` 가 `event_outbox(notification.plan.created.v1)` 를 생성하는지

---

## 10. 이번 단계가 구조를 지키는 이유

1. gate runner 는 worker 가 아니라 **one-shot control-plane tool** 이다.  
   즉, runtime hot path 를 침범하지 않는다.

2. batch recovery 는 `notification_plans` reset 이 아니라 **replay request / retry-intent event** 만 다시 만든다.  
   즉, notifier ownership 을 넘지 않는다.

3. gate 판정은 stage 42 scorecard 를 재사용한다.  
   즉, query contract 와 code draft 가 어긋나지 않는다.

4. full rollout 의 operator judgement 항목을 무리하게 hard-fail 자동화하지 않는다.  
   즉, stage 10의 governance 철학을 유지한다.

5. selected plan recovery 는 전략별 subcommand 로 나뉜다.  
   즉, send-disabled suppress 와 due retryable 을 같은 복구 경로로 섞지 않는다.

---

## 11. 다음 단계

이번 단계가 닫히면 다음 안전한 구현 순서는 아래가 맞다.

1. `44_delivery_gate_and_recovery_acceptance_hardening_v0_1.md`
   - gate runner fixture / scorecard acceptance hardening
   - batch recovery idempotency / duplicate replay-request safety hardening
   - maintenance CLI entrypoint / operator runbook / compose invocation acceptance 정리
   - restricted/full rollout handoff checklist 보강

2. 그 이후 문서가 누적되면
   - `10_delivery_hardening_stage39_plus_v0_1.md`
   같은 새 번들로 stage 39+ 문서를 묶는 것이 자연스럽다.

즉, 다음 단계도 여전히 **delivery line control-plane acceptance hardening** 이다.

---

## 최종 한 줄 결론

이번 단계의 최소-change 정답은 **stage 42에서 고정한 delivery scorecard / DLQ / batch recovery 규칙을 `maintenance` 패키지의 one-shot gate runner와 selected-plan batch recovery CLI code draft로 내리되, notifier ownership과 runtime hot path를 건드리지 않고 replay-request / retry-intent bridge만 재사용하도록 고정하는 것** 이다.



## Source file: `44_delivery_gate_and_recovery_acceptance_hardening_v0_1.md`

# 44단계: `delivery` gate and recovery acceptance hardening v0.1

## 0. 문서 목적

이 문서는 `43_delivery_gate_runner_and_batch_recovery_code_draft_v0_1.md`를 뒤엎는 문서가 아니다.  
목적은 **이미 잠긴 delivery line control-plane code draft를 유지한 채, gate runner / batch recovery CLI / compose invocation / operator handoff의 acceptance gap만 좁게 닫는 것**이다.

이번 단계에서 닫는 것은 정확히 아래 여섯 가지다.

1. gate runner fixture / scorecard acceptance를 **deterministic report contract**로 고정
2. `replay-selected`와 `retry-selected-due`의 **idempotency / duplicate safety**를 acceptance 규칙으로 고정
3. `maintenance` CLI entrypoint가 **read-only gate mode** 와 **write batch-recovery mode** 를 안전하게 분리하도록 고정
4. compose / runbook / operator invocation이 **hot-path worker와 섞이지 않도록** 고정
5. restricted / full rollout handoff checklist를 **hard block / warn / operator review required** 층으로 정리
6. 현재 markdown 소스 세트가 **실제 구현 코딩 시작 기준으로 충분한 상태**인지 명시적으로 고정

핵심 전제는 그대로 유지한다.

- `notifier-telegram`은 여전히 **presentation / delivery boundary** 다.
- `maintenance`는 여전히 **retry / replay orchestration boundary** 다.
- `notification_plans` / `notification_renders` / `notification_delivery_records` / `state_transitions` 직접 변경은 notifier ownership 이다.
- stage 43의 gate runner / batch recovery tool 은 여전히 **one-shot control-plane asset** 이지, 신규 runtime worker 가 아니다.
- delivery recovery 때문에 upstream `analysis` / `judge` / `bundle` / `candidate` / `artifact` 를 다시 계산하지 않는다.

---

## 1. 왜 지금 이 단계가 정확한 다음 단계인가

stage 43은 다음 단계로 아래를 명시했다.

- `44_delivery_gate_and_recovery_acceptance_hardening_v0_1.md`
  - gate runner fixture / scorecard acceptance hardening
  - batch recovery idempotency / duplicate replay-request safety hardening
  - maintenance CLI entrypoint / operator runbook / compose invocation acceptance 정리
  - restricted/full rollout handoff checklist 보강

즉, 지금 새 서비스를 설계하거나 notifier / maintenance hot path 를 다시 여는 것은 순서상 후퇴고,  
지금 닫아야 하는 것은 **stage 43 code draft를 실제 운영 acceptance 기준으로 고정하는 단계** 다.

---

## 2. 이번 단계에서 드러나는 충돌과 최소-change 해석

## 2-1. 충돌 A — stage 43의 manual retry-intent dedupe가 CLI 재실행에 대해 너무 약하다

43단계 초안은 manual retry-intent dedupe key 를 아래처럼 잡았다.

```text
notify:manual-retry-intent:{notification_plan_id}:{latest_attempt_count}:{recovery_batch_id}
```

이 방식의 문제는 명확하다.

- 같은 `notification_plan_id`
- 같은 `latest_attempt_count`
- 같은 `send_after`
- 즉, **동일한 recovery 대상 상태**인데도
- operator가 CLI를 한 번 더 실행하면 `recovery_batch_id`가 바뀌어 새 outbox row가 허용된다.

즉, control-plane 재실행이 **의미상 같은 retry-intent를 중복 발행**할 수 있다.

### 최소-change 해석 A

manual retry-intent dedupe 는 `recovery_batch_id`가 아니라 **대상 row의 현재 durable 상태**를 기준으로 잡는다.

권장 키:

```text
notify:manual-retry-intent:{notification_plan_id}:{latest_attempt_count}:{send_after_epoch}
```

의미:
- notifier가 새 delivery attempt 를 append하기 전에는 같은 key가 유지된다.
- operator가 같은 due row에 대해 CLI를 다시 돌려도 outbox dedupe 로 흡수된다.
- 새 attempt 또는 새 `send_after`가 생기면 다음 recovery intent는 다시 허용된다.

즉, **감사용 `recovery_batch_id`와 bridge dedupe key를 분리**하는 것이 최소-change 정답이다.

---

## 2-2. 충돌 B — `replay-selected` 는 validation은 있지만 insert path가 blind insert에 가깝다

43단계 초안은 selection 단계에서 `has_open_replay_request`를 읽고 skip 하도록 설계했지만, repository insert 는 사실상 아래 문제를 가진다.

- selection 직후 operator가 같은 CLI를 다시 돌릴 수 있다.
- 같은 plan에 이미 `requested|dispatched` replay row가 생겼더라도 blind insert가 가능하다.
- 결국 동일 `notification_plan_id`에 **open replay request가 중복 생성**될 수 있다.

### 최소-change 해석 B

`replay-selected` write path 는 repository 레벨에서도 **insert-if-absent** 로 고정한다.

원칙:
1. 입력 plan 집합을 다시 한 번 `requested|dispatched` open replay 기준으로 필터한다.
2. `INSERT ... SELECT ... WHERE NOT EXISTS (...)` 형태로 insert 한다.
3. CLI 결과에는 아래를 분리해 남긴다.
   - `accepted_count`
   - `inserted_count`
   - `skipped_open_replay_count`

즉, **selection validation + write-side duplicate safety** 를 둘 다 갖춰야 한다.

---

## 2-3. 충돌 C — gate runner output이 deterministic하지 않으면 fixture acceptance가 약하다

gate runner는 scorecard를 계산하지만, 아래가 흔들리면 fixture test가 약해진다.

- metric 순서
- blocking / warning reason code 순서
- `pass / warn / fail` 판정 우선순위
- `operator_review_required` 와 `operator_review_passed` 해석
- `recommended_flag_patch` 출력 shape

### 최소-change 해석 C

gate runner acceptance 는 아래처럼 고정한다.

1. metric 출력 순서는 **문서 순서 그대로 고정** 한다.
2. `blocking_reason_codes`, `warning_reason_codes` 는 **stable append order + final dedupe** 로 고정한다.
3. 판정 우선순위는 아래로 고정한다.
   - blocking reason 존재 → `fail`
   - blocking 없음 + warning 또는 operator review 미완료 → `warn`
   - 둘 다 없음 → `pass`
4. full mode에서 `DELIVERY_GATE_REQUIRE_OPERATOR_REVIEW_FOR_FULL=true` 이고
   `operator_review_passed is None` 이면 **warn** 이다.
   - hard fail 이 아니다.
   - rollout 자동 진입 금지 신호로만 쓴다.

즉, **gate report는 사람이 읽는 보고서이면서 동시에 fixture로 고정 가능한 deterministic 산출물**이어야 한다.

---

## 2-4. 충돌 D — CLI write path가 너무 가벼우면 accidental production invocation 위험이 있다

43단계는 subcommand 분리는 했지만, write 성격의 batch recovery가 아래처럼 너무 쉽게 실행될 수 있다.

```text
python -m src.services.maintenance.main batch-recovery ...
```

이 구조는 아래 위험이 있다.

- prod shell에서 오타 실행
- operator가 gate 조회와 recovery 실행을 혼동
- replay/manual acceptance 환경과 prod 환경의 의미를 헷갈림

### 최소-change 해석 D

write subcommand 에는 명시 확인 플래그를 강제한다.

권장 규칙:

```text
--confirm write
```

원칙:
- `delivery-gate` 는 read-only 이므로 confirm 불필요
- `batch-recovery replay-selected` / `retry-selected-due` 는 confirm 없으면 실행 금지
- `main.py` 는 confirm 누락 시 exit code 2 와 usage message 를 반환한다.

즉, **read-only control-plane 과 write control-plane 을 CLI UX 수준에서 분리**한다.

---

## 2-5. 충돌 E — compose invocation 이 service startup 과 섞이면 boundary가 흐려진다

stage 43의 control-plane 자산은 one-shot job 인데, compose 문서가 이를 long-running worker 와 같은 방식으로 다루면 문제가 생긴다.

- startup service 와 one-shot ops 명령이 섞임
- operator가 `up`와 `run`의 차이를 오해함
- recovery/gate 작업이 배포 시 자동 실행되는 것으로 오해될 수 있음

### 최소-change 해석 E

compose acceptance 는 아래처럼 고정한다.

- `maintenance` service 기본 동작은 계속 worker runtime 이다.
- gate / batch recovery 는 **`docker compose run --rm maintenance ...`** 로만 수행한다.
- compose 파일에 write subcommand 기본 command 를 넣지 않는다.
- operator runbook 은 `up` 경로와 `run --rm` 경로를 명시적으로 분리한다.

즉, **one-shot control-plane job 은 compose의 운영 서비스가 아니라 operator 실행 명령**이다.

---

## 3. 이번 단계에서 고정할 범위와 제외 범위

### 포함

- gate report deterministic contract
- fixture matrix / acceptance rules
- batch recovery write-side duplicate safety
- manual retry-intent stable dedupe
- CLI confirm / exit-code contract
- compose invocation acceptance
- restricted/full rollout handoff checklist
- implementation start readiness note

### 제외

- notifier / maintenance hot-path 재설계
- feature flag auto apply
- `notification_plans` 직접 mutate
- 새 durable table 추가
- user-facing dashboard 구현
- stage 39+ bundle 생성

즉, 이번 문서는 **delivery line control-plane acceptance hardening** 만 닫는다.

---

## 4. 대상 파일 트리

```text
src/services/maintenance/
  config.py                 # unchanged or tiny clarification
  models.py                 # updated
  repositories.py           # updated
  delivery_gate_runner.py   # updated
  batch_recovery_tool.py    # updated
  main.py                   # updated

ops/
  delivery/
    runbooks/
      delivery_gate_handoff.md             # new
      maintenance_cli_invocation.md        # new

tests/
  unit/
    services/
      maintenance/
        test_delivery_gate_runner.py                  # updated
        test_batch_recovery_validation.py             # updated
        test_batch_recovery_duplicate_safety.py       # new
        test_manual_retry_intent_stable_dedupe.py     # new
  integration/
    delivery/
      test_restricted_gate_runner_fixture.py          # updated
      test_full_gate_runner_warn_only_fields.py       # updated
      test_batch_replay_request_cli_flow.py           # updated
      test_selected_due_retry_cli_flow.py             # updated
      test_cli_confirm_required_for_batch_recovery.py # new
```

주의:
- stage 41의 worker 들은 그대로 유지한다.
- stage 43의 control-plane code draft 를 실제 실행 가능한 acceptance 기준으로 보강하는 단계다.
- `ops/delivery/sql/*` 는 여전히 stage 42의 source-of-truth query contract 이고, stage 44는 그 결과 해석과 실행 안전성만 닫는다.

---

## 5. acceptance hardening 규칙

## 5-1. `DeliveryGateReportV1` deterministic contract

### metric order

아래 순서로 고정한다.

#### restricted
1. `success_rate_1h`
2. `high_source_to_delivery_p95_sec`
3. `due_retry_oldest_lag_sec`
4. `open_delivery_dlq_count`
5. `unexpected_send_disabled_count`

#### full 추가
6. `success_rate_24h`
7. `replay_guard_reject_count_24h`
8. `retry_ceiling_exceeded_count_24h`
9. `duplicate_noop_ratio_1h` (warn-only)

### gate status algorithm

```text
if blocking_reason_codes not empty -> fail
elif warning_reason_codes not empty -> warn
elif operator_review_required and operator_review_passed is not True -> warn
else -> pass
```

### reason code ordering

- append order = metric evaluation order
- final output 직전에 stable dedupe
- alphabetical sort는 하지 않는다
- 문서 순서를 깨지 않는 것이 fixture 안정성에 유리하다

### recommended flag patch

반드시 아래 세 키만 포함한다.

- `ENABLE_NOTIFICATION_SEND`
- `MAINTENANCE_ENABLE_NOTIFICATION_RETRY_PROMOTION`
- `NOTIFIER_TELEGRAM_DRY_RUN`

`RESTRICTED_SCOPE_REVIEW_REQUIRED` 같은 값은 **report note** 로만 남기고, actual flag patch 에 넣지 않는다.

---

## 5-2. fixture matrix

최소 fixture set 을 아래로 고정한다.

1. `restricted_pass_minimal`
   - DLQ 0
   - success rate 통과
   - send-disabled 0
   - gate status = `pass`

2. `restricted_fail_open_dlq`
   - open delivery DLQ > 0
   - gate status = `fail`
   - reason = `delivery_gate_open_dlq_present`

3. `restricted_fail_unexpected_send_disabled`
   - unexpected send-disabled rows > 0
   - gate status = `fail`
   - reason = `delivery_gate_unexpected_send_disabled_rows_present`

4. `full_warn_operator_review_missing`
   - hard metrics 통과
   - `operator_review_required=true`
   - `operator_review_passed=None`
   - gate status = `warn`

5. `full_fail_replay_guard_rejects`
   - prod replay guard reject count > threshold
   - gate status = `fail`

즉, stage 44부터 gate runner는 **문서 텍스트가 아니라 fixture로 검증되는 control-plane report** 다.

---

## 5-3. `replay-selected` duplicate safety

### validation rule

아래면 skip:

- same plan에 `replay_requests.status in {requested, dispatched}` open row 존재

### write rule

repository insert 도 아래처럼 제한한다.

```sql
INSERT ...
SELECT ...
WHERE NOT EXISTS (
  SELECT 1
  FROM replay_requests rr
  WHERE rr.replay_type = 'delivery'::replay_type_enum
    AND rr.root_object_type = 'notification_plan'
    AND rr.root_object_id = CAST(src.notification_plan_id AS uuid)
    AND rr.status IN ('requested', 'dispatched')
)
```

### result accounting

`RecoveryBatchResult`는 아래를 분리해 가진다.

- `accepted_count`
- `inserted_count`
- `skipped_count`
- `skipped_reason_codes`

즉, **accepted 되었지만 concurrent duplicate-safe insert 에서 실제 insert 되지 않은 경우도 설명 가능** 해야 한다.

---

## 5-4. `retry-selected-due` stable dedupe

### validation rule

아래만 허용한다.

- latest delivery status = `failed_retryable`
- `send_after <= now()`
- `send_disabled != true`
- retry ceiling 미도달

### dedupe key

권장 최종 키:

```text
notify:manual-retry-intent:{notification_plan_id}:{latest_attempt_count}:{send_after_epoch}
```

설명:
- 동일 due row에 대한 CLI 재실행은 same key 로 흡수된다.
- notifier가 새 attempt 를 append하면 `latest_attempt_count` 가 바뀐다.
- notifier가 새 backoff 를 걸면 `send_after_epoch` 가 바뀐다.
- 따라서 **상태가 바뀌기 전까지는 같은 recovery intent가 재발행되지 않는다.**

### `recovery_batch_id` 역할

`recovery_batch_id` 는 아래에만 쓴다.

- `pipeline_runs.root_object_id`
- job audit / operator log
- CLI 결과 출력

즉, **audit correlation** 이지 dedupe source 가 아니다.

---

## 5-5. CLI entrypoint acceptance

## read-only

```text
python -m src.services.maintenance.main delivery-gate --mode restricted --format json
python -m src.services.maintenance.main delivery-gate --mode full --format json --operator-review-passed true
```

## write

```text
python -m src.services.maintenance.main batch-recovery replay-selected   --plan-id <uuid> --requested-by ops --confirm write

python -m src.services.maintenance.main batch-recovery retry-selected-due   --plan-id <uuid> --requested-by ops --confirm write
```

### exit code contract

- `0` = pass / successful write with no runtime error
- `2` = blocking fail or invalid confirmation / invalid selection
- `3` = warn
- `1` = unexpected runtime error

원칙:
- gate runner 는 warn/fail 을 구분해서 shell 단계에서 해석 가능해야 한다.
- write subcommand 는 confirm 누락 시 절대 write 하지 않는다.

---

## 5-6. compose invocation acceptance

control-plane 명령은 아래처럼만 수행한다.

```bash
docker compose run --rm maintenance   python -m src.services.maintenance.main delivery-gate --mode restricted --format json
```

```bash
docker compose run --rm maintenance   python -m src.services.maintenance.main batch-recovery replay-selected   --plan-id <uuid> --requested-by ops --confirm write
```

금지:
- `docker compose up` 시 batch-recovery 자동 실행
- compose service default command 를 write subcommand 로 바꾸는 것
- gate runner 결과를 곧바로 flag auto apply 로 연결하는 것

즉, **control-plane acceptance 는 compose service definition이 아니라 operator command contract** 다.

---

## 5-7. restricted / full rollout handoff checklist

## restricted rollout handoff

1. gate runner `restricted` = `pass`
2. `recommended_flag_patch`를 operator가 수동 적용
3. notifier / maintenance health 재확인
4. 15~30분 관측 window 동안 아래 확인
   - open delivery DLQ 신규 0
   - due retry lag 임계 초과 0
   - unexpected send-disabled 0
5. 그 후 restricted scope 유지

## full rollout handoff

1. restricted 상태에서 안정 관측 window 확보
2. gate runner `full`
3. hard metrics 모두 통과
4. `operator_review_passed=true`
5. warning only 항목이 있으면 scope/채널/템플릿 재검토 후 승인
6. 그 후에만 full rollout 승인

## fail 시 공통 규칙

- runtime/transport hard fail → `ENABLE_NOTIFICATION_SEND=false`, `MAINTENANCE_ENABLE_NOTIFICATION_RETRY_PROMOTION=false`
- rollout-only fail → transport 유지 가능, 단 full rollout 금지

---

## 6. 코드 보강 초안

## 6-1. `src/services/maintenance/repositories.py` (updated fragment)

```python
async def insert_replay_requests_for_selected_plans_if_absent(
    self,
    *,
    plan_ids: list[str],
    requested_by: str,
) -> tuple[int, int]:
    if not plan_ids:
        return 0, 0

    result = await self._session.execute(
        sa.text(
            """
            WITH src AS (
              SELECT unnest(CAST(:plan_ids AS uuid[])) AS notification_plan_id
            ), inserted AS (
              INSERT INTO replay_requests (
                replay_request_id,
                replay_type,
                root_object_type,
                root_object_id,
                requested_by,
                requested_at,
                status
              )
              SELECT gen_random_uuid(),
                     'delivery'::replay_type_enum,
                     'notification_plan',
                     src.notification_plan_id,
                     :requested_by,
                     now(),
                     'requested'
              FROM src
              WHERE NOT EXISTS (
                SELECT 1
                FROM replay_requests rr
                WHERE rr.replay_type = 'delivery'::replay_type_enum
                  AND rr.root_object_type = 'notification_plan'
                  AND rr.root_object_id = src.notification_plan_id
                  AND rr.status IN ('requested', 'dispatched')
              )
              RETURNING root_object_id
            )
            SELECT
              (SELECT COUNT(*) FROM src) AS selected_count,
              (SELECT COUNT(*) FROM inserted) AS inserted_count
            """
        ),
        {"plan_ids": plan_ids, "requested_by": requested_by},
    )
    row = result.mappings().one()
    return int(row["selected_count"]), int(row["inserted_count"])


def build_manual_retry_intent_dedupe_key(row: SelectedPlanRecoveryRow) -> str:
    send_after_epoch = int(row.send_after.timestamp()) if row.send_after else 0
    return (
        f"notify:manual-retry-intent:"
        f"{row.notification_plan_id}:"
        f"{row.attempt_count or 0}:"
        f"{send_after_epoch}"
    )
```

---

## 6-2. `src/services/maintenance/batch_recovery_tool.py` (updated fragment)

```python
async def replay_selected(self, *, plan_ids: list[str], requested_by: str) -> RecoveryBatchResult:
    recovery_batch_id = str(uuid.uuid4())
    rows = await self._repository.load_selected_plan_recovery_rows(notification_plan_ids=plan_ids)
    skipped: dict[str, int] = {}
    accepted_ids: list[str] = []

    for row in rows:
        reason = self._validate_replay_row(row)
        if reason is None:
            accepted_ids.append(row.notification_plan_id)
        else:
            skipped[reason] = skipped.get(reason, 0) + 1

    selected_count, inserted_count = await self._repository.insert_replay_requests_for_selected_plans_if_absent(
        plan_ids=accepted_ids,
        requested_by=requested_by,
    )

    duplicate_safe_skips = max(0, selected_count - inserted_count)
    if duplicate_safe_skips:
        skipped["open_replay_request_exists_write_side"] = duplicate_safe_skips

    return RecoveryBatchResult(
        recovery_batch_id=recovery_batch_id,
        recovery_mode="replay-selected",
        selected_count=len(plan_ids),
        accepted_count=len(accepted_ids),
        skipped_count=len(plan_ids) - inserted_count,
        emitted_count=inserted_count,
        skipped_reason_codes=skipped,
    )
```

---

## 6-3. `src/services/maintenance/main.py` (updated fragment)

```python
def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="maintenance")
    sub = parser.add_subparsers(dest="command", required=True)

    worker = sub.add_parser("worker")
    worker.set_defaults(command="worker")

    gate = sub.add_parser("delivery-gate")
    gate.add_argument("--mode", choices=["restricted", "full"], required=True)
    gate.add_argument("--format", choices=["json", "text"], default="json")
    gate.add_argument("--operator-review-passed", choices=["true", "false"], default=None)

    recovery = sub.add_parser("batch-recovery")
    recovery_sub = recovery.add_subparsers(dest="recovery_mode", required=True)

    replay = recovery_sub.add_parser("replay-selected")
    replay.add_argument("--plan-id", action="append", required=True)
    replay.add_argument("--requested-by", required=True)
    replay.add_argument("--confirm", required=True)

    retry = recovery_sub.add_parser("retry-selected-due")
    retry.add_argument("--plan-id", action="append", required=True)
    retry.add_argument("--requested-by", required=True)
    retry.add_argument("--confirm", required=True)

    return parser


async def run_delivery_gate(args, repo) -> int:
    cfg = MaintenanceConfig.from_env()
    runner = DeliveryGateRunner(cfg, repository=repo)
    op_review = None
    if args.operator_review_passed == "true":
        op_review = True
    elif args.operator_review_passed == "false":
        op_review = False

    report = await runner.run(mode=args.mode, operator_review_passed=op_review)
    print(json.dumps(asdict(report), ensure_ascii=False, default=str, indent=2))
    if report.gate_status == "pass":
        return 0
    if report.gate_status == "warn":
        return 3
    return 2


async def run_batch_recovery(args, repo) -> int:
    if args.confirm != "write":
        print("batch-recovery requires --confirm write")
        return 2
    ...
```

---

## 7. 테스트 포인트

### `tests/unit/services/maintenance/test_manual_retry_intent_stable_dedupe.py`
검증:
- 같은 `notification_plan_id / latest_attempt_count / send_after` 상태로 CLI를 두 번 실행해도 dedupe key 가 동일한지

### `tests/unit/services/maintenance/test_batch_recovery_duplicate_safety.py`
검증:
- selection 단계에서는 open replay request 가 없었더라도
- write 단계에서 insert-if-absent 가 duplicate 를 흡수하는지

### `tests/integration/delivery/test_cli_confirm_required_for_batch_recovery.py`
검증:
- `--confirm write` 가 없으면 replay/retry batch recovery 가 실제 write 하지 않는지

### `tests/integration/delivery/test_full_gate_runner_warn_only_fields.py`
검증:
- operator review 미입력 시 full gate 가 `warn` 이고 hard fail 이 아닌지

---

## 8. 이번 단계가 구조를 지키는 이유

1. gate runner 는 여전히 **one-shot calculator/reporter** 다.  
   즉, runtime hot path 가 아니다.

2. batch recovery 는 여전히 **bridge emitter** 다.  
   즉, `notification_plans` 를 직접 수정하지 않는다.

3. duplicate safety 는 notifier ownership 을 넘지 않고 **`replay_requests` / `event_outbox`** 차원에서 닫는다.  
   즉, 기존 경계를 유지한다.

4. compose invocation 은 worker startup 과 분리된다.  
   즉, operator 명령과 서비스 런타임이 섞이지 않는다.

5. 현재 소스 세트는 이제 delivery line control-plane 까지 acceptance 기준이 붙어 있다.  
   즉, 이후 실제 구현 코딩을 시작해도 문서 경계가 다시 흔들릴 가능성이 낮다.

---

## 9. 실제 구현 코딩 시작 기준

이 문서까지 반영되면, 현재 프로젝트 소스 세트는 아래 의미에서 **실제 구현 코딩 시작 기준을 충족** 한 것으로 본다.

- 정본 단계 문서가 아키텍처 / 제품 / 운영 경계를 고정했다.
- 실행 계약 / migration 정본이 durable schema 와 queue contract 를 고정했다.
- stage 17~43 구현 초안이 서비스별 code skeleton / hardening / delivery control-plane 까지 내려왔다.
- stage 44가 마지막으로 gate / recovery / operator acceptance 를 정리했다.

따라서 이후 실제 코딩은 **새 설계 토론이 아니라, 이미 고정된 markdown 산출물을 repo 코드로 옮기는 작업** 으로 보는 것이 맞다.

권장 시작 순서는 다음 두 갈래 중 하나다.

### A. repo 전면 구현 시작
1. migration 실제 파일 정리
2. collector → outbox-relay → router-normalizer
3. gh/x/web enricher → evidence-assembler
4. analysis-router → judge-openai → validator → policy-engine → notifier
5. maintenance / delivery control-plane CLI

### B. 문서 위생 후 구현 시작
1. `10_delivery_hardening_stage39_plus_v0_1.md` 로 stage 39~44 묶기
2. authoritative README를 최신본 하나로 정리
3. 그 다음 repo 구현 시작

즉, **이제 문서 체인은 구현 착수 가능한 수준까지 닫혔다.**

---

## 10. 다음 단계

이번 단계가 닫히면 다음 문서 작업은 **필수 구현 단계라기보다 정리 단계** 에 가깝다.

권장 우선순위는 아래 둘 중 하나다.

1. 실제 repo 구현 코딩 시작
2. 문서 위생이 필요하면 `10_delivery_hardening_stage39_plus_v0_1.md` 로 stage 39~44 번들을 생성

즉, stage 44 이후에는 **문서 확장보다 구현 전개가 우선** 이다.

---

## 최종 한 줄 결론

이번 단계의 최소-change 정답은 **stage 43의 delivery gate runner / batch recovery code draft 를 deterministic fixture contract, stable duplicate safety, explicit CLI confirmation, compose one-shot invocation, rollout handoff checklist 로 acceptance hardening 하여, notifier/maintenance ownership 을 건드리지 않은 채 실제 구현 코딩에 들어갈 수 있는 마지막 control-plane 문서 경계를 닫는 것** 이다.

