# 06 collector implementation stage17 stage25 bundle v0 1
이 문서는 프로젝트 소스 파일 수 제한(40개)을 피하기 위한 **통합 번들**이다.
구조 변경 문서가 아니며, 아래 원본 문서를 **순서와 내용을 보존한 채** 묶는다.

## 포함 원본 파일
- `17_collector_telegram_skeleton_spec_v0_1.md`
- `18_collector_telegram_code_skeleton_package_v0_1.md`
- `19_collector_telegram_bootstrap_code_draft_v0_1.md`
- `20_collector_tdlib_auth_code_draft_v0_1.md`
- `21_collector_repository_outbox_idempotency_code_draft_v0_1.md`
- `22_collector_projection_dispatch_handlers_code_draft_v0_1.md`
- `23_collector_reconcile_registry_sync_code_draft_v0_1.md`
- `24_collector_health_observability_code_draft_v0_1.md`
- `25_collector_acceptance_hardening_code_draft_v0_1.md`

## 통합 원칙
- 아키텍처 불변식은 유지한다.
- 원본 파일명은 아래 섹션 제목으로 보존한다.
- 충돌 시 해석 우선순위는 기존과 동일하게 정본 단계 문서 → 실행 계약 → migration 정본 → 구현 초안이다.


---

## Source file: `17_collector_telegram_skeleton_spec_v0_1.md`

# Collector-Telegram 구현 스켈레톤 명세 v0.1

## 0. 문서 목적

이 문서는 이미 잠긴 0~12단계 설계와 `0001_ingest_core` migration 방향을 바탕으로, **`collector-telegram` 서비스의 실제 구현 골격**을 고정하는 문서다.

이 문서는 코드 작성 문서가 아니다. 목적은 다음 네 가지를 구현 가능한 수준으로 명확히 하는 것이다.

1. 서비스 책임 경계
2. 런타임 루프와 상태 전이
3. 모듈/클래스/인터페이스 골격
4. DB 쓰기 규칙, idempotency, reconcile, outbox 발행 규칙

핵심 전제:

- collector는 **판단기**가 아니다.
- collector는 **원문/수정/삭제/reconcile/outbox**까지만 담당한다.
- 키워드 판정, GitHub/X 분석, LLM 호출은 전부 collector 바깥 책임이다.

---

## 1. 소스 오브 트루스와 현재 단계 위치

현재 구현 순서는 이미 고정되어 있다.

- 0~10단계 설계 문서로 제품/런타임/서비스 경계가 잠겨 있음
- 11단계에서 execution contracts가 잠겨 있음
- 12단계에서 `0001`~`0004` migration 설계가 잠겨 있음
- migration 초안은 이미 `0001_ingest_core`부터 `0004_judge_delivery_observability`까지 내려온 상태

따라서 다음 구현 단계는 **`collector-telegram` 서비스 구현 스켈레톤**이 맞다.

이 서비스는 아래 구조에서 가장 앞단에 위치한다.

```text
Telegram channels
  ↓
collector-telegram
  ↓
source_messages / source_message_versions / event_outbox
  ↓
router-normalizer
```

---

## 2. 범위와 비범위

### 2-1. 이 스펙이 포함하는 것

- TDLib 인증/세션 유지
- tracked channel onboarding resolution
- live update 수신
- raw update journal 적재
- canonical `source_messages` upsert
- immutable `source_message_versions` append
- delete tombstone 처리
- reconcile / backfill 루프
- transactional outbox 적재
- health / metrics / structured logging

### 2-2. 이 스펙이 포함하지 않는 것

- trigger / normalization
- URL canonicalization
- GitHub/X/web fetch
- LLM judge
- Telegram notifier
- operator command plane
- multi-node 분산 collector

즉, 이 문서는 **3단계 collector 경계만 구현**한다.

---

## 3. collector의 고정 책임

### 3-1. 반드시 수행

1. 등록된 채널의 새 메시지 수신
2. 수정 이벤트 추적
3. 삭제 이벤트 추적
4. startup/warm/authenticated reconcile
5. 메시지 canonical row 유지
6. immutable version history 유지
7. outbox event 발행용 row 적재

### 3-2. 금지

1. 키워드 기준 저장 여부 결정
2. GitHub/X 링크 의미 해석
3. candidate 생성
4. reroot
5. LLM 호출
6. 사용자에게 알림 전송

이 경계는 변경 금지다.

---

## 4. 서비스 단위 런타임 모델

`collector-telegram`은 **단일 프로세스 + 내부 다중 루프** 구조로 시작한다.

### 4-1. 내부 런타임 구성

- **authorization loop**
  - TDLib authorization state 처리
- **update ingest loop**
  - live updates 수신/저장
- **reconcile scheduler loop**
  - 주기적 history sync 대상 선정
- **reconcile worker loop**
  - `getChatHistory` 기반 recent backfill/reconcile
- **channel registry refresh loop**
  - unresolved/join_requested/access_lost 상태 확인
- **health publisher loop**
  - heartbeat, lag, counts 집계

### 4-2. 인스턴스 정책

- prod에서 **정확히 1개**만 실행
- dev에서는 live ingest 금지
- replay는 별도 경로로 처리

---

## 5. 구현 골격: 권장 모듈 트리

```text
src/services/collector_telegram/
  __init__.py
  main.py
  service.py
  config.py
  runtime.py
  tdlib_client.py
  auth_fsm.py
  registry_sync.py
  update_dispatcher.py
  update_handlers.py
  message_projection.py
  reconcile.py
  repositories.py
  outbox.py
  idempotency.py
  health.py
  models.py
  exceptions.py
```

### 5-1. 파일별 역할

#### `main.py`
- 프로세스 entrypoint
- config 로드
- DI 조립
- signal handling
- graceful shutdown

#### `service.py`
- `CollectorTelegramService` 상위 orchestration
- startup / run / shutdown lifecycle

#### `config.py`
- env 기반 설정 로딩
- 필수 secret path 검증
- prod/dev mode 제약 검증

#### `runtime.py`
- 내부 task group 구성
- authorization loop / ingest loop / reconcile scheduler 연결

#### `tdlib_client.py`
- TDLib low-level wrapper
- `send`, `receive`, `close`
- authorization state / update decode

#### `auth_fsm.py`
- authorization state machine 구현
- phone/code/password/manual intervention 흐름 관리

#### `registry_sync.py`
- `telegram_channel_registry` 대상 onboarding / refresh
- public username / invite link resolve
- joined/paused/access_lost 전이 처리

#### `update_dispatcher.py`
- TDLib update type 라우팅
- raw update journal write + handler dispatch

#### `update_handlers.py`
- `updateNewMessage`
- `updateMessageEdited`
- `updateMessageContent`
- `updateDeleteMessages`
- `updateChatLastMessage`

#### `message_projection.py`
- TDLib message -> canonical projection
- `text_body`, `caption_text`, `text_surface`, `entities_json`, `url_surface_json`, `logical_post_key`

#### `reconcile.py`
- warm backfill
- authoritative reconcile
- gap fill

#### `repositories.py`
- Postgres persistence adapter
- raw/current/version/outbox write API

#### `outbox.py`
- collector event payload builder
- semantic dedupe key 생성

#### `idempotency.py`
- content hash
- no-op version 방지
- duplicate update 방지 규칙

#### `health.py`
- metrics emission
- stale detection
- heartbeat publication

#### `models.py`
- internal dataclass / typed payload

#### `exceptions.py`
- domain-specific exception class

---

## 6. 핵심 클래스/인터페이스 골격

아래는 코드가 아니라 **필수 인터페이스 수준 명세**다.

### 6-1. `CollectorTelegramConfig`

필수 필드:

- `app_env`
- `database_url`
- `redis_url`  
  - collector 직접 사용은 제한적이어도 process contract상 보유 가능
- `telegram_api_id`
- `telegram_api_hash_file`
- `telegram_phone_number`
- `telegram_2fa_password_file`
- `tdlib_state_dir`
- `tdlib_files_dir`
- `tdlib_db_encryption_key_file`
- `reconcile_interval_sec`
- `reconcile_backfill_limit`
- `warm_backfill_limit`
- `history_page_limit`
- `collector_mode` (`live` / `replay`)
- `log_level`

검증 규칙:

- prod에서 `collector_mode`는 `live`만 허용
- dev에서 `live` 금지
- TDLib state path는 writable이어야 함
- `reconcile_backfill_limit <= 100`

### 6-2. `TDLibClient`

필수 메서드:

- `initialize() -> None`
- `send(request: dict) -> None`
- `receive(timeout: float) -> dict | None`
- `close() -> None`
- `is_ready() -> bool`

보장:

- authorization state 이벤트를 raw update와 분리 없이 받을 수 있어야 함
- TDLib request/response correlation을 내부적으로 관리해야 함

### 6-3. `AuthorizationFSM`

필수 메서드:

- `handle_state(state: dict) -> AuthTransitionResult`
- `is_ready() -> bool`
- `is_degraded() -> bool`
- `requires_manual_intervention() -> bool`

상태:

- `booting`
- `waiting_tdlib_parameters`
- `waiting_phone_number`
- `waiting_code`
- `waiting_password`
- `ready`
- `degraded`
- `closed`

### 6-4. `ChannelRegistrySyncService`

필수 메서드:

- `sync_unresolved_channels() -> SyncSummary`
- `sync_join_requested_channels() -> SyncSummary`
- `sync_access_lost_channels() -> SyncSummary`
- `load_active_channels() -> list[TrackedChat]`

### 6-5. `UpdateDispatcher`

필수 메서드:

- `dispatch(update: dict) -> DispatchResult`

규칙:

1. raw update journal 먼저 저장
2. update type 분기
3. handler 실행
4. 성공/실패 상태를 raw update row에 반영

### 6-6. `MessageProjectionBuilder`

필수 메서드:

- `build_source_message_projection(message: dict) -> SourceMessageProjection`
- `build_version_snapshot(message: dict, reason: str) -> SourceMessageVersionProjection`

출력 필드:

- `chat_id`
- `message_id`
- `logical_post_key`
- `posted_at`
- `edited_at`
- `is_channel_post`
- `author_signature`
- `forward_info_json`
- `content_type`
- `text_body`
- `caption_text`
- `text_surface`
- `entities_json`
- `url_surface_json`
- `raw_message_json`
- `content_hash`

### 6-7. `CollectorRepository`

필수 메서드:

- `insert_raw_update(...)`
- `mark_raw_update_applied(...)`
- `mark_raw_update_failed(...)`
- `upsert_source_message(...)`
- `append_source_message_version_if_changed(...)`
- `mark_message_deleted(...)`
- `insert_outbox_event(...)`
- `get_source_message(...)`
- `get_latest_version(...)`
- `list_reconcile_targets(...)`
- `update_channel_sync_cursor(...)`

중요:

- `upsert_source_message`와 `append_source_message_version_if_changed`는 **같은 DB transaction** 안에서 호출 가능해야 함
- outbox write도 동일 transaction 경계에 포함되어야 함

### 6-8. `ReconcileService`

필수 메서드:

- `run_startup_warm_backfill(chat_id: int) -> ReconcileSummary`
- `run_authoritative_reconcile(chat_id: int) -> ReconcileSummary`
- `run_gap_fill(chat_id: int, reason: str) -> ReconcileSummary`

---

## 7. update 처리 규칙

### 7-1. `updateNewMessage`

처리 순서:

1. raw update journal insert
2. message projection 생성
3. `source_messages` upsert
4. `source_message_versions`에 `version_reason = new` append
5. `event_outbox`에 `source_message.created.v1` 적재
6. raw update applied 표시

### 7-2. `updateMessageEdited`

처리 순서:

1. raw update journal insert
2. `source_messages.edited_at` 반영
3. 내부 상태로 `pending_edit_sync` 개념 처리 가능  
   - 단, v1에서는 DB 컬럼 없이 메모리/로그 이벤트로만 유지 가능
4. `source_message.edited.v1` outbox 적재 가능  
   - 실제 content 변경은 아직 아님

주의:
- 이 이벤트만으로 새 version을 append하지 않는다

### 7-3. `updateMessageContent`

처리 순서:

1. raw update journal insert
2. 최신 full message fetch 또는 TDLib 제공 content 기준 canonical projection 재구성
3. 새 content hash 계산
4. 기존 latest version과 hash 비교
5. 달라졌을 때만 새 version append (`version_reason = content_change`)
6. `source_messages.current_version_no` 갱신
7. `source_message.edited.v1` outbox 적재

### 7-4. `updateDeleteMessages`

처리 순서:

1. raw update journal insert
2. 각 `(chat_id, message_id)`에 대해 tombstone 적용
3. `delete_kind = permanent | cache_only`
4. 필요 시 delete marker version append
5. `source_message.deleted.v1` outbox 적재

### 7-5. `updateChatLastMessage`

처리 순서:

1. raw update journal insert
2. `last_message is null` 또는 cursor anomaly 감지
3. 해당 채널 reconcile 우선순위 상승
4. 직접 version append는 하지 않음

---

## 8. DB 쓰기 규칙

collector는 `0001_ingest_core` 범위의 테이블만 직접 쓴다.

### 직접 쓰는 테이블

- `telegram_channel_registry`
- `telegram_raw_updates`
- `source_messages`
- `source_message_versions`
- `event_outbox`

### 직접 쓰면 안 되는 테이블

- `normalization_runs`
- `artifact_registry`
- `candidate_group_proposals`
- `artifact_snapshots`
- `judge_runs`
- `analyses`
- `notification_*`

### 트랜잭션 규칙

한 메시지 반영의 최소 atomic unit:

1. `source_messages` current row 반영
2. 필요 시 `source_message_versions` append
3. `event_outbox` insert

이 세 개는 **하나의 DB transaction**으로 묶는다.

---

## 9. outbox 이벤트 계약

collector가 내보내는 이벤트는 아래 네 종류로 제한한다.

- `source_message.created.v1`
- `source_message.edited.v1`
- `source_message.deleted.v1`
- `source_message.reconciled.v1`

### payload 최소 필드

- `event_id`
- `source_message_id`
- `current_version_no`
- `logical_post_key`
- `occurred_at`
- 삭제 시 `delete_kind`
- reconcile 시 `reconcile_reason`

### aggregate 규칙

- `aggregate_type = source_message`
- `aggregate_id = source_message_id`

### dedupe key 규칙

예시:

- create: `srcmsg:create:{source_message_id}:{current_version_no}`
- edit: `srcmsg:edit:{source_message_id}:{current_version_no}`
- delete: `srcmsg:delete:{source_message_id}:{current_version_no}`
- reconcile: `srcmsg:reconcile:{source_message_id}:{current_version_no}:{reason}`

---

## 10. idempotency 규칙

collector는 중복 수신을 정상 상태로 가정한다.

### 고정 규칙

1. `source_messages`는 `(platform, chat_id, message_id)` unique
2. `source_message_versions`는 `(source_message_id, version_no)` unique
3. 새 content hash가 기존 latest hash와 같으면 version append 금지
4. startup backfill과 live update가 같은 메시지를 건드려도 noop 가능해야 함
5. outbox는 semantic dedupe key로 중복 방지

### 구현 원칙

- update handler는 **at-least-once 수신**을 가정한다
- DB unique violation은 fatal이 아니라 idempotent noop로 흡수 가능해야 한다

---

## 11. reconcile / backfill 명세

### 11-1. warm backfill

목적:
- restart 직후 빠른 상태 복구

규칙:
- `only_local = true`
- `from_message_id = 0`
- `limit = warm_backfill_limit`  
  권장 30

### 11-2. authoritative reconcile

목적:
- live update 누락 보정

규칙:
- `only_local = false`
- `from_message_id = 0`
- `limit = reconcile_backfill_limit`  
  권장 30~100
- `last_seen_message_id/date`와 비교해 gap fill

### 11-3. trigger 조건

- startup 직후 active chat 전체
- 주기적 interval
- `updateChatLastMessage.last_message = null`
- access recovery 직후
- manual replay/reconcile 요청

### 11-4. reconcile 결과 분류

- `no_changes`
- `gap_filled`
- `cursor_advanced`
- `access_denied`
- `transient_failed`

---

## 12. channel onboarding / access 상태 전이

### `telegram_channel_registry.desired_state`

- `active`
- `paused`
- `removed`

### `access_state`

- `unresolved`
- `resolved_not_joined`
- `join_attempted`
- `join_requested`
- `joined`
- `forbidden`
- `not_found`
- `left`
- `access_lost`

### 전이 규칙

- public username resolve 성공 → `resolved_not_joined`
- `joinChat` 성공 → `joined`
- `INVITE_REQUEST_SENT` → `join_requested`
- 접근 상실 → `access_lost`
- 운영자가 비활성화 → `paused`

`desired_state != active` 인 row는 live track 대상에서 제외한다.

---

## 13. logging / metrics / health 명세

### 13-1. structured log 공통 필드

- `ts`
- `level`
- `service = collector-telegram`
- `env`
- `pipeline_run_id`  
  - 없으면 null 허용
- `event`
- `object_type`
- `object_id`
- `chat_id`
- `message_id`
- `status`
- `duration_ms`
- `error_code`

### 13-2. 필수 metrics

- `tdlib_authorization_state`
- `updates_received_total{type}`
- `source_messages_created_total`
- `source_messages_edited_total`
- `source_messages_deleted_total`
- `reconcile_runs_total`
- `reconcile_gap_fills_total`
- `tracked_channels_active`
- `outbox_pending_count`
- `last_update_received_at`
- `last_successful_history_sync_at{chat}`

### 13-3. health 상태

- `starting`
- `ready`
- `degraded`
- `failing`
- `stopped`

`ready` 조건:
- authorization ready
- DB 연결 정상
- active channel set 로드 성공
- update ingest loop 동작 중

---

## 14. 예외/장애 처리 명세

### retryable

- TDLib receive timeout
- `getChatHistory` 일시 실패
- DB transient connection error
- raw update apply transient failure

### terminal / manual intervention

- authorization code/password 재입력 필요
- session corruption
- repeated DB schema mismatch
- tracked channel access permanently lost

### 원칙

- raw update 저장 실패는 강한 경보 대상
- outbox 적재 실패는 message persistence와 같은 transaction 단위에서 실패 처리
- collector는 unknown exception으로 프로세스를 조용히 계속 끌고 가지 말고, health degraded 또는 crash-fast 중 하나를 명시적으로 선택해야 함

v1 권장:
- DB schema/persistence invariant 위반은 crash-fast
- 외부 Telegram/network/transient는 degraded + retry

---

## 15. 테스트 스켈레톤

### 15-1. unit tests

- `logical_post_key` 계산
- text/caption/entity projection
- content hash 계산
- outbox dedupe key 생성
- delete kind mapping
- update type dispatch

### 15-2. repository tests

- `source_messages` upsert idempotency
- same hash duplicate version 방지
- outbox atomic insert
- tombstone 반영

### 15-3. integration tests

- `updateNewMessage -> current row + version + outbox`
- `updateMessageEdited + updateMessageContent` 순차 처리
- `updateDeleteMessages` 처리
- startup warm backfill noop/idempotency
- reconcile gap fill

### 15-4. replay fixture tests

- 저장된 TDLib raw update fixture를 재생해 deterministic 결과 확인

---

## 16. 구현 마일스톤

### C1. config + TDLib bootstrap skeleton
- config loader
- TDLib wrapper
- auth FSM
- process lifecycle

### C2. DB persistence skeleton
- repository layer
- raw/current/version/outbox transaction
- idempotent upsert

### C3. live update ingestion
- dispatcher
- new/edit/content/delete handlers

### C4. reconcile path
- warm backfill
- authoritative reconcile
- gap triggers

### C5. health/metrics/logging
- structured log
- heartbeat
- collector lag

### C6. acceptance hardening
- duplicate/no-op 확인
- restart recovery
- degraded/manual intervention path

---

## 17. 수용 기준

아래를 만족하면 collector skeleton 구현이 완료된 것으로 본다.

1. prod single instance 제약이 코드/배포에서 보장됨
2. tracked active channel 대상 live update 저장 가능
3. `source_messages` / `source_message_versions` / `event_outbox` atomic write 보장
4. edit/content/delete 흐름이 분리 처리됨
5. startup/restart reconcile 존재
6. 키워드/URL/LLM 판단 로직이 collector에 없음
7. structured logs + metrics + heartbeat 존재
8. repeated update/backfill에도 idempotent

---

## 18. 다음 단계 연결

이 skeleton 다음 단계는 두 갈래가 아니라 하나다.

1. `collector-telegram` 실제 코드 골격 구현
2. 이어서 `outbox-relay`
3. 그 다음 `router-normalizer`

이 순서를 바꾸면 안 된다.

이유:
- collector는 `0001_ingest_core`만 쓰면 구현 가능
- normalizer는 collector current/outbox가 있어야 움직임
- 4단계 규칙은 collector 위에서만 성립함

---

## 최종 한 줄 결론

**`collector-telegram`은 TDLib 단일 live ingest 인스턴스로서, 등록 채널의 모든 Telegram 원문을 raw/current/version/outbox 구조로 PostgreSQL에 내구성 있게 적재하고, live updates만 믿지 않고 reconcile/backfill을 통해 누락을 보정하는 증거 보존 서비스로 구현되어야 한다.**


---

## Source file: `18_collector_telegram_code_skeleton_package_v0_1.md`

# 18단계: `collector-telegram` 코드 스켈레톤 패키지 v0.1

## 0. 문서 목적

이 문서는 `17_collector_telegram_skeleton_spec_v0_1.md`를 한 단계 더 내려, 실제 저장소에 어떤 파일을 만들고 각 파일이 어떤 인터페이스와 책임을 가져야 하는지까지 고정하는 **코드 스켈레톤 패키지 문서**다.

이 문서는 아직 실제 파이썬 코드를 전부 쓰는 단계는 아니다. 목적은 아래 네 가지다.

1. 저장소에 추가할 **정확한 파일 트리** 고정
2. 각 파일의 **필수 클래스 / 함수 / 공개 인터페이스** 고정
3. 메시지 수집·버전화·삭제·reconcile·outbox의 **구현 순서** 고정
4. 다음 턴에서 실제 코드 파일을 만들 때 **추가 설계 토론 없이 바로 구현** 가능하게 만드는 것

핵심 전제:

- collector는 **증거 보존 서비스**다.
- collector는 `0001_ingest_core` 범위 테이블만 직접 쓴다.
- 키워드 판정, URL canonicalization, candidate 생성, GitHub/X 분석, LLM 호출은 전부 collector 바깥 책임이다.

---

## 1. 현재 단계 위치

현재 구현 순서는 이미 잠겨 있다.

1. `0001`~`0004` migration 초안 완료
2. `collector-telegram` 스켈레톤 명세 완료
3. **이번 단계: collector 코드 스켈레톤 패키지 확정**
4. 다음 단계: collector 실제 코드 파일 생성
5. 그 다음: `outbox-relay`
6. 그 다음: `router-normalizer`

즉, 지금은 collector를 실제 코드로 내릴 수 있게 **파일 단위 설계**를 끝내는 단계다.

---

## 2. 범위와 비범위

### 2-1. 포함 범위

- `src/services/collector_telegram/` 파일 트리
- collector 내부 도메인 모델 초안
- repository 인터페이스
- TDLib wrapper 경계
- authorization state machine 경계
- update dispatcher / handler 경계
- reconcile service 경계
- outbox event builder 경계
- 테스트 파일 트리
- 구현 순서와 commit 분할 기준

### 2-2. 제외 범위

- 실제 TDLib Python 바인딩 최종 선택
- 실제 Docker Compose 서비스 정의 전체 구현
- 실제 Alembic migration 적용 코드
- normalizer / enricher / judge / notifier 코드
- 운영용 admin UI / command plane

---

## 3. 저장소에 추가할 정확한 파일 트리

```text
src/
  services/
    collector_telegram/
      __init__.py
      main.py
      service.py
      config.py
      runtime.py
      models.py
      exceptions.py
      tdlib_client.py
      auth_fsm.py
      registry_sync.py
      update_dispatcher.py
      update_handlers.py
      message_projection.py
      reconcile.py
      repositories.py
      outbox.py
      idempotency.py
      health.py

tests/
  unit/
    services/
      collector_telegram/
        test_config.py
        test_auth_fsm.py
        test_message_projection.py
        test_idempotency.py
        test_outbox.py
        test_update_dispatcher.py
  component/
    services/
      collector_telegram/
        test_repository_atomic_write.py
        test_update_new_message_flow.py
        test_update_edit_content_flow.py
        test_update_delete_flow.py
        test_reconcile_gap_fill.py
  fixtures/
    tdlib/
      update_new_message.json
      update_message_edited.json
      update_message_content.json
      update_delete_messages.json
      update_chat_last_message.json
```

원칙:

- collector 관련 코드는 `src/services/collector_telegram/` 아래로만 모은다.
- 공통 infra helper로 올릴 가치가 명백해지기 전에는 `src/common/`으로 미리 추출하지 않는다.
- 테스트도 동일하게 서비스 경계 기준으로 둔다.

---

## 4. 파일별 책임과 필수 공개 인터페이스

## 4-1. `config.py`

역할:
- env / secret path 로드
- prod/dev 제약 검증
- TDLib 관련 경로 검증
- reconcile/backfill 기본값 제공

필수 공개 요소:

### `CollectorTelegramConfig`
필드:
- `app_env: str`
- `database_url: str`
- `redis_url: str | None`
- `collector_mode: str`
- `telegram_api_id: int`
- `telegram_api_hash: str`
- `telegram_phone_number: str`
- `telegram_2fa_password: str | None`
- `tdlib_state_dir: str`
- `tdlib_files_dir: str`
- `tdlib_db_encryption_key: str`
- `reconcile_interval_sec: int`
- `reconcile_backfill_limit: int`
- `warm_backfill_limit: int`
- `history_page_limit: int`
- `log_level: str`

필수 메서드:
- `from_env() -> CollectorTelegramConfig`
- `validate() -> None`

검증 규칙:
- prod에서는 `collector_mode == "live"`
- dev에서는 `collector_mode != "live"`
- `history_page_limit <= 100`
- TDLib state/files dir 존재 또는 생성 가능

---

## 4-2. `models.py`

역할:
- collector 내부 typed payload
- DB projection용 dataclass 정의

필수 데이터 구조:

### `TrackedChat`
- `registry_id`
- `chat_id`
- `desired_state`
- `access_state`
- `source_kind`
- `source_value`
- `priority_weight`
- `last_seen_message_id`
- `last_seen_message_date`

### `SourceMessageProjection`
- `chat_id`
- `message_id`
- `logical_post_key`
- `is_channel_post`
- `posted_at`
- `edited_at`
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
- `content_hash`

### `SourceMessageVersionProjection`
- `source_message_id | None`
- `version_no | None`
- `version_reason`
- `observed_at`
- `telegram_edit_date`
- `text_surface`
- `entities_json`
- `raw_message_json`
- `content_hash`

### `OutboxEventDraft`
- `event_type`
- `aggregate_type`
- `aggregate_id`
- `dedupe_key`
- `payload_json`

### `ReconcileSummary`
- `chat_id`
- `result_type`
- `processed_count`
- `inserted_count`
- `updated_count`
- `gap_filled_count`
- `error_code | None`

---

## 4-3. `exceptions.py`

역할:
- 예외를 의미 단위로 분리

필수 예외:
- `CollectorError`
- `ConfigurationError`
- `AuthorizationError`
- `AuthorizationManualInterventionRequired`
- `TDLibTransportError`
- `RepositoryInvariantError`
- `UpdateApplyRetryableError`
- `UpdateApplyTerminalError`
- `ReconcileRetryableError`
- `ReconcileTerminalError`

원칙:
- DB invariant 깨짐은 retryable이 아니라 terminal / crash-fast 후보
- Telegram 네트워크/일시 오류는 retryable

---

## 4-4. `tdlib_client.py`

역할:
- TDLib low-level wrapper
- request/response/update 경계 고정

필수 공개 인터페이스:

### `TDLibClient`
필수 메서드:
- `initialize() -> None`
- `send(request: dict) -> None`
- `receive(timeout: float) -> dict | None`
- `close() -> None`
- `is_ready() -> bool`

지원해야 하는 최소 요청 종류:
- `setTdlibParameters`
- `checkDatabaseEncryptionKey`
- `setAuthenticationPhoneNumber`
- `checkAuthenticationCode`
- `checkAuthenticationPassword`
- `searchPublicChat`
- `joinChat`
- `joinChatByInviteLink`
- `getChatHistory`
- `getMessageLink`

구현 주의:
- wrapper는 domain 로직을 가지지 않는다.
- request id / correlation은 내부적으로만 처리한다.
- raw TDLib payload를 가능한 한 손실 없이 상위 계층에 넘긴다.

---

## 4-5. `auth_fsm.py`

역할:
- authorization 상태 기계

필수 공개 요소:

### `AuthorizationState`
허용 상태:
- `booting`
- `waiting_tdlib_parameters`
- `waiting_phone_number`
- `waiting_code`
- `waiting_password`
- `ready`
- `degraded`
- `closed`

### `AuthorizationFSM`
필수 메서드:
- `handle_state(state: dict) -> str`
- `current_state() -> str`
- `is_ready() -> bool`
- `requires_manual_intervention() -> bool`

구현 규칙:
- 최초 1회 수동 인증은 허용
- 운영 중 다시 `waiting_code` / `waiting_password`가 뜨면 `degraded` + manual intervention
- 인증 흐름 우회 금지

---

## 4-6. `message_projection.py`

역할:
- TDLib message를 DB projection으로 변환

필수 공개 요소:

### `MessageProjectionBuilder`
필수 메서드:
- `build_source_projection(message: dict) -> SourceMessageProjection`
- `build_version_projection(message: dict, reason: str) -> SourceMessageVersionProjection`
- `compute_logical_post_key(message: dict) -> str`
- `compute_content_hash(projection: SourceMessageProjection) -> str`

구현 규칙:
- 원문 `raw_message_json`은 그대로 유지
- `text_surface`는 body + caption 기반 검색용 current row
- URL은 entity 우선, regex는 fallback
- album이면 `media_album_id` 기반 `logical_post_key` 사용
- 원문 overwrite 금지, projection은 파생 surface일 뿐

---

## 4-7. `idempotency.py`

역할:
- 중복 수신 / noop 처리 규칙 캡슐화

필수 공개 요소:

### `IdempotencyPolicy`
필수 메서드:
- `should_append_new_version(previous_hash: str | None, next_hash: str) -> bool`
- `semantic_event_dedupe_key(event_type: str, source_message_id: str, version_no: int, extra: str | None = None) -> str`

고정 규칙:
- same hash면 새 version append 금지
- 같은 semantic outbox event는 중복 발행 금지
- live update와 reconcile 중복은 noop 허용

---

## 4-8. `outbox.py`

역할:
- collector 전용 outbox payload builder

필수 공개 요소:

### `CollectorOutboxBuilder`
필수 메서드:
- `build_created(...) -> OutboxEventDraft`
- `build_edited(...) -> OutboxEventDraft`
- `build_deleted(...) -> OutboxEventDraft`
- `build_reconciled(...) -> OutboxEventDraft`

고정 event type:
- `source_message.created.v1`
- `source_message.edited.v1`
- `source_message.deleted.v1`
- `source_message.reconciled.v1`

payload 최소 필드:
- `event_id`는 DB default 허용 가능
- `source_message_id`
- `current_version_no`
- `logical_post_key`
- `occurred_at`
- 삭제 시 `delete_kind`
- reconcile 시 `reconcile_reason`

---

## 4-9. `repositories.py`

역할:
- collector persistence adapter

필수 공개 인터페이스:

### `CollectorRepository`
필수 메서드:
- `insert_raw_update(...) -> int`
- `mark_raw_update_applied(update_seq: int) -> None`
- `mark_raw_update_failed(update_seq: int, error_text: str) -> None`
- `get_source_message(platform: str, chat_id: int, message_id: int) -> dict | None`
- `upsert_source_message(projection: SourceMessageProjection) -> dict`
- `get_latest_version(source_message_id: str) -> dict | None`
- `append_source_message_version(...) -> dict`
- `append_source_message_version_if_changed(...) -> tuple[bool, dict | None]`
- `mark_message_deleted(...) -> dict`
- `insert_outbox_event(event: OutboxEventDraft) -> None`
- `list_active_tracked_chats() -> list[TrackedChat]`
- `list_reconcile_targets(limit: int) -> list[TrackedChat]`
- `update_channel_sync_cursor(...) -> None`

트랜잭션 규칙:
- current row 반영 + version append + outbox insert는 한 transaction
- outbox 실패 시 current/version도 rollback
- partial success 금지

---

## 4-10. `registry_sync.py`

역할:
- tracked chat onboarding / access refresh

필수 공개 요소:

### `ChannelRegistrySyncService`
필수 메서드:
- `sync_unresolved_channels() -> dict`
- `sync_join_requested_channels() -> dict`
- `sync_access_lost_channels() -> dict`
- `load_active_channels() -> list[TrackedChat]`

처리 규칙:
- `public_username`는 resolve 후 `chat_id` anchor 확보
- `join_requested`는 실패가 아니라 대기 상태
- `desired_state != active`는 live tracking 대상 제외
- access loss는 반복 aggressive retry 금지

---

## 4-11. `update_handlers.py`

역할:
- update type별 실제 적용

필수 공개 함수 또는 클래스 메서드:
- `handle_update_new_message(update: dict) -> None`
- `handle_update_message_edited(update: dict) -> None`
- `handle_update_message_content(update: dict) -> None`
- `handle_update_delete_messages(update: dict) -> None`
- `handle_update_chat_last_message(update: dict) -> None`

고정 처리 규칙:

### `updateNewMessage`
1. raw update 저장
2. projection 생성
3. source current upsert
4. `version_reason = new`
5. `source_message.created.v1` outbox

### `updateMessageEdited`
1. raw update 저장
2. `edited_at` 반영
3. content 변경은 아직 append하지 않음
4. 필요 시 memory-level pending edit sync

### `updateMessageContent`
1. raw update 저장
2. projection 재구성
3. content hash 비교
4. 달라졌을 때만 version append
5. `source_message.edited.v1` outbox

### `updateDeleteMessages`
1. raw update 저장
2. tombstone 반영
3. 필요 시 delete marker version
4. delete outbox

### `updateChatLastMessage`
1. raw update 저장
2. gap 가능성 감지
3. reconcile 대상 승격

---

## 4-12. `update_dispatcher.py`

역할:
- raw update → handler 라우팅

필수 공개 요소:

### `UpdateDispatcher`
필수 메서드:
- `dispatch(update: dict) -> None`

규칙:
- 지원하지 않는 update는 raw journal만 저장하고 skip 가능
- dispatcher는 의미 해석을 하지 않는다
- handler 예외는 retryable/terminal 분류 후 raw update 상태 반영

---

## 4-13. `reconcile.py`

역할:
- startup warm backfill / authoritative reconcile / gap fill

필수 공개 요소:

### `ReconcileService`
필수 메서드:
- `run_startup_warm_backfill(chat_id: int) -> ReconcileSummary`
- `run_authoritative_reconcile(chat_id: int, reason: str) -> ReconcileSummary`
- `run_gap_fill(chat_id: int, reason: str) -> ReconcileSummary`

고정 규칙:
- warm backfill: `only_local=true`, 최근 30개 권장
- authoritative reconcile: `only_local=false`, 최근 30~100개 권장
- `updateChatLastMessage.last_message is null`이면 우선 reconcile
- 동일 메시지 재조회는 idempotent noop 허용

---

## 4-14. `health.py`

역할:
- heartbeat / metrics / readiness 판단

필수 공개 요소:

### `CollectorHealthService`
필수 메서드:
- `mark_update_received(update_type: str) -> None`
- `mark_reconcile_result(summary: ReconcileSummary) -> None`
- `snapshot() -> dict`
- `readiness() -> str`

필수 metrics:
- `tdlib_authorization_state`
- `updates_received_total{type}`
- `source_messages_created_total`
- `source_messages_edited_total`
- `source_messages_deleted_total`
- `reconcile_runs_total`
- `reconcile_gap_fills_total`
- `tracked_channels_active`
- `outbox_pending_count`
- `last_update_received_at`
- `last_successful_history_sync_at{chat}`

---

## 4-15. `runtime.py`

역할:
- 내부 루프 orchestration

필수 공개 요소:

### `CollectorRuntime`
필수 메서드:
- `run_forever() -> None`
- `shutdown() -> None`

내부 루프:
- authorization loop
- live update ingest loop
- reconcile scheduler loop
- registry refresh loop
- health publisher loop

원칙:
- prod 단일 인스턴스 가정
- 서비스 내부 병렬 루프는 허용하되, 외부에서는 singleton

---

## 4-16. `service.py`

역할:
- 상위 lifecycle 관리

필수 공개 요소:

### `CollectorTelegramService`
필수 메서드:
- `start() -> None`
- `run() -> None`
- `stop() -> None`

책임:
- config 검증
- singleton guard 확인
- dependency wiring
- runtime 시작/종료

---

## 4-17. `main.py`

역할:
- CLI/entrypoint

필수 동작:
- config 로드
- structured logger init
- `CollectorTelegramService` 생성
- SIGTERM/SIGINT graceful shutdown
- exit code 표준화

---

## 5. 실제 구현 순서

다음 순서로 코드를 만드는 것이 맞다.

### C1. 최소 부팅 골격
대상 파일:
- `config.py`
- `exceptions.py`
- `models.py`
- `main.py`
- `service.py`
- `runtime.py`

목표:
- 프로세스 기동 / 종료 / config 검증 / 빈 runtime loop

### C2. TDLib 인증 골격
대상 파일:
- `tdlib_client.py`
- `auth_fsm.py`

목표:
- authorization ready/degraded 상태 전이 가능

### C3. DB repository 골격
대상 파일:
- `repositories.py`
- `outbox.py`
- `idempotency.py`

목표:
- raw/current/version/outbox transaction contract 확정

### C4. update 처리 골격
대상 파일:
- `message_projection.py`
- `update_handlers.py`
- `update_dispatcher.py`

목표:
- `updateNewMessage` end-to-end 처리 가능

### C5. reconcile 골격
대상 파일:
- `reconcile.py`
- `registry_sync.py`

목표:
- startup backfill / periodic reconcile / channel onboarding 가능

### C6. observability 골격
대상 파일:
- `health.py`
- logger wiring

목표:
- readiness / heartbeat / 주요 metrics 노출

---

## 6. 테스트 파일별 목적

### `test_config.py`
- env 누락 검증
- prod/dev 모드 제약 검증
- invalid limit 검증

### `test_auth_fsm.py`
- authorization state 전이
- manual intervention 상태 전이

### `test_message_projection.py`
- text/caption/entity projection
- album logical key
- content hash determinism

### `test_idempotency.py`
- same hash duplicate version 방지
- outbox dedupe key 안정성

### `test_outbox.py`
- event payload 최소 필드 검증

### `test_update_dispatcher.py`
- update type 라우팅
- unknown update noop

### `test_repository_atomic_write.py`
- current/version/outbox atomic write
- rollback 동작

### `test_update_new_message_flow.py`
- new message end-to-end

### `test_update_edit_content_flow.py`
- edit metadata + content change 분리

### `test_update_delete_flow.py`
- tombstone / delete outbox

### `test_reconcile_gap_fill.py`
- warm backfill noop
- authoritative reconcile gap fill

---

## 7. commit 분할 기준

collector 코드는 한 번에 크게 넣으면 리뷰가 안 된다. 아래처럼 쪼개는 게 맞다.

### Commit 1
`feat(collector): add config, lifecycle, and runtime skeleton`

### Commit 2
`feat(collector): add tdlib auth wrapper and authorization state machine`

### Commit 3
`feat(collector): add repository, outbox, and idempotency skeleton`

### Commit 4
`feat(collector): add message projection and update dispatch skeleton`

### Commit 5
`feat(collector): add reconcile and channel registry sync skeleton`

### Commit 6
`feat(collector): add health metrics and collector acceptance tests`

---

## 8. 현재 턴 이후 바로 들어갈 실제 구현 범위

다음 실제 코드 턴에서는 범위를 욕심내면 안 된다.

가장 먼저 만들 실제 파일 묶음은 아래가 맞다.

### 첫 구현 묶음
- `config.py`
- `exceptions.py`
- `models.py`
- `main.py`
- `service.py`
- `runtime.py`

이유:
- TDLib/DB/Telegram 네트워크 없이도 구조 검증 가능
- 이후 파일들이 기대할 lifecycle과 config contract를 먼저 고정할 수 있음
- 범위를 최소화해 깨지지 않게 만들 수 있음

즉, **다음 코드 생성 턴의 목표는 “collector가 기동 가능한 빈 뼈대”까지**다.

---

## 9. 수용 기준

아래를 만족하면 이 문서 기준의 code skeleton package가 완료된 것으로 본다.

1. 저장소에 추가할 정확한 파일 리스트가 고정됨
2. 각 파일의 공개 책임과 인터페이스가 고정됨
3. DB write / outbox / reconcile 규칙이 파일 단위로 연결됨
4. 테스트 파일과 목적이 고정됨
5. 다음 턴에서 실제 코드 생성을 바로 시작할 수 있음

---

## 최종 한 줄 결론

**이번 단계의 산출물은 `collector-telegram`을 실제 코드로 내리기 위한 파일 단위 구현 패키지이며, 다음 턴부터는 이 문서를 기준으로 최소 부팅 골격(`config.py`, `exceptions.py`, `models.py`, `main.py`, `service.py`, `runtime.py`)부터 실제 코드 생성을 시작하는 것이 맞다.**


---

## Source file: `19_collector_telegram_bootstrap_code_draft_v0_1.md`

# 19단계: `collector-telegram` 최소 부팅 골격 실제 코드 초안 v0.1

## 0. 문서 목적

이 문서는 `17_collector_telegram_skeleton_spec_v0_1.md`와 `18_collector_telegram_code_skeleton_package_v0_1.md`를 바탕으로,
`collector-telegram`의 **첫 실제 코드 묶음(C1)** 을 바로 구현할 수 있도록 만든 **복사-붙여넣기용 코드 초안**이다.

범위는 아래 6개 파일로 제한한다.

- `config.py`
- `exceptions.py`
- `models.py`
- `main.py`
- `service.py`
- `runtime.py`

이 문서의 코드는 의도적으로 아래 범위까지만 다룬다.

- 프로세스 기동
- 설정 로드 및 검증
- 서비스 라이프사이클
- 빈 런타임 루프
- graceful shutdown
- structured JSON logging의 최소 골격

이 문서는 아직 아래를 구현하지 않는다.

- TDLib 실제 연결
- DB repository
- outbox 적재
- update dispatch
- reconcile logic
- metrics exporter
- health endpoint

즉, 이번 묶음의 목표는 **collector가 “기동 가능한 빈 뼈대”까지 도달하는 것**이다.

---

## 1. 대상 파일 트리

```text
src/services/collector_telegram/
  __init__.py
  config.py
  exceptions.py
  models.py
  main.py
  service.py
  runtime.py
```

---

## 2. 코드 초안

## 2-1. `src/services/collector_telegram/__init__.py`

```python
from .config import CollectorTelegramConfig
from .runtime import CollectorRuntime
from .service import CollectorTelegramService

__all__ = [
    "CollectorTelegramConfig",
    "CollectorRuntime",
    "CollectorTelegramService",
]
```

---

## 2-2. `src/services/collector_telegram/exceptions.py`

```python
from __future__ import annotations


class CollectorError(Exception):
    """Base class for collector-specific failures."""


class ConfigurationError(CollectorError):
    """Raised when configuration is invalid or incomplete."""


class AuthorizationError(CollectorError):
    """Raised when TDLib authorization flow is invalid or broken."""


class AuthorizationManualInterventionRequired(AuthorizationError):
    """Raised when operator action is required to continue authorization."""


class TDLibTransportError(CollectorError):
    """Raised for low-level TDLib transport failures."""


class RepositoryInvariantError(CollectorError):
    """Raised when persistence invariants are broken.

    These are normally terminal and should fail fast.
    """


class UpdateApplyRetryableError(CollectorError):
    """Raised when an update application may succeed on retry."""


class UpdateApplyTerminalError(CollectorError):
    """Raised when an update application must not be retried as-is."""


class ReconcileRetryableError(CollectorError):
    """Raised when reconcile may succeed on retry."""


class ReconcileTerminalError(CollectorError):
    """Raised when reconcile encountered a terminal condition."""
```

---

## 2-3. `src/services/collector_telegram/models.py`

```python
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Literal


CollectorMode = Literal["live", "replay"]
AppEnv = Literal["prod", "dev", "test"]

DesiredState = Literal["active", "paused", "removed"]
AccessState = Literal[
    "unresolved",
    "resolved_not_joined",
    "join_attempted",
    "join_requested",
    "joined",
    "forbidden",
    "not_found",
    "left",
    "access_lost",
]

CollectorHealthState = Literal["starting", "ready", "degraded", "failing", "stopped"]


@dataclass(slots=True, frozen=True)
class TrackedChat:
    registry_id: str
    chat_id: int | None
    desired_state: DesiredState
    access_state: AccessState
    source_kind: str
    source_value: str
    priority_weight: int = 100
    last_seen_message_id: int | None = None
    last_seen_message_date: datetime | None = None


@dataclass(slots=True, frozen=True)
class SourceMessageProjection:
    chat_id: int
    message_id: int
    logical_post_key: str
    is_channel_post: bool
    posted_at: datetime
    edited_at: datetime | None
    message_link: str | None
    author_signature: str | None
    forward_info_json: dict[str, Any] | None
    content_type: str | None
    text_body: str | None
    caption_text: str | None
    text_surface: str | None
    entities_json: list[dict[str, Any]] | None
    url_surface_json: list[dict[str, Any]] | None
    raw_message_json: dict[str, Any]
    content_hash: str


@dataclass(slots=True, frozen=True)
class SourceMessageVersionProjection:
    source_message_id: str | None
    version_no: int | None
    version_reason: str
    observed_at: datetime
    telegram_edit_date: datetime | None
    text_surface: str | None
    entities_json: list[dict[str, Any]] | None
    raw_message_json: dict[str, Any]
    content_hash: str


@dataclass(slots=True, frozen=True)
class OutboxEventDraft:
    event_type: str
    aggregate_type: str
    aggregate_id: str
    dedupe_key: str
    payload_json: dict[str, Any]


@dataclass(slots=True, frozen=True)
class ReconcileSummary:
    chat_id: int
    result_type: str
    processed_count: int = 0
    inserted_count: int = 0
    updated_count: int = 0
    gap_filled_count: int = 0
    error_code: str | None = None


@dataclass(slots=True)
class RuntimeSnapshot:
    health_state: CollectorHealthState = "starting"
    started_at: datetime | None = None
    last_tick_at: datetime | None = None
    last_update_received_at: datetime | None = None
    tracked_channels_active: int = 0
    reconcile_runs_total: int = 0
    reconcile_gap_fills_total: int = 0
    notes: list[str] = field(default_factory=list)
```

---

## 2-4. `src/services/collector_telegram/config.py`

```python
from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from .exceptions import ConfigurationError
from .models import AppEnv, CollectorMode


_ALLOWED_APP_ENVS = {"prod", "dev", "test"}
_ALLOWED_MODES = {"live", "replay"}


def _read_text_file(path_str: str, *, field_name: str) -> str:
    path = Path(path_str)
    if not path.exists():
        raise ConfigurationError(f"{field_name} file does not exist: {path}")
    if not path.is_file():
        raise ConfigurationError(f"{field_name} path is not a file: {path}")

    value = path.read_text(encoding="utf-8").strip()
    if not value:
        raise ConfigurationError(f"{field_name} file is empty: {path}")
    return value


def _read_secret(
    *,
    env_name: str,
    allow_empty: bool = False,
    default: str | None = None,
) -> str | None:
    file_env_name = f"{env_name}_FILE"
    file_value = os.getenv(file_env_name)
    direct_value = os.getenv(env_name)

    value: str | None
    if file_value:
        value = _read_text_file(file_value, field_name=file_env_name)
    else:
        value = direct_value if direct_value is not None else default

    if value is None:
        return None

    if not value and not allow_empty:
        raise ConfigurationError(f"{env_name} is empty")
    return value


def _read_required(env_name: str) -> str:
    value = os.getenv(env_name)
    if value is None or not value.strip():
        raise ConfigurationError(f"Missing required environment variable: {env_name}")
    return value.strip()


def _read_required_int(env_name: str) -> int:
    raw = _read_required(env_name)
    try:
        return int(raw)
    except ValueError as exc:
        raise ConfigurationError(f"{env_name} must be an integer: {raw}") from exc


def _read_int(env_name: str, *, default: int) -> int:
    raw = os.getenv(env_name)
    if raw is None or not raw.strip():
        return default
    try:
        return int(raw)
    except ValueError as exc:
        raise ConfigurationError(f"{env_name} must be an integer: {raw}") from exc


@dataclass(slots=True, frozen=True)
class CollectorTelegramConfig:
    app_env: AppEnv
    database_url: str
    redis_url: str | None
    collector_mode: CollectorMode

    telegram_api_id: int
    telegram_api_hash: str
    telegram_phone_number: str
    telegram_2fa_password: str | None

    tdlib_state_dir: str
    tdlib_files_dir: str
    tdlib_db_encryption_key: str

    reconcile_interval_sec: int
    reconcile_backfill_limit: int
    warm_backfill_limit: int
    history_page_limit: int

    log_level: str = "INFO"

    @classmethod
    def from_env(cls) -> "CollectorTelegramConfig":
        app_env = os.getenv("APP_ENV", "dev").strip().lower()
        collector_mode = os.getenv("COLLECTOR_MODE", "replay").strip().lower()

        config = cls(
            app_env=app_env,  # type: ignore[arg-type]
            database_url=_read_required("DATABASE_URL"),
            redis_url=os.getenv("REDIS_URL"),
            collector_mode=collector_mode,  # type: ignore[arg-type]
            telegram_api_id=_read_required_int("TELEGRAM_API_ID"),
            telegram_api_hash=_read_secret(env_name="TELEGRAM_API_HASH"),
            telegram_phone_number=_read_required("TELEGRAM_PHONE_NUMBER"),
            telegram_2fa_password=_read_secret(
                env_name="TELEGRAM_2FA_PASSWORD",
                allow_empty=True,
                default=None,
            ),
            tdlib_state_dir=_read_required("TDLIB_STATE_DIR"),
            tdlib_files_dir=os.getenv("TDLIB_FILES_DIR", "").strip() or _read_required("TDLIB_STATE_DIR"),
            tdlib_db_encryption_key=_read_secret(env_name="TDLIB_DB_ENCRYPTION_KEY"),
            reconcile_interval_sec=_read_int("RECONCILE_INTERVAL_SEC", default=300),
            reconcile_backfill_limit=_read_int("RECONCILE_BACKFILL_LIMIT", default=50),
            warm_backfill_limit=_read_int("WARM_BACKFILL_LIMIT", default=30),
            history_page_limit=_read_int("HISTORY_PAGE_LIMIT", default=50),
            log_level=os.getenv("LOG_LEVEL", "INFO").strip().upper(),
        )
        config.validate()
        return config

    def validate(self) -> None:
        if self.app_env not in _ALLOWED_APP_ENVS:
            raise ConfigurationError(f"APP_ENV must be one of {_ALLOWED_APP_ENVS}, got: {self.app_env}")

        if self.collector_mode not in _ALLOWED_MODES:
            raise ConfigurationError(
                f"COLLECTOR_MODE must be one of {_ALLOWED_MODES}, got: {self.collector_mode}"
            )

        if self.app_env == "prod" and self.collector_mode != "live":
            raise ConfigurationError("prod environment requires COLLECTOR_MODE=live")

        if self.app_env in {"dev", "test"} and self.collector_mode == "live":
            raise ConfigurationError("dev/test environment must not use COLLECTOR_MODE=live")

        if self.reconcile_interval_sec <= 0:
            raise ConfigurationError("RECONCILE_INTERVAL_SEC must be > 0")

        if self.reconcile_backfill_limit <= 0 or self.reconcile_backfill_limit > 100:
            raise ConfigurationError("RECONCILE_BACKFILL_LIMIT must be between 1 and 100")

        if self.warm_backfill_limit <= 0 or self.warm_backfill_limit > 100:
            raise ConfigurationError("WARM_BACKFILL_LIMIT must be between 1 and 100")

        if self.history_page_limit <= 0 or self.history_page_limit > 100:
            raise ConfigurationError("HISTORY_PAGE_LIMIT must be between 1 and 100")

        if not self.telegram_api_hash:
            raise ConfigurationError("TELEGRAM_API_HASH must be configured")

        if not self.tdlib_db_encryption_key:
            raise ConfigurationError("TDLIB_DB_ENCRYPTION_KEY must be configured")

    def ensure_runtime_dirs(self) -> None:
        Path(self.tdlib_state_dir).mkdir(parents=True, exist_ok=True)
        Path(self.tdlib_files_dir).mkdir(parents=True, exist_ok=True)
```

---

## 2-5. `src/services/collector_telegram/runtime.py`

```python
from __future__ import annotations

import asyncio
import contextlib
import logging
from datetime import datetime, timezone

from .config import CollectorTelegramConfig
from .models import RuntimeSnapshot


class CollectorRuntime:
    """Minimal bootable runtime skeleton.

    This class intentionally does not talk to TDLib, PostgreSQL, or Redis yet.
    Its only responsibility in C1 is to prove:
    - lifecycle wiring
    - graceful shutdown
    - placeholder internal loop structure
    - structured runtime logging
    """

    def __init__(
        self,
        config: CollectorTelegramConfig,
        *,
        logger: logging.Logger | None = None,
    ) -> None:
        self._config = config
        self._logger = logger or logging.getLogger(__name__)
        self._stop_event = asyncio.Event()
        self._tasks: list[asyncio.Task[None]] = []
        self._snapshot = RuntimeSnapshot()

    @property
    def snapshot(self) -> RuntimeSnapshot:
        return self._snapshot

    async def run_forever(self) -> None:
        self._logger.info(
            "collector_runtime_starting",
            extra={
                "service": "collector-telegram",
                "event": "collector_runtime_starting",
                "collector_mode": self._config.collector_mode,
                "app_env": self._config.app_env,
            },
        )
        self._snapshot.started_at = datetime.now(timezone.utc)
        self._snapshot.health_state = "ready"

        self._tasks = [
            asyncio.create_task(self._authorization_loop(), name="collector.authorization"),
            asyncio.create_task(self._update_ingest_loop(), name="collector.update_ingest"),
            asyncio.create_task(self._reconcile_scheduler_loop(), name="collector.reconcile_scheduler"),
            asyncio.create_task(self._registry_refresh_loop(), name="collector.registry_refresh"),
            asyncio.create_task(self._health_publisher_loop(), name="collector.health"),
        ]

        try:
            await self._stop_event.wait()
        finally:
            await self.shutdown()

    async def shutdown(self) -> None:
        if self._stop_event.is_set():
            pass
        else:
            self._stop_event.set()

        self._snapshot.health_state = "stopped"

        current_task = asyncio.current_task()
        for task in self._tasks:
            if task is current_task:
                continue
            task.cancel()

        for task in self._tasks:
            if task is current_task:
                continue
            with contextlib.suppress(asyncio.CancelledError):
                await task

        self._logger.info(
            "collector_runtime_stopped",
            extra={
                "service": "collector-telegram",
                "event": "collector_runtime_stopped",
            },
        )

    async def _authorization_loop(self) -> None:
        await self._idle_loop("authorization_loop", interval_sec=5)

    async def _update_ingest_loop(self) -> None:
        await self._idle_loop("update_ingest_loop", interval_sec=2)

    async def _reconcile_scheduler_loop(self) -> None:
        await self._idle_loop(
            "reconcile_scheduler_loop",
            interval_sec=float(self._config.reconcile_interval_sec),
        )

    async def _registry_refresh_loop(self) -> None:
        await self._idle_loop("registry_refresh_loop", interval_sec=60)

    async def _health_publisher_loop(self) -> None:
        await self._idle_loop("health_publisher_loop", interval_sec=30)

    async def _idle_loop(self, loop_name: str, *, interval_sec: float) -> None:
        self._logger.info(
            "collector_loop_started",
            extra={
                "service": "collector-telegram",
                "event": "collector_loop_started",
                "loop_name": loop_name,
                "interval_sec": interval_sec,
            },
        )

        try:
            while not self._stop_event.is_set():
                self._snapshot.last_tick_at = datetime.now(timezone.utc)
                await asyncio.sleep(interval_sec)
        except asyncio.CancelledError:
            self._logger.info(
                "collector_loop_cancelled",
                extra={
                    "service": "collector-telegram",
                    "event": "collector_loop_cancelled",
                    "loop_name": loop_name,
                },
            )
            raise
```

---

## 2-6. `src/services/collector_telegram/service.py`

```python
from __future__ import annotations

import logging

from .config import CollectorTelegramConfig
from .runtime import CollectorRuntime


class CollectorTelegramService:
    def __init__(
        self,
        config: CollectorTelegramConfig,
        runtime: CollectorRuntime,
        *,
        logger: logging.Logger | None = None,
    ) -> None:
        self._config = config
        self._runtime = runtime
        self._logger = logger or logging.getLogger(__name__)
        self._started = False

    async def start(self) -> None:
        if self._started:
            return

        self._config.validate()
        self._config.ensure_runtime_dirs()

        self._logger.info(
            "collector_service_starting",
            extra={
                "service": "collector-telegram",
                "event": "collector_service_starting",
                "collector_mode": self._config.collector_mode,
                "app_env": self._config.app_env,
            },
        )
        self._started = True

    async def run(self) -> None:
        if not self._started:
            await self.start()

        await self._runtime.run_forever()

    async def stop(self) -> None:
        if not self._started:
            return

        self._logger.info(
            "collector_service_stopping",
            extra={
                "service": "collector-telegram",
                "event": "collector_service_stopping",
            },
        )
        await self._runtime.shutdown()
        self._started = False
```

---

## 2-7. `src/services/collector_telegram/main.py`

```python
from __future__ import annotations

import asyncio
import json
import logging
import signal
import sys
from datetime import datetime, timezone

from .config import CollectorTelegramConfig
from .exceptions import ConfigurationError
from .runtime import CollectorRuntime
from .service import CollectorTelegramService


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload = {
            "ts": datetime.now(timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }

        for field_name in (
            "service",
            "event",
            "app_env",
            "collector_mode",
            "loop_name",
            "interval_sec",
        ):
            if hasattr(record, field_name):
                payload[field_name] = getattr(record, field_name)

        if record.exc_info:
            payload["exc_info"] = self.formatException(record.exc_info)

        return json.dumps(payload, ensure_ascii=False)


def build_logger(log_level: str) -> logging.Logger:
    logger = logging.getLogger("collector_telegram")
    logger.setLevel(getattr(logging, log_level.upper(), logging.INFO))
    logger.handlers.clear()

    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(JsonFormatter())
    logger.addHandler(handler)
    logger.propagate = False
    return logger


async def _run() -> int:
    try:
        config = CollectorTelegramConfig.from_env()
    except ConfigurationError as exc:
        print(
            json.dumps(
                {
                    "ts": datetime.now(timezone.utc).isoformat(),
                    "level": "ERROR",
                    "service": "collector-telegram",
                    "event": "collector_config_invalid",
                    "error": str(exc),
                },
                ensure_ascii=False,
            ),
            file=sys.stderr,
        )
        return 2

    logger = build_logger(config.log_level)
    runtime = CollectorRuntime(config, logger=logger.getChild("runtime"))
    service = CollectorTelegramService(config, runtime, logger=logger)

    loop = asyncio.get_running_loop()

    def _schedule_stop() -> None:
        loop.create_task(service.stop())

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _schedule_stop)
        except NotImplementedError:
            signal.signal(sig, lambda *_: _schedule_stop())

    try:
        await service.run()
    except asyncio.CancelledError:
        logger.info(
            "collector_main_cancelled",
            extra={"service": "collector-telegram", "event": "collector_main_cancelled"},
        )
        return 0
    except Exception:
        logger.exception(
            "collector_main_failed",
            extra={"service": "collector-telegram", "event": "collector_main_failed"},
        )
        return 1

    return 0


def main() -> None:
    raise SystemExit(asyncio.run(_run()))


if __name__ == "__main__":
    main()
```

---

## 3. 이 묶음에서 의도적으로 넣지 않은 것

이번 초안에서 일부러 제외한 것은 아래다.

1. `tdlib_client.py`
2. `auth_fsm.py`
3. `repositories.py`
4. `outbox.py`
5. `idempotency.py`
6. `message_projection.py`
7. `update_dispatcher.py`
8. `update_handlers.py`
9. `reconcile.py`
10. `registry_sync.py`
11. `health.py`

이유는 단순하다.

- 지금 묶음의 목표는 **실제 네트워크/DB 없이도 기동 가능한 최소 collector 프로세스**를 먼저 세우는 것
- 이후 파일들이 기대할 lifecycle/config/logging contract를 먼저 고정하는 것
- 범위를 넓혀서 답변이 깨지거나, skeleton이 애매해지는 것을 막는 것

즉, 이번 묶음은 의도적으로 **빈 뼈대**다.

---

## 4. 바로 다음 구현 묶음

다음 코드 턴은 아래 2개 파일이 맞다.

### 다음 우선 구현
- `tdlib_client.py`
- `auth_fsm.py`

그다음 순서:
- `repositories.py`
- `outbox.py`
- `idempotency.py`

이 순서를 바꾸면 안 된다.

이유:
- collector는 3단계 문서상 **원문/버전/outbox 저장까지만** 책임진다.
- 12단계 migration과 17/18단계 문서가 그 경계를 이미 고정했다.
- 지금은 collector 내부 책임을 좁게 유지한 채 실제 연결점을 하나씩 여는 것이 맞다.

---

## 5. 권장 commit 메시지

### 이번 묶음 기준
```text
feat(collector): add bootstrap runtime skeleton for collector-telegram
```

### push 전 점검
- import cycle 없음
- `CollectorTelegramConfig.from_env()` 검증 정상
- prod/dev 모드 제약 정상
- SIGTERM/SIGINT graceful shutdown 정상
- JSON logging 출력 정상

---

## 6. 최종 정리

이번 코드 초안의 의미는 아래 한 줄이다.

> **`collector-telegram`을 바로 실제 코드로 내리기 시작하되, 첫 묶음은 네트워크/DB 의존성 없이 기동 가능한 최소 부팅 골격으로 제한한다.**

즉, 지금은 “잘 돌아가는 빈 서비스”를 먼저 만들고,
그 다음에 TDLib → Repository/Outbox → Update/Reconcile 순으로 좁게 연결하는 것이 맞다.


---

## Source file: `20_collector_tdlib_auth_code_draft_v0_1.md`

# 20단계: `collector-telegram` TDLib/Auth 실제 코드 초안 v0.1

## 0. 문서 목적

이 문서는 `19_collector_telegram_bootstrap_code_draft_v0_1.md` 다음 단계로,
`collector-telegram`의 **C2 구현 묶음**인 아래 두 파일의 실제 코드 초안을 제공한다.

- `tdlib_client.py`
- `auth_fsm.py`

이번 묶음의 목표는 다음 두 가지다.

1. **TDLib 저수준 래퍼 경계**를 코드로 고정
2. **authorization 상태 기계**를 코드로 고정

이 문서는 아직 아래를 구현하지 않는다.

- 실제 Python TDLib 바인딩 최종 선택
- DB persistence
- outbox 적재
- update dispatcher/handler
- reconcile

즉, 이번 단계의 목적은 **collector가 TDLib 인증 경계를 명시적으로 가질 수 있게 만드는 것**이다.

---

## 1. 대상 파일 트리

```text
src/services/collector_telegram/
  tdlib_client.py
  auth_fsm.py
```

---

## 2. 코드 초안

## 2-1. `src/services/collector_telegram/tdlib_client.py`

```python
from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any, Protocol

from .config import CollectorTelegramConfig
from .exceptions import TDLibTransportError


JsonDict = dict[str, Any]


class TDLibTransportProtocol(Protocol):
    """Minimal async transport contract for a concrete TDLib binding.

    A later implementation can satisfy this protocol with:
    - a ctypes/cffi wrapper,
    - a subprocess bridge,
    - a dedicated python tdlib adapter,
    without changing collector domain code.
    """

    async def initialize(self) -> None: ...

    async def send(self, request: JsonDict) -> None: ...

    async def receive(self, timeout: float) -> JsonDict | None: ...

    async def close(self) -> None: ...


@dataclass(slots=True, frozen=True)
class TDLibRequest:
    payload: JsonDict


class TDLibClient:
    """Low-level TDLib wrapper.

    This class intentionally contains no collector domain logic.
    It only:
    - builds well-formed TDLib requests,
    - sends/receives payloads through an injected transport,
    - tracks authorization-state visibility,
    - exposes a narrow, testable boundary.
    """

    def __init__(
        self,
        config: CollectorTelegramConfig,
        *,
        transport: TDLibTransportProtocol,
        logger: logging.Logger | None = None,
    ) -> None:
        self._config = config
        self._transport = transport
        self._logger = logger or logging.getLogger(__name__)
        self._initialized = False
        self._closed = False
        self._last_authorization_state: JsonDict | None = None

    async def initialize(self) -> None:
        if self._initialized:
            return
        try:
            await self._transport.initialize()
        except Exception as exc:  # pragma: no cover - concrete transport failure path
            raise TDLibTransportError("Failed to initialize TDLib transport") from exc
        self._initialized = True
        self._closed = False

    async def send(self, request: JsonDict) -> None:
        self._ensure_open()
        try:
            await self._transport.send(request)
        except Exception as exc:  # pragma: no cover - concrete transport failure path
            raise TDLibTransportError("Failed to send TDLib request") from exc

    async def receive(self, timeout: float) -> JsonDict | None:
        self._ensure_open()
        try:
            payload = await self._transport.receive(timeout)
        except Exception as exc:  # pragma: no cover - concrete transport failure path
            raise TDLibTransportError("Failed to receive TDLib payload") from exc

        if isinstance(payload, dict) and payload.get("@type") == "updateAuthorizationState":
            auth_state = payload.get("authorization_state")
            if isinstance(auth_state, dict):
                self._last_authorization_state = auth_state

        return payload

    async def close(self) -> None:
        if self._closed:
            return
        try:
            await self._transport.close()
        except Exception as exc:  # pragma: no cover - concrete transport failure path
            raise TDLibTransportError("Failed to close TDLib transport") from exc
        finally:
            self._closed = True
            self._initialized = False

    def is_ready(self) -> bool:
        return self.current_authorization_state_type() == "authorizationStateReady"

    def current_authorization_state(self) -> JsonDict | None:
        return self._last_authorization_state

    def current_authorization_state_type(self) -> str | None:
        state = self._last_authorization_state
        if not isinstance(state, dict):
            return None
        raw = state.get("@type")
        return raw if isinstance(raw, str) else None

    def _ensure_open(self) -> None:
        if not self._initialized:
            raise TDLibTransportError("TDLib client is not initialized")
        if self._closed:
            raise TDLibTransportError("TDLib client is closed")

    # ------------------------------------------------------------------
    # Request builders
    # ------------------------------------------------------------------
    def build_set_tdlib_parameters_request(self) -> TDLibRequest:
        return TDLibRequest(
            {
                "@type": "setTdlibParameters",
                "parameters": {
                    "use_test_dc": False,
                    "database_directory": self._config.tdlib_state_dir,
                    "files_directory": self._config.tdlib_files_dir,
                    "use_file_database": True,
                    "use_chat_info_database": True,
                    "use_message_database": True,
                    "use_secret_chats": False,
                    "api_id": self._config.telegram_api_id,
                    "api_hash": self._config.telegram_api_hash,
                    "system_language_code": "en",
                    "device_model": "catchbot-vps",
                    "system_version": "linux",
                    "application_version": "0.1.0",
                    "database_encryption_key": self._config.tdlib_db_encryption_key,
                },
            }
        )

    def build_check_database_encryption_key_request(self) -> TDLibRequest:
        return TDLibRequest(
            {
                "@type": "checkDatabaseEncryptionKey",
                "encryption_key": self._config.tdlib_db_encryption_key,
            }
        )

    def build_set_authentication_phone_number_request(self) -> TDLibRequest:
        return TDLibRequest(
            {
                "@type": "setAuthenticationPhoneNumber",
                "phone_number": self._config.telegram_phone_number,
                "settings": {
                    "allow_flash_call": False,
                    "allow_missed_call": False,
                    "is_current_phone_number": False,
                    "allow_sms_retriever_api": False,
                },
            }
        )

    def build_check_authentication_code_request(self, code: str) -> TDLibRequest:
        return TDLibRequest(
            {
                "@type": "checkAuthenticationCode",
                "code": code,
            }
        )

    def build_check_authentication_password_request(self, password: str) -> TDLibRequest:
        return TDLibRequest(
            {
                "@type": "checkAuthenticationPassword",
                "password": password,
            }
        )

    def build_search_public_chat_request(self, username: str) -> TDLibRequest:
        normalized = username.removeprefix("@").strip()
        return TDLibRequest(
            {
                "@type": "searchPublicChat",
                "username": normalized,
            }
        )

    def build_join_chat_request(self, chat_id: int) -> TDLibRequest:
        return TDLibRequest(
            {
                "@type": "joinChat",
                "chat_id": chat_id,
            }
        )

    def build_join_chat_by_invite_link_request(self, invite_link: str) -> TDLibRequest:
        return TDLibRequest(
            {
                "@type": "joinChatByInviteLink",
                "invite_link": invite_link,
            }
        )

    def build_get_chat_history_request(
        self,
        *,
        chat_id: int,
        from_message_id: int = 0,
        offset: int = 0,
        limit: int = 50,
        only_local: bool = False,
    ) -> TDLibRequest:
        if limit <= 0 or limit > 100:
            raise TDLibTransportError(f"getChatHistory limit must be between 1 and 100: {limit}")
        return TDLibRequest(
            {
                "@type": "getChatHistory",
                "chat_id": chat_id,
                "from_message_id": from_message_id,
                "offset": offset,
                "limit": limit,
                "only_local": only_local,
            }
        )

    def build_get_message_link_request(
        self,
        *,
        chat_id: int,
        message_id: int,
        for_album: bool = False,
        media_timestamp: int = 0,
    ) -> TDLibRequest:
        return TDLibRequest(
            {
                "@type": "getMessageLink",
                "chat_id": chat_id,
                "message_id": message_id,
                "media_timestamp": media_timestamp,
                "for_album": for_album,
                "for_comment": False,
            }
        )
```

---

## 2-2. `src/services/collector_telegram/auth_fsm.py`

```python
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any, Literal

from .config import CollectorTelegramConfig
from .exceptions import AuthorizationError, AuthorizationManualInterventionRequired


JsonDict = dict[str, Any]

AuthorizationState = Literal[
    "booting",
    "waiting_tdlib_parameters",
    "waiting_encryption_key",
    "waiting_phone_number",
    "waiting_code",
    "waiting_password",
    "ready",
    "degraded",
    "closed",
]


@dataclass(slots=True, frozen=True)
class AuthTransitionResult:
    new_state: AuthorizationState
    requests: list[JsonDict] = field(default_factory=list)
    requires_manual_intervention: bool = False
    note: str | None = None


class AuthorizationFSM:
    """Collector authorization state machine.

    Design rules carried from the stage docs:
    - first-time login may require manual operator action,
    - runtime regression back to code/password/phone states is degraded,
    - no automatic human-auth bypass is attempted.
    """

    def __init__(
        self,
        config: CollectorTelegramConfig,
        *,
        logger: logging.Logger | None = None,
    ) -> None:
        self._config = config
        self._logger = logger or logging.getLogger(__name__)
        self._state: AuthorizationState = "booting"
        self._has_been_ready = False
        self._requires_manual_intervention = False

    def current_state(self) -> AuthorizationState:
        return self._state

    def is_ready(self) -> bool:
        return self._state == "ready"

    def requires_manual_intervention(self) -> bool:
        return self._requires_manual_intervention

    def handle_state(self, state: JsonDict) -> AuthTransitionResult:
        state_type = state.get("@type")
        if not isinstance(state_type, str):
            raise AuthorizationError("authorization state payload is missing @type")

        if self._has_been_ready and state_type != "authorizationStateReady":
            return self._degraded_regression(state_type)

        match state_type:
            case "authorizationStateWaitTdlibParameters":
                return self._transition(
                    "waiting_tdlib_parameters",
                    requests=[self._build_set_tdlib_parameters_request()],
                )
            case "authorizationStateWaitEncryptionKey":
                return self._transition(
                    "waiting_encryption_key",
                    requests=[self._build_check_database_encryption_key_request()],
                )
            case "authorizationStateWaitPhoneNumber":
                return self._transition(
                    "waiting_phone_number",
                    requests=[self._build_set_authentication_phone_number_request()],
                )
            case "authorizationStateWaitCode":
                return self._manual_transition(
                    "waiting_code",
                    note="Telegram login code required from operator",
                )
            case "authorizationStateWaitOtherDeviceConfirmation":
                return self._manual_transition(
                    "waiting_code",
                    note="Other-device confirmation required from operator",
                )
            case "authorizationStateWaitPassword":
                if self._config.telegram_2fa_password:
                    return self._transition(
                        "waiting_password",
                        requests=[
                            self._build_check_authentication_password_request(
                                self._config.telegram_2fa_password
                            )
                        ],
                    )
                return self._manual_transition(
                    "waiting_password",
                    note="Telegram 2FA password required but not configured",
                )
            case "authorizationStateReady":
                self._has_been_ready = True
                self._requires_manual_intervention = False
                return self._transition("ready", note="TDLib authorization ready")
            case "authorizationStateLoggingOut" | "authorizationStateClosing":
                self._state = "degraded"
                return AuthTransitionResult(
                    new_state="degraded",
                    requests=[],
                    requires_manual_intervention=False,
                    note=f"TDLib authorization is leaving ready state: {state_type}",
                )
            case "authorizationStateClosed":
                self._state = "closed"
                return AuthTransitionResult(
                    new_state="closed",
                    requests=[],
                    requires_manual_intervention=False,
                    note="TDLib authorization closed",
                )
            case _:
                raise AuthorizationError(f"Unsupported authorization state: {state_type}")

    def assert_ready(self) -> None:
        if not self.is_ready():
            raise AuthorizationError(f"TDLib authorization is not ready: {self._state}")

    def raise_if_manual_intervention_required(self) -> None:
        if self._requires_manual_intervention:
            raise AuthorizationManualInterventionRequired(
                f"Manual authorization intervention required: current_state={self._state}"
            )

    def _transition(
        self,
        new_state: AuthorizationState,
        *,
        requests: list[JsonDict] | None = None,
        note: str | None = None,
    ) -> AuthTransitionResult:
        self._state = new_state
        return AuthTransitionResult(
            new_state=new_state,
            requests=requests or [],
            requires_manual_intervention=False,
            note=note,
        )

    def _manual_transition(self, new_state: AuthorizationState, *, note: str) -> AuthTransitionResult:
        self._state = new_state
        self._requires_manual_intervention = True
        return AuthTransitionResult(
            new_state=new_state,
            requests=[],
            requires_manual_intervention=True,
            note=note,
        )

    def _degraded_regression(self, tdlib_state_type: str) -> AuthTransitionResult:
        self._state = "degraded"
        self._requires_manual_intervention = True
        note = f"Authorization regressed after ready: {tdlib_state_type}"
        self._logger.warning(note)
        return AuthTransitionResult(
            new_state="degraded",
            requests=[],
            requires_manual_intervention=True,
            note=note,
        )

    def _build_set_tdlib_parameters_request(self) -> JsonDict:
        return {
            "@type": "setTdlibParameters",
            "parameters": {
                "use_test_dc": False,
                "database_directory": self._config.tdlib_state_dir,
                "files_directory": self._config.tdlib_files_dir,
                "use_file_database": True,
                "use_chat_info_database": True,
                "use_message_database": True,
                "use_secret_chats": False,
                "api_id": self._config.telegram_api_id,
                "api_hash": self._config.telegram_api_hash,
                "system_language_code": "en",
                "device_model": "catchbot-vps",
                "system_version": "linux",
                "application_version": "0.1.0",
                "database_encryption_key": self._config.tdlib_db_encryption_key,
            },
        }

    def _build_check_database_encryption_key_request(self) -> JsonDict:
        return {
            "@type": "checkDatabaseEncryptionKey",
            "encryption_key": self._config.tdlib_db_encryption_key,
        }

    def _build_set_authentication_phone_number_request(self) -> JsonDict:
        return {
            "@type": "setAuthenticationPhoneNumber",
            "phone_number": self._config.telegram_phone_number,
            "settings": {
                "allow_flash_call": False,
                "allow_missed_call": False,
                "is_current_phone_number": False,
                "allow_sms_retriever_api": False,
            },
        }

    def _build_check_authentication_password_request(self, password: str) -> JsonDict:
        return {
            "@type": "checkAuthenticationPassword",
            "password": password,
        }
```

---

## 3. 구현 메모

### 3-1. 왜 transport protocol을 먼저 두는가

이번 단계에서는 **실제 Python TDLib 바인딩을 아직 고정하지 않는다.**
18단계 문서도 이번 턴의 목표를 “TDLib wrapper 경계 고정”으로 뒀고, concrete binding 선택은 후순위로 남겨뒀다.
그래서 `TDLibTransportProtocol`을 먼저 두고, collector 상위 계층은 이 프로토콜만 알게 하는 편이 맞다.

### 3-2. 왜 `AuthorizationFSM`이 manual intervention을 노출하는가

3단계 collector 문서가 이미 다음을 잠갔다.

- 초기 로그인은 수동 가능
- 운영 중 다시 code/password state로 내려가면 **degraded** 처리
- 자동으로 사람 인증 절차를 우회하지 않음

그래서 이 FSM은 `authorizationStateWaitCode`, `authorizationStateWaitOtherDeviceConfirmation`,
런타임 회귀 상태를 모두 **manual intervention required**로 올린다.

### 3-3. 왜 request builder가 일부 중복되는가

장기적으로는 auth 전용 request builder를 분리해도 되지만,
지금 단계에서는 **auth 흐름을 collector 내부에서 독립적으로 테스트 가능하게 만드는 것**이 우선이다.
따라서 `auth_fsm.py` 안에 auth 전용 request 생성기를 두는 것이 더 안전하다.

---

## 4. 바로 다음 단계

다음 구현 순서는 그대로다.

1. `repositories.py`
2. `outbox.py`
3. `idempotency.py`

그 다음:
- `message_projection.py`
- `update_dispatcher.py`
- `update_handlers.py`

즉, 다음 턴은 **DB transaction/outbox/idempotency 골격**으로 가는 것이 맞다.


---

## Source file: `21_collector_repository_outbox_idempotency_code_draft_v0_1.md`

# 21단계: `collector-telegram` Repository / Outbox / Idempotency 실제 코드 초안 v0.1

## 0. 문서 목적

이 문서는 프로젝트 소스의 README/단계 문서, `11_stage11_execution_contracts_v0_1.md`, `12_migration_spec_0001_0004_v0_1.md`, 그리고 `17~20` 단계 collector 구현 문서를 바탕으로,
`collector-telegram`의 **C3 구현 묶음**인 아래 세 파일의 실제 코드 초안을 제공한다.

- `repositories.py`
- `outbox.py`
- `idempotency.py`

이번 묶음의 목표는 다음 세 가지다.

1. `0001_ingest_core` 테이블에 대한 **collector 전용 persistence 경계**를 코드로 고정
2. `source_message.created/edited/deleted/reconciled.v1` **outbox draft 생성 규칙**을 코드로 고정
3. **same-hash no-op / semantic event dedupe** 규칙을 코드로 고정

이 문서는 아직 아래를 구현하지 않는다.

- 실제 SQLAlchemy engine/session factory 조립
- `message_projection.py`
- `update_dispatcher.py`
- `update_handlers.py`
- `reconcile.py`
- `registry_sync.py`
- `health.py`

즉, 이번 단계의 목적은 **collector가 raw/current/version/outbox를 안전하게 묶어 쓸 수 있는 DB 골격**을 먼저 확보하는 것이다.

---

## 1. 대상 파일 트리

```text
src/services/collector_telegram/
  repositories.py
  outbox.py
  idempotency.py
```

---

## 2. 코드 초안

## 2-1. `src/services/collector_telegram/idempotency.py`

```python
from __future__ import annotations

from dataclasses import dataclass


_EVENT_PREFIX_MAP = {
    "source_message.created.v1": "srcmsg:create",
    "source_message.edited.v1": "srcmsg:edit",
    "source_message.deleted.v1": "srcmsg:delete",
    "source_message.reconciled.v1": "srcmsg:reconcile",
}


@dataclass(slots=True, frozen=True)
class IdempotencyPolicy:
    """Collector idempotency rules.

    The collector is designed under at-least-once delivery assumptions.
    Therefore, repeated live updates and repeated reconcile reads are normal.
    """

    def should_append_new_version(self, previous_hash: str | None, next_hash: str) -> bool:
        if not next_hash:
            raise ValueError("next_hash must not be empty")
        return previous_hash != next_hash

    def semantic_event_dedupe_key(
        self,
        event_type: str,
        source_message_id: str,
        version_no: int,
        extra: str | None = None,
    ) -> str:
        if not source_message_id:
            raise ValueError("source_message_id must not be empty")
        if version_no <= 0:
            raise ValueError("version_no must be > 0")

        prefix = _EVENT_PREFIX_MAP.get(event_type, event_type.replace(".", ":"))
        suffix = f":{extra}" if extra else ""
        return f"{prefix}:{source_message_id}:{version_no}{suffix}"
```

---

## 2-2. `src/services/collector_telegram/outbox.py`

```python
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from .idempotency import IdempotencyPolicy
from .models import OutboxEventDraft


JsonDict = dict[str, Any]
_SOURCE_MESSAGE_AGGREGATE = "source_message"


@dataclass(slots=True)
class CollectorOutboxBuilder:
    policy: IdempotencyPolicy

    def build_created(
        self,
        *,
        source_message_id: str,
        current_version_no: int,
        logical_post_key: str,
        occurred_at: datetime,
    ) -> OutboxEventDraft:
        event_type = "source_message.created.v1"
        return OutboxEventDraft(
            event_type=event_type,
            aggregate_type=_SOURCE_MESSAGE_AGGREGATE,
            aggregate_id=source_message_id,
            dedupe_key=self.policy.semantic_event_dedupe_key(
                event_type,
                source_message_id,
                current_version_no,
            ),
            payload_json=self._base_payload(
                source_message_id=source_message_id,
                current_version_no=current_version_no,
                logical_post_key=logical_post_key,
                occurred_at=occurred_at,
            ),
        )

    def build_edited(
        self,
        *,
        source_message_id: str,
        current_version_no: int,
        logical_post_key: str,
        occurred_at: datetime,
    ) -> OutboxEventDraft:
        event_type = "source_message.edited.v1"
        return OutboxEventDraft(
            event_type=event_type,
            aggregate_type=_SOURCE_MESSAGE_AGGREGATE,
            aggregate_id=source_message_id,
            dedupe_key=self.policy.semantic_event_dedupe_key(
                event_type,
                source_message_id,
                current_version_no,
            ),
            payload_json=self._base_payload(
                source_message_id=source_message_id,
                current_version_no=current_version_no,
                logical_post_key=logical_post_key,
                occurred_at=occurred_at,
            ),
        )

    def build_deleted(
        self,
        *,
        source_message_id: str,
        current_version_no: int,
        logical_post_key: str,
        occurred_at: datetime,
        delete_kind: str,
    ) -> OutboxEventDraft:
        event_type = "source_message.deleted.v1"
        payload = self._base_payload(
            source_message_id=source_message_id,
            current_version_no=current_version_no,
            logical_post_key=logical_post_key,
            occurred_at=occurred_at,
        )
        payload["delete_kind"] = delete_kind
        return OutboxEventDraft(
            event_type=event_type,
            aggregate_type=_SOURCE_MESSAGE_AGGREGATE,
            aggregate_id=source_message_id,
            dedupe_key=self.policy.semantic_event_dedupe_key(
                event_type,
                source_message_id,
                current_version_no,
            ),
            payload_json=payload,
        )

    def build_reconciled(
        self,
        *,
        source_message_id: str,
        current_version_no: int,
        logical_post_key: str,
        occurred_at: datetime,
        reconcile_reason: str,
    ) -> OutboxEventDraft:
        event_type = "source_message.reconciled.v1"
        payload = self._base_payload(
            source_message_id=source_message_id,
            current_version_no=current_version_no,
            logical_post_key=logical_post_key,
            occurred_at=occurred_at,
        )
        payload["reconcile_reason"] = reconcile_reason
        return OutboxEventDraft(
            event_type=event_type,
            aggregate_type=_SOURCE_MESSAGE_AGGREGATE,
            aggregate_id=source_message_id,
            dedupe_key=self.policy.semantic_event_dedupe_key(
                event_type,
                source_message_id,
                current_version_no,
                extra=reconcile_reason,
            ),
            payload_json=payload,
        )

    def _base_payload(
        self,
        *,
        source_message_id: str,
        current_version_no: int,
        logical_post_key: str,
        occurred_at: datetime,
    ) -> JsonDict:
        return {
            "source_message_id": source_message_id,
            "current_version_no": current_version_no,
            "logical_post_key": logical_post_key,
            "occurred_at": self._isoformat(occurred_at),
        }

    @staticmethod
    def _isoformat(value: datetime) -> str:
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc).isoformat()
```

---

## 2-3. `src/services/collector_telegram/repositories.py`

```python
from __future__ import annotations

import json
import logging
from contextlib import asynccontextmanager
from dataclasses import asdict, is_dataclass
from datetime import datetime, timezone
from typing import Any, AsyncIterator, Mapping

import sqlalchemy as sa
from sqlalchemy.engine import RowMapping
from sqlalchemy.ext.asyncio import AsyncSession

from .exceptions import RepositoryInvariantError
from .idempotency import IdempotencyPolicy
from .models import OutboxEventDraft, SourceMessageProjection, TrackedChat


JsonDict = dict[str, Any]


def _json_default(value: Any) -> str:
    if isinstance(value, datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc).isoformat()
    raise TypeError(f"Object of type {type(value)!r} is not JSON serializable")


def _jsonb_dumps(value: Any) -> str | None:
    if value is None:
        return None
    if is_dataclass(value):
        value = asdict(value)
    return json.dumps(value, ensure_ascii=False, default=_json_default)


class CollectorRepository:
    """Collector persistence adapter.

    This repository intentionally targets only `0001_ingest_core` tables:
    - telegram_channel_registry
    - telegram_raw_updates
    - source_messages
    - source_message_versions
    - event_outbox

    Atomicity rule:
    current row update + optional version append + outbox insert must run inside
    a single database transaction managed by the caller.
    """

    def __init__(
        self,
        session: AsyncSession,
        *,
        logger: logging.Logger | None = None,
        idempotency_policy: IdempotencyPolicy | None = None,
    ) -> None:
        self._session = session
        self._logger = logger or logging.getLogger(__name__)
        self._idempotency_policy = idempotency_policy or IdempotencyPolicy()

    @asynccontextmanager
    async def transaction(self) -> AsyncIterator[AsyncSession]:
        if self._session.in_transaction():
            yield self._session
            return

        async with self._session.begin():
            yield self._session

    async def insert_raw_update(
        self,
        *,
        update_type: str,
        payload_json: JsonDict,
        chat_id: int | None = None,
        message_id: int | None = None,
    ) -> int:
        result = await self._session.execute(
            sa.text(
                """
                INSERT INTO telegram_raw_updates (
                    update_type,
                    chat_id,
                    message_id,
                    payload_json,
                    apply_status
                )
                VALUES (
                    :update_type,
                    :chat_id,
                    :message_id,
                    CAST(:payload_json AS jsonb),
                    'pending'
                )
                RETURNING update_seq
                """
            ),
            {
                "update_type": update_type,
                "chat_id": chat_id,
                "message_id": message_id,
                "payload_json": _jsonb_dumps(payload_json),
            },
        )
        update_seq = result.scalar_one()
        return int(update_seq)

    async def mark_raw_update_applied(self, update_seq: int) -> None:
        await self._session.execute(
            sa.text(
                """
                UPDATE telegram_raw_updates
                SET
                    apply_status = 'applied',
                    applied_at = now(),
                    error_text = NULL
                WHERE update_seq = :update_seq
                """
            ),
            {"update_seq": update_seq},
        )

    async def mark_raw_update_failed(self, update_seq: int, error_text: str) -> None:
        await self._session.execute(
            sa.text(
                """
                UPDATE telegram_raw_updates
                SET
                    apply_status = 'failed',
                    error_text = :error_text
                WHERE update_seq = :update_seq
                """
            ),
            {
                "update_seq": update_seq,
                "error_text": error_text,
            },
        )

    async def get_source_message(
        self,
        *,
        platform: str,
        chat_id: int,
        message_id: int,
    ) -> Mapping[str, Any] | None:
        result = await self._session.execute(
            sa.text(
                """
                SELECT *
                FROM source_messages
                WHERE platform = :platform
                  AND chat_id = :chat_id
                  AND message_id = :message_id
                """
            ),
            {
                "platform": platform,
                "chat_id": chat_id,
                "message_id": message_id,
            },
        )
        return result.mappings().first()

    async def get_latest_version(self, source_message_id: str) -> Mapping[str, Any] | None:
        result = await self._session.execute(
            sa.text(
                """
                SELECT *
                FROM source_message_versions
                WHERE source_message_id = CAST(:source_message_id AS uuid)
                ORDER BY version_no DESC
                LIMIT 1
                """
            ),
            {"source_message_id": source_message_id},
        )
        return result.mappings().first()

    async def upsert_source_message(
        self,
        projection: SourceMessageProjection,
        *,
        platform: str = "telegram",
    ) -> Mapping[str, Any]:
        result = await self._session.execute(
            sa.text(
                """
                INSERT INTO source_messages (
                    platform,
                    chat_id,
                    message_id,
                    logical_post_key,
                    is_channel_post,
                    posted_at,
                    edited_at,
                    deleted_at,
                    delete_kind,
                    message_link,
                    author_signature,
                    forward_info_json,
                    content_type,
                    text_body,
                    caption_text,
                    text_surface,
                    entities_json,
                    url_surface_json,
                    raw_message_json,
                    first_seen_at,
                    last_seen_at
                )
                VALUES (
                    :platform,
                    :chat_id,
                    :message_id,
                    :logical_post_key,
                    :is_channel_post,
                    :posted_at,
                    :edited_at,
                    NULL,
                    'none',
                    :message_link,
                    :author_signature,
                    CAST(:forward_info_json AS jsonb),
                    :content_type,
                    :text_body,
                    :caption_text,
                    :text_surface,
                    CAST(:entities_json AS jsonb),
                    CAST(:url_surface_json AS jsonb),
                    CAST(:raw_message_json AS jsonb),
                    now(),
                    now()
                )
                ON CONFLICT (platform, chat_id, message_id)
                DO UPDATE SET
                    logical_post_key = EXCLUDED.logical_post_key,
                    is_channel_post = EXCLUDED.is_channel_post,
                    posted_at = LEAST(source_messages.posted_at, EXCLUDED.posted_at),
                    edited_at = CASE
                        WHEN EXCLUDED.edited_at IS NULL THEN source_messages.edited_at
                        WHEN source_messages.edited_at IS NULL THEN EXCLUDED.edited_at
                        ELSE GREATEST(source_messages.edited_at, EXCLUDED.edited_at)
                    END,
                    deleted_at = NULL,
                    delete_kind = 'none',
                    message_link = EXCLUDED.message_link,
                    author_signature = EXCLUDED.author_signature,
                    forward_info_json = EXCLUDED.forward_info_json,
                    content_type = EXCLUDED.content_type,
                    text_body = EXCLUDED.text_body,
                    caption_text = EXCLUDED.caption_text,
                    text_surface = EXCLUDED.text_surface,
                    entities_json = EXCLUDED.entities_json,
                    url_surface_json = EXCLUDED.url_surface_json,
                    raw_message_json = EXCLUDED.raw_message_json,
                    last_seen_at = now()
                RETURNING *
                """
            ),
            {
                "platform": platform,
                "chat_id": projection.chat_id,
                "message_id": projection.message_id,
                "logical_post_key": projection.logical_post_key,
                "is_channel_post": projection.is_channel_post,
                "posted_at": projection.posted_at,
                "edited_at": projection.edited_at,
                "message_link": projection.message_link,
                "author_signature": projection.author_signature,
                "forward_info_json": _jsonb_dumps(projection.forward_info_json),
                "content_type": projection.content_type,
                "text_body": projection.text_body,
                "caption_text": projection.caption_text,
                "text_surface": projection.text_surface,
                "entities_json": _jsonb_dumps(projection.entities_json),
                "url_surface_json": _jsonb_dumps(projection.url_surface_json),
                "raw_message_json": _jsonb_dumps(projection.raw_message_json),
            },
        )
        row = result.mappings().one()
        return row

    async def append_source_message_version(
        self,
        *,
        source_message_id: str,
        projection: SourceMessageProjection,
        version_reason: str,
        observed_at: datetime | None = None,
        telegram_edit_date: datetime | None = None,
    ) -> Mapping[str, Any]:
        latest = await self.get_latest_version(source_message_id)
        next_version_no = 1 if latest is None else int(latest["version_no"]) + 1

        result = await self._session.execute(
            sa.text(
                """
                INSERT INTO source_message_versions (
                    source_message_id,
                    version_no,
                    version_reason,
                    observed_at,
                    telegram_edit_date,
                    text_surface,
                    entities_json,
                    raw_message_json,
                    content_hash
                )
                VALUES (
                    CAST(:source_message_id AS uuid),
                    :version_no,
                    :version_reason,
                    :observed_at,
                    :telegram_edit_date,
                    :text_surface,
                    CAST(:entities_json AS jsonb),
                    CAST(:raw_message_json AS jsonb),
                    :content_hash
                )
                RETURNING *
                """
            ),
            {
                "source_message_id": source_message_id,
                "version_no": next_version_no,
                "version_reason": version_reason,
                "observed_at": observed_at or datetime.now(timezone.utc),
                "telegram_edit_date": telegram_edit_date,
                "text_surface": projection.text_surface,
                "entities_json": _jsonb_dumps(projection.entities_json),
                "raw_message_json": _jsonb_dumps(projection.raw_message_json),
                "content_hash": projection.content_hash,
            },
        )
        version_row = result.mappings().one()

        updated_current = await self._session.execute(
            sa.text(
                """
                UPDATE source_messages
                SET
                    current_version_no = :current_version_no,
                    edited_at = CASE
                        WHEN :edited_at IS NULL THEN edited_at
                        WHEN edited_at IS NULL THEN :edited_at
                        ELSE GREATEST(edited_at, :edited_at)
                    END,
                    deleted_at = NULL,
                    delete_kind = 'none',
                    message_link = :message_link,
                    author_signature = :author_signature,
                    forward_info_json = CAST(:forward_info_json AS jsonb),
                    content_type = :content_type,
                    text_body = :text_body,
                    caption_text = :caption_text,
                    text_surface = :text_surface,
                    entities_json = CAST(:entities_json AS jsonb),
                    url_surface_json = CAST(:url_surface_json AS jsonb),
                    raw_message_json = CAST(:raw_message_json AS jsonb),
                    last_seen_at = now()
                WHERE source_message_id = CAST(:source_message_id AS uuid)
                RETURNING source_message_id
                """
            ),
            {
                "source_message_id": source_message_id,
                "current_version_no": next_version_no,
                "edited_at": projection.edited_at,
                "message_link": projection.message_link,
                "author_signature": projection.author_signature,
                "forward_info_json": _jsonb_dumps(projection.forward_info_json),
                "content_type": projection.content_type,
                "text_body": projection.text_body,
                "caption_text": projection.caption_text,
                "text_surface": projection.text_surface,
                "entities_json": _jsonb_dumps(projection.entities_json),
                "url_surface_json": _jsonb_dumps(projection.url_surface_json),
                "raw_message_json": _jsonb_dumps(projection.raw_message_json),
            },
        )
        if updated_current.mappings().first() is None:
            raise RepositoryInvariantError(
                f"source_messages row missing while appending version: {source_message_id}"
            )

        return version_row

    async def append_source_message_version_if_changed(
        self,
        *,
        source_message_id: str,
        projection: SourceMessageProjection,
        version_reason: str,
        observed_at: datetime | None = None,
        telegram_edit_date: datetime | None = None,
    ) -> tuple[bool, Mapping[str, Any] | None]:
        latest = await self.get_latest_version(source_message_id)
        previous_hash = None if latest is None else str(latest["content_hash"])

        if not self._idempotency_policy.should_append_new_version(previous_hash, projection.content_hash):
            return False, None

        version_row = await self.append_source_message_version(
            source_message_id=source_message_id,
            projection=projection,
            version_reason=version_reason,
            observed_at=observed_at,
            telegram_edit_date=telegram_edit_date,
        )
        return True, version_row

    async def mark_message_deleted(
        self,
        *,
        platform: str,
        chat_id: int,
        message_id: int,
        delete_kind: str,
        deleted_at: datetime | None = None,
    ) -> Mapping[str, Any] | None:
        result = await self._session.execute(
            sa.text(
                """
                UPDATE source_messages
                SET
                    deleted_at = :deleted_at,
                    delete_kind = :delete_kind,
                    last_seen_at = now()
                WHERE platform = :platform
                  AND chat_id = :chat_id
                  AND message_id = :message_id
                RETURNING *
                """
            ),
            {
                "platform": platform,
                "chat_id": chat_id,
                "message_id": message_id,
                "delete_kind": delete_kind,
                "deleted_at": deleted_at or datetime.now(timezone.utc),
            },
        )
        return result.mappings().first()

    async def insert_outbox_event(self, event: OutboxEventDraft) -> None:
        await self._session.execute(
            sa.text(
                """
                INSERT INTO event_outbox (
                    event_type,
                    aggregate_type,
                    aggregate_id,
                    dedupe_key,
                    payload_json,
                    status
                )
                VALUES (
                    :event_type,
                    :aggregate_type,
                    CAST(:aggregate_id AS uuid),
                    :dedupe_key,
                    CAST(:payload_json AS jsonb),
                    'pending'::outbox_status_enum
                )
                ON CONFLICT (dedupe_key) DO NOTHING
                """
            ),
            {
                "event_type": event.event_type,
                "aggregate_type": event.aggregate_type,
                "aggregate_id": event.aggregate_id,
                "dedupe_key": event.dedupe_key,
                "payload_json": _jsonb_dumps(event.payload_json),
            },
        )

    async def list_active_tracked_chats(self) -> list[TrackedChat]:
        result = await self._session.execute(
            sa.text(
                """
                SELECT
                    registry_id,
                    chat_id,
                    desired_state,
                    access_state,
                    source_kind,
                    source_value,
                    priority_weight,
                    last_seen_message_id,
                    last_seen_message_date
                FROM telegram_channel_registry
                WHERE desired_state = 'active'
                  AND access_state = 'joined'
                  AND chat_id IS NOT NULL
                ORDER BY priority_weight DESC, registry_id ASC
                """
            )
        )
        return [self._tracked_chat_from_row(row) for row in result.mappings().all()]

    async def list_reconcile_targets(self, limit: int) -> list[TrackedChat]:
        result = await self._session.execute(
            sa.text(
                """
                SELECT
                    registry_id,
                    chat_id,
                    desired_state,
                    access_state,
                    source_kind,
                    source_value,
                    priority_weight,
                    last_seen_message_id,
                    last_seen_message_date
                FROM telegram_channel_registry
                WHERE desired_state = 'active'
                  AND access_state = 'joined'
                  AND chat_id IS NOT NULL
                ORDER BY
                    last_history_sync_at NULLS FIRST,
                    last_history_sync_at ASC,
                    priority_weight DESC,
                    registry_id ASC
                LIMIT :limit
                """
            ),
            {"limit": limit},
        )
        return [self._tracked_chat_from_row(row) for row in result.mappings().all()]

    async def update_channel_sync_cursor(
        self,
        *,
        registry_id: str,
        last_seen_message_id: int | None = None,
        last_seen_message_date: datetime | None = None,
        last_history_sync_at: datetime | None = None,
    ) -> None:
        await self._session.execute(
            sa.text(
                """
                UPDATE telegram_channel_registry
                SET
                    last_seen_message_id = COALESCE(:last_seen_message_id, last_seen_message_id),
                    last_seen_message_date = COALESCE(:last_seen_message_date, last_seen_message_date),
                    last_history_sync_at = COALESCE(:last_history_sync_at, last_history_sync_at),
                    updated_at = now()
                WHERE registry_id = CAST(:registry_id AS uuid)
                """
            ),
            {
                "registry_id": registry_id,
                "last_seen_message_id": last_seen_message_id,
                "last_seen_message_date": last_seen_message_date,
                "last_history_sync_at": last_history_sync_at,
            },
        )

    @staticmethod
    def _tracked_chat_from_row(row: RowMapping) -> TrackedChat:
        return TrackedChat(
            registry_id=str(row["registry_id"]),
            chat_id=row["chat_id"],
            desired_state=row["desired_state"],
            access_state=row["access_state"],
            source_kind=row["source_kind"],
            source_value=row["source_value"],
            priority_weight=int(row["priority_weight"]),
            last_seen_message_id=row["last_seen_message_id"],
            last_seen_message_date=row["last_seen_message_date"],
        )
```

---

## 3. 구현 메모

### 3-1. 왜 repository를 SQLAlchemy `AsyncSession` 주입형으로 두는가

이번 단계의 목표는 **DB contract 고정**이지 ORM 모델링 전체를 끝내는 것이 아니다.
따라서 이 초안은 `AsyncSession`을 주입받아 직접 SQL을 실행하는 방식으로 좁게 잡았다.

이 선택의 장점은 다음과 같다.

- `0001_ingest_core` 스키마와 직접 맞닿는다.
- 아직 공통 ORM 모델 계층이 없어도 된다.
- collector가 다른 도메인 계층을 모르도록 경계를 유지할 수 있다.
- `source_messages / source_message_versions / event_outbox` atomic write 규칙을 눈으로 검토하기 쉽다.

### 3-2. 왜 `insert_outbox_event`는 duplicate를 예외가 아니라 noop로 처리하는가

collector는 **at-least-once** 업데이트와 reconcile 중복을 정상 경로로 가정해야 한다.
그래서 outbox는 `dedupe_key` unique를 hard guard로 두고,
repository는 `ON CONFLICT (dedupe_key) DO NOTHING`으로 중복을 흡수한다.

즉, semantic duplicate는 crash 원인이 아니라 **idempotent noop** 여야 한다.

### 3-3. 왜 `upsert_source_message`가 tombstone을 해제하는가

`updateDeleteMessages`의 `cache_only`나 stale delete 이후,
reconcile 또는 실제 subsequent update에서 같은 메시지를 다시 관측할 수 있다.
그 경우 current row가 계속 `deleted_at != NULL` 상태로 남아 있으면 현재 canonical row가 왜곡된다.

그래서 실제 message observation이 들어온 `upsert_source_message`는:

- `deleted_at = NULL`
- `delete_kind = 'none'`

로 current row를 복구한다.

삭제 이력 보존은 history/version/outbox로 별도로 다룬다.

### 3-4. 왜 `append_source_message_version_if_changed`는 current row update를 내부에서 하지 않는가

이 draft는 다음 update handler 순서를 전제로 한다.

1. `upsert_source_message(projection)`
2. `append_source_message_version_if_changed(...)`
3. `insert_outbox_event(...)`

즉, current row 최신화는 먼저 하고,
version append 여부는 hash comparison으로 뒤에서 결정한다.

이렇게 해야:

- metadata-only edit도 current row에는 반영되고
- content hash가 같으면 불필요한 version append는 막을 수 있다.

### 3-5. transaction 경계

caller는 아래처럼 써야 한다.

```python
async with repository.transaction():
    raw_update_seq = await repository.insert_raw_update(...)
    current_row = await repository.upsert_source_message(projection)
    changed, version_row = await repository.append_source_message_version_if_changed(
        source_message_id=str(current_row["source_message_id"]),
        projection=projection,
        version_reason="content_change",
    )
    await repository.insert_outbox_event(event)
    await repository.mark_raw_update_applied(raw_update_seq)
```

핵심은 **current row + version row + outbox row + raw update status**가 같은 transaction 안에 있어야 한다는 점이다.

---

## 4. 바로 다음 단계

다음 구현 묶음은 그대로 아래가 맞다.

- `message_projection.py`
- `update_dispatcher.py`
- `update_handlers.py`

이유는 이번 단계에서:

- repository atomic write contract
- outbox event draft contract
- idempotency contract

이 세 개가 고정됐기 때문이다.

즉, 다음 턴부터는 실제 update 흐름인

- `updateNewMessage`
- `updateMessageEdited`
- `updateMessageContent`
- `updateDeleteMessages`
- `updateChatLastMessage`

를 코드로 내릴 수 있다.


---

## Source file: `22_collector_projection_dispatch_handlers_code_draft_v0_1.md`

# 22단계: `collector-telegram` Projection / Dispatcher / Handlers 실제 코드 초안 v0.1

## 0. 문서 목적

이 문서는 프로젝트 소스의 README/정본 단계 문서, `11_stage11_execution_contracts_v0_1.md`, `12_migration_spec_0001_0004_v0_1.md`, 그리고 `17~21` 단계 collector 구현 문서를 바탕으로,
`collector-telegram`의 **C4 구현 묶음**인 아래 세 파일의 실제 코드 초안을 제공한다.

- `message_projection.py`
- `update_dispatcher.py`
- `update_handlers.py`

이번 묶음의 목표는 다음 네 가지다.

1. TDLib `message`를 collector canonical projection으로 바꾸는 규칙을 코드로 고정
2. raw update journal → handler dispatch → raw status 반영의 실행 흐름을 코드로 고정
3. `updateNewMessage / updateMessageEdited / updateMessageContent / updateDeleteMessages / updateChatLastMessage`의 처리 경계를 코드로 고정
4. collector가 **원문/current/version/outbox**까지만 건드리고, 판단/정규화/LLM 계층으로 새 책임이 새지 않게 고정

이 문서는 아직 아래를 구현하지 않는다.

- `reconcile.py`
- `registry_sync.py`
- `health.py`
- concrete TDLib binding의 실제 wire-up
- 실제 queue enqueue / reconcile scheduler 연결

즉, 이번 단계의 목적은 **live update를 안전하게 current/version/outbox 흐름으로 반영하는 collector 내부 실행 골격**을 먼저 확보하는 것이다.

---

## 1. 대상 파일 트리

```text
src/services/collector_telegram/
  message_projection.py
  update_dispatcher.py
  update_handlers.py
```

---

## 2. 코드 초안

## 2-1. `src/services/collector_telegram/message_projection.py`

```python
from __future__ import annotations

import copy
import hashlib
import json
import logging
import re
import unicodedata
from datetime import datetime, timezone
from typing import Any

from .models import SourceMessageProjection, SourceMessageVersionProjection


JsonDict = dict[str, Any]
_URL_REGEX = re.compile(r"https?://[^\s<>()\[\]{}\"']+")

_CAPTION_CONTENT_TYPES = {
    "messageAnimation",
    "messageAudio",
    "messageDocument",
    "messagePaidMedia",
    "messagePhoto",
    "messageVideo",
    "messageVoiceNote",
}


class MessageProjectionBuilder:
    """Build collector-side current/version projections from TDLib messages.

    Design constraints carried from stage 3 / 17 / 18:
    - raw message JSON is preserved,
    - projection is derived surface only,
    - entity-first URL extraction is preferred,
    - logical_post_key keeps message-level storage while allowing later post-level merge.
    """

    def __init__(self, *, logger: logging.Logger | None = None) -> None:
        self._logger = logger or logging.getLogger(__name__)

    def build_source_projection(self, message: JsonDict) -> SourceMessageProjection:
        chat_id = int(message["chat_id"])
        message_id = int(message["id"])
        content = self._get_mapping(message.get("content"))
        content_type = self._content_type_name(content.get("@type"))

        text_body, text_entities = self._extract_text_body(content)
        caption_text, caption_entities = self._extract_caption(content)
        combined_entities = self._combine_entities(text_entities, caption_entities)
        url_surface = self._extract_url_surface(
            message=message,
            text_body=text_body,
            caption_text=caption_text,
            entities=combined_entities,
        )

        projection = SourceMessageProjection(
            chat_id=chat_id,
            message_id=message_id,
            logical_post_key=self.compute_logical_post_key(message),
            is_channel_post=bool(message.get("is_channel_post", False)),
            posted_at=self._unix_to_datetime(message.get("date")),
            edited_at=self._unix_to_datetime(message.get("edit_date")),
            message_link=self._extract_message_link(message),
            author_signature=self._coerce_str_or_none(message.get("author_signature")),
            forward_info_json=self._get_mapping_or_none(message.get("forward_info")),
            content_type=content_type,
            text_body=text_body,
            caption_text=caption_text,
            text_surface=self._build_text_surface(text_body=text_body, caption_text=caption_text),
            entities_json=combined_entities or None,
            url_surface_json=url_surface or None,
            raw_message_json=copy.deepcopy(message),
            content_hash="",
        )

        content_hash = self.compute_content_hash(projection)
        return SourceMessageProjection(
            chat_id=projection.chat_id,
            message_id=projection.message_id,
            logical_post_key=projection.logical_post_key,
            is_channel_post=projection.is_channel_post,
            posted_at=projection.posted_at,
            edited_at=projection.edited_at,
            message_link=projection.message_link,
            author_signature=projection.author_signature,
            forward_info_json=projection.forward_info_json,
            content_type=projection.content_type,
            text_body=projection.text_body,
            caption_text=projection.caption_text,
            text_surface=projection.text_surface,
            entities_json=projection.entities_json,
            url_surface_json=projection.url_surface_json,
            raw_message_json=projection.raw_message_json,
            content_hash=content_hash,
        )

    def build_version_projection(
        self,
        message: JsonDict,
        reason: str,
        *,
        source_message_id: str | None = None,
        version_no: int | None = None,
    ) -> SourceMessageVersionProjection:
        source_projection = self.build_source_projection(message)
        return SourceMessageVersionProjection(
            source_message_id=source_message_id,
            version_no=version_no,
            version_reason=reason,
            observed_at=datetime.now(timezone.utc),
            telegram_edit_date=source_projection.edited_at,
            text_surface=source_projection.text_surface,
            entities_json=source_projection.entities_json,
            raw_message_json=source_projection.raw_message_json,
            content_hash=source_projection.content_hash,
        )

    def compute_logical_post_key(self, message: JsonDict) -> str:
        chat_id = int(message["chat_id"])
        message_id = int(message["id"])
        media_album_id = message.get("media_album_id")
        try:
            media_album_id_int = int(media_album_id or 0)
        except (TypeError, ValueError):
            media_album_id_int = 0

        if media_album_id_int != 0:
            return f"tg:{chat_id}:album:{media_album_id_int}"
        return f"tg:{chat_id}:{message_id}"

    def compute_content_hash(self, projection: SourceMessageProjection) -> str:
        canonical_payload = {
            "content_type": projection.content_type,
            "text_body": self._normalize_for_hash(projection.text_body),
            "caption_text": self._normalize_for_hash(projection.caption_text),
            "text_surface": self._normalize_for_hash(projection.text_surface),
            "entities_json": projection.entities_json or [],
            "url_surface_json": projection.url_surface_json or [],
            "author_signature": projection.author_signature,
            "forward_info_json": projection.forward_info_json,
            "logical_post_key": projection.logical_post_key,
            "is_channel_post": projection.is_channel_post,
        }
        payload = json.dumps(
            canonical_payload,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        )
        return hashlib.sha256(payload.encode("utf-8")).hexdigest()

    def _extract_text_body(self, content: JsonDict) -> tuple[str | None, list[JsonDict]]:
        if content.get("@type") != "messageText":
            return None, []
        formatted_text = self._get_mapping_or_none(content.get("text")) or {}
        return self._extract_formatted_text(formatted_text, surface="text_body")

    def _extract_caption(self, content: JsonDict) -> tuple[str | None, list[JsonDict]]:
        if content.get("@type") not in _CAPTION_CONTENT_TYPES:
            return None, []
        formatted_text = self._get_mapping_or_none(content.get("caption")) or {}
        return self._extract_formatted_text(formatted_text, surface="caption_text")

    def _extract_formatted_text(
        self,
        formatted_text: JsonDict,
        *,
        surface: str,
    ) -> tuple[str | None, list[JsonDict]]:
        text = self._coerce_str_or_none(formatted_text.get("text"))
        entities_raw = formatted_text.get("entities") or []
        entities: list[JsonDict] = []
        for entity in entities_raw:
            entity_map = self._get_mapping_or_none(entity)
            if entity_map is None:
                continue
            entity_copy = copy.deepcopy(entity_map)
            entity_copy["surface"] = surface
            entities.append(entity_copy)
        return text, entities

    def _combine_entities(
        self,
        text_entities: list[JsonDict],
        caption_entities: list[JsonDict],
    ) -> list[JsonDict]:
        combined: list[JsonDict] = []
        combined.extend(text_entities)
        combined.extend(caption_entities)
        return combined

    def _build_text_surface(self, *, text_body: str | None, caption_text: str | None) -> str | None:
        parts = [part.strip() for part in [text_body, caption_text] if part and part.strip()]
        if not parts:
            return None
        return "\n\n".join(parts)

    def _extract_url_surface(
        self,
        *,
        message: JsonDict,
        text_body: str | None,
        caption_text: str | None,
        entities: list[JsonDict],
    ) -> list[JsonDict]:
        urls: list[JsonDict] = []
        seen: set[tuple[str, str]] = set()

        def add(url: str | None, source_kind: str, *, context: str | None = None) -> None:
            normalized = self._normalize_observed_url(url)
            if not normalized:
                return
            key = (source_kind, normalized)
            if key in seen:
                return
            seen.add(key)
            entry: JsonDict = {
                "observed_url": normalized,
                "source_kind": source_kind,
            }
            if context:
                entry["context"] = context
            urls.append(entry)

        for entity in entities:
            entity_type = self._entity_type_name(entity)
            surface_name = self._coerce_str_or_none(entity.get("surface")) or "unknown"
            surface_text = text_body if surface_name == "text_body" else caption_text

            if entity_type == "textEntityTypeTextUrl":
                add(self._extract_text_url_entity_url(entity), "entity", context=surface_name)
                continue

            if entity_type == "textEntityTypeUrl":
                add(
                    self._extract_url_from_entity_slice(surface_text, entity),
                    "entity",
                    context=surface_name,
                )
                continue

        for preview_url in self._extract_preview_urls(message):
            add(preview_url, "preview")

        for raw_text in [text_body, caption_text]:
            if not raw_text:
                continue
            for match in _URL_REGEX.findall(raw_text):
                add(match, "regex")

        return urls

    def _extract_preview_urls(self, message: JsonDict) -> list[str]:
        content = self._get_mapping(message.get("content"))
        preview = self._get_mapping_or_none(content.get("link_preview"))
        if preview is None:
            return []

        candidates: list[str] = []
        for key in ("url", "site_name", "title"):
            value = preview.get(key)
            if isinstance(value, str) and value.startswith(("http://", "https://")):
                candidates.append(value)

        type_specific = preview.get("type")
        if isinstance(type_specific, dict):
            for nested_value in type_specific.values():
                if isinstance(nested_value, str) and nested_value.startswith(("http://", "https://")):
                    candidates.append(nested_value)
        return candidates

    def _extract_message_link(self, message: JsonDict) -> str | None:
        for key in ("message_link", "_message_link"):
            value = message.get(key)
            if isinstance(value, str) and value.strip():
                return value.strip()
        return None

    def _extract_url_from_entity_slice(self, text: str | None, entity: JsonDict) -> str | None:
        if not text:
            return None
        offset = entity.get("offset")
        length = entity.get("length")
        if not isinstance(offset, int) or not isinstance(length, int):
            return None
        if offset < 0 or length <= 0:
            return None
        try:
            return text[offset : offset + length]
        except Exception:
            self._logger.debug("failed_entity_slice", exc_info=True)
            return None

    def _extract_text_url_entity_url(self, entity: JsonDict) -> str | None:
        entity_type = self._get_mapping_or_none(entity.get("type")) or {}
        url = entity_type.get("url")
        return self._coerce_str_or_none(url)

    def _entity_type_name(self, entity: JsonDict) -> str | None:
        entity_type = self._get_mapping_or_none(entity.get("type")) or {}
        raw = entity_type.get("@type")
        return raw if isinstance(raw, str) else None

    def _content_type_name(self, raw_type: Any) -> str | None:
        if not isinstance(raw_type, str):
            return None
        if raw_type.startswith("message"):
            suffix = raw_type[len("message") :]
            return suffix[:1].lower() + suffix[1:] if suffix else "message"
        return raw_type

    def _normalize_observed_url(self, value: str | None) -> str | None:
        if value is None:
            return None
        normalized = unicodedata.normalize("NFKC", value).strip()
        if not normalized:
            return None
        if not normalized.startswith(("http://", "https://")):
            return None
        return normalized

    def _normalize_for_hash(self, value: str | None) -> str | None:
        if value is None:
            return None
        normalized = unicodedata.normalize("NFKC", value)
        normalized = re.sub(r"\s+", " ", normalized).strip()
        return normalized or None

    def _unix_to_datetime(self, value: Any) -> datetime | None:
        if value in (None, 0, ""):
            return None
        if isinstance(value, datetime):
            if value.tzinfo is None:
                return value.replace(tzinfo=timezone.utc)
            return value.astimezone(timezone.utc)
        if not isinstance(value, (int, float)):
            return None
        return datetime.fromtimestamp(float(value), tz=timezone.utc)

    def _get_mapping(self, value: Any) -> JsonDict:
        mapping = self._get_mapping_or_none(value)
        return mapping or {}

    def _get_mapping_or_none(self, value: Any) -> JsonDict | None:
        return value if isinstance(value, dict) else None

    def _coerce_str_or_none(self, value: Any) -> str | None:
        if isinstance(value, str):
            stripped = value.strip()
            return stripped or None
        return None
```

---

## 2-2. `src/services/collector_telegram/update_handlers.py`

```python
from __future__ import annotations

import copy
import json
import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Mapping

from .exceptions import (
    RepositoryInvariantError,
    UpdateApplyRetryableError,
    UpdateApplyTerminalError,
)
from .message_projection import MessageProjectionBuilder
from .models import SourceMessageProjection
from .outbox import CollectorOutboxBuilder
from .repositories import CollectorRepository


JsonDict = dict[str, Any]


@dataclass(slots=True, frozen=True)
class UpdateHandlingResult:
    handled: bool
    source_message_ids: list[str] = field(default_factory=list)
    version_appended: bool = False
    outbox_events_created: int = 0
    reconcile_requested: bool = False
    reconcile_reason: str | None = None
    note: str | None = None


class CollectorUpdateHandlers:
    """Apply collector-side update handling.

    Transaction rule:
    - dispatcher manages raw update journaling and outer success/failure marking,
    - handlers mutate only current/version/outbox state,
    - handlers do not open their own transactions.
    """

    def __init__(
        self,
        repository: CollectorRepository,
        projection_builder: MessageProjectionBuilder,
        outbox_builder: CollectorOutboxBuilder,
        *,
        logger: logging.Logger | None = None,
    ) -> None:
        self._repository = repository
        self._projection_builder = projection_builder
        self._outbox_builder = outbox_builder
        self._logger = logger or logging.getLogger(__name__)

    async def handle_update_new_message(self, update: JsonDict) -> UpdateHandlingResult:
        message = self._require_mapping(update.get("message"), "updateNewMessage.message")
        projection = self._projection_builder.build_source_projection(message)

        current_row = await self._repository.upsert_source_message(projection)
        source_message_id = self._require_uuid(current_row.get("source_message_id"), "source_message_id")

        changed, version_row = await self._repository.append_source_message_version_if_changed(
            source_message_id=source_message_id,
            projection=projection,
            version_reason="new",
            observed_at=datetime.now(timezone.utc),
            telegram_edit_date=projection.edited_at,
        )

        outbox_events_created = 0
        if changed:
            version_no = self._require_int(version_row, "version_no")
            event = self._outbox_builder.build_created(
                source_message_id=source_message_id,
                current_version_no=version_no,
                logical_post_key=projection.logical_post_key,
                occurred_at=projection.posted_at,
            )
            await self._repository.insert_outbox_event(event)
            outbox_events_created = 1

        return UpdateHandlingResult(
            handled=True,
            source_message_ids=[source_message_id],
            version_appended=changed,
            outbox_events_created=outbox_events_created,
        )

    async def handle_update_message_edited(self, update: JsonDict) -> UpdateHandlingResult:
        chat_id = self._require_int_from_value(update.get("chat_id"), "updateMessageEdited.chat_id")
        message_id = self._require_int_from_value(update.get("message_id"), "updateMessageEdited.message_id")
        current_row = await self._load_current_source_message(chat_id=chat_id, message_id=message_id)

        existing_raw = self._load_raw_message_json(current_row)
        synthetic_message = copy.deepcopy(existing_raw)
        if isinstance(update.get("edit_date"), (int, float)):
            synthetic_message["edit_date"] = update["edit_date"]

        projection = self._projection_builder.build_source_projection(synthetic_message)
        await self._repository.upsert_source_message(projection)

        return UpdateHandlingResult(
            handled=True,
            source_message_ids=[self._require_uuid(current_row.get("source_message_id"), "source_message_id")],
            version_appended=False,
            outbox_events_created=0,
            note="metadata-only edit observed; waiting for updateMessageContent or reconcile for version append",
        )

    async def handle_update_message_content(self, update: JsonDict) -> UpdateHandlingResult:
        chat_id = self._require_int_from_value(update.get("chat_id"), "updateMessageContent.chat_id")
        message_id = self._require_int_from_value(update.get("message_id"), "updateMessageContent.message_id")
        current_row = await self._load_current_source_message(chat_id=chat_id, message_id=message_id)

        existing_raw = self._load_raw_message_json(current_row)
        synthetic_message = copy.deepcopy(existing_raw)

        new_content = self._get_mapping_or_none(update.get("new_content")) or self._get_mapping_or_none(update.get("content"))
        if new_content is None:
            raise UpdateApplyRetryableError("updateMessageContent arrived without content payload")
        synthetic_message["content"] = copy.deepcopy(new_content)

        if isinstance(update.get("edit_date"), (int, float)):
            synthetic_message["edit_date"] = update["edit_date"]

        projection = self._projection_builder.build_source_projection(synthetic_message)
        current_row = await self._repository.upsert_source_message(projection)
        source_message_id = self._require_uuid(current_row.get("source_message_id"), "source_message_id")

        changed, version_row = await self._repository.append_source_message_version_if_changed(
            source_message_id=source_message_id,
            projection=projection,
            version_reason="content_change",
            observed_at=datetime.now(timezone.utc),
            telegram_edit_date=projection.edited_at,
        )

        outbox_events_created = 0
        if changed:
            version_no = self._require_int(version_row, "version_no")
            event = self._outbox_builder.build_edited(
                source_message_id=source_message_id,
                current_version_no=version_no,
                logical_post_key=projection.logical_post_key,
                occurred_at=projection.edited_at or datetime.now(timezone.utc),
            )
            await self._repository.insert_outbox_event(event)
            outbox_events_created = 1

        return UpdateHandlingResult(
            handled=True,
            source_message_ids=[source_message_id],
            version_appended=changed,
            outbox_events_created=outbox_events_created,
        )

    async def handle_update_delete_messages(self, update: JsonDict) -> UpdateHandlingResult:
        chat_id = self._require_int_from_value(update.get("chat_id"), "updateDeleteMessages.chat_id")
        message_ids = update.get("message_ids") or []
        if not isinstance(message_ids, list):
            raise UpdateApplyTerminalError("updateDeleteMessages.message_ids must be a list")

        is_permanent = bool(update.get("is_permanent", False))
        from_cache = bool(update.get("from_cache", False))
        delete_kind = self._map_delete_kind(is_permanent=is_permanent, from_cache=from_cache)
        deleted_at = datetime.now(timezone.utc)

        source_message_ids: list[str] = []
        outbox_events_created = 0
        for raw_message_id in message_ids:
            if not isinstance(raw_message_id, int):
                continue
            current_row = await self._repository.mark_message_deleted(
                platform="telegram",
                chat_id=chat_id,
                message_id=raw_message_id,
                delete_kind=delete_kind,
                deleted_at=deleted_at,
            )
            if current_row is None:
                self._logger.warning(
                    "delete_update_for_unknown_message",
                    extra={
                        "service": "collector-telegram",
                        "event": "delete_update_for_unknown_message",
                        "chat_id": chat_id,
                        "message_id": raw_message_id,
                    },
                )
                continue

            source_message_id = self._require_uuid(current_row.get("source_message_id"), "source_message_id")
            logical_post_key = self._coerce_non_empty_str(current_row.get("logical_post_key"), "logical_post_key")
            current_version_no = self._require_int_from_value(current_row.get("current_version_no"), "current_version_no")

            event = self._outbox_builder.build_deleted(
                source_message_id=source_message_id,
                current_version_no=current_version_no,
                logical_post_key=logical_post_key,
                occurred_at=deleted_at,
                delete_kind=delete_kind,
            )
            await self._repository.insert_outbox_event(event)

            source_message_ids.append(source_message_id)
            outbox_events_created += 1

        return UpdateHandlingResult(
            handled=True,
            source_message_ids=source_message_ids,
            version_appended=False,
            outbox_events_created=outbox_events_created,
        )

    async def handle_update_chat_last_message(self, update: JsonDict) -> UpdateHandlingResult:
        chat_id = self._require_int_from_value(update.get("chat_id"), "updateChatLastMessage.chat_id")
        last_message = self._get_mapping_or_none(update.get("last_message"))

        if last_message is None:
            return UpdateHandlingResult(
                handled=True,
                reconcile_requested=True,
                reconcile_reason="last_message_missing",
                note=f"chat {chat_id} reported null last_message; reconcile should be prioritized",
            )

        current_last_message_id = last_message.get("id")
        return UpdateHandlingResult(
            handled=True,
            reconcile_requested=False,
            note=f"chat {chat_id} last message observed: {current_last_message_id}",
        )

    async def _load_current_source_message(self, *, chat_id: int, message_id: int) -> Mapping[str, Any]:
        current_row = await self._repository.get_source_message(
            platform="telegram",
            chat_id=chat_id,
            message_id=message_id,
        )
        if current_row is None:
            raise UpdateApplyRetryableError(
                f"current source message missing for chat_id={chat_id}, message_id={message_id}; reconcile may recover"
            )
        return current_row

    def _load_raw_message_json(self, current_row: Mapping[str, Any]) -> JsonDict:
        raw_value = current_row.get("raw_message_json")
        if isinstance(raw_value, dict):
            return raw_value
        if isinstance(raw_value, str):
            try:
                decoded = json.loads(raw_value)
            except json.JSONDecodeError as exc:
                raise RepositoryInvariantError("raw_message_json is not valid JSON") from exc
            if isinstance(decoded, dict):
                return decoded
        raise RepositoryInvariantError("raw_message_json is missing or not a JSON object")

    def _map_delete_kind(self, *, is_permanent: bool, from_cache: bool) -> str:
        if is_permanent:
            return "permanent"
        if from_cache:
            return "cache_only"
        return "cache_only"

    def _require_mapping(self, value: Any, label: str) -> JsonDict:
        if not isinstance(value, dict):
            raise UpdateApplyTerminalError(f"{label} must be an object")
        return value

    def _get_mapping_or_none(self, value: Any) -> JsonDict | None:
        return value if isinstance(value, dict) else None

    def _require_uuid(self, value: Any, label: str) -> str:
        if isinstance(value, str) and value:
            return value
        raise RepositoryInvariantError(f"{label} is missing or invalid")

    def _require_int(self, mapping: Mapping[str, Any] | None, key: str) -> int:
        if mapping is None:
            raise RepositoryInvariantError(f"mapping missing while reading {key}")
        return self._require_int_from_value(mapping.get(key), key)

    def _require_int_from_value(self, value: Any, label: str) -> int:
        if isinstance(value, bool):
            raise UpdateApplyTerminalError(f"{label} must be an integer")
        if not isinstance(value, int):
            raise UpdateApplyTerminalError(f"{label} must be an integer")
        return value

    def _coerce_non_empty_str(self, value: Any, label: str) -> str:
        if isinstance(value, str) and value.strip():
            return value.strip()
        raise RepositoryInvariantError(f"{label} is missing or empty")
```

---

## 2-3. `src/services/collector_telegram/update_dispatcher.py`

```python
from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any, Awaitable, Callable

from .exceptions import (
    UpdateApplyRetryableError,
    UpdateApplyTerminalError,
)
from .repositories import CollectorRepository
from .update_handlers import CollectorUpdateHandlers, UpdateHandlingResult


JsonDict = dict[str, Any]
HandlerCallable = Callable[[JsonDict], Awaitable[UpdateHandlingResult]]


@dataclass(slots=True, frozen=True)
class DispatchContext:
    raw_update_seq: int
    update_type: str
    chat_id: int | None
    message_id: int | None


class UpdateDispatcher:
    """Route raw TDLib updates into collector handlers.

    Journaling rule used here:
    1. raw update row is persisted first,
    2. business mutation runs after that,
    3. raw row is marked applied/failed in follow-up transaction.

    This intentionally preserves failed raw updates for replay/debug,
    even if business-state mutation rolls back.
    """

    def __init__(
        self,
        repository: CollectorRepository,
        handlers: CollectorUpdateHandlers,
        *,
        logger: logging.Logger | None = None,
    ) -> None:
        self._repository = repository
        self._handlers = handlers
        self._logger = logger or logging.getLogger(__name__)
        self._route_map: dict[str, HandlerCallable] = {
            "updateNewMessage": self._handlers.handle_update_new_message,
            "updateMessageEdited": self._handlers.handle_update_message_edited,
            "updateMessageContent": self._handlers.handle_update_message_content,
            "updateDeleteMessages": self._handlers.handle_update_delete_messages,
            "updateChatLastMessage": self._handlers.handle_update_chat_last_message,
        }

    async def dispatch(self, update: JsonDict) -> UpdateHandlingResult:
        update_type = self._require_update_type(update)
        chat_id, message_id = self._extract_chat_message_ids(update_type, update)

        async with self._repository.transaction():
            raw_update_seq = await self._repository.insert_raw_update(
                update_type=update_type,
                payload_json=update,
                chat_id=chat_id,
                message_id=message_id,
            )

        context = DispatchContext(
            raw_update_seq=raw_update_seq,
            update_type=update_type,
            chat_id=chat_id,
            message_id=message_id,
        )

        handler = self._route_map.get(update_type)
        if handler is None:
            async with self._repository.transaction():
                await self._repository.mark_raw_update_applied(raw_update_seq)
            self._logger.info(
                "collector_update_ignored",
                extra={
                    "service": "collector-telegram",
                    "event": "collector_update_ignored",
                    "update_type": update_type,
                    "chat_id": chat_id,
                    "message_id": message_id,
                },
            )
            return UpdateHandlingResult(
                handled=False,
                note=f"ignored unsupported update type: {update_type}",
            )

        try:
            async with self._repository.transaction():
                result = await handler(update)
                await self._repository.mark_raw_update_applied(raw_update_seq)
                return result
        except (UpdateApplyRetryableError, UpdateApplyTerminalError) as exc:
            await self._mark_failed(raw_update_seq, exc)
            raise
        except Exception as exc:
            wrapped = UpdateApplyRetryableError(
                f"unexpected collector update application failure: {update_type}"
            )
            await self._mark_failed(raw_update_seq, wrapped)
            raise wrapped from exc

    async def _mark_failed(self, raw_update_seq: int, exc: Exception) -> None:
        async with self._repository.transaction():
            await self._repository.mark_raw_update_failed(raw_update_seq, str(exc))

    def _require_update_type(self, update: JsonDict) -> str:
        raw = update.get("@type")
        if not isinstance(raw, str) or not raw:
            raise UpdateApplyTerminalError("update payload is missing @type")
        return raw

    def _extract_chat_message_ids(self, update_type: str, update: JsonDict) -> tuple[int | None, int | None]:
        if update_type == "updateNewMessage":
            message = update.get("message")
            if isinstance(message, dict):
                chat_id = message.get("chat_id")
                message_id = message.get("id")
                return self._coerce_int_or_none(chat_id), self._coerce_int_or_none(message_id)
            return None, None

        if update_type in {"updateMessageEdited", "updateMessageContent", "updateChatLastMessage"}:
            return (
                self._coerce_int_or_none(update.get("chat_id")),
                self._coerce_int_or_none(update.get("message_id")),
            )

        if update_type == "updateDeleteMessages":
            message_ids = update.get("message_ids")
            first_message_id = None
            if isinstance(message_ids, list) and message_ids:
                first_message_id = self._coerce_int_or_none(message_ids[0])
            return self._coerce_int_or_none(update.get("chat_id")), first_message_id

        return None, None

    def _coerce_int_or_none(self, value: Any) -> int | None:
        if isinstance(value, bool):
            return None
        if isinstance(value, int):
            return value
        return None
```

---

## 3. 구현 메모

### 3-1. 왜 `message_projection.py`에서 raw/current/version을 분리하되 hash는 projection 기준으로 계산하는가

collector의 역할은 Telegram 원문 자체를 잃지 않는 것이지, raw JSON을 그대로 비교 키로 삼는 것이 아니다. 그래서 raw JSON은 `raw_message_json`으로 그대로 보존하되, version append/no-op 판단은 `text_body`, `caption_text`, `entities_json`, `url_surface_json`, `logical_post_key` 같은 **collector canonical projection** 기준으로 계산해야 한다. 그래야 TDLib가 부수 필드를 바꾸거나 필드 순서가 달라도 불필요한 새 version이 쌓이지 않는다. 이건 collector가 원문/current/version/outbox까지만 책임지고, 판단은 뒤 단계로 넘긴다는 구조와 맞다. fileciteturn39file6turn39file7turn39file8

### 3-2. 왜 dispatcher가 raw update row를 먼저 별도 commit하는가

이번 draft는 21단계의 atomic write 계약을 존중하되, `telegram_raw_updates`를 **실패 시에도 남기는 journal**로 해석했다. 그래서 raw update row는 먼저 `pending`으로 내구성 있게 적재하고, 그다음 business mutation(current/version/outbox)을 별도 transaction으로 수행한다. 성공하면 `applied`, 실패하면 `failed`로 마킹한다. 이렇게 하면 handler 실패 시에도 raw update 자체는 남아서 replay/debug가 가능하다. Redis나 외부 상태가 아니라 PostgreSQL journal을 기준으로 복구해야 한다는 8단계 원칙과도 맞다. fileciteturn36file7turn37file18turn37file19

### 3-3. 왜 `updateMessageEdited`는 version append 없이 current row만 갱신하는가

3단계 문서가 명시적으로 `updateMessageEdited`와 `updateMessageContent`를 분리해서 처리하라고 잠가두었다. `updateMessageEdited`는 edit metadata 반영과 pending content sync 신호까지만 담당하고, 실제 새 version은 `updateMessageContent`에서만 append하는 것이 맞다. 이번 draft는 repository 경계를 깨지 않기 위해 current raw snapshot에 `edit_date`만 얹어 projection을 다시 만들고 current row만 upsert한다. content hash가 같으므로 version append는 일어나지 않는다. fileciteturn39file7turn39file11

### 3-4. 왜 `updateDeleteMessages`는 tombstone + outbox만 하고 delete marker version은 보류했는가

현재 repository draft에는 "current row mutation 없이 history-only delete marker version append" 전용 메서드가 없다. 기존 `append_source_message_version()`은 current row를 같이 최신화하면서 tombstone을 해제하는 경향이 있어서 그대로 delete marker에 쓰면 current canonical row가 왜곡된다. 그래서 이번 단계는 구조를 깨지 않는 선에서 **tombstone 반영 + delete outbox**까지만 고정했다. delete marker history row는 다음 단계에서 reconcile/registry와 함께 repository 보강을 할 때 안전하게 추가하는 편이 낫다. 이건 minimal-change 선택이다. fileciteturn39file8turn39file11

### 3-5. 왜 `updateChatLastMessage`는 즉시 reconcile을 실행하지 않고 hint만 반환하는가

reconcile scheduler/worker는 아직 C5 범위다. 그래서 이번 단계의 handler는 `updateChatLastMessage`에서 null `last_message` 같은 gap 신호를 감지하면 `reconcile_requested=True`만 반환하고, 실제 `getChatHistory` 호출은 하지 않는다. 이게 현재 구현 순서와 일치한다. C4는 update 처리 골격까지만, C5에서 reconcile path를 붙이는 것이 맞다. fileciteturn39file10turn39file11

---

## 4. 바로 다음 단계

다음 구현 묶음은 그대로 아래가 맞다.

- `reconcile.py`
- `registry_sync.py`

이유는 이번 단계에서 아래가 고정됐기 때문이다.

- message → current/version projection
- raw update journal → handler dispatch 흐름
- new/edit/content/delete/chat_last_message 처리 골격

즉, 이제 다음은 **startup warm backfill / authoritative reconcile / tracked chat onboarding/access refresh**를 코드로 내리는 단계다. collector는 여전히 `0001_ingest_core`만 직접 쓰고, 정규화/후보 생성/LLM 판단 책임은 여전히 바깥에 남아 있어야 한다. fileciteturn38file10turn39file18


---

## Source file: `23_collector_reconcile_registry_sync_code_draft_v0_1.md`

# 23단계: `collector-telegram` Reconcile / Registry Sync 실제 코드 초안 v0.1

## 0. 문서 목적

이 문서는 프로젝트 소스의 README/정본 단계 문서, `11_stage11_execution_contracts_v0_1.md`, `12_migration_spec_0001_0004_v0_1.md`, 그리고 `17~22` 단계 collector 구현 문서를 바탕으로, `collector-telegram`의 **C5 구현 묶음**인 아래 두 파일의 실제 코드 초안을 제공한다.

- `reconcile.py`
- `registry_sync.py`

이번 묶음의 목표는 다음 세 가지다.

1. startup warm backfill / authoritative reconcile / gap fill 경계를 코드로 고정
2. tracked chat onboarding / access refresh 경계를 코드로 고정
3. collector가 `0001_ingest_core` 범위만 직접 쓰면서도, live ingest 누락 보정과 채널 상태 동기화를 구조적으로 유지하도록 만드는 것

이 문서는 아직 아래를 구현하지 않는다.

- `health.py`
- metrics exporter
- singleton lock 구현
- registry 관련 repository SQL 실제 구현
- TDLib low-level `call()` concrete implementation

즉, 이번 단계의 목적은 **collector의 reconcile/registry 제어면을 먼저 코드 경계로 잠그는 것**이다.

---

## 1. 대상 파일 트리

```text
src/services/collector_telegram/
  reconcile.py
  registry_sync.py
```

---

## 2. 코드 초안

## 2-1. `src/services/collector_telegram/reconcile.py`

```python
from __future__ import annotations

import logging
from collections.abc import Mapping, Sequence
from datetime import datetime, timezone
from typing import Any, Protocol

from .config import CollectorTelegramConfig
from .exceptions import ReconcileRetryableError, ReconcileTerminalError
from .models import ReconcileSummary, SourceMessageProjection
from .outbox import CollectorOutboxBuilder


JsonDict = dict[str, Any]


class TDLibHistoryProtocol(Protocol):
    def build_get_chat_history_request(
        self,
        *,
        chat_id: int,
        from_message_id: int = 0,
        offset: int = 0,
        limit: int = 50,
        only_local: bool = False,
    ) -> Any: ...

    async def call(self, request: JsonDict, timeout: float = 30.0) -> JsonDict | None: ...


class ProjectionBuilderProtocol(Protocol):
    def build_source_projection(self, message: JsonDict) -> SourceMessageProjection: ...


class ReconcileRepositoryProtocol(Protocol):
    async def transaction(self): ...

    async def get_source_message(
        self,
        *,
        platform: str,
        chat_id: int,
        message_id: int,
    ) -> Mapping[str, Any] | None: ...

    async def upsert_source_message(
        self,
        projection: SourceMessageProjection,
        *,
        platform: str = "telegram",
    ) -> Mapping[str, Any]: ...

    async def append_source_message_version_if_changed(
        self,
        *,
        source_message_id: str,
        projection: SourceMessageProjection,
        version_reason: str,
        observed_at: datetime | None = None,
        telegram_edit_date: datetime | None = None,
    ) -> tuple[bool, Mapping[str, Any] | None]: ...

    async def insert_outbox_event(self, event) -> None: ...

    async def list_reconcile_targets(self, limit: int) -> list[Any]: ...

    async def update_channel_sync_cursor(
        self,
        *,
        registry_id: str,
        last_seen_message_id: int | None = None,
        last_seen_message_date: datetime | None = None,
        last_history_sync_at: datetime | None = None,
    ) -> None: ...


class ReconcileService:
    """History-based recovery and gap-fill logic for collector-telegram.

    Design constraints preserved from stage docs:
    - warm backfill uses only_local=true,
    - authoritative reconcile uses only_local=false,
    - repeated history reads are expected and must be idempotent,
    - reconcile emits collector outbox events only when message state actually advances.
    """

    def __init__(
        self,
        config: CollectorTelegramConfig,
        *,
        tdlib: TDLibHistoryProtocol,
        repository: ReconcileRepositoryProtocol,
        projection_builder: ProjectionBuilderProtocol,
        outbox_builder: CollectorOutboxBuilder,
        logger: logging.Logger | None = None,
    ) -> None:
        self._config = config
        self._tdlib = tdlib
        self._repository = repository
        self._projection_builder = projection_builder
        self._outbox_builder = outbox_builder
        self._logger = logger or logging.getLogger(__name__)

    async def run_startup_warm_backfill(self, chat_id: int) -> ReconcileSummary:
        return await self._run_history_scan(
            chat_id=chat_id,
            only_local=True,
            limit=self._config.warm_backfill_limit,
            reason="startup_warm_backfill",
        )

    async def run_authoritative_reconcile(self, chat_id: int, reason: str = "scheduled") -> ReconcileSummary:
        return await self._run_history_scan(
            chat_id=chat_id,
            only_local=False,
            limit=self._config.reconcile_backfill_limit,
            reason=reason,
        )

    async def run_gap_fill(self, chat_id: int, reason: str) -> ReconcileSummary:
        return await self._run_history_scan(
            chat_id=chat_id,
            only_local=False,
            limit=self._config.reconcile_backfill_limit,
            reason=reason,
        )

    async def run_scheduled_targets(self, *, limit: int = 20) -> list[ReconcileSummary]:
        targets = await self._repository.list_reconcile_targets(limit)
        results: list[ReconcileSummary] = []
        for target in targets:
            chat_id = getattr(target, "chat_id", None)
            if chat_id is None:
                continue
            results.append(
                await self.run_authoritative_reconcile(
                    chat_id=int(chat_id),
                    reason="scheduled_reconcile",
                )
            )
        return results

    async def _run_history_scan(
        self,
        *,
        chat_id: int,
        only_local: bool,
        limit: int,
        reason: str,
    ) -> ReconcileSummary:
        observed_at = datetime.now(timezone.utc)
        messages = await self._fetch_chat_history(
            chat_id=chat_id,
            only_local=only_local,
            limit=limit,
        )
        if not messages:
            return ReconcileSummary(
                chat_id=chat_id,
                result_type="no_changes",
                processed_count=0,
                inserted_count=0,
                updated_count=0,
                gap_filled_count=0,
            )

        processed_count = 0
        inserted_count = 0
        updated_count = 0
        gap_filled_count = 0
        max_message_id: int | None = None
        max_message_date: datetime | None = None

        # TDLib history is newest-first; apply oldest-first for deterministic state advancement.
        for message in reversed(messages):
            processed_count += 1
            applied = await self._apply_history_message(
                message=message,
                reason=reason,
                observed_at=observed_at,
            )
            if applied["inserted"]:
                inserted_count += 1
                gap_filled_count += 1
            if applied["updated"]:
                updated_count += 1
                gap_filled_count += 1

            message_id = self._safe_int(message.get("id"))
            message_date = self._message_date(message)
            if message_id is not None and (max_message_id is None or message_id > max_message_id):
                max_message_id = message_id
            if message_date is not None and (max_message_date is None or message_date > max_message_date):
                max_message_date = message_date

        result_type = "gap_filled" if gap_filled_count > 0 else "cursor_advanced"
        return ReconcileSummary(
            chat_id=chat_id,
            result_type=result_type,
            processed_count=processed_count,
            inserted_count=inserted_count,
            updated_count=updated_count,
            gap_filled_count=gap_filled_count,
            error_code=None,
        )

    async def _fetch_chat_history(
        self,
        *,
        chat_id: int,
        only_local: bool,
        limit: int,
    ) -> Sequence[JsonDict]:
        request = self._tdlib.build_get_chat_history_request(
            chat_id=chat_id,
            from_message_id=0,
            offset=0,
            limit=limit,
            only_local=only_local,
        )
        payload = self._unwrap_request(request)
        response = await self._tdlib.call(payload, timeout=30.0)
        if response is None:
            raise ReconcileRetryableError("TDLib returned no response for getChatHistory")

        if response.get("@type") == "error":
            code = response.get("code")
            message = response.get("message")
            error_text = f"getChatHistory failed: code={code}, message={message}"
            if self._is_access_error(response):
                raise ReconcileTerminalError(error_text)
            raise ReconcileRetryableError(error_text)

        messages = response.get("messages")
        if not isinstance(messages, list):
            raise ReconcileRetryableError("TDLib getChatHistory response missing messages list")
        return [m for m in messages if isinstance(m, dict)]

    async def _apply_history_message(
        self,
        *,
        message: JsonDict,
        reason: str,
        observed_at: datetime,
    ) -> dict[str, bool]:
        projection = self._projection_builder.build_source_projection(message)
        existing = await self._repository.get_source_message(
            platform="telegram",
            chat_id=projection.chat_id,
            message_id=projection.message_id,
        )

        async with self._repository.transaction():
            current_row = await self._repository.upsert_source_message(projection, platform="telegram")
            source_message_id = str(current_row["source_message_id"])

            changed, version_row = await self._repository.append_source_message_version_if_changed(
                source_message_id=source_message_id,
                projection=projection,
                version_reason="reconcile",
                observed_at=observed_at,
                telegram_edit_date=projection.edited_at,
            )

            if changed and version_row is not None:
                outbox = self._outbox_builder.build_reconciled(
                    source_message_id=source_message_id,
                    current_version_no=int(version_row["version_no"]),
                    logical_post_key=projection.logical_post_key,
                    occurred_at=observed_at,
                    reconcile_reason=reason,
                )
                await self._repository.insert_outbox_event(outbox)

        inserted = existing is None and changed
        updated = existing is not None and changed
        return {
            "inserted": inserted,
            "updated": updated,
        }

    @staticmethod
    def _unwrap_request(request: Any) -> JsonDict:
        payload = getattr(request, "payload", request)
        if not isinstance(payload, dict):
            raise ReconcileTerminalError("TDLib request payload must be a dict")
        return payload

    @staticmethod
    def _safe_int(value: Any) -> int | None:
        if isinstance(value, bool):
            return None
        if isinstance(value, int):
            return value
        try:
            return int(value)
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _message_date(message: JsonDict) -> datetime | None:
        raw = message.get("date")
        if raw is None:
            return None
        try:
            return datetime.fromtimestamp(int(raw), tz=timezone.utc)
        except (TypeError, ValueError, OSError):
            return None

    @staticmethod
    def _is_access_error(response: JsonDict) -> bool:
        message = str(response.get("message", "")).upper()
        return any(
            token in message
            for token in (
                "CHAT_NOT_FOUND",
                "FORBIDDEN",
                "CHANNEL_PRIVATE",
                "USER_BANNED_IN_CHANNEL",
            )
        )
```

---

## 2-2. `src/services/collector_telegram/registry_sync.py`

```python
from __future__ import annotations

import logging
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Protocol

from .config import CollectorTelegramConfig
from .exceptions import UpdateApplyRetryableError, UpdateApplyTerminalError
from .models import TrackedChat


JsonDict = dict[str, Any]


class TDLibRegistryProtocol(Protocol):
    def build_search_public_chat_request(self, username: str) -> Any: ...

    def build_join_chat_request(self, chat_id: int) -> Any: ...

    def build_join_chat_by_invite_link_request(self, invite_link: str) -> Any: ...

    def build_get_chat_history_request(
        self,
        *,
        chat_id: int,
        from_message_id: int = 0,
        offset: int = 0,
        limit: int = 1,
        only_local: bool = False,
    ) -> Any: ...

    async def call(self, request: JsonDict, timeout: float = 30.0) -> JsonDict | None: ...


class RegistrySyncRepositoryProtocol(Protocol):
    async def list_active_tracked_chats(self) -> list[TrackedChat]: ...

    async def list_registry_rows_by_access_states(
        self,
        access_states: Sequence[str],
        *,
        desired_state: str = "active",
    ) -> list[Mapping[str, Any]]: ...

    async def mark_channel_resolved(
        self,
        *,
        registry_id: str,
        chat_id: int,
        username_snapshot: str | None,
        title_snapshot: str | None,
        chat_type: str | None,
        access_state: str,
        last_resolved_at: datetime,
    ) -> None: ...

    async def mark_channel_access_state(
        self,
        *,
        registry_id: str,
        access_state: str,
        last_join_attempt_at: datetime | None = None,
        last_resolved_at: datetime | None = None,
        chat_id: int | None = None,
        username_snapshot: str | None = None,
        title_snapshot: str | None = None,
        chat_type: str | None = None,
        notes_append: str | None = None,
    ) -> None: ...


@dataclass(slots=True, frozen=True)
class RegistrySyncSummary:
    processed_count: int = 0
    joined_count: int = 0
    join_requested_count: int = 0
    access_lost_count: int = 0
    forbidden_count: int = 0
    not_found_count: int = 0
    transient_failed_count: int = 0
    no_change_count: int = 0


class ChannelRegistrySyncService:
    """Tracked chat onboarding and access refresh.

    This service keeps collector ownership narrow:
    - resolve source_value -> chat_id anchor,
    - attempt join where allowed,
    - refresh access states conservatively,
    - load active tracked chats for downstream collector loops.
    """

    def __init__(
        self,
        config: CollectorTelegramConfig,
        *,
        tdlib: TDLibRegistryProtocol,
        repository: RegistrySyncRepositoryProtocol,
        logger: logging.Logger | None = None,
    ) -> None:
        self._config = config
        self._tdlib = tdlib
        self._repository = repository
        self._logger = logger or logging.getLogger(__name__)

    async def load_active_channels(self) -> list[TrackedChat]:
        return await self._repository.list_active_tracked_chats()

    async def sync_unresolved_channels(self) -> RegistrySyncSummary:
        rows = await self._repository.list_registry_rows_by_access_states(
            ["unresolved", "resolved_not_joined"],
            desired_state="active",
        )
        return await self._sync_rows(rows, mode="onboarding")

    async def sync_join_requested_channels(self) -> RegistrySyncSummary:
        rows = await self._repository.list_registry_rows_by_access_states(
            ["join_requested"],
            desired_state="active",
        )
        return await self._sync_rows(rows, mode="join_requested")

    async def sync_access_lost_channels(self) -> RegistrySyncSummary:
        rows = await self._repository.list_registry_rows_by_access_states(
            ["access_lost", "left"],
            desired_state="active",
        )
        return await self._sync_rows(rows, mode="access_recovery")

    async def _sync_rows(self, rows: Sequence[Mapping[str, Any]], *, mode: str) -> RegistrySyncSummary:
        summary = RegistrySyncSummary()
        processed = joined = join_requested = access_lost = forbidden = not_found = transient_failed = no_change = 0

        for row in rows:
            processed += 1
            outcome = await self._sync_single_row(row, mode=mode)

            match outcome:
                case "joined":
                    joined += 1
                case "join_requested":
                    join_requested += 1
                case "access_lost":
                    access_lost += 1
                case "forbidden":
                    forbidden += 1
                case "not_found":
                    not_found += 1
                case "transient_failed":
                    transient_failed += 1
                case _:
                    no_change += 1

        return RegistrySyncSummary(
            processed_count=processed,
            joined_count=joined,
            join_requested_count=join_requested,
            access_lost_count=access_lost,
            forbidden_count=forbidden,
            not_found_count=not_found,
            transient_failed_count=transient_failed,
            no_change_count=no_change,
        )

    async def _sync_single_row(self, row: Mapping[str, Any], *, mode: str) -> str:
        registry_id = str(row["registry_id"])
        source_kind = str(row["source_kind"])
        source_value = str(row["source_value"])
        now = datetime.now(timezone.utc)

        try:
            if source_kind == "public_username":
                return await self._sync_public_username_row(
                    registry_id=registry_id,
                    source_value=source_value,
                    existing_chat_id=self._safe_int(row.get("chat_id")),
                    mode=mode,
                    now=now,
                )

            if source_kind == "invite_link":
                return await self._sync_invite_link_row(
                    registry_id=registry_id,
                    invite_link=source_value,
                    mode=mode,
                    now=now,
                )

            if source_kind == "chat_id":
                chat_id = self._safe_int(source_value) or self._safe_int(row.get("chat_id"))
                if chat_id is None:
                    await self._repository.mark_channel_access_state(
                        registry_id=registry_id,
                        access_state="not_found",
                        notes_append="chat_id source_kind row missing numeric chat_id",
                        last_resolved_at=now,
                    )
                    return "not_found"
                return await self._probe_chat_access(
                    registry_id=registry_id,
                    chat_id=chat_id,
                    now=now,
                )

            await self._repository.mark_channel_access_state(
                registry_id=registry_id,
                access_state="not_found",
                notes_append=f"unsupported source_kind={source_kind}",
                last_resolved_at=now,
            )
            return "not_found"
        except UpdateApplyTerminalError as exc:
            self._logger.warning(
                "collector_registry_sync_terminal",
                extra={
                    "service": "collector-telegram",
                    "event": "collector_registry_sync_terminal",
                    "registry_id": registry_id,
                    "source_kind": source_kind,
                    "error": str(exc),
                },
            )
            return "forbidden"
        except UpdateApplyRetryableError as exc:
            self._logger.warning(
                "collector_registry_sync_retryable",
                extra={
                    "service": "collector-telegram",
                    "event": "collector_registry_sync_retryable",
                    "registry_id": registry_id,
                    "source_kind": source_kind,
                    "error": str(exc),
                },
            )
            return "transient_failed"

    async def _sync_public_username_row(
        self,
        *,
        registry_id: str,
        source_value: str,
        existing_chat_id: int | None,
        mode: str,
        now: datetime,
    ) -> str:
        request = self._tdlib.build_search_public_chat_request(source_value)
        response = await self._tdlib.call(self._unwrap_request(request), timeout=30.0)
        if response is None:
            raise UpdateApplyRetryableError("searchPublicChat returned no response")

        if response.get("@type") == "error":
            access_state = self._classify_error_state(response)
            await self._repository.mark_channel_access_state(
                registry_id=registry_id,
                access_state=access_state,
                last_resolved_at=now,
                notes_append=self._error_note(response),
            )
            return access_state

        chat_id = self._safe_int(response.get("id"))
        if chat_id is None:
            raise UpdateApplyRetryableError("searchPublicChat response missing chat id")

        username_snapshot = self._extract_username_snapshot(response)
        title_snapshot = self._extract_title_snapshot(response)
        chat_type = self._extract_chat_type(response)

        await self._repository.mark_channel_resolved(
            registry_id=registry_id,
            chat_id=chat_id,
            username_snapshot=username_snapshot,
            title_snapshot=title_snapshot,
            chat_type=chat_type,
            access_state="resolved_not_joined",
            last_resolved_at=now,
        )

        # In recovery mode, probe access first if we already know the chat anchor.
        if mode == "access_recovery" and existing_chat_id is not None:
            return await self._probe_chat_access(
                registry_id=registry_id,
                chat_id=existing_chat_id,
                now=now,
            )

        join_request = self._tdlib.build_join_chat_request(chat_id)
        join_response = await self._tdlib.call(self._unwrap_request(join_request), timeout=30.0)
        access_state = self._classify_join_result(join_response)
        await self._repository.mark_channel_access_state(
            registry_id=registry_id,
            access_state=access_state,
            chat_id=chat_id,
            username_snapshot=username_snapshot,
            title_snapshot=title_snapshot,
            chat_type=chat_type,
            last_join_attempt_at=now,
            last_resolved_at=now,
            notes_append=None if access_state == "joined" else self._safe_response_note(join_response),
        )
        return access_state

    async def _sync_invite_link_row(
        self,
        *,
        registry_id: str,
        invite_link: str,
        mode: str,
        now: datetime,
    ) -> str:
        if mode == "join_requested":
            # Conservative policy: do not aggressively re-join pending invite approvals.
            return "no_change"

        request = self._tdlib.build_join_chat_by_invite_link_request(invite_link)
        response = await self._tdlib.call(self._unwrap_request(request), timeout=30.0)
        if response is None:
            raise UpdateApplyRetryableError("joinChatByInviteLink returned no response")

        if response.get("@type") == "error":
            access_state = self._classify_error_state(response)
            await self._repository.mark_channel_access_state(
                registry_id=registry_id,
                access_state=access_state,
                last_join_attempt_at=now,
                last_resolved_at=now,
                notes_append=self._error_note(response),
            )
            return access_state

        chat_id = self._safe_int(response.get("id"))
        username_snapshot = self._extract_username_snapshot(response)
        title_snapshot = self._extract_title_snapshot(response)
        chat_type = self._extract_chat_type(response)

        if chat_id is None:
            raise UpdateApplyRetryableError("joinChatByInviteLink response missing chat id")

        await self._repository.mark_channel_access_state(
            registry_id=registry_id,
            access_state="joined",
            chat_id=chat_id,
            username_snapshot=username_snapshot,
            title_snapshot=title_snapshot,
            chat_type=chat_type,
            last_join_attempt_at=now,
            last_resolved_at=now,
        )
        return "joined"

    async def _probe_chat_access(self, *, registry_id: str, chat_id: int, now: datetime) -> str:
        request = self._tdlib.build_get_chat_history_request(
            chat_id=chat_id,
            from_message_id=0,
            offset=0,
            limit=1,
            only_local=False,
        )
        response = await self._tdlib.call(self._unwrap_request(request), timeout=30.0)
        if response is None:
            raise UpdateApplyRetryableError("getChatHistory returned no response while probing chat access")

        if response.get("@type") == "error":
            access_state = self._classify_error_state(response)
            await self._repository.mark_channel_access_state(
                registry_id=registry_id,
                access_state=access_state,
                last_resolved_at=now,
                notes_append=self._error_note(response),
            )
            return access_state

        await self._repository.mark_channel_access_state(
            registry_id=registry_id,
            access_state="joined",
            chat_id=chat_id,
            last_resolved_at=now,
        )
        return "joined"

    @staticmethod
    def _unwrap_request(request: Any) -> JsonDict:
        payload = getattr(request, "payload", request)
        if not isinstance(payload, dict):
            raise UpdateApplyTerminalError("TDLib request payload must be a dict")
        return payload

    @staticmethod
    def _classify_join_result(response: JsonDict | None) -> str:
        if response is None:
            return "joined"
        if response.get("@type") != "error":
            return "joined"
        return ChannelRegistrySyncService._classify_error_state(response)

    @staticmethod
    def _classify_error_state(response: JsonDict) -> str:
        message = str(response.get("message", "")).upper()
        if "INVITE_REQUEST_SENT" in message:
            return "join_requested"
        if any(token in message for token in ("CHAT_NOT_FOUND", "INVITE_LINK_INVALID", "USERNAME_NOT_OCCUPIED")):
            return "not_found"
        if any(token in message for token in ("FORBIDDEN", "CHANNEL_PRIVATE", "USER_BANNED_IN_CHANNEL")):
            return "access_lost"
        return "access_lost"

    @staticmethod
    def _safe_int(value: Any) -> int | None:
        if isinstance(value, bool):
            return None
        if isinstance(value, int):
            return value
        try:
            return int(value)
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _extract_title_snapshot(chat_payload: JsonDict) -> str | None:
        value = chat_payload.get("title")
        return value if isinstance(value, str) and value.strip() else None

    @staticmethod
    def _extract_chat_type(chat_payload: JsonDict) -> str | None:
        raw = chat_payload.get("type")
        if not isinstance(raw, dict):
            return None
        type_name = raw.get("@type")
        if not isinstance(type_name, str):
            return None
        return type_name.removeprefix("chatType") or type_name

    @staticmethod
    def _extract_username_snapshot(chat_payload: JsonDict) -> str | None:
        usernames = chat_payload.get("usernames")
        if isinstance(usernames, dict):
            active = usernames.get("active_usernames")
            if isinstance(active, list) and active:
                candidate = active[0]
                if isinstance(candidate, str) and candidate.strip():
                    return candidate.strip()
        username = chat_payload.get("username")
        if isinstance(username, str) and username.strip():
            return username.strip()
        return None

    @staticmethod
    def _error_note(response: JsonDict) -> str:
        code = response.get("code")
        message = response.get("message")
        return f"tdlib_error code={code} message={message}"

    @staticmethod
    def _safe_response_note(response: JsonDict | None) -> str | None:
        if not isinstance(response, dict):
            return None
        if response.get("@type") != "error":
            return None
        return ChannelRegistrySyncService._error_note(response)
```

---

## 3. 구현 메모

### 3-1. 왜 `call()` 기반 protocol을 추가로 가정하는가

이 단계부터는 `searchPublicChat`, `joinChat`, `joinChatByInviteLink`, `getChatHistory`처럼 **request-response 성격의 TDLib 호출**이 필요하다. 그런데 현재 `TDLibClient` 초안은 `send()` / `receive()` 경계까지만 고정돼 있고, 상위에서 안전하게 RPC처럼 쓰는 `call()` helper는 아직 없다. 그래서 이번 문서는 **구조를 안 깨기 위해 high-level call protocol을 먼저 명시**하고, concrete TDLib wrapper 확장은 다음 실제 통합 커밋에서 붙이는 방향으로 뒀다. 이는 collector 경계를 유지하면서도 onboarding/reconcile 구현을 진행하기 위한 최소 변경안이다.

### 3-2. 왜 `registry_sync.py`가 repository protocol을 별도로 요구하는가

이전 `repositories.py` 초안은 `source_messages / source_message_versions / event_outbox` 중심이었다. 그런데 registry sync는 `telegram_channel_registry`에 대한 조회/상태 전이 메서드가 추가로 필요하다. 기존 repository를 억지로 이 파일에서 직접 수정해버리면 현재 단계 범위를 넘는다. 그래서 이번 문서는 **registry 전용 protocol을 먼저 선언**하고, 다음 실제 통합 단계에서 repository 확장 SQL을 붙이도록 남겼다.

### 3-3. reconcile에서 왜 `reconcile` reason으로 version/outbox를 남기나

3단계 collector 문서와 17/18단계 스켈레톤 문서는 reconcile을 단순 조회가 아니라 **history 기반 상태 보정 경계**로 본다. 따라서 history에서 새로 반영된 메시지나 실제 변화가 확인된 메시지는 `version_reason = reconcile`로 append하고, `source_message.reconciled.v1` outbox를 남기는 것이 맞다. 반대로 same-hash no-op이면 current touch만 하고 version/outbox를 만들지 않는다. 이게 중복 허용 + idempotent upsert 원칙과 맞다.

### 3-4. access 상태는 왜 보수적으로 유지하나

`join_requested`는 실패가 아니라 대기 상태이고, `access_lost`는 공격적으로 반복 재시도하지 말아야 한다는 점이 3단계와 17/18단계 문서에서 이미 고정됐다. 그래서 이 코드도 `join_requested`를 강제 재가입 대상으로 보지 않고, `access_lost`는 보수적으로 probe만 하도록 만들었다. collector는 운영자 확인 없이 사람 계정 인증/접근을 우회하려고 하면 안 된다.

---

## 4. 다음 단계

이제 다음 구현 묶음은 **C6: `health.py` + logger/metrics wiring** 이다.

순서가 이렇다.

1. `reconcile.py`
2. `registry_sync.py`
3. `health.py`
4. collector acceptance hardening

즉, 다음 턴에서는 readiness/heartbeat/lag/reconcile counters를 실제 코드로 내리는 **`health.py` 초안**으로 가면 된다.


---

## Source file: `24_collector_health_observability_code_draft_v0_1.md`

# 24단계: `collector-telegram` Health / Logger / Observability 실제 코드 초안 v0.1

## 0. 문서 목적

이 문서는 프로젝트 소스의 README/정본 단계 문서, `11_stage11_execution_contracts_v0_1.md`, `12_migration_spec_0001_0004_v0_1.md`, 그리고 `17~23` 단계 collector 구현 문서를 바탕으로,
`collector-telegram`의 **다음 구현 묶음**인 아래 항목의 실제 코드 초안을 제공한다.

- `health.py`
- `main.py` logger wiring 업데이트
- `runtime.py` health/heartbeat wiring 업데이트

이번 묶음의 목표는 다음 네 가지다.

1. collector 전용 **heartbeat / readiness / 주요 카운터** 경계를 코드로 고정
2. structured logging 필드와 health snapshot 발행 방식을 코드로 고정
3. runtime loop와 observability 계층의 연결을 명시적으로 고정
4. collector가 여전히 **원문/current/version/outbox/reconcile까지만** 담당하도록 유지하면서, 운영 상태를 설명 가능한 형태로 노출

이 문서는 아직 아래를 구현하지 않는다.

- Prometheus exporter
- HTTP health endpoint
- 실제 `outbox_pending_count` DB 집계 쿼리
- singleton lock 구현
- acceptance hardening 테스트 묶음 전체

즉, 이번 단계의 목적은 **collector observability 골격을 먼저 코드로 잠그는 것**이다.

---

## 1. 현재 단계 위치

현재 collector 구현 순서는 아래처럼 고정되어 있다.

1. C1 부팅 골격
2. C2 TDLib/Auth
3. C3 Repository/Outbox/Idempotency
4. C4 Projection/Dispatcher/Handlers
5. C5 Reconcile/Registry Sync
6. **이번 단계: Health / Logger / Observability**
7. 그 다음: **acceptance hardening**
8. collector가 끝나면 그 다음은 **`outbox-relay`**, 그 다음이 **`router-normalizer`**

즉, collector 내부 구현 기준으로는 이번 observability 묶음 다음에 acceptance hardening이 한 번 더 남아 있고, 그 이후에는 collector 바깥 서비스로 넘어간다.

---

## 2. 대상 파일 트리

```text
src/services/collector_telegram/
  health.py
  main.py        # updated
  runtime.py     # updated
```

---

## 3. 코드 초안

## 3-1. `src/services/collector_telegram/health.py`

```python
from __future__ import annotations

import logging
import threading
from collections import defaultdict, deque
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from .models import ReconcileSummary


CollectorHealthState = str



def _utcnow() -> datetime:
    return datetime.now(timezone.utc)



def _isoformat(value: datetime | None) -> str | None:
    if value is None:
        return None
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc).isoformat()


@dataclass(slots=True, frozen=True)
class CollectorHealthSnapshot:
    health_state: CollectorHealthState
    readiness: str
    tdlib_authorization_state: str | None
    started_at: str | None
    last_heartbeat_at: str | None
    last_update_received_at: str | None
    tracked_channels_active: int
    outbox_pending_count: int | None
    update_counters: dict[str, int]
    reconcile_runs_total: int
    reconcile_gap_fills_total: int
    loop_states: dict[str, str]
    last_successful_history_sync_at: dict[str, str]
    notes: list[str]


class CollectorHealthService:
    """In-memory collector observability state.

    Design constraints preserved from the stage documents:
    - structured logs and metrics are part of collector completion,
    - readiness/heartbeat must be explicit,
    - collector observability must remain collector-local and not reinterpret downstream state.

    This class is intentionally transport/exporter agnostic.
    It only owns in-memory state and snapshot generation.
    """

    _REQUIRED_RUNTIME_LOOPS = {
        "authorization_loop",
        "update_ingest_loop",
        "reconcile_scheduler_loop",
        "registry_refresh_loop",
        "health_publisher_loop",
    }

    def __init__(self, *, logger: logging.Logger | None = None) -> None:
        self._logger = logger or logging.getLogger(__name__)
        self._lock = threading.RLock()

        self._health_state: CollectorHealthState = "starting"
        self._tdlib_authorization_state: str | None = None
        self._started_at: datetime | None = None
        self._last_heartbeat_at: datetime | None = None
        self._last_update_received_at: datetime | None = None
        self._tracked_channels_active: int = 0
        self._outbox_pending_count: int | None = None

        self._update_counters: dict[str, int] = defaultdict(int)
        self._reconcile_runs_total: int = 0
        self._reconcile_gap_fills_total: int = 0
        self._last_successful_history_sync_at: dict[str, datetime] = {}
        self._loop_states: dict[str, str] = {}
        self._notes: deque[str] = deque(maxlen=25)

    # ------------------------------------------------------------------
    # Lifecycle / readiness state
    # ------------------------------------------------------------------
    def mark_starting(self, *, note: str | None = None) -> None:
        with self._lock:
            self._health_state = "starting"
            self._started_at = self._started_at or _utcnow()
            self._last_heartbeat_at = _utcnow()
            if note:
                self._notes.append(note)

    def mark_ready(self, *, note: str | None = None) -> None:
        with self._lock:
            self._health_state = "ready"
            self._started_at = self._started_at or _utcnow()
            self._last_heartbeat_at = _utcnow()
            if note:
                self._notes.append(note)

    def mark_degraded(self, *, note: str | None = None) -> None:
        with self._lock:
            self._health_state = "degraded"
            self._last_heartbeat_at = _utcnow()
            if note:
                self._notes.append(note)

    def mark_failing(self, *, note: str | None = None) -> None:
        with self._lock:
            self._health_state = "failing"
            self._last_heartbeat_at = _utcnow()
            if note:
                self._notes.append(note)

    def mark_stopped(self, *, note: str | None = None) -> None:
        with self._lock:
            self._health_state = "stopped"
            self._last_heartbeat_at = _utcnow()
            if note:
                self._notes.append(note)

    def mark_authorization_state(self, state_name: str | None) -> None:
        with self._lock:
            self._tdlib_authorization_state = state_name
            self._last_heartbeat_at = _utcnow()

    def mark_runtime_loop_started(self, loop_name: str) -> None:
        with self._lock:
            self._loop_states[loop_name] = "running"
            self._last_heartbeat_at = _utcnow()

    def mark_runtime_loop_stopped(self, loop_name: str) -> None:
        with self._lock:
            self._loop_states[loop_name] = "stopped"
            self._last_heartbeat_at = _utcnow()

    def heartbeat(self) -> None:
        with self._lock:
            self._last_heartbeat_at = _utcnow()

    # ------------------------------------------------------------------
    # Metrics-like counters
    # ------------------------------------------------------------------
    def mark_update_received(self, update_type: str) -> None:
        with self._lock:
            self._update_counters[update_type] += 1
            self._last_update_received_at = _utcnow()
            self._last_heartbeat_at = self._last_update_received_at

    def mark_reconcile_result(self, summary: ReconcileSummary) -> None:
        with self._lock:
            self._reconcile_runs_total += 1
            self._reconcile_gap_fills_total += int(summary.gap_filled_count)
            self._last_heartbeat_at = _utcnow()
            if summary.error_code is None:
                self._last_successful_history_sync_at[str(summary.chat_id)] = _utcnow()
            else:
                self._notes.append(
                    f"reconcile chat={summary.chat_id} result={summary.result_type} error={summary.error_code}"
                )

    def mark_tracked_channels_active(self, count: int) -> None:
        with self._lock:
            self._tracked_channels_active = max(0, count)
            self._last_heartbeat_at = _utcnow()

    def set_outbox_pending_count(self, count: int | None) -> None:
        with self._lock:
            self._outbox_pending_count = None if count is None else max(0, count)
            self._last_heartbeat_at = _utcnow()

    # ------------------------------------------------------------------
    # Snapshot / readiness
    # ------------------------------------------------------------------
    def readiness(self) -> str:
        with self._lock:
            if self._health_state in {"failing", "stopped"}:
                return self._health_state

            missing_loops = self._REQUIRED_RUNTIME_LOOPS - {
                name for name, state in self._loop_states.items() if state == "running"
            }
            if missing_loops:
                return "starting"

            auth_state = self._tdlib_authorization_state
            if auth_state and auth_state != "authorizationStateReady":
                return "degraded"

            return self._health_state

    def heartbeat_age_seconds(self) -> float | None:
        with self._lock:
            if self._last_heartbeat_at is None:
                return None
            return (_utcnow() - self._last_heartbeat_at).total_seconds()

    def snapshot(self) -> CollectorHealthSnapshot:
        with self._lock:
            return CollectorHealthSnapshot(
                health_state=self._health_state,
                readiness=self.readiness(),
                tdlib_authorization_state=self._tdlib_authorization_state,
                started_at=_isoformat(self._started_at),
                last_heartbeat_at=_isoformat(self._last_heartbeat_at),
                last_update_received_at=_isoformat(self._last_update_received_at),
                tracked_channels_active=self._tracked_channels_active,
                outbox_pending_count=self._outbox_pending_count,
                update_counters=dict(sorted(self._update_counters.items())),
                reconcile_runs_total=self._reconcile_runs_total,
                reconcile_gap_fills_total=self._reconcile_gap_fills_total,
                loop_states=dict(sorted(self._loop_states.items())),
                last_successful_history_sync_at={
                    key: _isoformat(value) or ""
                    for key, value in sorted(self._last_successful_history_sync_at.items())
                },
                notes=list(self._notes),
            )

    def snapshot_dict(self) -> dict[str, Any]:
        snapshot = self.snapshot()
        return {
            "health_state": snapshot.health_state,
            "readiness": snapshot.readiness,
            "tdlib_authorization_state": snapshot.tdlib_authorization_state,
            "started_at": snapshot.started_at,
            "last_heartbeat_at": snapshot.last_heartbeat_at,
            "last_update_received_at": snapshot.last_update_received_at,
            "tracked_channels_active": snapshot.tracked_channels_active,
            "outbox_pending_count": snapshot.outbox_pending_count,
            "update_counters": snapshot.update_counters,
            "reconcile_runs_total": snapshot.reconcile_runs_total,
            "reconcile_gap_fills_total": snapshot.reconcile_gap_fills_total,
            "loop_states": snapshot.loop_states,
            "last_successful_history_sync_at": snapshot.last_successful_history_sync_at,
            "notes": snapshot.notes,
            "heartbeat_age_seconds": self.heartbeat_age_seconds(),
        }
```

---

## 3-2. `src/services/collector_telegram/runtime.py` (updated)

```python
from __future__ import annotations

import asyncio
import contextlib
import logging
from datetime import datetime, timezone

from .config import CollectorTelegramConfig
from .health import CollectorHealthService
from .models import RuntimeSnapshot


class CollectorRuntime:
    """Runtime orchestration skeleton with health/observability wiring.

    This stage still does not wire concrete TDLib/DB/update flows directly.
    Its responsibility here is to make loop lifecycle and collector-local
    observability explicit and stable.
    """

    def __init__(
        self,
        config: CollectorTelegramConfig,
        *,
        health: CollectorHealthService,
        logger: logging.Logger | None = None,
    ) -> None:
        self._config = config
        self._health = health
        self._logger = logger or logging.getLogger(__name__)
        self._stop_event = asyncio.Event()
        self._tasks: list[asyncio.Task[None]] = []
        self._snapshot = RuntimeSnapshot()

    @property
    def snapshot(self) -> RuntimeSnapshot:
        return self._snapshot

    async def run_forever(self) -> None:
        self._logger.info(
            "collector_runtime_starting",
            extra={
                "service": "collector-telegram",
                "event": "collector_runtime_starting",
                "collector_mode": self._config.collector_mode,
                "app_env": self._config.app_env,
                "stage": "collector_runtime",
            },
        )
        self._snapshot.started_at = datetime.now(timezone.utc)
        self._snapshot.health_state = "starting"

        self._health.mark_starting(note="collector runtime booting")
        self._health.mark_tracked_channels_active(0)
        self._health.mark_authorization_state(None)

        self._tasks = [
            asyncio.create_task(self._authorization_loop(), name="collector.authorization"),
            asyncio.create_task(self._update_ingest_loop(), name="collector.update_ingest"),
            asyncio.create_task(self._reconcile_scheduler_loop(), name="collector.reconcile_scheduler"),
            asyncio.create_task(self._registry_refresh_loop(), name="collector.registry_refresh"),
            asyncio.create_task(self._health_publisher_loop(), name="collector.health"),
        ]

        # Runtime-level ready means the loop set is alive.
        # Auth-specific degraded/ready refinement happens via health service updates.
        self._snapshot.health_state = "ready"
        self._health.mark_ready(note="collector runtime loops started")

        try:
            await self._stop_event.wait()
        finally:
            await self.shutdown()

    async def shutdown(self) -> None:
        already_stopping = self._stop_event.is_set()
        if not already_stopping:
            self._stop_event.set()

        self._snapshot.health_state = "stopped"
        self._health.mark_stopped(note="collector runtime stopping")

        current_task = asyncio.current_task()
        for task in self._tasks:
            if task is current_task:
                continue
            task.cancel()

        for task in self._tasks:
            if task is current_task:
                continue
            with contextlib.suppress(asyncio.CancelledError):
                await task

        self._logger.info(
            "collector_runtime_stopped",
            extra={
                "service": "collector-telegram",
                "event": "collector_runtime_stopped",
                "stage": "collector_runtime",
            },
        )

    async def _authorization_loop(self) -> None:
        await self._idle_loop(
            loop_name="authorization_loop",
            interval_sec=5,
            on_tick=self._authorization_tick,
        )

    async def _update_ingest_loop(self) -> None:
        await self._idle_loop(
            loop_name="update_ingest_loop",
            interval_sec=2,
            on_tick=self._update_ingest_tick,
        )

    async def _reconcile_scheduler_loop(self) -> None:
        await self._idle_loop(
            loop_name="reconcile_scheduler_loop",
            interval_sec=float(self._config.reconcile_interval_sec),
            on_tick=self._reconcile_scheduler_tick,
        )

    async def _registry_refresh_loop(self) -> None:
        await self._idle_loop(
            loop_name="registry_refresh_loop",
            interval_sec=60,
            on_tick=self._registry_refresh_tick,
        )

    async def _health_publisher_loop(self) -> None:
        await self._idle_loop(
            loop_name="health_publisher_loop",
            interval_sec=30,
            on_tick=self._health_publisher_tick,
        )

    async def _idle_loop(self, loop_name: str, *, interval_sec: float, on_tick) -> None:
        self._health.mark_runtime_loop_started(loop_name)
        self._logger.info(
            "collector_loop_started",
            extra={
                "service": "collector-telegram",
                "event": "collector_loop_started",
                "stage": "collector_runtime",
                "loop_name": loop_name,
                "interval_sec": interval_sec,
            },
        )

        try:
            while not self._stop_event.is_set():
                self._snapshot.last_tick_at = datetime.now(timezone.utc)
                self._health.heartbeat()
                await on_tick()
                await asyncio.sleep(interval_sec)
        except asyncio.CancelledError:
            self._logger.info(
                "collector_loop_cancelled",
                extra={
                    "service": "collector-telegram",
                    "event": "collector_loop_cancelled",
                    "stage": "collector_runtime",
                    "loop_name": loop_name,
                },
            )
            raise
        except Exception:
            self._snapshot.health_state = "degraded"
            self._health.mark_degraded(note=f"loop failure: {loop_name}")
            self._logger.exception(
                "collector_loop_failed",
                extra={
                    "service": "collector-telegram",
                    "event": "collector_loop_failed",
                    "stage": "collector_runtime",
                    "loop_name": loop_name,
                    "status": "failed",
                },
            )
            raise
        finally:
            self._health.mark_runtime_loop_stopped(loop_name)

    async def _authorization_tick(self) -> None:
        # Placeholder until TDLib/auth FSM wiring is connected in the next integration pass.
        if self._health.snapshot().tdlib_authorization_state is None:
            self._health.mark_authorization_state("authorizationStateReady")

    async def _update_ingest_tick(self) -> None:
        # Placeholder heartbeat-only tick. Real update ingestion will call
        # health.mark_update_received(update_type) from dispatcher path.
        return None

    async def _reconcile_scheduler_tick(self) -> None:
        # Placeholder. Real scheduler integration will attach ReconcileService later.
        return None

    async def _registry_refresh_tick(self) -> None:
        # Placeholder. Real registry integration will update tracked channel counts later.
        return None

    async def _health_publisher_tick(self) -> None:
        snapshot = self._health.snapshot_dict()
        self._logger.info(
            "collector_health_snapshot",
            extra={
                "service": "collector-telegram",
                "event": "collector_health_snapshot",
                "stage": "collector_observability",
                **snapshot,
            },
        )
```

---

## 3-3. `src/services/collector_telegram/main.py` (updated)

```python
from __future__ import annotations

import asyncio
import json
import logging
import signal
import sys
from datetime import datetime, timezone
from typing import Any

from .config import CollectorTelegramConfig
from .exceptions import ConfigurationError
from .health import CollectorHealthService
from .runtime import CollectorRuntime
from .service import CollectorTelegramService


_BASE_RECORD_KEYS = set(logging.makeLogRecord({}).__dict__.keys())


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "ts": datetime.now(timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }

        for key, value in record.__dict__.items():
            if key in _BASE_RECORD_KEYS or key in {"message", "asctime"}:
                continue
            if key.startswith("_"):
                continue
            payload[key] = self._normalize(value)

        if record.exc_info:
            payload["exc_info"] = self.formatException(record.exc_info)

        return json.dumps(payload, ensure_ascii=False)

    def _normalize(self, value: Any) -> Any:
        if isinstance(value, datetime):
            if value.tzinfo is None:
                value = value.replace(tzinfo=timezone.utc)
            return value.astimezone(timezone.utc).isoformat()
        if isinstance(value, dict):
            return {str(k): self._normalize(v) for k, v in value.items()}
        if isinstance(value, (list, tuple)):
            return [self._normalize(v) for v in value]
        if isinstance(value, (str, int, float, bool)) or value is None:
            return value
        return str(value)



def build_logger(log_level: str) -> logging.Logger:
    logger = logging.getLogger("collector_telegram")
    logger.setLevel(getattr(logging, log_level.upper(), logging.INFO))
    logger.handlers.clear()

    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(JsonFormatter())
    logger.addHandler(handler)
    logger.propagate = False
    return logger


async def _run() -> int:
    try:
        config = CollectorTelegramConfig.from_env()
    except ConfigurationError as exc:
        print(
            json.dumps(
                {
                    "ts": datetime.now(timezone.utc).isoformat(),
                    "level": "ERROR",
                    "service": "collector-telegram",
                    "event": "collector_config_invalid",
                    "stage": "collector_bootstrap",
                    "error": str(exc),
                },
                ensure_ascii=False,
            ),
            file=sys.stderr,
        )
        return 2

    logger = build_logger(config.log_level)
    health = CollectorHealthService(logger=logger.getChild("health"))
    runtime = CollectorRuntime(
        config,
        health=health,
        logger=logger.getChild("runtime"),
    )
    service = CollectorTelegramService(config, runtime, logger=logger)

    loop = asyncio.get_running_loop()

    def _schedule_stop() -> None:
        loop.create_task(service.stop())

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _schedule_stop)
        except NotImplementedError:
            signal.signal(sig, lambda *_: _schedule_stop())

    try:
        await service.run()
    except asyncio.CancelledError:
        logger.info(
            "collector_main_cancelled",
            extra={
                "service": "collector-telegram",
                "event": "collector_main_cancelled",
                "stage": "collector_bootstrap",
            },
        )
        return 0
    except Exception:
        logger.exception(
            "collector_main_failed",
            extra={
                "service": "collector-telegram",
                "event": "collector_main_failed",
                "stage": "collector_bootstrap",
            },
        )
        return 1

    return 0



def main() -> None:
    raise SystemExit(asyncio.run(_run()))


if __name__ == "__main__":
    main()
```

---

## 4. 구현 메모

### 4-1. 왜 `health.py`를 runtime 안에 섞지 않았는가

8단계 observability 문서는 telemetry를

- structured logs
- metrics
- durable audit rows

로 분리해서 보라고 잠가뒀다. collector 쪽에서는 아직 durable audit row를 직접 늘리는 단계가 아니므로, 이번 구현은 **in-memory health/metrics snapshot + structured log publishing**까지만 고정했다. runtime 루프 안에 카운터를 흩뿌리면 이후 exporter나 health endpoint를 붙일 때 다시 뜯어야 하므로 `CollectorHealthService`를 별도 경계로 두는 편이 맞다.

### 4-2. 왜 `tdlib_authorization_state`가 아직 placeholder처럼 보이는가

이번 단계의 범위는 observability wiring이지, TDLib/Auth와 runtime의 full integration 재설계가 아니다. 이미 C2에서 `tdlib_client.py`와 `auth_fsm.py` 경계는 만들어졌고, 지금은 그 위에 **health hook를 꽂을 자리**를 먼저 고정하는 것이 맞다. 그래서 `runtime.py`에서는 authorization loop가 최소한 health state를 갱신하는 골격만 넣고, concrete TDLib/Auth 호출 wiring은 acceptance hardening 또는 실제 통합 커밋에서 붙이는 것이 구조적으로 안전하다.

### 4-3. 왜 `outbox_pending_count`는 setter만 두고 직접 집계하지 않았는가

현재 repository 초안은 `0001_ingest_core` 핵심 write contract에 집중돼 있고, observability 전용 read query를 대량 추가하는 턴이 아니었다. 따라서 이번 단계에서는 `CollectorHealthService.set_outbox_pending_count()` 자리만 만들고, 이후 repository 확장이나 outbox-relay 구현 단계에서 실제 집계 값을 꽂는 방향이 맞다. 지금 여기서 억지로 query를 추가하면 단계 책임이 섞인다.

### 4-4. readiness를 왜 완전 엄격하게 잠그지 않았는가

17단계 수용 기준상 readiness에는

- authorization ready
- DB 연결 정상
- active channel set 로드 성공
- update ingest loop 동작

이 포함돼야 한다. 다만 이번 코드는 observability 골격 단계라 concrete DB/TDLib wiring이 아직 다 연결되지 않았다. 그래서 readiness는

- 필수 loop running 여부
- 명시적 failing/stopped 여부
- TDLib auth state가 알려져 있을 때 degraded 반영

까지만 먼저 고정했다. 이건 최종 readiness가 아니라 **관찰 가능한 readiness 골격**이다. acceptance hardening 단계에서 DB probe와 auth probe를 붙이면 된다.

---

## 5. 다음 단계

이제 collector 내부에서 남은 것은 **acceptance hardening** 묶음이다.

즉, 다음 단계는 아래가 맞다.

1. `health.py` 실제 통합 마무리
2. runtime/service에 TDLib/Auth 및 registry/reconcile 실제 hook 연결 보강
3. duplicate/no-op/restart/degraded path 테스트 보강
4. prod single-instance 제약 점검

그 다음에야 collector 내부 구현을 일단락하고, **`outbox-relay`** 로 넘어가는 순서가 맞다.


---

## Source file: `25_collector_acceptance_hardening_code_draft_v0_1.md`

# 25단계: `collector-telegram` Acceptance Hardening 실제 코드 초안 v0.1

## 0. 문서 목적

이 문서는 프로젝트 소스의 README/정본 단계 문서, `11_stage11_execution_contracts_v0_1.md`, `12_migration_spec_0001_0004_v0_1.md`, 그리고 `17~24` 단계 collector 구현 문서를 바탕으로,
`collector-telegram`의 **마지막 내부 구현 묶음(C6)** 인 아래 항목의 실제 코드 초안을 제공한다.

- `exceptions.py` 업데이트
- `config.py` 업데이트
- `singleton_guard.py` 신규
- `service.py` 업데이트
- `runtime.py` 업데이트
- acceptance hardening 테스트 초안

이번 묶음의 목표는 다음 네 가지다.

1. **prod single-instance 제약**을 코드 경계로 고정
2. **startup/restart recovery**를 서비스 시작 경로에 명시적으로 연결
3. **degraded/manual intervention path**를 health/runtime에 실제 반영
4. duplicate/no-op/restart/degraded path를 **테스트 관점에서 고정**

이 문서는 아직 아래를 구현하지 않는다.

- Docker Compose/service 배포 파일
- 실제 Redis singleton lock
- 실제 DB readiness probe SQL
- Prometheus exporter / HTTP health endpoint
- `outbox-relay`
- `router-normalizer`

즉, 이번 단계의 목적은 **collector 내부 구현을 acceptance 수준까지 닫는 것**이다.

---

## 1. 대상 파일 트리

```text
src/services/collector_telegram/
  exceptions.py              # updated
  config.py                  # updated
  singleton_guard.py         # new
  service.py                 # updated
  runtime.py                 # updated

tests/
  unit/
    services/
      collector_telegram/
        test_singleton_guard.py
        test_runtime_manual_intervention.py
  component/
    services/
      collector_telegram/
        test_runtime_startup_acceptance.py
        test_service_single_instance.py
```

---

## 2. 코드 초안

## 2-1. `src/services/collector_telegram/exceptions.py` (updated)

```python
from __future__ import annotations


class CollectorError(Exception):
    """Base class for collector-specific failures."""


class ConfigurationError(CollectorError):
    """Raised when configuration is invalid or incomplete."""


class AuthorizationError(CollectorError):
    """Raised when TDLib authorization flow is invalid or broken."""


class AuthorizationManualInterventionRequired(AuthorizationError):
    """Raised when operator action is required to continue authorization."""


class TDLibTransportError(CollectorError):
    """Raised for low-level TDLib transport failures."""


class RepositoryInvariantError(CollectorError):
    """Raised when persistence invariants are broken.

    These are normally terminal and should fail fast.
    """


class UpdateApplyRetryableError(CollectorError):
    """Raised when an update application may succeed on retry."""


class UpdateApplyTerminalError(CollectorError):
    """Raised when an update application must not be retried as-is."""


class ReconcileRetryableError(CollectorError):
    """Raised when reconcile may succeed on retry."""


class ReconcileTerminalError(CollectorError):
    """Raised when reconcile encountered a terminal condition."""


class SingletonViolationError(CollectorError):
    """Raised when prod single-instance collector guard is violated."""
```

---

## 2-2. `src/services/collector_telegram/config.py` (updated)

```python
from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from .exceptions import ConfigurationError
from .models import AppEnv, CollectorMode


_ALLOWED_APP_ENVS = {"prod", "dev", "test"}
_ALLOWED_MODES = {"live", "replay"}


def _read_text_file(path_str: str, *, field_name: str) -> str:
    path = Path(path_str)
    if not path.exists():
        raise ConfigurationError(f"{field_name} file does not exist: {path}")
    if not path.is_file():
        raise ConfigurationError(f"{field_name} path is not a file: {path}")

    value = path.read_text(encoding="utf-8").strip()
    if not value:
        raise ConfigurationError(f"{field_name} file is empty: {path}")
    return value


def _read_secret(
    *,
    env_name: str,
    allow_empty: bool = False,
    default: str | None = None,
) -> str | None:
    file_env_name = f"{env_name}_FILE"
    file_value = os.getenv(file_env_name)
    direct_value = os.getenv(env_name)

    value: str | None
    if file_value:
        value = _read_text_file(file_value, field_name=file_env_name)
    else:
        value = direct_value if direct_value is not None else default

    if value is None:
        return None

    if not value and not allow_empty:
        raise ConfigurationError(f"{env_name} is empty")
    return value


def _read_required(env_name: str) -> str:
    value = os.getenv(env_name)
    if value is None or not value.strip():
        raise ConfigurationError(f"Missing required environment variable: {env_name}")
    return value.strip()


def _read_required_int(env_name: str) -> int:
    raw = _read_required(env_name)
    try:
        return int(raw)
    except ValueError as exc:
        raise ConfigurationError(f"{env_name} must be an integer: {raw}") from exc


def _read_int(env_name: str, *, default: int) -> int:
    raw = os.getenv(env_name)
    if raw is None or not raw.strip():
        return default
    try:
        return int(raw)
    except ValueError as exc:
        raise ConfigurationError(f"{env_name} must be an integer: {raw}") from exc


@dataclass(slots=True, frozen=True)
class CollectorTelegramConfig:
    app_env: AppEnv
    database_url: str
    redis_url: str | None
    collector_mode: CollectorMode

    telegram_api_id: int
    telegram_api_hash: str
    telegram_phone_number: str
    telegram_2fa_password: str | None

    tdlib_state_dir: str
    tdlib_files_dir: str
    tdlib_db_encryption_key: str

    reconcile_interval_sec: int
    reconcile_backfill_limit: int
    warm_backfill_limit: int
    history_page_limit: int

    singleton_lock_path: str
    startup_probe_timeout_sec: int
    startup_warm_backfill_enabled: bool

    log_level: str = "INFO"

    @classmethod
    def from_env(cls) -> "CollectorTelegramConfig":
        app_env = os.getenv("APP_ENV", "dev").strip().lower()
        collector_mode = os.getenv("COLLECTOR_MODE", "replay").strip().lower()
        tdlib_state_dir = _read_required("TDLIB_STATE_DIR")

        config = cls(
            app_env=app_env,  # type: ignore[arg-type]
            database_url=_read_required("DATABASE_URL"),
            redis_url=os.getenv("REDIS_URL"),
            collector_mode=collector_mode,  # type: ignore[arg-type]
            telegram_api_id=_read_required_int("TELEGRAM_API_ID"),
            telegram_api_hash=_read_secret(env_name="TELEGRAM_API_HASH"),
            telegram_phone_number=_read_required("TELEGRAM_PHONE_NUMBER"),
            telegram_2fa_password=_read_secret(
                env_name="TELEGRAM_2FA_PASSWORD",
                allow_empty=True,
                default=None,
            ),
            tdlib_state_dir=tdlib_state_dir,
            tdlib_files_dir=os.getenv("TDLIB_FILES_DIR", "").strip() or tdlib_state_dir,
            tdlib_db_encryption_key=_read_secret(env_name="TDLIB_DB_ENCRYPTION_KEY"),
            reconcile_interval_sec=_read_int("RECONCILE_INTERVAL_SEC", default=300),
            reconcile_backfill_limit=_read_int("RECONCILE_BACKFILL_LIMIT", default=50),
            warm_backfill_limit=_read_int("WARM_BACKFILL_LIMIT", default=30),
            history_page_limit=_read_int("HISTORY_PAGE_LIMIT", default=50),
            singleton_lock_path=(
                os.getenv("COLLECTOR_SINGLETON_LOCK_PATH", "").strip()
                or str(Path(tdlib_state_dir) / "collector-live.lock")
            ),
            startup_probe_timeout_sec=_read_int("STARTUP_PROBE_TIMEOUT_SEC", default=30),
            startup_warm_backfill_enabled=(
                os.getenv("STARTUP_WARM_BACKFILL_ENABLED", "true").strip().lower() not in {"0", "false", "no"}
            ),
            log_level=os.getenv("LOG_LEVEL", "INFO").strip().upper(),
        )
        config.validate()
        return config

    def validate(self) -> None:
        if self.app_env not in _ALLOWED_APP_ENVS:
            raise ConfigurationError(f"APP_ENV must be one of {_ALLOWED_APP_ENVS}, got: {self.app_env}")

        if self.collector_mode not in _ALLOWED_MODES:
            raise ConfigurationError(
                f"COLLECTOR_MODE must be one of {_ALLOWED_MODES}, got: {self.collector_mode}"
            )

        if self.app_env == "prod" and self.collector_mode != "live":
            raise ConfigurationError("prod environment requires COLLECTOR_MODE=live")

        if self.app_env in {"dev", "test"} and self.collector_mode == "live":
            raise ConfigurationError("dev/test environment must not use COLLECTOR_MODE=live")

        if self.reconcile_interval_sec <= 0:
            raise ConfigurationError("RECONCILE_INTERVAL_SEC must be > 0")

        if self.reconcile_backfill_limit <= 0 or self.reconcile_backfill_limit > 100:
            raise ConfigurationError("RECONCILE_BACKFILL_LIMIT must be between 1 and 100")

        if self.warm_backfill_limit <= 0 or self.warm_backfill_limit > 100:
            raise ConfigurationError("WARM_BACKFILL_LIMIT must be between 1 and 100")

        if self.history_page_limit <= 0 or self.history_page_limit > 100:
            raise ConfigurationError("HISTORY_PAGE_LIMIT must be between 1 and 100")

        if self.startup_probe_timeout_sec <= 0:
            raise ConfigurationError("STARTUP_PROBE_TIMEOUT_SEC must be > 0")

        if not self.telegram_api_hash:
            raise ConfigurationError("TELEGRAM_API_HASH must be configured")

        if not self.tdlib_db_encryption_key:
            raise ConfigurationError("TDLIB_DB_ENCRYPTION_KEY must be configured")

        if not self.singleton_lock_path.strip():
            raise ConfigurationError("COLLECTOR_SINGLETON_LOCK_PATH must not be empty")

    def ensure_runtime_dirs(self) -> None:
        Path(self.tdlib_state_dir).mkdir(parents=True, exist_ok=True)
        Path(self.tdlib_files_dir).mkdir(parents=True, exist_ok=True)
        Path(self.singleton_lock_path).parent.mkdir(parents=True, exist_ok=True)
```

---

## 2-3. `src/services/collector_telegram/singleton_guard.py` (new)

```python
from __future__ import annotations

import fcntl
import os
from dataclasses import dataclass
from pathlib import Path

from .exceptions import SingletonViolationError


@dataclass(slots=True)
class CollectorSingletonGuard:
    """Process-level singleton guard for live collector.

    This is intentionally Linux/VPS-oriented because prod runtime is already fixed to
    a single Ubuntu-class VPS. `flock` releases automatically when the process exits,
    which makes it suitable for restart recovery without stale manual cleanup.
    """

    lock_path: str
    _fd: int | None = None

    def acquire(self) -> None:
        if self._fd is not None:
            return

        path = Path(self.lock_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        fd = os.open(path, os.O_CREAT | os.O_RDWR, 0o600)

        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            os.close(fd)
            raise SingletonViolationError(
                f"collector singleton already held: {self.lock_path}"
            ) from exc

        os.ftruncate(fd, 0)
        os.write(fd, str(os.getpid()).encode("utf-8"))
        self._fd = fd

    def release(self) -> None:
        if self._fd is None:
            return
        try:
            fcntl.flock(self._fd, fcntl.LOCK_UN)
        finally:
            os.close(self._fd)
            self._fd = None

    def is_held(self) -> bool:
        return self._fd is not None

    def __enter__(self) -> "CollectorSingletonGuard":
        self.acquire()
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.release()
```

---

## 2-4. `src/services/collector_telegram/service.py` (updated)

```python
from __future__ import annotations

import logging

from .config import CollectorTelegramConfig
from .runtime import CollectorRuntime
from .singleton_guard import CollectorSingletonGuard


class CollectorTelegramService:
    def __init__(
        self,
        config: CollectorTelegramConfig,
        runtime: CollectorRuntime,
        *,
        singleton_guard: CollectorSingletonGuard | None = None,
        logger: logging.Logger | None = None,
    ) -> None:
        self._config = config
        self._runtime = runtime
        self._logger = logger or logging.getLogger(__name__)
        self._singleton_guard = singleton_guard or CollectorSingletonGuard(
            lock_path=config.singleton_lock_path,
        )
        self._started = False

    async def start(self) -> None:
        if self._started:
            return

        self._config.validate()
        self._config.ensure_runtime_dirs()

        if self._config.collector_mode == "live":
            self._singleton_guard.acquire()

        self._logger.info(
            "collector_service_starting",
            extra={
                "service": "collector-telegram",
                "event": "collector_service_starting",
                "collector_mode": self._config.collector_mode,
                "app_env": self._config.app_env,
                "stage": "collector_acceptance_hardening",
            },
        )

        try:
            await self._runtime.startup_acceptance_check()
        except Exception:
            if self._config.collector_mode == "live":
                self._singleton_guard.release()
            raise

        self._started = True

    async def run(self) -> None:
        if not self._started:
            await self.start()

        await self._runtime.run_forever()

    async def stop(self) -> None:
        if not self._started:
            return

        self._logger.info(
            "collector_service_stopping",
            extra={
                "service": "collector-telegram",
                "event": "collector_service_stopping",
                "stage": "collector_acceptance_hardening",
            },
        )
        try:
            await self._runtime.shutdown()
        finally:
            if self._config.collector_mode == "live":
                self._singleton_guard.release()
            self._started = False
```

---

## 2-5. `src/services/collector_telegram/runtime.py` (updated)

```python
from __future__ import annotations

import asyncio
import contextlib
import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Protocol

from .config import CollectorTelegramConfig
from .exceptions import AuthorizationManualInterventionRequired
from .health import CollectorHealthService
from .models import ReconcileSummary, RuntimeSnapshot, TrackedChat


@dataclass(slots=True, frozen=True)
class AuthorizationPumpResult:
    state_name: str | None = None
    requires_manual_intervention: bool = False
    note: str | None = None


@dataclass(slots=True, frozen=True)
class UpdateIngestBatchResult:
    update_counts: dict[str, int] = field(default_factory=dict)


class AuthorizationPumpProtocol(Protocol):
    async def pump_once(self) -> AuthorizationPumpResult | None: ...


class UpdateIngestRunnerProtocol(Protocol):
    async def pump_once(self) -> UpdateIngestBatchResult | None: ...


class RegistrySyncProtocol(Protocol):
    async def load_active_channels(self) -> list[TrackedChat]: ...
    async def sync_unresolved_channels(self): ...
    async def sync_join_requested_channels(self): ...
    async def sync_access_lost_channels(self): ...


class ReconcileProtocol(Protocol):
    async def run_startup_warm_backfill(self, chat_id: int) -> ReconcileSummary: ...
    async def run_scheduled_targets(self, *, limit: int = 20) -> list[ReconcileSummary]: ...


class CollectorRuntime:
    """Runtime orchestration skeleton with acceptance hardening hooks.

    This step closes the collector-local acceptance gap by wiring:
    - startup warm backfill,
    - active channel loading,
    - manual intervention degraded state,
    - per-loop health transitions.
    """

    def __init__(
        self,
        config: CollectorTelegramConfig,
        *,
        health: CollectorHealthService,
        authorization_pump: AuthorizationPumpProtocol | None = None,
        update_ingest_runner: UpdateIngestRunnerProtocol | None = None,
        registry_sync: RegistrySyncProtocol | None = None,
        reconcile: ReconcileProtocol | None = None,
        logger: logging.Logger | None = None,
    ) -> None:
        self._config = config
        self._health = health
        self._authorization_pump = authorization_pump
        self._update_ingest_runner = update_ingest_runner
        self._registry_sync = registry_sync
        self._reconcile = reconcile
        self._logger = logger or logging.getLogger(__name__)
        self._stop_event = asyncio.Event()
        self._tasks: list[asyncio.Task[None]] = []
        self._snapshot = RuntimeSnapshot()

    @property
    def snapshot(self) -> RuntimeSnapshot:
        return self._snapshot

    async def startup_acceptance_check(self) -> None:
        """Startup validation before entering the forever-loop path.

        Acceptance goals covered here:
        - active channel set can be loaded,
        - startup warm backfill exists,
        - health/readiness state is updated,
        - restart recovery has an explicit entrypoint.
        """
        self._health.mark_starting(note="collector startup acceptance check begin")

        active_channels: list[TrackedChat] = []
        if self._registry_sync is not None:
            active_channels = await self._registry_sync.load_active_channels()
            self._health.mark_tracked_channels_active(len(active_channels))

        if self._config.startup_warm_backfill_enabled and self._reconcile is not None:
            for chat in active_channels:
                if chat.chat_id is None:
                    continue
                summary = await self._reconcile.run_startup_warm_backfill(int(chat.chat_id))
                self._health.mark_reconcile_result(summary)

        self._snapshot.health_state = "ready"
        self._health.mark_ready(note="collector startup acceptance check complete")

    async def run_forever(self) -> None:
        self._logger.info(
            "collector_runtime_starting",
            extra={
                "service": "collector-telegram",
                "event": "collector_runtime_starting",
                "collector_mode": self._config.collector_mode,
                "app_env": self._config.app_env,
                "stage": "collector_runtime",
            },
        )
        self._snapshot.started_at = datetime.now(timezone.utc)
        self._snapshot.health_state = "starting"

        self._health.mark_starting(note="collector runtime booting")
        self._health.mark_authorization_state(None)

        self._tasks = [
            asyncio.create_task(self._authorization_loop(), name="collector.authorization"),
            asyncio.create_task(self._update_ingest_loop(), name="collector.update_ingest"),
            asyncio.create_task(self._reconcile_scheduler_loop(), name="collector.reconcile_scheduler"),
            asyncio.create_task(self._registry_refresh_loop(), name="collector.registry_refresh"),
            asyncio.create_task(self._health_publisher_loop(), name="collector.health"),
        ]

        self._snapshot.health_state = "ready"
        self._health.mark_ready(note="collector runtime loops started")

        try:
            await self._stop_event.wait()
        finally:
            await self.shutdown()

    async def shutdown(self) -> None:
        already_stopping = self._stop_event.is_set()
        if not already_stopping:
            self._stop_event.set()

        self._snapshot.health_state = "stopped"
        self._health.mark_stopped(note="collector runtime stopping")

        current_task = asyncio.current_task()
        for task in self._tasks:
            if task is current_task:
                continue
            task.cancel()

        for task in self._tasks:
            if task is current_task:
                continue
            with contextlib.suppress(asyncio.CancelledError):
                await task

        self._logger.info(
            "collector_runtime_stopped",
            extra={
                "service": "collector-telegram",
                "event": "collector_runtime_stopped",
                "stage": "collector_runtime",
            },
        )

    async def _authorization_loop(self) -> None:
        await self._idle_loop(
            loop_name="authorization_loop",
            interval_sec=5,
            on_tick=self._authorization_tick,
        )

    async def _update_ingest_loop(self) -> None:
        await self._idle_loop(
            loop_name="update_ingest_loop",
            interval_sec=2,
            on_tick=self._update_ingest_tick,
        )

    async def _reconcile_scheduler_loop(self) -> None:
        await self._idle_loop(
            loop_name="reconcile_scheduler_loop",
            interval_sec=float(self._config.reconcile_interval_sec),
            on_tick=self._reconcile_scheduler_tick,
        )

    async def _registry_refresh_loop(self) -> None:
        await self._idle_loop(
            loop_name="registry_refresh_loop",
            interval_sec=60,
            on_tick=self._registry_refresh_tick,
        )

    async def _health_publisher_loop(self) -> None:
        await self._idle_loop(
            loop_name="health_publisher_loop",
            interval_sec=30,
            on_tick=self._health_publisher_tick,
        )

    async def _idle_loop(self, loop_name: str, *, interval_sec: float, on_tick) -> None:
        self._health.mark_runtime_loop_started(loop_name)
        self._logger.info(
            "collector_loop_started",
            extra={
                "service": "collector-telegram",
                "event": "collector_loop_started",
                "stage": "collector_runtime",
                "loop_name": loop_name,
                "interval_sec": interval_sec,
            },
        )

        try:
            while not self._stop_event.is_set():
                self._snapshot.last_tick_at = datetime.now(timezone.utc)
                self._health.heartbeat()
                await on_tick()
                await asyncio.sleep(interval_sec)
        except asyncio.CancelledError:
            self._logger.info(
                "collector_loop_cancelled",
                extra={
                    "service": "collector-telegram",
                    "event": "collector_loop_cancelled",
                    "stage": "collector_runtime",
                    "loop_name": loop_name,
                },
            )
            raise
        except Exception:
            self._snapshot.health_state = "degraded"
            self._health.mark_degraded(note=f"loop failure: {loop_name}")
            self._logger.exception(
                "collector_loop_failed",
                extra={
                    "service": "collector-telegram",
                    "event": "collector_loop_failed",
                    "stage": "collector_runtime",
                    "loop_name": loop_name,
                    "status": "failed",
                },
            )
            raise
        finally:
            self._health.mark_runtime_loop_stopped(loop_name)

    async def _authorization_tick(self) -> None:
        if self._authorization_pump is None:
            return

        result = await self._authorization_pump.pump_once()
        if result is None:
            return

        self._health.mark_authorization_state(result.state_name)
        if result.requires_manual_intervention:
            self._snapshot.health_state = "degraded"
            self._health.mark_degraded(note=result.note or "authorization manual intervention required")
            raise AuthorizationManualInterventionRequired(
                result.note or "authorization manual intervention required"
            )

    async def _update_ingest_tick(self) -> None:
        if self._update_ingest_runner is None:
            return

        result = await self._update_ingest_runner.pump_once()
        if result is None:
            return

        for update_type, count in result.update_counts.items():
            for _ in range(max(0, count)):
                self._health.mark_update_received(update_type)

    async def _reconcile_scheduler_tick(self) -> None:
        if self._reconcile is None:
            return
        summaries = await self._reconcile.run_scheduled_targets(limit=20)
        for summary in summaries:
            self._health.mark_reconcile_result(summary)

    async def _registry_refresh_tick(self) -> None:
        if self._registry_sync is None:
            return

        await self._registry_sync.sync_unresolved_channels()
        await self._registry_sync.sync_join_requested_channels()
        await self._registry_sync.sync_access_lost_channels()
        active_channels = await self._registry_sync.load_active_channels()
        self._health.mark_tracked_channels_active(len(active_channels))

    async def _health_publisher_tick(self) -> None:
        snapshot = self._health.snapshot_dict()
        self._logger.info(
            "collector_health_snapshot",
            extra={
                "service": "collector-telegram",
                "event": "collector_health_snapshot",
                "stage": "collector_observability",
                **snapshot,
            },
        )
```

---

## 2-6. `tests/unit/services/collector_telegram/test_singleton_guard.py`

```python
from __future__ import annotations

from pathlib import Path

import pytest

from src.services.collector_telegram.exceptions import SingletonViolationError
from src.services.collector_telegram.singleton_guard import CollectorSingletonGuard


def test_singleton_guard_blocks_second_acquire(tmp_path: Path) -> None:
    lock_path = tmp_path / "collector.lock"

    guard_a = CollectorSingletonGuard(str(lock_path))
    guard_b = CollectorSingletonGuard(str(lock_path))

    guard_a.acquire()
    try:
        with pytest.raises(SingletonViolationError):
            guard_b.acquire()
    finally:
        guard_a.release()


def test_singleton_guard_release_allows_reacquire(tmp_path: Path) -> None:
    lock_path = tmp_path / "collector.lock"

    guard_a = CollectorSingletonGuard(str(lock_path))
    guard_b = CollectorSingletonGuard(str(lock_path))

    guard_a.acquire()
    guard_a.release()
    guard_b.acquire()
    guard_b.release()
```

---

## 2-7. `tests/unit/services/collector_telegram/test_runtime_manual_intervention.py`

```python
from __future__ import annotations

import pytest

from src.services.collector_telegram.config import CollectorTelegramConfig
from src.services.collector_telegram.exceptions import AuthorizationManualInterventionRequired
from src.services.collector_telegram.health import CollectorHealthService
from src.services.collector_telegram.runtime import (
    AuthorizationPumpResult,
    CollectorRuntime,
)


class StubAuthorizationPump:
    async def pump_once(self) -> AuthorizationPumpResult:
        return AuthorizationPumpResult(
            state_name="authorizationStateWaitCode",
            requires_manual_intervention=True,
            note="operator code required",
        )


def _config() -> CollectorTelegramConfig:
    return CollectorTelegramConfig(
        app_env="dev",
        database_url="postgresql+asyncpg://test:test@localhost/test",
        redis_url=None,
        collector_mode="replay",
        telegram_api_id=1,
        telegram_api_hash="hash",
        telegram_phone_number="+10000000000",
        telegram_2fa_password=None,
        tdlib_state_dir="/tmp/collector-state",
        tdlib_files_dir="/tmp/collector-files",
        tdlib_db_encryption_key="enc-key",
        reconcile_interval_sec=300,
        reconcile_backfill_limit=50,
        warm_backfill_limit=30,
        history_page_limit=50,
        singleton_lock_path="/tmp/collector.lock",
        startup_probe_timeout_sec=30,
        startup_warm_backfill_enabled=True,
        log_level="INFO",
    )


@pytest.mark.asyncio
async def test_authorization_manual_intervention_marks_runtime_degraded() -> None:
    health = CollectorHealthService()
    runtime = CollectorRuntime(
        _config(),
        health=health,
        authorization_pump=StubAuthorizationPump(),
    )

    with pytest.raises(AuthorizationManualInterventionRequired):
        await runtime._authorization_tick()  # acceptance hardening-level internal contract test

    snapshot = health.snapshot()
    assert snapshot.health_state == "degraded"
    assert snapshot.tdlib_authorization_state == "authorizationStateWaitCode"
```

---

## 2-8. `tests/component/services/collector_telegram/test_runtime_startup_acceptance.py`

```python
from __future__ import annotations

import pytest

from src.services.collector_telegram.config import CollectorTelegramConfig
from src.services.collector_telegram.health import CollectorHealthService
from src.services.collector_telegram.models import ReconcileSummary, TrackedChat
from src.services.collector_telegram.runtime import CollectorRuntime


class StubRegistrySync:
    async def load_active_channels(self):
        return [
            TrackedChat(
                registry_id="r1",
                chat_id=1001,
                desired_state="active",
                access_state="joined",
                source_kind="public_username",
                source_value="channel_a",
            ),
            TrackedChat(
                registry_id="r2",
                chat_id=1002,
                desired_state="active",
                access_state="joined",
                source_kind="public_username",
                source_value="channel_b",
            ),
        ]

    async def sync_unresolved_channels(self):
        return None

    async def sync_join_requested_channels(self):
        return None

    async def sync_access_lost_channels(self):
        return None


class StubReconcile:
    def __init__(self) -> None:
        self.calls: list[int] = []

    async def run_startup_warm_backfill(self, chat_id: int) -> ReconcileSummary:
        self.calls.append(chat_id)
        return ReconcileSummary(
            chat_id=chat_id,
            result_type="no_changes",
            processed_count=5,
            inserted_count=0,
            updated_count=0,
            gap_filled_count=0,
        )

    async def run_scheduled_targets(self, *, limit: int = 20):
        return []


def _config() -> CollectorTelegramConfig:
    return CollectorTelegramConfig(
        app_env="dev",
        database_url="postgresql+asyncpg://test:test@localhost/test",
        redis_url=None,
        collector_mode="replay",
        telegram_api_id=1,
        telegram_api_hash="hash",
        telegram_phone_number="+10000000000",
        telegram_2fa_password=None,
        tdlib_state_dir="/tmp/collector-state",
        tdlib_files_dir="/tmp/collector-files",
        tdlib_db_encryption_key="enc-key",
        reconcile_interval_sec=300,
        reconcile_backfill_limit=50,
        warm_backfill_limit=30,
        history_page_limit=50,
        singleton_lock_path="/tmp/collector.lock",
        startup_probe_timeout_sec=30,
        startup_warm_backfill_enabled=True,
        log_level="INFO",
    )


@pytest.mark.asyncio
async def test_startup_acceptance_runs_warm_backfill_for_active_channels() -> None:
    health = CollectorHealthService()
    reconcile = StubReconcile()
    runtime = CollectorRuntime(
        _config(),
        health=health,
        registry_sync=StubRegistrySync(),
        reconcile=reconcile,
    )

    await runtime.startup_acceptance_check()

    assert reconcile.calls == [1001, 1002]
    snapshot = health.snapshot()
    assert snapshot.health_state == "ready"
    assert snapshot.tracked_channels_active == 2
    assert snapshot.reconcile_runs_total == 2
```

---

## 2-9. `tests/component/services/collector_telegram/test_service_single_instance.py`

```python
from __future__ import annotations

from pathlib import Path

import pytest

from src.services.collector_telegram.config import CollectorTelegramConfig
from src.services.collector_telegram.exceptions import SingletonViolationError
from src.services.collector_telegram.health import CollectorHealthService
from src.services.collector_telegram.runtime import CollectorRuntime
from src.services.collector_telegram.service import CollectorTelegramService
from src.services.collector_telegram.singleton_guard import CollectorSingletonGuard


class StubRuntime(CollectorRuntime):
    async def startup_acceptance_check(self) -> None:  # type: ignore[override]
        return None


def _config(lock_path: str) -> CollectorTelegramConfig:
    return CollectorTelegramConfig(
        app_env="prod",
        database_url="postgresql+asyncpg://test:test@localhost/test",
        redis_url=None,
        collector_mode="live",
        telegram_api_id=1,
        telegram_api_hash="hash",
        telegram_phone_number="+10000000000",
        telegram_2fa_password=None,
        tdlib_state_dir="/tmp/collector-state",
        tdlib_files_dir="/tmp/collector-files",
        tdlib_db_encryption_key="enc-key",
        reconcile_interval_sec=300,
        reconcile_backfill_limit=50,
        warm_backfill_limit=30,
        history_page_limit=50,
        singleton_lock_path=lock_path,
        startup_probe_timeout_sec=30,
        startup_warm_backfill_enabled=True,
        log_level="INFO",
    )


@pytest.mark.asyncio
async def test_live_service_enforces_single_instance(tmp_path: Path) -> None:
    lock_path = str(tmp_path / "collector.lock")
    config = _config(lock_path)

    service_a = CollectorTelegramService(
        config,
        StubRuntime(config, health=CollectorHealthService()),
        singleton_guard=CollectorSingletonGuard(lock_path),
    )
    service_b = CollectorTelegramService(
        config,
        StubRuntime(config, health=CollectorHealthService()),
        singleton_guard=CollectorSingletonGuard(lock_path),
    )

    await service_a.start()
    try:
        with pytest.raises(SingletonViolationError):
            await service_b.start()
    finally:
        await service_a.stop()
```

---

## 3. 구현 메모

### 3-1. 왜 singleton guard를 지금 넣는가

17단계 수용 기준과 2/3단계 정본 문서는 prod live collector를 **정확히 1개**로 유지하라고 고정한다. 이 제약이 배포에만 있고 코드에 없으면, 운영 중 잘못된 compose override나 수동 재기동으로 쉽게 깨진다. 그래서 acceptance hardening 단계에서 `flock` 기반 guard를 서비스 시작 경계에 넣는 것이 맞다.

### 3-2. 왜 startup acceptance check를 `service.start()`에 넣는가

이번 단계의 목적은 단순 기동이 아니라 **restart recovery 존재를 코드로 보이게 하는 것**이다. 따라서 active channel load와 startup warm backfill은 forever loop에 묻히는 것보다, 서비스 start 경계에서 명시적으로 실행되는 편이 더 안전하다.

### 3-3. 왜 manual intervention을 예외로 올리나

3단계 collector 문서는 운영 중 `waiting_code` / `waiting_password` 회귀를 **degraded + manual intervention**으로 보라고 잠갔다. 이 상태를 로그만 남기고 삼키면 readiness가 거짓으로 유지될 수 있다. 그래서 authorization tick은 health를 degraded로 내린 뒤 `AuthorizationManualInterventionRequired`를 올리는 편이 맞다.

### 3-4. duplicate/no-op는 왜 새 구현보다 테스트 중심으로 닫나

same-hash no-op와 semantic outbox dedupe 자체는 이미 C3/C4에서 구현 경계가 생겼다. acceptance hardening의 역할은 새로운 중복 억제 로직을 추가하는 것이 아니라, **반복 live update / restart backfill에도 기존 contract가 안 깨지는지 확인하는 것**이다. 그래서 이번 묶음은 service/runtime hardening + acceptance tests 중심으로 두는 게 맞다.

---

## 4. 다음 단계

collector 내부 기준으로는 이번 acceptance hardening이 마지막이다.

즉, collector 다음 순서는 아래로 고정된다.

1. `outbox-relay`
2. `router-normalizer`
3. 그 다음에야 stage 4 canonicalization/candidate 생성 경로 본체로 이동

이 순서를 바꾸면 안 된다. collector current/version/outbox contract가 먼저 닫혀 있어야 후단이 안정적으로 붙는다.
