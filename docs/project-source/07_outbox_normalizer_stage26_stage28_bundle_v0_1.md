# 07 outbox normalizer stage26 stage28 bundle v0 1
이 문서는 프로젝트 소스 파일 수 제한(40개)을 피하기 위한 **통합 번들**이다.
구조 변경 문서가 아니며, 아래 원본 문서를 **순서와 내용을 보존한 채** 묶는다.

## 포함 원본 파일
- `26_outbox_relay_skeleton_and_code_draft_v0_1.md`
- `27_router_normalizer_skeleton_and_code_draft_v0_1.md`
- `28_router_normalizer_consumer_integration_hardening_v0_1.md`

## 통합 원칙
- 아키텍처 불변식은 유지한다.
- 원본 파일명은 아래 섹션 제목으로 보존한다.
- 충돌 시 해석 우선순위는 기존과 동일하게 정본 단계 문서 → 실행 계약 → migration 정본 → 구현 초안이다.


---

## Source file: `26_outbox_relay_skeleton_and_code_draft_v0_1.md`

# 26단계: `outbox-relay` 스켈레톤 + 실제 코드 초안 v0.1

## 0. 문서 목적

이 문서는 프로젝트 소스의 README/정본 단계 문서, `11_stage11_execution_contracts_v0_1.md`, `12_migration_spec_0001_0004_v0_1.md`, 그리고 `17~25` 단계 collector 구현 문서를 바탕으로,
`outbox-relay`의 **첫 구현 묶음**을 실제 코드 초안 수준까지 내리는 문서다.

이번 단계의 목적은 다음 네 가지다.

1. `event_outbox`의 **pending row를 Redis Streams로 발행하는 단일 책임 서비스**를 코드로 고정
2. **event_type → queue_name / stage_name** 라우팅 계약을 코드로 고정
3. **Redis publish 성공 후에만 `event_outbox.published_*`를 반영**하는 흐름을 고정
4. collector 이후 경계를 `router-normalizer`로 자연스럽게 잇되, 구조를 깨지 않도록 유지

핵심 전제:

- `outbox-relay`는 **판단기**가 아니다.
- `outbox-relay`는 **이벤트 fan-out / queue publish**만 담당한다.
- 비즈니스 데이터는 Redis에 싣지 않고, **얇은 ID payload만** 싣는다.
- durable truth는 여전히 **PostgreSQL**이다.

---

## 1. 현재 단계 위치

현재 구현 순서는 이미 고정되어 있다.

1. `0001`~`0004` migration 초안 완료
2. `collector-telegram` 내부 구현 완료
3. **이번 단계: `outbox-relay`**
4. 다음 단계: `router-normalizer`

즉, 지금은 collector가 적재한 `event_outbox`를 후단 queue로 넘기는 **좁은 중간 서비스**를 구현하는 단계다.

구조상 위치는 아래와 같다.

```text
collector-telegram
  ↓
event_outbox (pending)
  ↓
outbox-relay
  ↓
Redis Streams
  ↓
router-normalizer / downstream workers
```

---

## 2. 책임과 비책임

### 2-1. 반드시 하는 일

- `event_outbox`에서 `pending` 이벤트를 배치 조회
- event type을 queue/stage로 라우팅
- 얇은 Redis Stream message publish
- publish 성공 시 `event_outbox.status = published`, `published_at` 갱신
- publish 실패 시 `event_outbox.status = failed`, `fail_count`, `last_error` 갱신
- `job_attempts`에 relay 시도 기록
- structured log 남김

### 2-2. 하면 안 되는 일

- source message 내용을 해석
- URL canonicalization
- candidate 생성
- judge / policy / notifier 호출
- Redis payload에 큰 JSON 싣기
- `event_outbox.payload_json`을 비즈니스 가공용 원천처럼 사용

즉, 이 서비스는 **DB row를 queue message로 옮기는 전달 계층**일 뿐이다.

---

## 3. 대상 파일 트리

```text
src/services/outbox_relay/
  __init__.py
  config.py
  models.py
  routing.py
  redis_streams.py
  repositories.py
  service.py
  main.py

tests/
  unit/
    services/
      outbox_relay/
        test_routing.py
        test_stream_message_shape.py
  component/
    services/
      outbox_relay/
        test_outbox_publish_success.py
        test_outbox_publish_failure.py
```

---

## 4. 고정 라우팅 계약

### 4-1. 기본 queue/stage 매핑

| event_type | queue_name | stage_name |
|---|---|---|
| `source_message.created.v1` | `q.source.normalize` | `normalize` |
| `source_message.edited.v1` | `q.source.normalize` | `normalize` |
| `source_message.deleted.v1` | `q.source.normalize` | `normalize` |
| `source_message.reconciled.v1` | `q.source.normalize` | `normalize` |
| `artifact.enrich.requested.v1` + `provider_route=github` | `q.artifact.enrich.github` | `enrich_github` |
| `artifact.enrich.requested.v1` + `provider_route=x` | `q.artifact.enrich.x` | `enrich_x` |
| `artifact.enrich.requested.v1` + `provider_route=web` | `q.artifact.enrich.web` | `enrich_web` |
| `candidate.bundle.refresh.v1` | `q.candidate.bundle` | `bundle` |
| `analysis.requested.v1` | `q.analysis.route` | `analysis_route` |
| `judge.call.requested.v1` | `q.analysis.judge` | `judge` |
| `judge.output.ready.v1` | `q.analysis.validate` | `analysis_validate` |
| `analysis.policy.apply.v1` | `q.analysis.policy` | `analysis_policy` |
| `notification.plan.created.v1` | `q.notification.send` | `notify` |
| `replay.requested.v1` | `q.replay` | `replay` |
| `notification.delivery.result.v1` | `q.maintenance` | `maintenance` |

### 4-2. Redis message shape

Redis payload는 아래 최소 필드만 싣는다.

```json
{
  "job_id": "<event_id>",
  "stage_name": "normalize",
  "root_object_type": "source_message",
  "root_object_id": "<aggregate_id>",
  "idempotency_key": "<dedupe_key>",
  "pipeline_run_id": null,
  "not_before": null,
  "trigger_event_id": "<event_id>"
}
```

중요:
- `payload_json` 전체를 Redis에 넣지 않는다.
- consumer는 `root_object_id` 기준으로 PostgreSQL을 재조회한다.

---

## 5. 구현 범위와 bounded assumption

이번 v0.1은 **단일 outbox-relay 인스턴스**를 기본 가정으로 둔다.

이유:
- 현재 런타임이 단일 VPS, 저동시성, 단계별 좁은 worker로 잠겨 있음
- `event_outbox` 스키마에 `claimed_at / claimed_by`가 아직 없음
- 구조를 깨지 않으려면 먼저 **correctness**를 고정하고, scale-out claim hardening은 뒤 단계로 미루는 것이 맞음

즉, 이번 초안은 다음을 보장한다.
- 단일 relay 인스턴스에서 안정적으로 publish 가능
- publish 성공/실패가 durable row로 남음
- Redis 유실 시 PostgreSQL 기준으로 재구성 가능

반면 아래는 이번 단계에서 의도적으로 하지 않는다.
- 다중 relay 인스턴스 병행 claim
- advisory lock 기반 고급 claim 경합 제어
- delayed retry 승격기
- DLQ 자동 승격기

그건 maintenance 단계 책임이다.

---

## 6. 코드 초안

## 6-1. `src/services/outbox_relay/__init__.py`

```python
from .config import OutboxRelayConfig
from .service import OutboxRelayService

__all__ = [
    "OutboxRelayConfig",
    "OutboxRelayService",
]
```

---

## 6-2. `src/services/outbox_relay/config.py`

```python
from __future__ import annotations

import os
from dataclasses import dataclass


class OutboxRelayConfigurationError(ValueError):
    pass


@dataclass(slots=True, frozen=True)
class OutboxRelayConfig:
    app_env: str
    database_url: str
    redis_url: str
    poll_interval_ms: int
    batch_size: int
    xadd_maxlen: int | None
    log_level: str

    @classmethod
    def from_env(cls) -> "OutboxRelayConfig":
        database_url = os.getenv("DATABASE_URL", "").strip()
        redis_url = os.getenv("REDIS_URL", "").strip()
        if not database_url:
            raise OutboxRelayConfigurationError("DATABASE_URL is required")
        if not redis_url:
            raise OutboxRelayConfigurationError("REDIS_URL is required")

        poll_interval_ms = int(os.getenv("OUTBOX_RELAY_POLL_INTERVAL_MS", "1000"))
        batch_size = int(os.getenv("OUTBOX_RELAY_BATCH_SIZE", "100"))
        xadd_maxlen_raw = os.getenv("OUTBOX_RELAY_XADD_MAXLEN", "10000").strip()
        xadd_maxlen = int(xadd_maxlen_raw) if xadd_maxlen_raw else None

        cfg = cls(
            app_env=os.getenv("APP_ENV", "dev").strip().lower(),
            database_url=database_url,
            redis_url=redis_url,
            poll_interval_ms=poll_interval_ms,
            batch_size=batch_size,
            xadd_maxlen=xadd_maxlen,
            log_level=os.getenv("LOG_LEVEL", "INFO").strip().upper(),
        )
        cfg.validate()
        return cfg

    def validate(self) -> None:
        if self.poll_interval_ms <= 0:
            raise OutboxRelayConfigurationError("OUTBOX_RELAY_POLL_INTERVAL_MS must be > 0")
        if self.batch_size <= 0:
            raise OutboxRelayConfigurationError("OUTBOX_RELAY_BATCH_SIZE must be > 0")
        if self.batch_size > 1000:
            raise OutboxRelayConfigurationError("OUTBOX_RELAY_BATCH_SIZE must be <= 1000")
        if self.xadd_maxlen is not None and self.xadd_maxlen <= 0:
            raise OutboxRelayConfigurationError("OUTBOX_RELAY_XADD_MAXLEN must be > 0 when set")
```

---

## 6-3. `src/services/outbox_relay/models.py`

```python
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any
from uuid import UUID


@dataclass(slots=True, frozen=True)
class OutboxEventRow:
    event_id: UUID
    event_type: str
    aggregate_type: str
    aggregate_id: UUID
    dedupe_key: str
    payload_json: dict[str, Any]
    status: str
    fail_count: int
    created_at: datetime


@dataclass(slots=True, frozen=True)
class QueueRoute:
    queue_name: str
    stage_name: str


@dataclass(slots=True, frozen=True)
class RedisQueuedMessage:
    job_id: str
    stage_name: str
    root_object_type: str
    root_object_id: str
    idempotency_key: str
    pipeline_run_id: str | None
    not_before: str | None
    trigger_event_id: str

    def as_stream_fields(self) -> dict[str, str]:
        return {
            "job_id": self.job_id,
            "stage_name": self.stage_name,
            "root_object_type": self.root_object_type,
            "root_object_id": self.root_object_id,
            "idempotency_key": self.idempotency_key,
            "pipeline_run_id": self.pipeline_run_id or "",
            "not_before": self.not_before or "",
            "trigger_event_id": self.trigger_event_id,
        }
```

---

## 6-4. `src/services/outbox_relay/routing.py`

```python
from __future__ import annotations

from typing import Any

from .models import OutboxEventRow, QueueRoute


class UnsupportedOutboxEventTypeError(ValueError):
    pass


class OutboxRouteResolver:
    """Resolve event_outbox rows into queue/stage routes.

    Design rule:
    - routing is deterministic,
    - business interpretation does not happen here,
    - provider-specific enrichment routing is derived from payload_json["provider_route"].
    """

    _SOURCE_ROUTE = QueueRoute(queue_name="q.source.normalize", stage_name="normalize")
    _BUNDLE_ROUTE = QueueRoute(queue_name="q.candidate.bundle", stage_name="bundle")
    _ANALYSIS_ROUTE = QueueRoute(queue_name="q.analysis.route", stage_name="analysis_route")
    _JUDGE_ROUTE = QueueRoute(queue_name="q.analysis.judge", stage_name="judge")
    _VALIDATE_ROUTE = QueueRoute(queue_name="q.analysis.validate", stage_name="analysis_validate")
    _POLICY_ROUTE = QueueRoute(queue_name="q.analysis.policy", stage_name="analysis_policy")
    _NOTIFY_ROUTE = QueueRoute(queue_name="q.notification.send", stage_name="notify")
    _REPLAY_ROUTE = QueueRoute(queue_name="q.replay", stage_name="replay")
    _MAINTENANCE_ROUTE = QueueRoute(queue_name="q.maintenance", stage_name="maintenance")

    def resolve(self, row: OutboxEventRow) -> QueueRoute:
        event_type = row.event_type

        if event_type in {
            "source_message.created.v1",
            "source_message.edited.v1",
            "source_message.deleted.v1",
            "source_message.reconciled.v1",
        }:
            return self._SOURCE_ROUTE

        if event_type == "artifact.enrich.requested.v1":
            provider_route = self._payload_value(row.payload_json, "provider_route")
            if provider_route == "github":
                return QueueRoute("q.artifact.enrich.github", "enrich_github")
            if provider_route == "x":
                return QueueRoute("q.artifact.enrich.x", "enrich_x")
            if provider_route == "web":
                return QueueRoute("q.artifact.enrich.web", "enrich_web")
            raise UnsupportedOutboxEventTypeError(
                f"artifact.enrich.requested.v1 missing/invalid provider_route: {provider_route!r}"
            )

        if event_type in {"candidate.bundle.refresh.v1", "artifact.snapshot.updated.v1"}:
            return self._BUNDLE_ROUTE

        if event_type == "analysis.requested.v1":
            return self._ANALYSIS_ROUTE

        if event_type == "judge.call.requested.v1":
            return self._JUDGE_ROUTE

        if event_type == "judge.output.ready.v1":
            return self._VALIDATE_ROUTE

        if event_type == "analysis.policy.apply.v1":
            return self._POLICY_ROUTE

        if event_type == "notification.plan.created.v1":
            return self._NOTIFY_ROUTE

        if event_type == "replay.requested.v1":
            return self._REPLAY_ROUTE

        if event_type == "notification.delivery.result.v1":
            return self._MAINTENANCE_ROUTE

        raise UnsupportedOutboxEventTypeError(f"unsupported outbox event_type: {event_type}")

    @staticmethod
    def _payload_value(payload: dict[str, Any], key: str) -> str | None:
        value = payload.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
        return None
```

---

## 6-5. `src/services/outbox_relay/redis_streams.py`

```python
from __future__ import annotations

from typing import Any

from redis.asyncio import Redis

from .models import QueueRoute, RedisQueuedMessage


class RedisStreamsPublisher:
    def __init__(self, client: Redis, *, maxlen: int | None = None) -> None:
        self._client = client
        self._maxlen = maxlen

    async def publish(self, route: QueueRoute, message: RedisQueuedMessage) -> str:
        fields = message.as_stream_fields()
        if self._maxlen is None:
            message_id = await self._client.xadd(route.queue_name, fields)
        else:
            message_id = await self._client.xadd(
                route.queue_name,
                fields,
                maxlen=self._maxlen,
                approximate=True,
            )
        if isinstance(message_id, bytes):
            return message_id.decode("utf-8")
        return str(message_id)
```

---

## 6-6. `src/services/outbox_relay/repositories.py`

```python
from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any, Iterable
from uuid import UUID

import sqlalchemy as sa
from sqlalchemy.ext.asyncio import AsyncSession

from .models import OutboxEventRow


class OutboxRelayRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def fetch_pending_batch(self, *, limit: int) -> list[OutboxEventRow]:
        result = await self._session.execute(
            sa.text(
                """
                SELECT
                    event_id,
                    event_type,
                    aggregate_type,
                    aggregate_id,
                    dedupe_key,
                    payload_json,
                    status,
                    fail_count,
                    created_at
                FROM event_outbox
                WHERE status = 'pending'::outbox_status_enum
                ORDER BY created_at ASC, event_id ASC
                LIMIT :limit
                """
            ),
            {"limit": limit},
        )
        rows = []
        for row in result.mappings().all():
            payload = row["payload_json"]
            if isinstance(payload, str):
                payload = json.loads(payload)
            rows.append(
                OutboxEventRow(
                    event_id=row["event_id"],
                    event_type=row["event_type"],
                    aggregate_type=row["aggregate_type"],
                    aggregate_id=row["aggregate_id"],
                    dedupe_key=row["dedupe_key"],
                    payload_json=payload or {},
                    status=str(row["status"]),
                    fail_count=int(row["fail_count"]),
                    created_at=row["created_at"],
                )
            )
        return rows

    async def mark_published(self, *, event_id: UUID, published_at: datetime | None = None) -> None:
        published_at = published_at or datetime.now(timezone.utc)
        await self._session.execute(
            sa.text(
                """
                UPDATE event_outbox
                SET
                    status = 'published'::outbox_status_enum,
                    published_at = :published_at,
                    last_error = NULL
                WHERE event_id = CAST(:event_id AS uuid)
                """
            ),
            {
                "event_id": str(event_id),
                "published_at": published_at,
            },
        )

    async def mark_failed(self, *, event_id: UUID, error_text: str) -> None:
        await self._session.execute(
            sa.text(
                """
                UPDATE event_outbox
                SET
                    status = 'failed'::outbox_status_enum,
                    fail_count = fail_count + 1,
                    last_error = :error_text
                WHERE event_id = CAST(:event_id AS uuid)
                """
            ),
            {
                "event_id": str(event_id),
                "error_text": error_text,
            },
        )

    async def reset_failed_to_pending(self, *, event_ids: Iterable[UUID]) -> None:
        values = [str(v) for v in event_ids]
        if not values:
            return
        await self._session.execute(
            sa.text(
                """
                UPDATE event_outbox
                SET status = 'pending'::outbox_status_enum
                WHERE event_id = ANY(CAST(:event_ids AS uuid[]))
                  AND status = 'failed'::outbox_status_enum
                """
            ),
            {"event_ids": values},
        )

    async def insert_job_attempt(
        self,
        *,
        stage_name: str,
        queue_name: str,
        root_object_type: str,
        root_object_id: UUID,
        attempt_status: str,
        error_code: str | None = None,
    ) -> None:
        await self._session.execute(
            sa.text(
                """
                INSERT INTO job_attempts (
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
                "root_object_id": str(root_object_id),
                "attempt_status": attempt_status,
                "error_code": error_code,
            },
        )
```

---

## 6-7. `src/services/outbox_relay/service.py`

```python
from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone

from .config import OutboxRelayConfig
from .models import OutboxEventRow, QueueRoute, RedisQueuedMessage
from .redis_streams import RedisStreamsPublisher
from .repositories import OutboxRelayRepository
from .routing import OutboxRouteResolver, UnsupportedOutboxEventTypeError


class OutboxRelayService:
    """Single-worker outbox relay.

    v0.1 assumptions:
    - one relay instance on the single VPS,
    - no distributed claim state in `event_outbox` yet,
    - correctness and thin-message routing first, scale-out later.
    """

    def __init__(
        self,
        config: OutboxRelayConfig,
        *,
        repository: OutboxRelayRepository,
        publisher: RedisStreamsPublisher,
        route_resolver: OutboxRouteResolver,
        logger: logging.Logger | None = None,
    ) -> None:
        self._config = config
        self._repository = repository
        self._publisher = publisher
        self._route_resolver = route_resolver
        self._logger = logger or logging.getLogger(__name__)
        self._stop_event = asyncio.Event()

    async def run_forever(self) -> None:
        self._logger.info(
            "outbox_relay_starting",
            extra={
                "service": "outbox-relay",
                "event": "outbox_relay_starting",
                "batch_size": self._config.batch_size,
                "poll_interval_ms": self._config.poll_interval_ms,
            },
        )
        try:
            while not self._stop_event.is_set():
                processed = await self.run_once()
                if processed == 0:
                    await asyncio.sleep(self._config.poll_interval_ms / 1000.0)
        finally:
            self._logger.info(
                "outbox_relay_stopped",
                extra={"service": "outbox-relay", "event": "outbox_relay_stopped"},
            )

    async def stop(self) -> None:
        self._stop_event.set()

    async def run_once(self) -> int:
        rows = await self._repository.fetch_pending_batch(limit=self._config.batch_size)
        processed = 0
        for row in rows:
            processed += 1
            await self._process_row(row)
        return processed

    async def _process_row(self, row: OutboxEventRow) -> None:
        try:
            route = self._route_resolver.resolve(row)
            message = self._build_stream_message(row, route)
            redis_message_id = await self._publisher.publish(route, message)
            await self._repository.mark_published(event_id=row.event_id, published_at=datetime.now(timezone.utc))
            await self._repository.insert_job_attempt(
                stage_name=route.stage_name,
                queue_name=route.queue_name,
                root_object_type=row.aggregate_type,
                root_object_id=row.aggregate_id,
                attempt_status="succeeded",
                error_code=None,
            )
            self._logger.info(
                "outbox_event_published",
                extra={
                    "service": "outbox-relay",
                    "event": "outbox_event_published",
                    "event_id": str(row.event_id),
                    "event_type": row.event_type,
                    "queue_name": route.queue_name,
                    "redis_message_id": redis_message_id,
                },
            )
        except UnsupportedOutboxEventTypeError as exc:
            await self._repository.mark_failed(event_id=row.event_id, error_text=str(exc))
            await self._repository.insert_job_attempt(
                stage_name="outbox_route",
                queue_name="unsupported",
                root_object_type=row.aggregate_type,
                root_object_id=row.aggregate_id,
                attempt_status="failed_terminal",
                error_code="unsupported_event_type",
            )
            self._logger.exception(
                "outbox_event_unsupported",
                extra={
                    "service": "outbox-relay",
                    "event": "outbox_event_unsupported",
                    "event_id": str(row.event_id),
                    "event_type": row.event_type,
                },
            )
        except Exception as exc:
            await self._repository.mark_failed(event_id=row.event_id, error_text=str(exc))
            route = self._safe_route(row)
            await self._repository.insert_job_attempt(
                stage_name=route.stage_name,
                queue_name=route.queue_name,
                root_object_type=row.aggregate_type,
                root_object_id=row.aggregate_id,
                attempt_status="failed_retryable",
                error_code=type(exc).__name__,
            )
            self._logger.exception(
                "outbox_event_publish_failed",
                extra={
                    "service": "outbox-relay",
                    "event": "outbox_event_publish_failed",
                    "event_id": str(row.event_id),
                    "event_type": row.event_type,
                },
            )

    def _build_stream_message(self, row: OutboxEventRow, route: QueueRoute) -> RedisQueuedMessage:
        return RedisQueuedMessage(
            job_id=str(row.event_id),
            stage_name=route.stage_name,
            root_object_type=row.aggregate_type,
            root_object_id=str(row.aggregate_id),
            idempotency_key=row.dedupe_key,
            pipeline_run_id=None,
            not_before=None,
            trigger_event_id=str(row.event_id),
        )

    def _safe_route(self, row: OutboxEventRow) -> QueueRoute:
        try:
            return self._route_resolver.resolve(row)
        except Exception:
            return QueueRoute(queue_name="unknown", stage_name="outbox_route")
```

---

## 6-8. `src/services/outbox_relay/main.py`

```python
from __future__ import annotations

import asyncio
import logging

from redis.asyncio import Redis
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from .config import OutboxRelayConfig
from .redis_streams import RedisStreamsPublisher
from .repositories import OutboxRelayRepository
from .routing import OutboxRouteResolver
from .service import OutboxRelayService


def _build_logger(level: str) -> logging.Logger:
    logging.basicConfig(level=getattr(logging, level, logging.INFO))
    return logging.getLogger("outbox-relay")


async def _run() -> int:
    config = OutboxRelayConfig.from_env()
    logger = _build_logger(config.log_level)

    engine = create_async_engine(config.database_url, future=True)
    session_factory = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    redis_client = Redis.from_url(config.redis_url, decode_responses=True)

    try:
        async with session_factory() as session:
            repository = OutboxRelayRepository(session)
            publisher = RedisStreamsPublisher(redis_client, maxlen=config.xadd_maxlen)
            service = OutboxRelayService(
                config,
                repository=repository,
                publisher=publisher,
                route_resolver=OutboxRouteResolver(),
                logger=logger,
            )
            await service.run_forever()
    except asyncio.CancelledError:
        logger.info("outbox_relay_cancelled", extra={"service": "outbox-relay", "event": "cancelled"})
        return 0
    finally:
        await redis_client.close()
        await engine.dispose()

    return 0


def main() -> None:
    raise SystemExit(asyncio.run(_run()))


if __name__ == "__main__":
    main()
```

---

## 7. 테스트 초안 포인트

### `tests/unit/services/outbox_relay/test_routing.py`

검증:
- `source_message.* -> q.source.normalize`
- `artifact.enrich.requested.v1 + provider_route -> provider queue`
- `judge.output.ready.v1 -> q.analysis.validate`
- unsupported event type 예외

### `tests/unit/services/outbox_relay/test_stream_message_shape.py`

검증:
- Redis payload가 ID-only인지
- `payload_json` 전체가 안 실리는지
- `job_id == trigger_event_id == event_id`인지

### `tests/component/services/outbox_relay/test_outbox_publish_success.py`

검증:
- pending row 1개
- Redis xadd 성공
- `event_outbox.status = published`
- `published_at` 채워짐
- `job_attempts.attempt_status = succeeded`

### `tests/component/services/outbox_relay/test_outbox_publish_failure.py`

검증:
- Redis xadd 예외
- `event_outbox.status = failed`
- `fail_count + 1`
- `last_error` 채워짐
- `job_attempts.attempt_status = failed_retryable`

---

## 8. 구현 메모

### 8-1. 왜 Redis payload를 더 얇게 두는가

11단계 실행 계약 문서가 Redis를 durable source가 아니라 **queue / lock / short-lived state**로 고정했고, queue payload도 **ID만** 싣도록 못 박았다. 따라서 relay가 `payload_json` 전체를 Redis로 넘기면 구조가 깨진다. 후단 worker는 PostgreSQL에서 canonical row를 다시 읽어야 한다.\

### 8-2. 왜 `notification.delivery.result.v1`를 `q.maintenance`로 보냈는가

이 이벤트는 hot path 소비자가 아직 명확하지 않지만, outbox에 남는 이벤트를 완전히 unsupported로 두면 후속 운영 제어면을 붙일 때 구조가 흔들린다. 그래서 v0.1에서는 maintenance 경로로 흘려보내도록 두고, 실제 소비자는 maintenance 단계에서 붙이는 것이 가장 작은 변경이다.

### 8-3. 왜 다중 relay claim hardening을 지금 안 넣는가

현재 스키마에는 `claimed_at / claimed_by`가 없고, 지금 단계의 목적은 **collector 다음 경계인 queue publish contract를 먼저 닫는 것**이다. 저동시성 단일 VPS 전제에서 먼저 correctness를 고정하는 편이 맞다. 다중 relay scale-out은 이후 maintenance/observability hardening에서 advisory lock 또는 claim-state patch로 확장하면 된다.

### 8-4. 왜 `job_attempts`를 outbox-relay에서 바로 쓰는가

11단계 서비스 책임 매트릭스가 이미 `outbox-relay -> event_outbox + job_attempts`를 고정했다. relay가 publish 시도 자체를 durable하게 남기지 않으면, Redis 유실·재시도·운영 복기에서 outbox가 실제로 언제 어디로 publish됐는지 설명할 수 없다.

---

## 9. 다음 단계

다음 단계는 그대로 **`router-normalizer` 스켈레톤 명세 + 코드 초안**이다.

순서:
1. `router-normalizer` 스켈레톤 명세
2. `text surface / URL extraction / short URL expansion / canonicalization / trigger rules / candidate proposal` 코드 초안

이 순서를 바꾸면 안 된다. collector/outbox 경계가 닫힌 다음에야, 4단계 deterministic normalization 경로를 안전하게 붙일 수 있다.


---

## Source file: `27_router_normalizer_skeleton_and_code_draft_v0_1.md`

# 27단계: `router-normalizer` 스켈레톤 + 실제 코드 초안 v0.1

## 0. 문서 목적

이 문서는 프로젝트 소스의 README/정본 단계 문서, `11_stage11_execution_contracts_v0_1.md`, `12_migration_spec_0001_0004_v0_1.md`, 그리고 `26_outbox_relay_skeleton_and_code_draft_v0_1.md`까지의 구현 흐름을 바탕으로,  
**`router-normalizer`의 첫 구현 묶음**을 실제 코드 초안 수준으로 내리는 문서다.

이번 단계의 목적은 다음 다섯 가지다.

1. `source_message.*.v1` 이벤트를 입력으로 받아 **deterministic normalization run**을 수행하는 서비스 경계를 코드로 고정
2. **surface normalization / entity-first URL extraction / short URL expansion / canonicalization / trigger evaluation / candidate proposal** 흐름을 코드로 고정
3. `normalization_runs`, `normalization_suppression_traces`, `artifact_registry`, `artifact_observations`, `candidate_group_proposals`, `candidate_group_members`, `event_outbox`에 대한 **router-normalizer 전용 DB 경계**를 코드로 고정
4. `ai`를 hard trigger로 쓰지 않고, **`signal_detected`와 `candidate_eligible`를 분리**하는 규칙을 코드로 고정
5. collector / outbox-relay / enricher / judge 구조를 깨지 않으면서, **router-normalizer가 완전 결정적·비-LLM 경계**로 남도록 고정

핵심 전제:

- `router-normalizer`는 **판단기**가 아니다.
- `router-normalizer`는 **크롤러**가 아니다.
- `router-normalizer`는 Telegram 원문을 **Artifact / CandidateGroup proposal**로 바꾸는 좁은 경계다.
- 외부 네트워크는 **short URL expansion 허용 리스트** 수준으로만 제한한다.
- 실제 final verdict는 뒤 단계 LLM + policy-engine 책임이다.

---

## 1. 현재 단계 위치

현재 구현 순서는 이미 고정되어 있다.

1. `0001`~`0004` migration 초안 완료
2. `collector-telegram` 내부 구현 완료
3. `outbox-relay` 구현 완료
4. **이번 단계: `router-normalizer`**
5. 다음 단계: `gh-enricher`
6. 그 다음: `x-enricher`
7. 그 다음: `web-enricher`
8. 그 다음: `evidence-assembler`

즉, 지금은 collector가 적재한 `source_message`를 **분석 후보화(candidate-ization)** 하는 첫 경계를 구현하는 단계다.

구조상 위치는 아래와 같다.

```text
collector-telegram
  ↓
event_outbox (source_message.*.v1)
  ↓
outbox-relay
  ↓
Redis Streams q.source.normalize
  ↓
router-normalizer
  ↓
artifact_registry / observations / candidate_group_proposals / suppression_traces / outbox
  ↓
gh-enricher / x-enricher / web-enricher
```

---

## 2. 책임과 비책임

### 2-1. 반드시 하는 일

- `source_message.*.v1` 이벤트 수신
- `source_messages` current row 및 해당 version 재조회
- text normalization surface 생성
- entity 우선 URL 추출
- short URL allowlist expansion
- GitHub/X/web/text_idea classification
- canonical artifact upsert
- artifact observation append
- trigger rules 평가
- suppression trace append
- candidate group proposal / members upsert
- 필요 시 `artifact.enrich.requested.v1` outbox 적재

### 2-2. 하면 안 되는 일

- Telegram 원문을 수정하거나 overwrite
- GitHub/X/web 본문 크롤링
- LLM 호출
- 최종 usefulness/quality 판정
- reroot 확정
- evidence bundle 조립
- notifier 포맷 생성

즉, 이 서비스는 **결정적 입력 정규화 + 후보 제안 계층**일 뿐이다.

---

## 3. 대상 파일 트리

```text
src/services/router_normalizer/
  __init__.py
  config.py
  models.py
  text_surfaces.py
  url_extraction.py
  short_url_resolver.py
  canonicalizer.py
  trigger_rules.py
  repositories.py
  service.py
  main.py

tests/
  unit/
    services/
      router_normalizer/
        test_text_surfaces.py
        test_url_extraction.py
        test_canonicalizer.py
        test_trigger_rules.py
  component/
    services/
      router_normalizer/
        test_normalize_source_message_flow.py
        test_suppression_trace_flow.py
        test_candidate_group_proposal_flow.py
```

---

## 4. 고정 입력/출력 계약

### 4-1. 입력

기본 입력 이벤트는 아래 네 종류다.

- `source_message.created.v1`
- `source_message.edited.v1`
- `source_message.deleted.v1`
- `source_message.reconciled.v1`

하지만 router-normalizer는 이벤트 본문을 비즈니스 원천으로 사용하지 않는다.  
이벤트는 **rehydration key** 용도이며, 실제 입력 원천은 PostgreSQL의 `source_messages` + `source_message_versions`다.

### 4-2. 내부 실행 결과

실행 1회당 최소 아래를 남긴다.

- `normalization_runs` 1 row
- suppression된 경우 `normalization_suppression_traces` 1개 이상
- artifact 관측 시 `artifact_registry` upsert + `artifact_observations` append
- 후보 생성 시 `candidate_group_proposals` upsert + `candidate_group_members` upsert
- enrich 필요 시 `artifact.enrich.requested.v1` outbox insert

### 4-3. 출력 이벤트

이번 v0.1에서는 아래만 emit한다.

- `artifact.enrich.requested.v1`

`candidate.bundle.refresh.v1`는 뒤 단계에서 snapshot update 이후 더 자연스럽게 연결되므로, v0.1 normalizer에서는 기본 emit 대상에서 제외한다.

---

## 5. 코드 초안

## 5-1. `src/services/router_normalizer/__init__.py`

```python
from .config import RouterNormalizerConfig
from .service import RouterNormalizerService

__all__ = [
    "RouterNormalizerConfig",
    "RouterNormalizerService",
]
```

---

## 5-2. `src/services/router_normalizer/config.py`

```python
from __future__ import annotations

import os
from dataclasses import dataclass


class RouterNormalizerConfigurationError(ValueError):
    pass


@dataclass(slots=True, frozen=True)
class RouterNormalizerConfig:
    app_env: str
    database_url: str
    redis_url: str
    queue_name: str
    consumer_group: str
    consumer_name: str
    batch_size: int
    block_ms: int
    normalizer_version: str
    short_url_timeout_ms: int
    short_url_max_redirects: int
    shortener_allowlist: tuple[str, ...]
    log_level: str

    @classmethod
    def from_env(cls) -> "RouterNormalizerConfig":
        database_url = os.getenv("DATABASE_URL", "").strip()
        redis_url = os.getenv("REDIS_URL", "").strip()

        if not database_url:
            raise RouterNormalizerConfigurationError("DATABASE_URL is required")
        if not redis_url:
            raise RouterNormalizerConfigurationError("REDIS_URL is required")

        allowlist_raw = os.getenv(
            "NORMALIZER_SHORTENER_ALLOWLIST",
            "t.co,bit.ly,tinyurl.com,goo.gl,ow.ly,buff.ly,lnkd.in",
        ).strip()

        cfg = cls(
            app_env=os.getenv("APP_ENV", "dev").strip().lower(),
            database_url=database_url,
            redis_url=redis_url,
            queue_name=os.getenv("NORMALIZER_QUEUE_NAME", "q.source.normalize").strip(),
            consumer_group=os.getenv("NORMALIZER_CONSUMER_GROUP", "router-normalizer").strip(),
            consumer_name=os.getenv("NORMALIZER_CONSUMER_NAME", "router-normalizer-1").strip(),
            batch_size=int(os.getenv("NORMALIZER_BATCH_SIZE", "20")),
            block_ms=int(os.getenv("NORMALIZER_BLOCK_MS", "5000")),
            normalizer_version=os.getenv("NORMALIZER_VERSION", "router_normalizer_v1").strip(),
            short_url_timeout_ms=int(os.getenv("NORMALIZER_SHORT_URL_TIMEOUT_MS", "1500")),
            short_url_max_redirects=int(os.getenv("NORMALIZER_SHORT_URL_MAX_REDIRECTS", "3")),
            shortener_allowlist=tuple(
                part.strip().lower() for part in allowlist_raw.split(",") if part.strip()
            ),
            log_level=os.getenv("LOG_LEVEL", "INFO").strip().upper(),
        )
        cfg.validate()
        return cfg

    def validate(self) -> None:
        if self.batch_size <= 0 or self.batch_size > 100:
            raise RouterNormalizerConfigurationError("NORMALIZER_BATCH_SIZE must be between 1 and 100")
        if self.block_ms <= 0:
            raise RouterNormalizerConfigurationError("NORMALIZER_BLOCK_MS must be > 0")
        if self.short_url_timeout_ms <= 0:
            raise RouterNormalizerConfigurationError("NORMALIZER_SHORT_URL_TIMEOUT_MS must be > 0")
        if self.short_url_max_redirects <= 0 or self.short_url_max_redirects > 10:
            raise RouterNormalizerConfigurationError("NORMALIZER_SHORT_URL_MAX_REDIRECTS must be between 1 and 10")
        if not self.queue_name:
            raise RouterNormalizerConfigurationError("NORMALIZER_QUEUE_NAME must not be empty")
        if not self.consumer_group:
            raise RouterNormalizerConfigurationError("NORMALIZER_CONSUMER_GROUP must not be empty")
        if not self.consumer_name:
            raise RouterNormalizerConfigurationError("NORMALIZER_CONSUMER_NAME must not be empty")
        if not self.normalizer_version:
            raise RouterNormalizerConfigurationError("NORMALIZER_VERSION must not be empty")
```

---

## 5-3. `src/services/router_normalizer/models.py`

```python
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Literal


TriggerStrength = Literal["strong", "medium", "weak"]
ArtifactType = Literal[
    "github_repo",
    "github_subpath",
    "github_gist",
    "github_repo_page",
    "x_post",
    "web_article",
    "text_idea",
    "unknown_link",
    "short_url_unresolved",
]


@dataclass(slots=True, frozen=True)
class SourceMessageEnvelope:
    event_id: str
    event_type: str
    source_message_id: str
    current_version_no: int
    logical_post_key: str
    occurred_at: datetime


@dataclass(slots=True, frozen=True)
class SourceMessageRecord:
    source_message_id: str
    chat_id: int
    message_id: int
    logical_post_key: str
    text_body: str | None
    caption_text: str | None
    text_surface: str | None
    entities_json: list[dict[str, Any]] | None
    url_surface_json: list[dict[str, Any]] | None
    raw_message_json: dict[str, Any]
    current_version_no: int
    posted_at: datetime
    edited_at: datetime | None
    deleted_at: datetime | None


@dataclass(slots=True, frozen=True)
class NormalizedTextSurfaces:
    raw_text_surface: str | None
    keyword_scan_surface: str | None
    hash_surface: str | None
    display_surface: str | None


@dataclass(slots=True, frozen=True)
class ObservedUrl:
    observed_url: str
    source_kind: str
    normalized_url: str | None = None
    resolved_url: str | None = None
    canonical_url: str | None = None
    classification: str | None = None
    context_path: str | None = None


@dataclass(slots=True, frozen=True)
class CanonicalArtifactDraft:
    artifact_type: ArtifactType
    canonical_id: str
    canonical_url: str | None
    normalized_host: str | None
    artifact_key_json: dict[str, Any] | None = None


@dataclass(slots=True, frozen=True)
class CandidateMemberDraft:
    artifact_canonical_id: str
    artifact_type: ArtifactType
    member_role: str
    member_order: int | None = None


@dataclass(slots=True, frozen=True)
class CandidateGroupProposalDraft:
    source_message_id: str
    source_version_no: int
    initial_primary_artifact_canonical_id: str
    current_primary_artifact_canonical_id: str
    normalizer_version: str
    dedupe_subject_key: str
    members: list[CandidateMemberDraft]


@dataclass(slots=True, frozen=True)
class TriggerEvaluation:
    signal_detected: bool
    candidate_eligible: bool
    trigger_strength: TriggerStrength | None
    reason_codes: list[str] = field(default_factory=list)
    suppression_reason_codes: list[str] = field(default_factory=list)


@dataclass(slots=True, frozen=True)
class NormalizeExecutionResult:
    signal_detected: bool
    candidate_eligible: bool
    trigger_strength: TriggerStrength | None
    artifacts: list[CanonicalArtifactDraft] = field(default_factory=list)
    observations: list[ObservedUrl] = field(default_factory=list)
    proposals: list[CandidateGroupProposalDraft] = field(default_factory=list)
    suppression_reason_codes: list[str] = field(default_factory=list)
```

---

## 5-4. `src/services/router_normalizer/text_surfaces.py`

```python
from __future__ import annotations

import re
import unicodedata

from .models import NormalizedTextSurfaces

_ZERO_WIDTH_RE = re.compile(r"[\u200b-\u200f\u2060\ufeff]")
_WHITESPACE_RE = re.compile(r"\s+")


class TextSurfaceNormalizer:
    def normalize(
        self,
        *,
        text_body: str | None,
        caption_text: str | None,
        text_surface: str | None,
    ) -> NormalizedTextSurfaces:
        raw = self._first_non_empty(text_surface, self._join(text_body, caption_text))
        if raw is None:
            return NormalizedTextSurfaces(
                raw_text_surface=None,
                keyword_scan_surface=None,
                hash_surface=None,
                display_surface=None,
            )

        nfkc = unicodedata.normalize("NFKC", raw)
        no_zero_width = _ZERO_WIDTH_RE.sub("", nfkc)
        line_normalized = no_zero_width.replace("\r\n", "\n").replace("\r", "\n")
        display_surface = line_normalized.strip() or None

        if display_surface is None:
            return NormalizedTextSurfaces(
                raw_text_surface=raw,
                keyword_scan_surface=None,
                hash_surface=None,
                display_surface=None,
            )

        keyword_scan_surface = display_surface.lower()
        hash_surface = _WHITESPACE_RE.sub(" ", display_surface).strip()

        return NormalizedTextSurfaces(
            raw_text_surface=raw,
            keyword_scan_surface=keyword_scan_surface or None,
            hash_surface=hash_surface or None,
            display_surface=display_surface,
        )

    @staticmethod
    def _join(text_body: str | None, caption_text: str | None) -> str | None:
        parts = [p.strip() for p in (text_body, caption_text) if p and p.strip()]
        return "\n\n".join(parts) if parts else None

    @staticmethod
    def _first_non_empty(*values: str | None) -> str | None:
        for value in values:
            if value and value.strip():
                return value
        return None
```

---

## 5-5. `src/services/router_normalizer/url_extraction.py`

```python
from __future__ import annotations

import re
from typing import Any

from .models import ObservedUrl


_URL_REGEX = re.compile(r"https?://[^\s<>()\[\]{}\"']+")


class UrlExtractor:
    """Entity-first URL extraction.

    Order:
    1. collector-provided url_surface_json entries
    2. entities_json hidden URLs
    3. regex fallback over display surface
    """

    def extract(
        self,
        *,
        entities_json: list[dict[str, Any]] | None,
        url_surface_json: list[dict[str, Any]] | None,
        display_surface: str | None,
    ) -> list[ObservedUrl]:
        seen: set[tuple[str, str]] = set()
        observed: list[ObservedUrl] = []

        def add(url: str | None, source_kind: str, *, context_path: str | None = None) -> None:
            normalized = self._normalize_url(url)
            if not normalized:
                return
            key = (source_kind, normalized)
            if key in seen:
                return
            seen.add(key)
            observed.append(
                ObservedUrl(
                    observed_url=normalized,
                    source_kind=source_kind,
                    context_path=context_path,
                )
            )

        for idx, entry in enumerate(url_surface_json or []):
            if not isinstance(entry, dict):
                continue
            add(
                entry.get("observed_url"),
                str(entry.get("source_kind", "collector_surface")),
                context_path=f"url_surface_json[{idx}]",
            )

        for idx, entity in enumerate(entities_json or []):
            if not isinstance(entity, dict):
                continue
            entity_type = self._entity_type_name(entity)
            surface = entity.get("surface")
            if entity_type == "textEntityTypeTextUrl":
                url = self._extract_text_url_entity_url(entity)
                add(url, "entity", context_path=f"entities_json[{idx}].{surface or 'unknown'}")

        if display_surface:
            for idx, match in enumerate(_URL_REGEX.findall(display_surface)):
                add(match, "regex", context_path=f"display_surface.regex[{idx}]")

        return observed

    @staticmethod
    def _entity_type_name(entity: dict[str, Any]) -> str | None:
        entity_type = entity.get("type")
        if isinstance(entity_type, dict):
            raw = entity_type.get("@type")
            if isinstance(raw, str):
                return raw
        return None

    @staticmethod
    def _extract_text_url_entity_url(entity: dict[str, Any]) -> str | None:
        entity_type = entity.get("type")
        if isinstance(entity_type, dict):
            value = entity_type.get("url")
            if isinstance(value, str):
                return value.strip() or None
        return None

    @staticmethod
    def _normalize_url(value: str | None) -> str | None:
        if not value:
            return None
        stripped = value.strip()
        if stripped.startswith(("http://", "https://")):
            return stripped
        return None
```

---

## 5-6. `src/services/router_normalizer/short_url_resolver.py`

```python
from __future__ import annotations

from dataclasses import replace
from typing import Protocol
from urllib.parse import urlparse

from .models import ObservedUrl


class ShortUrlTransportProtocol(Protocol):
    async def expand(
        self,
        url: str,
        *,
        timeout_ms: int,
        max_redirects: int,
    ) -> str | None: ...


class ShortUrlResolver:
    """Limited short URL expansion.

    Rules:
    - allowlist only
    - failure must not abort normalization
    - this is not a crawler
    """

    def __init__(
        self,
        *,
        transport: ShortUrlTransportProtocol | None,
        allowlist: tuple[str, ...],
        timeout_ms: int,
        max_redirects: int,
    ) -> None:
        self._transport = transport
        self._allowlist = tuple(host.lower() for host in allowlist)
        self._timeout_ms = timeout_ms
        self._max_redirects = max_redirects

    async def resolve_many(self, urls: list[ObservedUrl]) -> list[ObservedUrl]:
        results: list[ObservedUrl] = []
        for item in urls:
            if not self._is_shortener(item.observed_url):
                results.append(item)
                continue

            resolved = None
            if self._transport is not None:
                try:
                    resolved = await self._transport.expand(
                        item.observed_url,
                        timeout_ms=self._timeout_ms,
                        max_redirects=self._max_redirects,
                    )
                except Exception:
                    resolved = None

            if resolved:
                results.append(replace(item, normalized_url=item.observed_url, resolved_url=resolved))
            else:
                results.append(replace(item, normalized_url=item.observed_url))
        return results

    def _is_shortener(self, url: str) -> bool:
        hostname = (urlparse(url).hostname or "").lower()
        return hostname in self._allowlist
```

---

## 5-7. `src/services/router_normalizer/canonicalizer.py`

```python
from __future__ import annotations

import hashlib
from dataclasses import replace
from typing import Iterable
from urllib.parse import parse_qs, urlparse

from .models import CanonicalArtifactDraft, ObservedUrl


class Canonicalizer:
    def canonicalize_many(self, urls: Iterable[ObservedUrl]) -> tuple[list[CanonicalArtifactDraft], list[ObservedUrl]]:
        artifacts: dict[str, CanonicalArtifactDraft] = {}
        normalized_observations: list[ObservedUrl] = []

        for url in urls:
            artifact, normalized = self.canonicalize_one(url)
            normalized_observations.append(normalized)
            if artifact is None:
                continue
            artifacts[artifact.canonical_id] = artifact

        return list(artifacts.values()), normalized_observations

    def canonicalize_one(self, observed: ObservedUrl) -> tuple[CanonicalArtifactDraft | None, ObservedUrl]:
        url = observed.resolved_url or observed.observed_url
        parsed = urlparse(url)
        host = (parsed.hostname or "").lower()

        if host in {"github.com", "www.github.com"}:
            artifact = self._canonicalize_github(url)
            return artifact, replace(
                observed,
                normalized_url=observed.normalized_url or url,
                resolved_url=observed.resolved_url,
                canonical_url=artifact.canonical_url if artifact else None,
                classification=artifact.artifact_type if artifact else "unknown_link",
            )

        if host in {"gist.github.com", "www.gist.github.com"}:
            artifact = self._canonicalize_gist(url)
            return artifact, replace(
                observed,
                normalized_url=observed.normalized_url or url,
                resolved_url=observed.resolved_url,
                canonical_url=artifact.canonical_url if artifact else None,
                classification=artifact.artifact_type if artifact else "unknown_link",
            )

        if host in {"x.com", "www.x.com", "twitter.com", "www.twitter.com", "mobile.twitter.com"}:
            artifact = self._canonicalize_x(url)
            return artifact, replace(
                observed,
                normalized_url=observed.normalized_url or url,
                resolved_url=observed.resolved_url,
                canonical_url=artifact.canonical_url if artifact else None,
                classification=artifact.artifact_type if artifact else "unknown_link",
            )

        if host:
            artifact = self._canonicalize_web(url)
            return artifact, replace(
                observed,
                normalized_url=observed.normalized_url or url,
                resolved_url=observed.resolved_url,
                canonical_url=artifact.canonical_url,
                classification=artifact.artifact_type,
            )

        return None, replace(
            observed,
            normalized_url=observed.normalized_url or url,
            resolved_url=observed.resolved_url,
            classification="unknown_link",
        )

    def _canonicalize_github(self, url: str) -> CanonicalArtifactDraft | None:
        parsed = urlparse(url)
        parts = [p for p in parsed.path.split("/") if p]
        if len(parts) < 2:
            return CanonicalArtifactDraft(
                artifact_type="unknown_link",
                canonical_id=f"unknown:{self._stable_hash(url)}",
                canonical_url=url,
                normalized_host="github.com",
                artifact_key_json={"original_url": url},
            )

        owner, repo = parts[0], parts[1]
        repo_name = repo.removesuffix(".git")
        repo_anchor = f"{owner}/{repo_name}"
        base_url = f"https://github.com/{repo_anchor}"

        if len(parts) == 2:
            return CanonicalArtifactDraft(
                artifact_type="github_repo",
                canonical_id=f"github:repo:{repo_anchor.lower()}",
                canonical_url=base_url,
                normalized_host="github.com",
                artifact_key_json={"owner": owner, "repo": repo_name},
            )

        if parts[2] in {"tree", "blob"} and len(parts) >= 5:
            path_tail = "/".join(parts[4:])
            canonical_url = f"{base_url}/{parts[2]}/{parts[3]}/{path_tail}"
            return CanonicalArtifactDraft(
                artifact_type="github_subpath",
                canonical_id=f"github:subpath:{repo_anchor.lower()}:{parts[3]}:{path_tail}",
                canonical_url=canonical_url,
                normalized_host="github.com",
                artifact_key_json={
                    "owner": owner,
                    "repo": repo_name,
                    "ref": parts[3],
                    "path": path_tail,
                    "repo_anchor_canonical_id": f"github:repo:{repo_anchor.lower()}",
                },
            )

        if parts[2] in {"issues", "pull", "releases"}:
            page_tail = "/".join(parts[2:])
            canonical_url = f"{base_url}/{page_tail}"
            return CanonicalArtifactDraft(
                artifact_type="github_repo_page",
                canonical_id=f"github:repo_page:{repo_anchor.lower()}:{page_tail}",
                canonical_url=canonical_url,
                normalized_host="github.com",
                artifact_key_json={
                    "owner": owner,
                    "repo": repo_name,
                    "page_path": page_tail,
                    "repo_anchor_canonical_id": f"github:repo:{repo_anchor.lower()}",
                },
            )

        return CanonicalArtifactDraft(
            artifact_type="github_repo_page",
            canonical_id=f"github:repo_page:{repo_anchor.lower()}:{'/'.join(parts[2:])}",
            canonical_url=url,
            normalized_host="github.com",
            artifact_key_json={
                "owner": owner,
                "repo": repo_name,
                "page_path": "/".join(parts[2:]),
                "repo_anchor_canonical_id": f"github:repo:{repo_anchor.lower()}",
            },
        )

    def _canonicalize_gist(self, url: str) -> CanonicalArtifactDraft | None:
        parsed = urlparse(url)
        parts = [p for p in parsed.path.split("/") if p]
        gist_id = parts[-1] if parts else None
        if not gist_id:
            return None
        canonical_url = f"https://gist.github.com/{'/'.join(parts[:-1])}/{gist_id}" if len(parts) > 1 else url
        return CanonicalArtifactDraft(
            artifact_type="github_gist",
            canonical_id=f"github:gist:{gist_id.lower()}",
            canonical_url=canonical_url,
            normalized_host="gist.github.com",
            artifact_key_json={"gist_id": gist_id},
        )

    def _canonicalize_x(self, url: str) -> CanonicalArtifactDraft | None:
        parsed = urlparse(url)
        parts = [p for p in parsed.path.split("/") if p]
        post_id = None
        if len(parts) >= 3 and parts[1] == "status":
            post_id = parts[2]
        elif len(parts) >= 4 and parts[0] == "i" and parts[1] == "web" and parts[2] == "status":
            post_id = parts[3]
        if not post_id:
            return None
        canonical_url = f"https://x.com/i/web/status/{post_id}"
        return CanonicalArtifactDraft(
            artifact_type="x_post",
            canonical_id=f"x:post:{post_id}",
            canonical_url=canonical_url,
            normalized_host=(parsed.hostname or "").lower(),
            artifact_key_json={"post_id": post_id},
        )

    def _canonicalize_web(self, url: str) -> CanonicalArtifactDraft:
        parsed = urlparse(url)
        host = (parsed.hostname or "").lower()
        stable_path = parsed.path or "/"
        path_hash = self._stable_hash(f"{host}{stable_path}")
        canonical_url = f"{parsed.scheme}://{host}{stable_path}"
        return CanonicalArtifactDraft(
            artifact_type="web_article",
            canonical_id=f"web:{host}:{path_hash}",
            canonical_url=canonical_url,
            normalized_host=host,
            artifact_key_json={
                "host": host,
                "path": stable_path,
                "query_keys": sorted(parse_qs(parsed.query).keys()),
            },
        )

    @staticmethod
    def _stable_hash(value: str) -> str:
        return hashlib.sha256(value.encode("utf-8")).hexdigest()[:16]
```

---

## 5-8. `src/services/router_normalizer/trigger_rules.py`

```python
from __future__ import annotations

from .models import CanonicalArtifactDraft, NormalizedTextSurfaces, TriggerEvaluation


class TriggerRules:
    def evaluate(
        self,
        *,
        surfaces: NormalizedTextSurfaces,
        artifacts: list[CanonicalArtifactDraft],
    ) -> TriggerEvaluation:
        keyword_surface = surfaces.keyword_scan_surface or ""
        has_github = any(a.artifact_type in {"github_repo", "github_subpath", "github_gist", "github_repo_page"} for a in artifacts)
        has_x = any(a.artifact_type == "x_post" for a in artifacts)

        has_github_keyword = "github" in keyword_surface
        has_vibe = "vibe coding" in keyword_surface or "vibe-coding" in keyword_surface
        has_ai = "ai" in keyword_surface
        has_dev_context = any(
            token in keyword_surface
            for token in (
                "tool",
                "workflow",
                "prototype",
                "repo",
                "library",
                "sdk",
                "agent",
                "coding",
                "prompt",
                "developer",
                "dev ",
            )
        )

        signal_detected = any([has_github, has_x, has_github_keyword, has_vibe, has_ai])
        if not signal_detected:
            return TriggerEvaluation(
                signal_detected=False,
                candidate_eligible=False,
                trigger_strength=None,
                reason_codes=[],
                suppression_reason_codes=["no_signal_detected"],
            )

        if has_github or has_x or has_vibe or (has_github_keyword and has_dev_context):
            return TriggerEvaluation(
                signal_detected=True,
                candidate_eligible=True,
                trigger_strength="strong",
                reason_codes=["strong_signal_detected"],
                suppression_reason_codes=[],
            )

        if has_ai and has_dev_context:
            return TriggerEvaluation(
                signal_detected=True,
                candidate_eligible=True,
                trigger_strength="medium",
                reason_codes=["ai_with_dev_context"],
                suppression_reason_codes=[],
            )

        if not artifacts and has_dev_context and surfaces.display_surface:
            return TriggerEvaluation(
                signal_detected=True,
                candidate_eligible=True,
                trigger_strength="medium",
                reason_codes=["text_idea_candidate"],
                suppression_reason_codes=[],
            )

        if has_ai and not has_dev_context:
            return TriggerEvaluation(
                signal_detected=True,
                candidate_eligible=False,
                trigger_strength="weak",
                reason_codes=["weak_ai_signal"],
                suppression_reason_codes=["ai_without_dev_context"],
            )

        return TriggerEvaluation(
            signal_detected=True,
            candidate_eligible=False,
            trigger_strength="weak",
            reason_codes=["weak_signal_detected"],
            suppression_reason_codes=["insufficient_candidate_context"],
        )
```

---

## 5-9. `src/services/router_normalizer/repositories.py`

```python
from __future__ import annotations

import json
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Any, AsyncIterator, Mapping

import sqlalchemy as sa
from sqlalchemy.ext.asyncio import AsyncSession

from .models import (
    CandidateGroupProposalDraft,
    CanonicalArtifactDraft,
    ObservedUrl,
    SourceMessageEnvelope,
    SourceMessageRecord,
    TriggerEvaluation,
)


def _json_default(value: Any) -> str:
    if isinstance(value, datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc).isoformat()
    raise TypeError(f"Unsupported JSON type: {type(value)!r}")


def _jsonb_dumps(value: Any) -> str | None:
    if value is None:
        return None
    return json.dumps(value, ensure_ascii=False, default=_json_default)


class RouterNormalizerRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    @asynccontextmanager
    async def transaction(self) -> AsyncIterator[AsyncSession]:
        if self._session.in_transaction():
            yield self._session
            return
        async with self._session.begin():
            yield self._session

    async def load_source_message_for_envelope(self, envelope: SourceMessageEnvelope) -> SourceMessageRecord | None:
        result = await self._session.execute(
            sa.text(
                """
                SELECT
                    source_message_id,
                    chat_id,
                    message_id,
                    logical_post_key,
                    text_body,
                    caption_text,
                    text_surface,
                    entities_json,
                    url_surface_json,
                    raw_message_json,
                    current_version_no,
                    posted_at,
                    edited_at,
                    deleted_at
                FROM source_messages
                WHERE source_message_id = CAST(:source_message_id AS uuid)
                """
            ),
            {"source_message_id": envelope.source_message_id},
        )
        row = result.mappings().first()
        if row is None:
            return None

        return SourceMessageRecord(
            source_message_id=str(row["source_message_id"]),
            chat_id=int(row["chat_id"]),
            message_id=int(row["message_id"]),
            logical_post_key=row["logical_post_key"],
            text_body=row["text_body"],
            caption_text=row["caption_text"],
            text_surface=row["text_surface"],
            entities_json=row["entities_json"],
            url_surface_json=row["url_surface_json"],
            raw_message_json=row["raw_message_json"],
            current_version_no=int(row["current_version_no"]),
            posted_at=row["posted_at"],
            edited_at=row["edited_at"],
            deleted_at=row["deleted_at"],
        )

    async def insert_normalization_run(
        self,
        *,
        source_message_id: str,
        source_version_no: int,
        normalizer_version: str,
        evaluation: TriggerEvaluation,
        result_hash: str | None,
    ) -> str:
        result = await self._session.execute(
            sa.text(
                """
                INSERT INTO normalization_runs (
                    source_message_id,
                    source_version_no,
                    normalizer_version,
                    signal_detected,
                    candidate_eligible,
                    trigger_strength,
                    result_hash,
                    completed_at
                )
                VALUES (
                    CAST(:source_message_id AS uuid),
                    :source_version_no,
                    :normalizer_version,
                    :signal_detected,
                    :candidate_eligible,
                    :trigger_strength,
                    :result_hash,
                    now()
                )
                ON CONFLICT (source_message_id, source_version_no, normalizer_version)
                DO UPDATE SET
                    signal_detected = EXCLUDED.signal_detected,
                    candidate_eligible = EXCLUDED.candidate_eligible,
                    trigger_strength = EXCLUDED.trigger_strength,
                    result_hash = EXCLUDED.result_hash,
                    completed_at = now()
                RETURNING normalization_run_id
                """
            ),
            {
                "source_message_id": source_message_id,
                "source_version_no": source_version_no,
                "normalizer_version": normalizer_version,
                "signal_detected": evaluation.signal_detected,
                "candidate_eligible": evaluation.candidate_eligible,
                "trigger_strength": evaluation.trigger_strength,
                "result_hash": result_hash,
            },
        )
        return str(result.scalar_one())

    async def insert_suppression_trace(
        self,
        *,
        normalization_run_id: str,
        reason_code: str,
        trigger_strength: str | None,
        notes_json: dict[str, Any] | None,
    ) -> None:
        await self._session.execute(
            sa.text(
                """
                INSERT INTO normalization_suppression_traces (
                    normalization_run_id,
                    reason_code,
                    trigger_strength,
                    notes_json,
                    created_at
                )
                VALUES (
                    CAST(:normalization_run_id AS uuid),
                    :reason_code,
                    :trigger_strength,
                    CAST(:notes_json AS jsonb),
                    now()
                )
                """
            ),
            {
                "normalization_run_id": normalization_run_id,
                "reason_code": reason_code,
                "trigger_strength": trigger_strength,
                "notes_json": _jsonb_dumps(notes_json),
            },
        )

    async def upsert_artifact(self, artifact: CanonicalArtifactDraft) -> Mapping[str, Any]:
        result = await self._session.execute(
            sa.text(
                """
                INSERT INTO artifact_registry (
                    artifact_type,
                    canonical_id,
                    canonical_url,
                    normalized_host,
                    artifact_key_json,
                    created_at,
                    updated_at
                )
                VALUES (
                    CAST(:artifact_type AS artifact_type_enum),
                    :canonical_id,
                    :canonical_url,
                    :normalized_host,
                    CAST(:artifact_key_json AS jsonb),
                    now(),
                    now()
                )
                ON CONFLICT (canonical_id)
                DO UPDATE SET
                    canonical_url = COALESCE(EXCLUDED.canonical_url, artifact_registry.canonical_url),
                    normalized_host = COALESCE(EXCLUDED.normalized_host, artifact_registry.normalized_host),
                    artifact_key_json = COALESCE(EXCLUDED.artifact_key_json, artifact_registry.artifact_key_json),
                    updated_at = now()
                RETURNING *
                """
            ),
            {
                "artifact_type": artifact.artifact_type,
                "canonical_id": artifact.canonical_id,
                "canonical_url": artifact.canonical_url,
                "normalized_host": artifact.normalized_host,
                "artifact_key_json": _jsonb_dumps(artifact.artifact_key_json),
            },
        )
        return result.mappings().one()

    async def insert_artifact_observation(
        self,
        *,
        artifact_id: str,
        source_message_id: str,
        source_version_no: int,
        observed: ObservedUrl,
    ) -> None:
        await self._session.execute(
            sa.text(
                """
                INSERT INTO artifact_observations (
                    artifact_id,
                    source_message_id,
                    source_version_no,
                    observed_url,
                    source_kind,
                    normalized_url,
                    resolved_url,
                    canonical_url,
                    classification,
                    context_path,
                    created_at
                )
                VALUES (
                    CAST(:artifact_id AS uuid),
                    CAST(:source_message_id AS uuid),
                    :source_version_no,
                    :observed_url,
                    :source_kind,
                    :normalized_url,
                    :resolved_url,
                    :canonical_url,
                    :classification,
                    :context_path,
                    now()
                )
                """
            ),
            {
                "artifact_id": artifact_id,
                "source_message_id": source_message_id,
                "source_version_no": source_version_no,
                "observed_url": observed.observed_url,
                "source_kind": observed.source_kind,
                "normalized_url": observed.normalized_url,
                "resolved_url": observed.resolved_url,
                "canonical_url": observed.canonical_url,
                "classification": observed.classification,
                "context_path": observed.context_path,
            },
        )

    async def upsert_candidate_group_proposal(
        self,
        *,
        proposal: CandidateGroupProposalDraft,
        canonical_id_to_artifact_id: dict[str, str],
    ) -> Mapping[str, Any]:
        initial_primary_artifact_id = canonical_id_to_artifact_id[proposal.initial_primary_artifact_canonical_id]
        current_primary_artifact_id = canonical_id_to_artifact_id[proposal.current_primary_artifact_canonical_id]

        result = await self._session.execute(
            sa.text(
                """
                INSERT INTO candidate_group_proposals (
                    source_message_id,
                    source_version_no,
                    initial_primary_artifact_id,
                    current_primary_artifact_id,
                    proposal_status,
                    normalizer_version,
                    dedupe_subject_key,
                    created_at,
                    updated_at
                )
                VALUES (
                    CAST(:source_message_id AS uuid),
                    :source_version_no,
                    CAST(:initial_primary_artifact_id AS uuid),
                    CAST(:current_primary_artifact_id AS uuid),
                    'proposed',
                    :normalizer_version,
                    :dedupe_subject_key,
                    now(),
                    now()
                )
                ON CONFLICT (source_message_id, source_version_no, dedupe_subject_key)
                DO UPDATE SET
                    current_primary_artifact_id = EXCLUDED.current_primary_artifact_id,
                    proposal_status = 'proposed',
                    updated_at = now()
                RETURNING *
                """
            ),
            {
                "source_message_id": proposal.source_message_id,
                "source_version_no": proposal.source_version_no,
                "initial_primary_artifact_id": initial_primary_artifact_id,
                "current_primary_artifact_id": current_primary_artifact_id,
                "normalizer_version": proposal.normalizer_version,
                "dedupe_subject_key": proposal.dedupe_subject_key,
            },
        )
        return result.mappings().one()

    async def replace_candidate_group_members(
        self,
        *,
        candidate_group_id: str,
        proposal: CandidateGroupProposalDraft,
        canonical_id_to_artifact_id: dict[str, str],
    ) -> None:
        await self._session.execute(
            sa.text(
                """
                DELETE FROM candidate_group_members
                WHERE candidate_group_id = CAST(:candidate_group_id AS uuid)
                """
            ),
            {"candidate_group_id": candidate_group_id},
        )

        for member in proposal.members:
            await self._session.execute(
                sa.text(
                    """
                    INSERT INTO candidate_group_members (
                        candidate_group_id,
                        artifact_id,
                        member_role,
                        member_order,
                        created_at
                    )
                    VALUES (
                        CAST(:candidate_group_id AS uuid),
                        CAST(:artifact_id AS uuid),
                        :member_role,
                        :member_order,
                        now()
                    )
                    """
                ),
                {
                    "candidate_group_id": candidate_group_id,
                    "artifact_id": canonical_id_to_artifact_id[member.artifact_canonical_id],
                    "member_role": member.member_role,
                    "member_order": member.member_order,
                },
            )

    async def insert_outbox_enrich_request(
        self,
        *,
        candidate_group_id: str,
        artifact_id: str,
        artifact_type: str,
        provider_route: str,
        refresh_mode: str = "standard",
        depth_budget: int = 1,
        dedupe_key: str,
    ) -> None:
        await self._session.execute(
            sa.text(
                """
                INSERT INTO event_outbox (
                    event_type,
                    aggregate_type,
                    aggregate_id,
                    dedupe_key,
                    payload_json,
                    status,
                    created_at
                )
                VALUES (
                    'artifact.enrich.requested.v1',
                    'candidate_group',
                    CAST(:aggregate_id AS uuid),
                    :dedupe_key,
                    CAST(:payload_json AS jsonb),
                    'pending',
                    now()
                )
                ON CONFLICT (dedupe_key)
                DO NOTHING
                """
            ),
            {
                "aggregate_id": candidate_group_id,
                "dedupe_key": dedupe_key,
                "payload_json": _jsonb_dumps(
                    {
                        "candidate_group_id": candidate_group_id,
                        "artifact_id": artifact_id,
                        "artifact_type": artifact_type,
                        "provider_route": provider_route,
                        "refresh_mode": refresh_mode,
                        "depth_budget": depth_budget,
                    }
                ),
            },
        )
```

---

## 5-10. `src/services/router_normalizer/service.py`

```python
from __future__ import annotations

import hashlib
import logging
from dataclasses import dataclass
from typing import Any

from .canonicalizer import Canonicalizer
from .config import RouterNormalizerConfig
from .models import (
    CandidateGroupProposalDraft,
    CandidateMemberDraft,
    NormalizeExecutionResult,
    SourceMessageEnvelope,
    SourceMessageRecord,
)
from .repositories import RouterNormalizerRepository
from .short_url_resolver import ShortUrlResolver
from .text_surfaces import TextSurfaceNormalizer
from .trigger_rules import TriggerRules
from .url_extraction import UrlExtractor


@dataclass(slots=True, frozen=True)
class NormalizationJobResult:
    source_message_id: str
    signal_detected: bool
    candidate_eligible: bool
    proposals_created: int
    observations_created: int
    enrich_events_created: int
    suppression_reason_codes: list[str]


class RouterNormalizerService:
    def __init__(
        self,
        config: RouterNormalizerConfig,
        *,
        repository: RouterNormalizerRepository,
        surface_normalizer: TextSurfaceNormalizer,
        url_extractor: UrlExtractor,
        short_url_resolver: ShortUrlResolver,
        canonicalizer: Canonicalizer,
        trigger_rules: TriggerRules,
        logger: logging.Logger | None = None,
    ) -> None:
        self._config = config
        self._repository = repository
        self._surface_normalizer = surface_normalizer
        self._url_extractor = url_extractor
        self._short_url_resolver = short_url_resolver
        self._canonicalizer = canonicalizer
        self._trigger_rules = trigger_rules
        self._logger = logger or logging.getLogger(__name__)

    async def handle_envelope(self, envelope: SourceMessageEnvelope) -> NormalizationJobResult:
        source_message = await self._repository.load_source_message_for_envelope(envelope)
        if source_message is None:
            return NormalizationJobResult(
                source_message_id=envelope.source_message_id,
                signal_detected=False,
                candidate_eligible=False,
                proposals_created=0,
                observations_created=0,
                enrich_events_created=0,
                suppression_reason_codes=["source_message_missing"],
            )

        execution = await self._execute(source_message)
        result_hash = self._result_hash(source_message, execution)
        surfaces = self._surface_normalizer.normalize(
            text_body=source_message.text_body,
            caption_text=source_message.caption_text,
            text_surface=source_message.text_surface,
        )
        evaluation = self._trigger_rules.evaluate(surfaces=surfaces, artifacts=execution.artifacts)

        async with self._repository.transaction():
            normalization_run_id = await self._repository.insert_normalization_run(
                source_message_id=source_message.source_message_id,
                source_version_no=source_message.current_version_no,
                normalizer_version=self._config.normalizer_version,
                evaluation=evaluation,
                result_hash=result_hash,
            )

            if execution.suppression_reason_codes:
                for reason_code in execution.suppression_reason_codes:
                    await self._repository.insert_suppression_trace(
                        normalization_run_id=normalization_run_id,
                        reason_code=reason_code,
                        trigger_strength=execution.trigger_strength,
                        notes_json={"source_message_id": source_message.source_message_id},
                    )

            canonical_id_to_artifact_id: dict[str, str] = {}
            for artifact in execution.artifacts:
                row = await self._repository.upsert_artifact(artifact)
                canonical_id_to_artifact_id[artifact.canonical_id] = str(row["artifact_id"])

            observations_created = 0
            for observed in execution.observations:
                matched_artifact_id = None
                if observed.canonical_url:
                    for artifact in execution.artifacts:
                        if artifact.canonical_url == observed.canonical_url:
                            matched_artifact_id = canonical_id_to_artifact_id[artifact.canonical_id]
                            break
                if matched_artifact_id is None:
                    continue
                await self._repository.insert_artifact_observation(
                    artifact_id=matched_artifact_id,
                    source_message_id=source_message.source_message_id,
                    source_version_no=source_message.current_version_no,
                    observed=observed,
                )
                observations_created += 1

            proposals_created = 0
            enrich_events_created = 0
            for proposal in execution.proposals:
                proposal_row = await self._repository.upsert_candidate_group_proposal(
                    proposal=proposal,
                    canonical_id_to_artifact_id=canonical_id_to_artifact_id,
                )
                candidate_group_id = str(proposal_row["candidate_group_id"])
                await self._repository.replace_candidate_group_members(
                    candidate_group_id=candidate_group_id,
                    proposal=proposal,
                    canonical_id_to_artifact_id=canonical_id_to_artifact_id,
                )
                proposals_created += 1

                for member in proposal.members:
                    provider_route = self._provider_route_for_artifact(member.artifact_type)
                    if provider_route is None:
                        continue
                    artifact_id = canonical_id_to_artifact_id[member.artifact_canonical_id]
                    await self._repository.insert_outbox_enrich_request(
                        candidate_group_id=candidate_group_id,
                        artifact_id=artifact_id,
                        artifact_type=member.artifact_type,
                        provider_route=provider_route,
                        dedupe_key=self._build_enrich_dedupe_key(candidate_group_id, artifact_id, provider_route),
                    )
                    enrich_events_created += 1

        return NormalizationJobResult(
            source_message_id=source_message.source_message_id,
            signal_detected=execution.signal_detected,
            candidate_eligible=execution.candidate_eligible,
            proposals_created=proposals_created,
            observations_created=observations_created,
            enrich_events_created=enrich_events_created,
            suppression_reason_codes=execution.suppression_reason_codes,
        )

    async def _execute(self, source_message: SourceMessageRecord) -> NormalizeExecutionResult:
        surfaces = self._surface_normalizer.normalize(
            text_body=source_message.text_body,
            caption_text=source_message.caption_text,
            text_surface=source_message.text_surface,
        )
        observed_urls = self._url_extractor.extract(
            entities_json=source_message.entities_json,
            url_surface_json=source_message.url_surface_json,
            display_surface=surfaces.display_surface,
        )
        observed_urls = await self._short_url_resolver.resolve_many(observed_urls)
        artifacts, observations = self._canonicalizer.canonicalize_many(observed_urls)

        if not artifacts and self._should_create_text_idea(surfaces):
            artifacts.append(self._build_text_idea_artifact(source_message, surfaces))

        evaluation = self._trigger_rules.evaluate(surfaces=surfaces, artifacts=artifacts)
        proposals = self._propose_candidate_groups(source_message, artifacts) if evaluation.candidate_eligible else []

        return NormalizeExecutionResult(
            signal_detected=evaluation.signal_detected,
            candidate_eligible=evaluation.candidate_eligible,
            trigger_strength=evaluation.trigger_strength,
            artifacts=artifacts,
            observations=observations,
            proposals=proposals,
            suppression_reason_codes=evaluation.suppression_reason_codes,
        )

    def _propose_candidate_groups(
        self,
        source_message: SourceMessageRecord,
        artifacts: list,
    ) -> list[CandidateGroupProposalDraft]:
        github_artifacts = [a for a in artifacts if a.artifact_type in {"github_repo", "github_gist"}]
        x_artifacts = [a for a in artifacts if a.artifact_type == "x_post"]
        web_artifacts = [a for a in artifacts if a.artifact_type == "web_article"]
        text_ideas = [a for a in artifacts if a.artifact_type == "text_idea"]
        others = [a for a in artifacts if a.artifact_type in {"github_subpath", "github_repo_page", "unknown_link", "short_url_unresolved"}]

        proposals: list[CandidateGroupProposalDraft] = []

        if github_artifacts:
            for primary in github_artifacts:
                members = [
                    CandidateMemberDraft(primary.canonical_id, primary.artifact_type, "primary", 0)
                ]
                for idx, artifact in enumerate([a for a in artifacts if a.canonical_id != primary.canonical_id], start=1):
                    members.append(CandidateMemberDraft(artifact.canonical_id, artifact.artifact_type, "supporting", idx))
                proposals.append(
                    CandidateGroupProposalDraft(
                        source_message_id=source_message.source_message_id,
                        source_version_no=source_message.current_version_no,
                        initial_primary_artifact_canonical_id=primary.canonical_id,
                        current_primary_artifact_canonical_id=primary.canonical_id,
                        normalizer_version=self._config.normalizer_version,
                        dedupe_subject_key=f"{source_message.source_message_id}:{primary.canonical_id}",
                        members=members,
                    )
                )
            return proposals

        if x_artifacts:
            for primary in x_artifacts:
                members = [CandidateMemberDraft(primary.canonical_id, primary.artifact_type, "primary", 0)]
                for idx, artifact in enumerate([*web_artifacts, *others], start=1):
                    members.append(CandidateMemberDraft(artifact.canonical_id, artifact.artifact_type, "supporting", idx))
                proposals.append(
                    CandidateGroupProposalDraft(
                        source_message_id=source_message.source_message_id,
                        source_version_no=source_message.current_version_no,
                        initial_primary_artifact_canonical_id=primary.canonical_id,
                        current_primary_artifact_canonical_id=primary.canonical_id,
                        normalizer_version=self._config.normalizer_version,
                        dedupe_subject_key=f"{source_message.source_message_id}:{primary.canonical_id}",
                        members=members,
                    )
                )
            return proposals

        if web_artifacts:
            primary = web_artifacts[0]
            members = [CandidateMemberDraft(primary.canonical_id, primary.artifact_type, "primary", 0)]
            for idx, artifact in enumerate(web_artifacts[1:] + others, start=1):
                members.append(CandidateMemberDraft(artifact.canonical_id, artifact.artifact_type, "supporting", idx))
            proposals.append(
                CandidateGroupProposalDraft(
                    source_message_id=source_message.source_message_id,
                    source_version_no=source_message.current_version_no,
                    initial_primary_artifact_canonical_id=primary.canonical_id,
                    current_primary_artifact_canonical_id=primary.canonical_id,
                    normalizer_version=self._config.normalizer_version,
                    dedupe_subject_key=f"{source_message.source_message_id}:{primary.canonical_id}",
                    members=members,
                )
            )
            return proposals

        if text_ideas:
            primary = text_ideas[0]
            proposals.append(
                CandidateGroupProposalDraft(
                    source_message_id=source_message.source_message_id,
                    source_version_no=source_message.current_version_no,
                    initial_primary_artifact_canonical_id=primary.canonical_id,
                    current_primary_artifact_canonical_id=primary.canonical_id,
                    normalizer_version=self._config.normalizer_version,
                    dedupe_subject_key=f"{source_message.source_message_id}:{primary.canonical_id}",
                    members=[CandidateMemberDraft(primary.canonical_id, primary.artifact_type, "primary", 0)],
                )
            )
        return proposals

    @staticmethod
    def _provider_route_for_artifact(artifact_type: str) -> str | None:
        if artifact_type in {"github_repo", "github_subpath", "github_gist", "github_repo_page"}:
            return "github"
        if artifact_type == "x_post":
            return "x"
        if artifact_type == "web_article":
            return "web"
        return None

    @staticmethod
    def _build_enrich_dedupe_key(candidate_group_id: str, artifact_id: str, provider_route: str) -> str:
        return f"enrich_request:{candidate_group_id}:{artifact_id}:{provider_route}:standard:1"

    @staticmethod
    def _should_create_text_idea(surfaces) -> bool:
        return bool(surfaces.display_surface)

    @staticmethod
    def _build_text_idea_artifact(source_message: SourceMessageRecord, surfaces) -> Any:
        from .models import CanonicalArtifactDraft
        hash_surface = surfaces.hash_surface or source_message.logical_post_key
        digest = hashlib.sha256(hash_surface.encode("utf-8")).hexdigest()
        return CanonicalArtifactDraft(
            artifact_type="text_idea",
            canonical_id=f"text_idea:{source_message.source_message_id}:{digest[:16]}",
            canonical_url=None,
            normalized_host=None,
            artifact_key_json={
                "source_message_id": source_message.source_message_id,
                "hash_surface_sha256": digest,
            },
        )

    @staticmethod
    def _result_hash(source_message: SourceMessageRecord, execution: NormalizeExecutionResult) -> str:
        payload = {
            "source_message_id": source_message.source_message_id,
            "source_version_no": source_message.current_version_no,
            "signal_detected": execution.signal_detected,
            "candidate_eligible": execution.candidate_eligible,
            "trigger_strength": execution.trigger_strength,
            "artifact_ids": sorted(a.canonical_id for a in execution.artifacts),
            "suppression_reason_codes": sorted(execution.suppression_reason_codes),
            "proposal_subjects": sorted(p.dedupe_subject_key for p in execution.proposals),
        }
        return hashlib.sha256(repr(payload).encode("utf-8")).hexdigest()
```

---

## 5-11. `src/services/router_normalizer/main.py`

```python
from __future__ import annotations

import asyncio
import logging
import sys

from .config import RouterNormalizerConfig


def _configure_logging(log_level: str) -> None:
    logging.basicConfig(
        level=getattr(logging, log_level, logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
        stream=sys.stdout,
    )


async def _run() -> None:
    config = RouterNormalizerConfig.from_env()
    _configure_logging(config.log_level)

    logger = logging.getLogger("router_normalizer")
    logger.info(
        "router_normalizer_bootstrap_ready",
        extra={
            "service": "router-normalizer",
            "event": "router_normalizer_bootstrap_ready",
            "queue_name": config.queue_name,
            "normalizer_version": config.normalizer_version,
        },
    )
    await asyncio.sleep(0)


def main() -> None:
    asyncio.run(_run())


if __name__ == "__main__":
    main()
```

---

## 6. 구현 메모

### 6-1. 왜 `router-normalizer`를 한 번에 전부 완성하지 않고 core deterministic package로 끊는가

이번 단계의 목적은 구조를 깨지 않는 것이다.  
따라서 먼저 고정해야 하는 것은:

- source message rehydration
- surface normalization
- URL extraction
- short URL expansion 경계
- canonicalization 규칙
- trigger evaluation
- proposal generation
- DB write contract

이다.

반대로 지금 당장 넣지 않아도 되는 것은:

- Redis Streams consumer 구현 세부
- retry/reclaim/DLQ 승격
- channel override 고도화
- shared common canonicalization 추출
- event schema registry 파일 완성

지금 여기서 너무 많은 인프라를 한 번에 넣으면, deterministic normalizer 경계가 흐려진다.

### 6-2. 왜 `text_idea`는 message-scoped로 잡았는가

4단계 문서가 `text_idea`는 전역 완벽 dedupe보다 **누락 방지 우선**이라고 잠갔다.  
그래서 v0.1에서는 `source_message_id + hash_surface` 기반의 message-scoped artifact로 두는 편이 맞다.

### 6-3. 왜 `candidate.bundle.refresh.v1`는 아직 내보내지 않았는가

현재 순서상 enrichers가 먼저 붙어야 하고, snapshot이 생겨야 evidence-assembler가 candidate 중심 bundle을 만들 수 있다.  
따라서 normalizer v0.1은 `artifact.enrich.requested.v1`까지만 내보내는 쪽이 더 보수적이고 구조 안정적이다.

### 6-4. 왜 GitHub repo inferred anchor row를 여기서 별도 artifact로 만들지 않았는가

4단계 문서는 issue/pull/release 같은 GitHub repo page에서 상위 repo anchor를 같이 만들라고 했지만, 이 초안에서는 `artifact_key_json["repo_anchor_canonical_id"]`로 연결만 남겼다.  
이유는 inferred anchor 자동 artifact 생성까지 한 번에 넣으면 `artifact_registry` upsert 흐름과 candidate grouping이 과하게 복잡해지기 때문이다.

이 부분은 다음 개선 턴에서 **GitHub repo page / subpath inferred repo anchor 보강**으로 좁게 확장하는 것이 더 안전하다.

---

## 7. 다음 단계

다음 구현 순서는 그대로다.

1. `router-normalizer` Redis Streams consumer / repository integration hardening
2. `gh-enricher`
3. `x-enricher`
4. `web-enricher`
5. `evidence-assembler`

즉, 지금 이 산출물은 **normalizer의 deterministic core**를 먼저 고정하는 단계다.  
다음 턴에서는 이 문서를 기준으로 **`router-normalizer` consumer/integration hardening** 또는 바로 **`gh-enricher` 스켈레톤 + 코드 초안**으로 내려가면 된다.


---

## Source file: `28_router_normalizer_consumer_integration_hardening_v0_1.md`

# 28단계: `router-normalizer` Consumer / Integration Hardening 실제 코드 초안 v0.1

## 0. 문서 목적

이 문서는 프로젝트 소스의 README/정본 단계 문서, `11_stage11_execution_contracts_v0_1.md`, `04_stage4_trigger_normalization.md`, `05_stage5_external_enrichers.md`, `26_outbox_relay_skeleton_and_code_draft_v0_1.md`, 그리고 `27_router_normalizer_skeleton_and_code_draft_v0_1.md`까지의 구현 흐름을 바탕으로,
**`router-normalizer`의 다음 구현 묶음**을 실제 코드 초안 수준으로 내리는 문서다.

이번 단계의 목적은 다섯 가지다.

1. `q.source.normalize` Redis Streams를 실제로 소비하는 **좁은 consumer/integration 경계**를 코드로 고정
2. outbox-relay가 Redis에 싣는 **얇은 ID payload**를 기준으로, `event_outbox`에서 다시 envelope를 재구성하는 **rehydration 경계**를 코드로 고정
3. `source_messages.current_version_no`와 이벤트 version이 어긋날 때를 대비해, **requested source version 재조회**를 추가해 normalizer를 version-aware로 하드닝
4. 4단계 정본이 잠근 규칙대로, `github_subpath` / `github_repo_page`에서 **inferred repo anchor artifact**를 함께 만드는 보강을 추가
5. 이 모든 하드닝을 넣어도 `router-normalizer`가 여전히 **deterministic / non-LLM / non-crawler** 경계로 남도록 고정

핵심 전제:

- `router-normalizer`는 여전히 **판단기**가 아니다.
- `router-normalizer`는 여전히 **LLM을 호출하지 않는다.**
- `router-normalizer`는 여전히 **enricher 역할을 하지 않는다.**
- 이번 단계는 구조 변경이 아니라 **27단계 core deterministic package의 최소-change hardening**이다.

---

## 1. 왜 이 단계가 지금의 정확한 다음 단계인가

27단계 문서는 현재 산출물이 `router-normalizer`의 **deterministic core**라고 명시했고, 바로 다음 순서로 **`router-normalizer` consumer / integration hardening**을 먼저 두었다. 즉, 지금 시점에서 `gh-enricher`로 바로 가는 것보다, normalizer가 실제 queue/DB 경계에서 안전하게 동작하도록 먼저 닫는 것이 문서 체계상 더 보수적이고 맞는 순서다.

또 하나의 이유가 있다.
4단계 정본 문서는 **GitHub URL에서 owner/repo를 파싱할 수 있으면 repo artifact도 같이 만들라**고 잠갔다. 하지만 27단계 초안은 `github_subpath` / `github_repo_page`에 대해 `artifact_key_json["repo_anchor_canonical_id"]`만 남기고, **실제 inferred repo artifact 생성은 다음 개선 턴으로 미뤘다.**

즉, 여기에는 작은 충돌이 있다.

- **정본 계약(4단계)**: repo anchor artifact를 같이 만든다.
- **현재 구현 초안(27단계)**: repo anchor canonical id만 남기고 artifact 생성은 defer.

이 충돌에 대한 최소-change 해석은 다음이 맞다.

1. 27단계의 deterministic core는 유지한다.
2. 다음 단계에서 inferred repo anchor만 좁게 보강한다.
3. reroot는 여전히 assembler 이후 책임으로 남긴다.

즉, 지금 해야 할 것은 **gh-enricher 시작이 아니라 normalizer hardening**이다.

---

## 2. 이번 단계에서 고정하는 범위와 비범위

### 2-1. 포함 범위

- Redis Streams consumer group bootstrap
- stream message → `trigger_event_id` rehydration
- `event_outbox` 기반 `SourceMessageEnvelope` 재구성
- requested version-aware source rehydration
- deleted current row에 대한 보수적 suppression
- GitHub inferred repo anchor artifact 보강
- main/runtime 수준 wire-up
- 최소 단위 tests

### 2-2. 제외 범위

- retry/reclaim/DLQ 고도화
- multi-consumer claim hardening
- channel override 확장
- gh/x/web enricher 구현
- bundle refresh 경로
- reroot 확정
- notifier / judge / policy 연동

즉, 이번 문서는 **실제 소비 가능한 normalizer worker**를 닫되, 그 범위를 stage 4 경계 안으로 제한한다.

---

## 3. 대상 파일 트리

```text
src/services/router_normalizer/
  redis_streams.py                # new
  worker.py                       # new
  repositories.py                 # updated
  canonicalizer.py                # updated
  service.py                      # updated
  main.py                         # updated

tests/
  unit/
    services/
      router_normalizer/
        test_github_repo_anchor_inference.py
        test_source_version_rehydration.py
  component/
    services/
      router_normalizer/
        test_normalizer_worker_reads_stream_and_rehydrates_event.py
        test_deleted_current_row_is_suppressed.py
```

---

## 4. 이번 단계에서 고정할 구현 규칙

## 4-1. Redis payload는 계속 얇게 유지한다

outbox-relay가 Redis Streams에 싣는 메시지는 이미 다음 최소 필드로 잠겨 있다.

- `job_id`
- `stage_name`
- `root_object_type`
- `root_object_id`
- `idempotency_key`
- `pipeline_run_id`
- `not_before`
- `trigger_event_id`

따라서 normalizer consumer는 Redis 본문에서 business payload를 기대하면 안 된다.
**반드시 `trigger_event_id`로 `event_outbox`를 다시 조회해 envelope를 복원**해야 한다.

이 규칙이 중요한 이유는,
Redis를 durable source처럼 취급하지 않기 위해서다.

---

## 4-2. normalization 입력은 “현재 row만” 보면 안 된다

27단계 초안의 `load_source_message_for_envelope()`는 `source_messages` current row만 읽는다.
그런데 Redis queue lag가 생기면, 아래 문제가 생긴다.

- `source_message.created.v1`가 늦게 소비되는 동안
- source row가 이미 여러 번 수정될 수 있고
- current row만 읽으면 **이벤트가 가리키는 version이 아니라 최신 version**을 normalize하게 된다.

이번 단계의 최소-change 보강은 이렇다.

1. current row를 먼저 조회한다.
2. `current_version_no == envelope.current_version_no`면 current row 그대로 사용한다.
3. 다르면 `source_message_versions`에서 **requested version row**를 다시 읽는다.
4. requested version의 `text_surface / entities_json / raw_message_json`을 우선 사용한다.

이렇게 하면 stage 4의 deterministic 성격을 유지하면서도, queue lag 때문에 version semantics가 흐려지는 문제를 줄일 수 있다.

---

## 4-3. 삭제된 current row는 보수적으로 suppress한다

live path에서 이미 삭제된 current row를 뒤늦게 normalizer가 소비하는 경우,
새 candidate proposal을 만드는 것은 precision-first 원칙과 맞지 않는다.

따라서 이번 단계의 운영 기본값은 아래처럼 둔다.

- `source_messages.deleted_at IS NOT NULL`
- → `normalization_runs`는 남김
- → `normalization_suppression_traces`에 `source_message_deleted_current` 남김
- → proposal / enrich request는 만들지 않음

이건 replay path의 최종 정책이 아니라,
**live consumer hardening의 보수적 기본값**이다.

---

## 4-4. inferred repo anchor는 ახლა normalizer에서 보강한다

4단계 정본이 요구한 규칙은 다음이다.

- issue / pull / release / subpath URL에서도
- owner/repo를 파싱할 수 있으면
- `github_repo` artifact를 같이 만든다.

이번 단계에서는 이 규칙을 **가장 작은 변경**으로 반영한다.

### 보강 방식

- `canonicalizer.py`의 기존 `github_subpath` / `github_repo_page` canonicalization은 유지
- 다만 `artifact_key_json`에 이미 들어 있는
  - `owner`
  - `repo`
  - `repo_anchor_canonical_id`
  를 사용해
- service 계층에서 **추가 `github_repo` artifact draft**를 생성
- source observation도 synthetic supporting observation으로 같이 남김

이렇게 하면,

- 27단계 core 흐름을 깨지 않고
- 4단계 계약과의 충돌을 해소할 수 있다.

중요:
- 이건 **reroot 확정**이 아니다.
- inferred repo anchor를 artifact/member로 같이 남기는 수준이다.
- primary 변경은 여전히 후단 evidence-assembler 책임으로 남는다.

---

## 5. 코드 초안

## 5-1. `src/services/router_normalizer/redis_streams.py` (new)

```python
from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from redis.asyncio import Redis


@dataclass(slots=True, frozen=True)
class StreamMessage:
    stream: str
    message_id: str
    fields: dict[str, str]


class RedisStreamConsumer:
    def __init__(
        self,
        client: Redis,
        *,
        queue_name: str,
        consumer_group: str,
        consumer_name: str,
        block_ms: int,
        batch_size: int,
    ) -> None:
        self._client = client
        self._queue_name = queue_name
        self._consumer_group = consumer_group
        self._consumer_name = consumer_name
        self._block_ms = block_ms
        self._batch_size = batch_size

    async def ensure_group(self) -> None:
        try:
            await self._client.xgroup_create(
                name=self._queue_name,
                groupname=self._consumer_group,
                id="0",
                mkstream=True,
            )
        except Exception as exc:
            if "BUSYGROUP" not in str(exc):
                raise

    async def read_batch(self) -> list[StreamMessage]:
        payload = await self._client.xreadgroup(
            groupname=self._consumer_group,
            consumername=self._consumer_name,
            streams={self._queue_name: ">"},
            count=self._batch_size,
            block=self._block_ms,
        )
        messages: list[StreamMessage] = []
        for stream_name, entries in payload or []:
            stream_name_str = stream_name.decode() if isinstance(stream_name, bytes) else str(stream_name)
            for message_id, fields in entries:
                msg_id = message_id.decode() if isinstance(message_id, bytes) else str(message_id)
                decoded_fields: dict[str, str] = {}
                for key, value in fields.items():
                    k = key.decode() if isinstance(key, bytes) else str(key)
                    v = value.decode() if isinstance(value, bytes) else str(value)
                    decoded_fields[k] = v
                messages.append(StreamMessage(stream=stream_name_str, message_id=msg_id, fields=decoded_fields))
        return messages

    async def ack(self, message_id: str) -> None:
        await self._client.xack(self._queue_name, self._consumer_group, message_id)
```

---

## 5-2. `src/services/router_normalizer/worker.py` (new)

```python
from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass

from .config import RouterNormalizerConfig
from .redis_streams import RedisStreamConsumer, StreamMessage
from .service import RouterNormalizerService


@dataclass(slots=True, frozen=True)
class WorkerBatchResult:
    processed: int = 0
    acked: int = 0
    skipped: int = 0


class RouterNormalizerWorker:
    def __init__(
        self,
        config: RouterNormalizerConfig,
        *,
        consumer: RedisStreamConsumer,
        service: RouterNormalizerService,
        logger: logging.Logger | None = None,
    ) -> None:
        self._config = config
        self._consumer = consumer
        self._service = service
        self._logger = logger or logging.getLogger(__name__)
        self._stop_event = asyncio.Event()

    async def run_forever(self) -> None:
        await self._consumer.ensure_group()
        self._logger.info(
            "router_normalizer_worker_started",
            extra={
                "service": "router-normalizer",
                "event": "router_normalizer_worker_started",
                "queue_name": self._config.queue_name,
                "consumer_group": self._config.consumer_group,
                "consumer_name": self._config.consumer_name,
            },
        )
        while not self._stop_event.is_set():
            batch = await self.run_once()
            if batch.processed == 0:
                await asyncio.sleep(0)

    async def stop(self) -> None:
        self._stop_event.set()

    async def run_once(self) -> WorkerBatchResult:
        messages = await self._consumer.read_batch()
        if not messages:
            return WorkerBatchResult()

        processed = 0
        acked = 0
        skipped = 0
        for message in messages:
            processed += 1
            ack_now = await self._process_message(message)
            if ack_now:
                await self._consumer.ack(message.message_id)
                acked += 1
            else:
                skipped += 1
        return WorkerBatchResult(processed=processed, acked=acked, skipped=skipped)

    async def _process_message(self, message: StreamMessage) -> bool:
        trigger_event_id = message.fields.get("trigger_event_id")
        if not trigger_event_id:
            self._logger.error(
                "router_normalizer_stream_missing_trigger_event_id",
                extra={
                    "service": "router-normalizer",
                    "event": "router_normalizer_stream_missing_trigger_event_id",
                    "stream_message_id": message.message_id,
                },
            )
            return True

        envelope = await self._service.rehydrate_envelope(trigger_event_id)
        if envelope is None:
            self._logger.warning(
                "router_normalizer_missing_event_outbox_row",
                extra={
                    "service": "router-normalizer",
                    "event": "router_normalizer_missing_event_outbox_row",
                    "trigger_event_id": trigger_event_id,
                    "stream_message_id": message.message_id,
                },
            )
            return True

        await self._service.handle_envelope(envelope)
        return True
```

---

## 5-3. `src/services/router_normalizer/repositories.py` (updated)

아래 두 메서드를 추가하거나 교체한다.

```python
    async def load_envelope_by_trigger_event_id(self, trigger_event_id: str) -> SourceMessageEnvelope | None:
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
        if row is None:
            return None

        payload = row["payload_json"] or {}
        return SourceMessageEnvelope(
            event_id=str(row["event_id"]),
            event_type=str(row["event_type"]),
            source_message_id=str(payload["source_message_id"]),
            current_version_no=int(payload["current_version_no"]),
            logical_post_key=str(payload["logical_post_key"]),
            occurred_at=datetime.fromisoformat(str(payload["occurred_at"])),
        )

    async def load_source_message_for_envelope(self, envelope: SourceMessageEnvelope) -> SourceMessageRecord | None:
        current = await self._session.execute(
            sa.text(
                """
                SELECT
                    source_message_id,
                    chat_id,
                    message_id,
                    logical_post_key,
                    text_body,
                    caption_text,
                    text_surface,
                    entities_json,
                    url_surface_json,
                    raw_message_json,
                    current_version_no,
                    posted_at,
                    edited_at,
                    deleted_at
                FROM source_messages
                WHERE source_message_id = CAST(:source_message_id AS uuid)
                """
            ),
            {"source_message_id": envelope.source_message_id},
        )
        current_row = current.mappings().first()
        if current_row is None:
            return None

        current_version_no = int(current_row["current_version_no"])
        if current_version_no == envelope.current_version_no:
            return SourceMessageRecord(
                source_message_id=str(current_row["source_message_id"]),
                chat_id=int(current_row["chat_id"]),
                message_id=int(current_row["message_id"]),
                logical_post_key=current_row["logical_post_key"],
                text_body=current_row["text_body"],
                caption_text=current_row["caption_text"],
                text_surface=current_row["text_surface"],
                entities_json=current_row["entities_json"],
                url_surface_json=current_row["url_surface_json"],
                raw_message_json=current_row["raw_message_json"],
                current_version_no=current_version_no,
                posted_at=current_row["posted_at"],
                edited_at=current_row["edited_at"],
                deleted_at=current_row["deleted_at"],
            )

        requested = await self._session.execute(
            sa.text(
                """
                SELECT
                    version_no,
                    text_surface,
                    entities_json,
                    raw_message_json
                FROM source_message_versions
                WHERE source_message_id = CAST(:source_message_id AS uuid)
                  AND version_no = :version_no
                """
            ),
            {
                "source_message_id": envelope.source_message_id,
                "version_no": envelope.current_version_no,
            },
        )
        version_row = requested.mappings().first()
        if version_row is None:
            return None

        return SourceMessageRecord(
            source_message_id=str(current_row["source_message_id"]),
            chat_id=int(current_row["chat_id"]),
            message_id=int(current_row["message_id"]),
            logical_post_key=current_row["logical_post_key"],
            text_body=None,
            caption_text=None,
            text_surface=version_row["text_surface"],
            entities_json=version_row["entities_json"],
            url_surface_json=None,
            raw_message_json=version_row["raw_message_json"],
            current_version_no=int(version_row["version_no"]),
            posted_at=current_row["posted_at"],
            edited_at=current_row["edited_at"],
            deleted_at=current_row["deleted_at"],
        )
```

추가 메서드 하나를 더 둔다.

```python
    async def insert_artifact_observation_if_not_exists(
        self,
        *,
        artifact_id: str,
        source_message_id: str,
        source_version_no: int,
        observed: ObservedUrl,
    ) -> None:
        await self._session.execute(
            sa.text(
                """
                INSERT INTO artifact_observations (
                    artifact_id,
                    source_message_id,
                    source_version_no,
                    observed_url,
                    source_kind,
                    normalized_url,
                    resolved_url,
                    canonical_url,
                    classification,
                    context_path,
                    created_at
                )
                VALUES (
                    CAST(:artifact_id AS uuid),
                    CAST(:source_message_id AS uuid),
                    :source_version_no,
                    :observed_url,
                    :source_kind,
                    :normalized_url,
                    :resolved_url,
                    :canonical_url,
                    :classification,
                    :context_path,
                    now()
                )
                ON CONFLICT DO NOTHING
                """
            ),
            {
                "artifact_id": artifact_id,
                "source_message_id": source_message_id,
                "source_version_no": source_version_no,
                "observed_url": observed.observed_url,
                "source_kind": observed.source_kind,
                "normalized_url": observed.normalized_url,
                "resolved_url": observed.resolved_url,
                "canonical_url": observed.canonical_url,
                "classification": observed.classification,
                "context_path": observed.context_path,
            },
        )
```

주의:
- `artifact_observations`에는 unique 제약이 없으므로, 이 `ON CONFLICT DO NOTHING`는 실제 인덱스 추가가 없다면 의미가 약하다.
- 따라서 이 메서드는 strict dedupe가 아니라 **worker 재시도 폭주 완화 placeholder**로 이해해야 한다.
- 진짜 hard dedupe가 필요하면 뒤 단계에서 unique index patch를 별도 migration으로 추가하는 편이 맞다.

---

## 5-4. `src/services/router_normalizer/canonicalizer.py` (updated)

`Canonicalizer`에 보조 helper를 추가한다.

```python
from dataclasses import replace

    def infer_github_repo_anchors(
        self,
        artifacts: list[CanonicalArtifactDraft],
        observations: list[ObservedUrl],
    ) -> tuple[list[CanonicalArtifactDraft], list[ObservedUrl]]:
        existing_ids = {artifact.canonical_id for artifact in artifacts}
        extra_artifacts: list[CanonicalArtifactDraft] = []
        extra_observations: list[ObservedUrl] = []

        for artifact in artifacts:
            if artifact.artifact_type not in {"github_subpath", "github_repo_page"}:
                continue

            artifact_key = artifact.artifact_key_json or {}
            owner = artifact_key.get("owner")
            repo = artifact_key.get("repo")
            repo_anchor_canonical_id = artifact_key.get("repo_anchor_canonical_id")
            if not owner or not repo or not repo_anchor_canonical_id:
                continue
            if repo_anchor_canonical_id in existing_ids:
                continue

            repo_url = f"https://github.com/{owner}/{repo}"
            repo_artifact = CanonicalArtifactDraft(
                artifact_type="github_repo",
                canonical_id=str(repo_anchor_canonical_id),
                canonical_url=repo_url,
                normalized_host="github.com",
                artifact_key_json={"owner": owner, "repo": repo},
            )
            extra_artifacts.append(repo_artifact)
            existing_ids.add(repo_artifact.canonical_id)

            for observation in observations:
                if observation.canonical_url != artifact.canonical_url:
                    continue
                extra_observations.append(
                    replace(
                        observation,
                        source_kind="inferred_repo_anchor",
                        canonical_url=repo_url,
                        classification="github_repo",
                        context_path=(observation.context_path or "") + ":repo_anchor",
                    )
                )

        return artifacts + extra_artifacts, observations + extra_observations
```

이 helper는 다음 성격만 가진다.

- 새 `github_repo` artifact draft 생성
- synthetic supporting observation 생성
- proposal/reroot 자체는 하지 않음

즉, **정본 계약을 맞추되, side effect는 최소화**한다.

---

## 5-5. `src/services/router_normalizer/service.py` (updated)

`RouterNormalizerService`에 아래 보강을 넣는다.

```python
    async def rehydrate_envelope(self, trigger_event_id: str) -> SourceMessageEnvelope | None:
        return await self._repository.load_envelope_by_trigger_event_id(trigger_event_id)

    async def handle_envelope(self, envelope: SourceMessageEnvelope) -> NormalizationJobResult:
        source_message = await self._repository.load_source_message_for_envelope(envelope)
        if source_message is None:
            return NormalizationJobResult(
                source_message_id=envelope.source_message_id,
                signal_detected=False,
                candidate_eligible=False,
                proposals_created=0,
                observations_created=0,
                enrich_events_created=0,
                suppression_reason_codes=["source_message_missing"],
            )

        if source_message.deleted_at is not None:
            evaluation = TriggerEvaluation(
                signal_detected=False,
                candidate_eligible=False,
                trigger_strength=None,
                reason_codes=[],
                suppression_reason_codes=["source_message_deleted_current"],
            )
            async with self._repository.transaction():
                normalization_run_id = await self._repository.insert_normalization_run(
                    source_message_id=source_message.source_message_id,
                    source_version_no=envelope.current_version_no,
                    normalizer_version=self._config.normalizer_version,
                    evaluation=evaluation,
                    result_hash="deleted-current-row",
                )
                await self._repository.insert_suppression_trace(
                    normalization_run_id=normalization_run_id,
                    reason_code="source_message_deleted_current",
                    trigger_strength=None,
                    notes_json={
                        "source_message_id": source_message.source_message_id,
                        "event_type": envelope.event_type,
                    },
                )
            return NormalizationJobResult(
                source_message_id=source_message.source_message_id,
                signal_detected=False,
                candidate_eligible=False,
                proposals_created=0,
                observations_created=0,
                enrich_events_created=0,
                suppression_reason_codes=["source_message_deleted_current"],
            )

        execution = await self._execute(source_message)
        ...
```

그리고 `_execute()` 안에서 canonicalizer 보강 helper를 호출한다.

```python
    async def _execute(self, source_message: SourceMessageRecord) -> NormalizeExecutionResult:
        surfaces = self._surface_normalizer.normalize(
            text_body=source_message.text_body,
            caption_text=source_message.caption_text,
            text_surface=source_message.text_surface,
        )
        observed_urls = self._url_extractor.extract(
            entities_json=source_message.entities_json,
            url_surface_json=source_message.url_surface_json,
            display_surface=surfaces.display_surface,
        )
        observed_urls = await self._short_url_resolver.resolve_many(observed_urls)
        artifacts, observations = self._canonicalizer.canonicalize_many(observed_urls)
        artifacts, observations = self._canonicalizer.infer_github_repo_anchors(artifacts, observations)

        if not artifacts and self._should_create_text_idea(surfaces):
            artifacts.append(self._build_text_idea_artifact(source_message, surfaces))

        evaluation = self._trigger_rules.evaluate(surfaces=surfaces, artifacts=artifacts)
        proposals = self._propose_candidate_groups(source_message, artifacts) if evaluation.candidate_eligible else []

        return NormalizeExecutionResult(
            signal_detected=evaluation.signal_detected,
            candidate_eligible=evaluation.candidate_eligible,
            trigger_strength=evaluation.trigger_strength,
            artifacts=artifacts,
            observations=observations,
            proposals=proposals,
            suppression_reason_codes=evaluation.suppression_reason_codes,
        )
```

관찰 저장은 synthetic observation까지 같이 허용한다.

```python
            for observed in execution.observations:
                matched_artifact_id = None
                for artifact in execution.artifacts:
                    if artifact.canonical_url == observed.canonical_url:
                        matched_artifact_id = canonical_id_to_artifact_id[artifact.canonical_id]
                        break
                if matched_artifact_id is None:
                    continue
                await self._repository.insert_artifact_observation_if_not_exists(
                    artifact_id=matched_artifact_id,
                    source_message_id=source_message.source_message_id,
                    source_version_no=source_message.current_version_no,
                    observed=observed,
                )
                observations_created += 1
```

주의:
- 이 변화는 `github_repo_page only` 메시지가 **빈 proposal**로 끝나는 문제를 좁게 해결한다.
- proposal primary 선택 로직 자체를 크게 바꾸지 않아도, inferred `github_repo` artifact가 artifact set에 추가되므로 기존 `github_artifacts` 우선 규칙이 자연스럽게 repo primary를 택하게 된다.

---

## 5-6. `src/services/router_normalizer/main.py` (updated)

```python
from __future__ import annotations

import asyncio
import logging
import sys

from redis.asyncio import Redis
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from .canonicalizer import Canonicalizer
from .config import RouterNormalizerConfig
from .redis_streams import RedisStreamConsumer
from .repositories import RouterNormalizerRepository
from .service import RouterNormalizerService
from .short_url_resolver import ShortUrlResolver
from .text_surfaces import TextSurfaceNormalizer
from .trigger_rules import TriggerRules
from .url_extraction import UrlExtractor
from .worker import RouterNormalizerWorker


def _configure_logging(log_level: str) -> None:
    logging.basicConfig(
        level=getattr(logging, log_level, logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
        stream=sys.stdout,
    )


async def _run() -> int:
    config = RouterNormalizerConfig.from_env()
    _configure_logging(config.log_level)
    logger = logging.getLogger("router_normalizer")

    engine = create_async_engine(config.database_url, future=True)
    session_factory = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    redis_client = Redis.from_url(config.redis_url, decode_responses=True)

    try:
        async with session_factory() as session:
            repository = RouterNormalizerRepository(session)
            service = RouterNormalizerService(
                config,
                repository=repository,
                surface_normalizer=TextSurfaceNormalizer(),
                url_extractor=UrlExtractor(),
                short_url_resolver=ShortUrlResolver(
                    transport=None,
                    allowlist=config.shortener_allowlist,
                    timeout_ms=config.short_url_timeout_ms,
                    max_redirects=config.short_url_max_redirects,
                ),
                canonicalizer=Canonicalizer(),
                trigger_rules=TriggerRules(),
                logger=logger,
            )
            consumer = RedisStreamConsumer(
                redis_client,
                queue_name=config.queue_name,
                consumer_group=config.consumer_group,
                consumer_name=config.consumer_name,
                block_ms=config.block_ms,
                batch_size=config.batch_size,
            )
            worker = RouterNormalizerWorker(
                config,
                consumer=consumer,
                service=service,
                logger=logger,
            )
            await worker.run_forever()
    finally:
        await redis_client.close()
        await engine.dispose()

    return 0


def main() -> None:
    raise SystemExit(asyncio.run(_run()))


if __name__ == "__main__":
    main()
```

주의:
- short URL transport는 아직 `None`으로 둔다.
- 즉, 이번 단계의 초점은 **consumer/integration hardening**이지, network adapter 완성 자체가 아니다.
- 실제 HTTP redirect expander는 이후 동일 인터페이스로 좁게 주입하면 된다.

---

## 6. 테스트 초안 포인트

### `tests/unit/services/router_normalizer/test_github_repo_anchor_inference.py`

검증:
- `github_repo_page`만 있는 artifact set
- `infer_github_repo_anchors()` 호출 후
- 추가 `github_repo` artifact가 생기는지
- synthetic observation이 `classification = github_repo`로 생기는지

### `tests/unit/services/router_normalizer/test_source_version_rehydration.py`

검증:
- current row `current_version_no = 3`
- envelope `current_version_no = 1`
- repository가 `source_message_versions.version_no = 1` row를 읽는지
- returned `SourceMessageRecord.current_version_no == 1` 인지

### `tests/component/services/router_normalizer/test_normalizer_worker_reads_stream_and_rehydrates_event.py`

검증:
- Redis Streams message에는 `trigger_event_id`만 있음
- `event_outbox.payload_json`에서 envelope 복원
- service 호출 후 ack 수행

### `tests/component/services/router_normalizer/test_deleted_current_row_is_suppressed.py`

검증:
- `source_messages.deleted_at IS NOT NULL`
- `normalization_runs`는 기록됨
- suppression trace `source_message_deleted_current`
- proposal / enrich request 없음

---

## 7. 왜 이 단계에서 `gh-enricher`로 바로 가지 않는가

지금 `gh-enricher`로 넘어가면, 아래 문제가 남는다.

1. normalizer가 실제 queue를 소비하지 않음
2. Redis thin-payload와 DB rehydration 경계가 안 닫힘
3. version lag 시 잘못된 source version을 normalize할 수 있음
4. 4단계 정본의 inferred repo anchor 규칙이 아직 구현과 어긋남

즉, 지금 gh-enricher를 붙이면 **앞단 deterministic contract가 미세하게 열린 상태**로 넘어가게 된다.
이건 최소-change가 아니라 debt carry-forward다.

따라서 이번 단계에서 normalizer를 한 번 더 닫고,
그 다음에 `gh-enricher`로 가는 순서가 맞다.

---

## 8. 다음 단계

이 단계가 끝나면 다음 구현 순서는 아래가 맞다.

1. `gh-enricher` 스켈레톤 + 실제 코드 초안
2. `x-enricher`
3. `web-enricher`
4. `evidence-assembler`

즉, 이번 문서는 **stage 4 deterministic boundary의 operational hardening**이고,
다음 단계부터 비로소 stage 5 external enricher 본체로 넘어간다.

---

## 최종 한 줄 결론

이번 단계의 최소-change 정답은 **`router-normalizer`를 gh-enricher로 넘기기 전에, Redis thin-payload consumer / event_outbox rehydration / requested-version 재조회 / GitHub inferred repo anchor 보강까지 닫아 deterministic boundary를 실제 운영 가능한 상태로 만드는 것**이다.
