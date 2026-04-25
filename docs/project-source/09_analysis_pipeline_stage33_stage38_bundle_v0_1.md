# 09 analysis pipeline stage33 stage38 bundle v0 1

이 문서는 프로젝트 소스 파일 수 제한(40개)을 피하기 위한 **통합 번들**이다.
구조 변경 문서가 아니며, 아래 원본 문서를 **순서와 내용을 보존한 채** 묶는다.

## 포함 원본 파일
- `33_evidence_assembler_integration_hardening_v0_1.md`
- `34_analysis_router_skeleton_and_code_draft_v0_1.md`
- `35_judge_openai_skeleton_and_code_draft_v0_1.md`
- `36_analysis_validator_skeleton_and_code_draft_v0_1.md`
- `37_policy_engine_skeleton_and_code_draft_v0_1.md`
- `38_notifier_telegram_skeleton_and_code_draft_v0_1.md`

## 통합 원칙
- 아키텍처 불변식은 유지한다.
- 원본 파일명은 아래 섹션 제목으로 보존한다.
- 충돌 시 해석 우선순위는 기존과 동일하게 최신 README → 정본 단계 문서 → 실행 계약 → migration 정본 → 구현 초안 → advisory design note 이다.
- `03_GitHub_AI_application_plan.md`는 이 번들에 포함하지 않는다.
  - 이유: 적용 검토용 advisory 문서이며, phase/ordering authority가 아니기 때문이다.
- 이번 통합은 **standalone 33~38을 새 분석/전달 파이프라인 번들 1개로 교체** 하기 위한 것이다.
  - 내용은 합치되, 구조는 바꾸지 않는다.

---


## Source file: `33_evidence_assembler_integration_hardening_v0_1.md`

# 33단계: `evidence-assembler` integration hardening v0.1

## 0. 문서 목적

이 문서는 이미 작성된 `32_evidence_assembler_skeleton_and_code_draft_v0_1.md`를 뒤엎는 문서가 아니다.  
목적은 **구조를 유지한 채, 32단계 초안의 operational gap만 좁게 닫는 것**이다.

이번 단계에서 닫는 것은 아래 다섯 가지다.

1. `artifact.snapshot.updated.v1`가 들어왔을 때 **candidate_group fan-out rehydration**을 정확히 고정
2. **중복 trigger / 동일 bundle input hash / current bundle reuse** 경계를 고정
3. **reroot edge-case / text_idea snapshot reuse / discovered observation filtering**을 보수적으로 고정
4. `candidate_evidence_bundles` / `candidate_evidence_members` write가 **현재 schema와 정확히 맞도록** 보강
5. 다음 단계인 `analysis-router`가 바로 붙을 수 있게 `analysis.requested.v1` handoff를 안정화

핵심 전제는 그대로 유지한다.

- `evidence-assembler`는 **판단기**가 아니다.
- `evidence-assembler`는 **외부 fetcher**가 아니다.
- `evidence-assembler`는 **LLM을 호출하지 않는다.**
- `evidence-assembler`만 **current primary 변경(reroot)** 을 반영할 수 있다.
- history는 append-only, current pointer만 mutable이다.

---

## 1. 왜 지금 이 단계가 정확한 다음 단계인가

소스 오브 트루스 기준 현재 구현 상태는 아래로 고정돼 있다.

- `router-normalizer` deterministic core + consumer/integration hardening 완료
- `gh-enricher` v0.1 완료
- `x-enricher` v0.1 완료
- `web-enricher` v0.1 완료
- `evidence-assembler` skeleton + code draft v0.1 완료
- 다음 자연스러운 순서는 **`33_evidence_assembler_integration_hardening_v0_1.md` → `34_analysis_router_skeleton_and_code_draft_v0_1.md`** 다.

즉, 지금 collector / normalizer / source enricher를 다시 여는 것은 순서상 후퇴다.  
지금 닫아야 하는 것은 **stage 5 마지막 operational hardening** 이다.

---

## 2. 이번 단계에서 드러난 충돌과 최소-change 해석

### 충돌 A — `artifact.snapshot.updated.v1` payload가 항상 `candidate_group_id`를 주지 않는다

32단계 assembler 초안은 trigger payload에서 `candidate_group_id`를 바로 뽑는 방향으로 단순화돼 있었다.  
하지만 source enricher 구현 초안은 서로 다르다.

- `gh-enricher`의 `artifact.snapshot.updated.v1` payload는 `artifact_id`, `snapshot_id`, `provider`, `status`, `content_anchor`만 넣는다.
- `x-enricher` / `web-enricher` payload는 `candidate_group_id`도 넣는다.

즉, **GitHub snapshot update는 current candidate를 직접 가리키지 않는다.**

### 최소-change 해석 A

이번 v0.1 hardening에서는 아래처럼 고정한다.

1. `candidate.bundle.refresh.v1`
   - payload에 `candidate_group_id`가 있으면 **그 candidate 하나**를 refresh target으로 쓴다.

2. `artifact.snapshot.updated.v1`
   - payload의 `artifact_id`를 기준으로
   - `candidate_group_members`에서 해당 artifact를 참조하는 **모든 candidate group** 을 조회한다.
   - 즉, **artifact update → impacted candidate fan-out** 으로 해석한다.

이 해석의 장점은 다음과 같다.

- 29/30/31 단계의 서로 다른 outbox shape를 다 수용할 수 있다.
- PostgreSQL이 여전히 rehydration source라는 계약을 유지한다.
- 같은 artifact를 공유하는 여러 candidate가 있을 때도 snapshot refresh가 누락되지 않는다.

중요:
- assembler는 여전히 **새 candidate를 만들지 않는다.**
- 단지 **기존 candidate membership을 fan-out 재조회** 할 뿐이다.

---

### 충돌 B — 32단계의 `judge_profile = idea_primary` 는 잠긴 profile 이름과 충돌한다

6단계 정본과 11단계 실행 계약의 prompt/profile 경로는 아래 셋으로 잠겨 있다.

- `github_primary`
- `x_primary`
- `text_idea_primary`

그런데 32단계 초안은 아래처럼 썼다.

- `web_article` / `text_idea` → `idea_primary`

즉, **잠긴 계약에 없는 profile 이름** 이 들어갔다.

### 최소-change 해석 B

이번 v0.1 hardening에서는 아래처럼 고정한다.

- `github_*` primary → `github_primary`
- `x_post` primary → `x_primary`
- `web_article` 또는 `text_idea` primary → **`text_idea_primary`**

즉, `web_article`은 별도 신설 profile을 만들지 않고,  
**text-centric / idea-centric 보수 profile을 공유** 하는 것이 가장 작은 변경이다.

이 해석의 장점:

- prompt/profile 디렉터리 구조를 바꾸지 않는다.
- `analysis-router`가 바로 붙을 수 있다.
- dedicated `web_primary` profile을 나중에 추가하더라도 지금 구조를 깨지 않는다.

---

### 충돌 C — 32단계 bundle write는 schema와 완전히 맞지 않는다

32단계 초안에는 다음 두 가지가 있다.

1. `candidate_evidence_bundles.bundle_version`을 항상 `1`로 넣는다.
2. `candidate_evidence_members` insert에서 `candidate_evidence_member_id`를 명시하지 않는다.

하지만 migration 정본은 아래를 잠갔다.

- `candidate_evidence_bundles`는 append-only row이며 `bundle_version` 컬럼을 가진다.
- `candidate_evidence_members`는 PK `candidate_evidence_member_id`를 가진다.

### 최소-change 해석 C

이번 단계에서는 아래처럼 고정한다.

1. `bundle_version`
   - `candidate_group_id`별로 `MAX(bundle_version) + 1` 로 계산한다.
   - 동일 input hash 재사용이면 새 bundle row를 만들지 않는다.

2. `candidate_evidence_members`
   - insert 시 `candidate_evidence_member_id = gen_random_uuid()`를 명시한다.

이렇게 해야:

- schema contract를 정확히 따른다.
- append-only history 의미가 살아난다.
- current pointer와 bundle history가 분리된다.

---

### 충돌 D — discovered observations를 그대로 모두 읽으면 stale snapshot noise가 섞일 수 있다

32단계 초안은 `discovered_url_observations`를 candidate narrative에 반영하는 방향을 열어뒀다.  
그런데 아무 필터 없이 candidate_group 단위로 전부 읽으면 아래 문제가 생긴다.

- 예전 snapshot에서 본 링크가 계속 살아남음
- 현재 candidate membership과 무관한 observation이 섞일 수 있음
- bundle input hash가 snapshot churn과 무관하게 흔들릴 수 있음

### 최소-change 해석 D

이번 단계에서는 아래처럼 고정한다.

1. discovered observations는 **current candidate member의 artifact_id 집합** 으로 먼저 필터한다.
2. 그 다음 `(observed_url, parent_artifact_id, context_path)` 기준으로 newest-first dedupe 한다.
3. `discovered_links_summary_json`에는 **deduped summary only** 를 넣는다.
4. 새 artifact/member 생성은 여전히 하지 않는다.

즉, discovered observations는 **supporting narrative** 로만 쓰고,  
candidate graph를 확장하는 데 쓰지 않는다.

---

## 3. 이번 단계에서 고정할 hardening 범위

### 포함

- `artifact.snapshot.updated.v1` → candidate fan-out rehydration
- duplicate trigger absorption
- current bundle reuse
- text_idea snapshot idempotent reuse
- reroot edge-case hardening
- discovered observation filtering
- `analysis.requested.v1` handoff stabilizing

### 제외

- 새 artifact creation / artifact_registry mutation
- candidate membership expansion from discovered URLs
- external HTTP fetch
- LLM 호출
- judge / policy / notifier 구현
- queue reclaim / DLQ hardening
- eval/governance UI

즉, 이번 문서는 **stage 5 마지막 hardening** 이고,  
다음 단계는 이제 `analysis-router`가 맞다.

---

## 4. 대상 파일 트리

변경/보강 대상은 좁게 잡는다.

```text
src/services/evidence_assembler/
  models.py          # updated
  repositories.py    # updated
  service.py         # updated
  worker.py          # tiny update or unchanged

tests/
  unit/
    services/
      evidence_assembler/
        test_judge_profile_mapping.py        # new
        test_existing_bundle_reuse.py        # new
        test_discovered_link_filtering.py    # new
  component/
    services/
      evidence_assembler/
        test_snapshot_updated_fanout.py      # new
        test_text_idea_snapshot_reuse.py     # new
        test_duplicate_trigger_no_new_bundle.py  # new
```

`config.py`, `text_idea_builder.py`, `reroot_rules.py`, `readiness.py`, `token_budget.py`, `redis_streams.py`, `main.py`는 이번 턴에서 구조 변경 없이 재사용 가능하다.

---

## 5. 이번 단계에서 고정할 구현 규칙

### 5-1. trigger → refresh target 해석

#### `candidate.bundle.refresh.v1`
- payload에 `candidate_group_id`가 있으면 단일 target
- replay/manual/maintenance 경로

#### `artifact.snapshot.updated.v1`
- payload의 `artifact_id`로 `candidate_group_members` 조회
- 해당 artifact를 member로 가진 **모든 candidate_group_id** 를 target으로 fan-out
- snapshot update payload에 `candidate_group_id`가 있어도 **artifact fan-out 기준이 우선**
  - 이유: shared artifact가 여러 candidate에 걸릴 수 있기 때문

---

### 5-2. current bundle reuse 규칙

권장 순서:

1. 새 bundle input hash 계산
2. 같은 `(candidate_group_id, bundle_profile_version, bundle_input_hash)` 가 이미 있으면
   - 새 bundle row append 생략
   - 필요 시 `candidate_group_proposals.current_bundle_id`만 해당 existing bundle로 맞춤
   - `analysis.requested.v1`는 **재emit하지 않음**

이렇게 둬야:

- duplicate snapshot update에 따라 analysis가 폭주하지 않는다.
- append-only bundle history는 유지된다.
- explicit manual replay가 필요한 경우는 나중에 별도 replay contract로 처리할 수 있다.

---

### 5-3. `text_idea` snapshot reuse 규칙

`text_idea`는 32단계 초안 그대로 매 refresh 때 append하면 중복 snapshot이 과도하게 쌓인다.  
이번 hardening에서는 아래처럼 고정한다.

1. `TextIdeaBuilder.input_hash(draft)`를 content anchor로 쓴다.
2. `(artifact_id, provider = local_text_idea, content_anchor = text_idea_hash, snapshot_type = text_idea)` 기준으로 existing snapshot 조회
3. 있으면 재사용
4. 없을 때만 parent/child snapshot append

즉, text_idea도 **snapshot idempotency** 를 가진다.

---

### 5-4. reroot edge-case 규칙

32단계의 기본 reroot 규칙은 유지하되, 다음 예외를 보강한다.

#### A. current primary snapshot이 없고 supporting repo snapshot만 준비된 경우
- supporting `github_repo` 가 `ready/partial_ready`
- current primary snapshot은 없음 또는 unusable
- → repo로 reroot 허용

#### B. current primary가 이미 `github_repo`
- 유지
- 다른 repo 후보가 있어도 지금 단계에서는 candidate membership re-ranking을 하지 않음

#### C. multiple ready repo candidates
- deterministic tie-break 필요
- member_role precedence + member_order + artifact_id lexical order로 안정 선택

권장 precedence:

1. `primary`
2. `supporting`
3. `inferred_anchor`

즉, reroot는 aggressive optimization이 아니라 **deterministic recovery** 여야 한다.

---

### 5-5. discovered observation filtering 규칙

bundle에 반영할 discovered observations는 아래 조건을 모두 만족해야 한다.

1. `parent_candidate_group_id == candidate_group_id`
2. `parent_artifact_id in current_member_artifact_ids`
3. latest-first dedupe 후 stable sort

권장 dedupe key:

```text
(parent_artifact_id, observed_url, context_path)
```

권장 stable sort:

1. `parent_artifact_id`
2. `context_path`
3. `observed_url`

이렇게 해야 bundle input hash가 stochastic하게 흔들리지 않는다.

---

### 5-6. `ready_for_analysis`는 더 보수적으로 유지한다

32단계 조건은 유지하되 아래를 추가한다.

- primary snapshot status가 `failed_* / rate_limited / access_denied / unsupported` 면 not-ready
- `text_idea` fallback snapshot이 있고 current primary가 `text_idea`이면 low_evidence라도 ready 허용
- bundle member list가 비면 not-ready

즉, **weak-but-formed bundle** 은 허용하지만, **broken bundle** 은 막는다.

---

### 5-7. judge profile mapping은 다음으로 고정한다

```text
github_repo / github_subpath / github_repo_page / github_gist -> github_primary
x_post -> x_primary
web_article / text_idea -> text_idea_primary
```

이건 dedicated `web_primary`를 새로 만드는 설계가 아니다.  
**기존 잠긴 profile 이름만으로 다음 단계를 잇기 위한 최소-change bridge** 다.

---

## 6. 코드 초안

## 6-1. `src/services/evidence_assembler/models.py` (updated)

```python
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any


@dataclass(slots=True, frozen=True)
class TriggerEventRecord:
    event_id: str
    event_type: str
    aggregate_id: str | None
    payload_json: dict[str, Any]


@dataclass(slots=True, frozen=True)
class BundleRefreshTarget:
    candidate_group_id: str
    trigger_event_id: str
    trigger_event_type: str
    trigger_artifact_id: str | None = None
    trigger_snapshot_id: str | None = None


@dataclass(slots=True, frozen=True)
class BundleTriggerEnvelope:
    event_id: str
    event_type: str
    candidate_group_id: str
    trigger_object_type: str | None = None
    trigger_object_id: str | None = None
    snapshot_id: str | None = None
    occurred_at: datetime | None = None


@dataclass(slots=True, frozen=True)
class CandidateGroupRecord:
    candidate_group_id: str
    source_message_id: str
    source_version_no: int
    initial_primary_artifact_id: str
    current_primary_artifact_id: str
    proposal_status: str
    current_bundle_id: str | None


@dataclass(slots=True, frozen=True)
class CandidateMemberRecord:
    artifact_id: str
    artifact_type: str
    member_role: str
    member_order: int | None


@dataclass(slots=True, frozen=True)
class SnapshotRecord:
    snapshot_id: str
    artifact_id: str
    provider: str
    snapshot_type: str
    status: str
    fetched_at: datetime
    content_anchor: str
    normalized_projection: dict[str, Any] | None
    evidence_limitations: list[str] | None = None
    fetch_anomalies: list[str] | None = None


@dataclass(slots=True, frozen=True)
class ExistingBundleRecord:
    bundle_id: str
    candidate_group_id: str
    bundle_version: int
    bundle_profile_version: str
    bundle_input_hash: str
    ready_for_analysis: bool


@dataclass(slots=True, frozen=True)
class DiscoveredLinkSummary:
    observed_url: str
    context_path: str | None
    discovery_reason: str
    parent_artifact_id: str
    parent_snapshot_id: str | None


@dataclass(slots=True, frozen=True)
class TextIdeaSnapshotDraft:
    artifact_id: str
    source_message_id: str
    source_version_no: int
    hash_surface: str
    display_surface: str | None
    dev_context_signals_json: dict[str, Any] | None
    status: str
    evidence_limitations: list[str] = field(default_factory=list)


@dataclass(slots=True, frozen=True)
class RerootDecision:
    changed: bool
    from_artifact_id: str
    to_artifact_id: str
    reason_code: str | None = None


@dataclass(slots=True, frozen=True)
class BundleMemberDraft:
    artifact_id: str
    snapshot_id: str
    member_role: str
    member_order: int | None


@dataclass(slots=True, frozen=True)
class EvidenceBundleDraft:
    candidate_group_id: str
    initial_primary_artifact_id: str
    current_primary_artifact_id: str
    bundle_profile_version: str
    bundle_input_hash: str
    reroot_count: int
    primary_summary: dict[str, Any]
    supporting_summaries_json: list[dict[str, Any]]
    discovered_links_summary_json: list[dict[str, Any]]
    evidence_limitations: list[str]
    ready_for_analysis: bool
    token_budget_profile: str
    members: list[BundleMemberDraft]
    judge_profile: str | None = None
```

---

## 6-2. `src/services/evidence_assembler/repositories.py` (updated)

```python
from __future__ import annotations

import hashlib
import json
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Any, Iterable

import sqlalchemy as sa
from sqlalchemy.ext.asyncio import AsyncSession

from .models import (
    BundleRefreshTarget,
    CandidateGroupRecord,
    CandidateMemberRecord,
    DiscoveredLinkSummary,
    EvidenceBundleDraft,
    ExistingBundleRecord,
    SnapshotRecord,
    TextIdeaSnapshotDraft,
    TriggerEventRecord,
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


class EvidenceAssemblerRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    @asynccontextmanager
    async def transaction(self):
        if self._session.in_transaction():
            yield self._session
            return
        async with self._session.begin():
            yield self._session

    async def load_trigger_event(self, trigger_event_id: str) -> TriggerEventRecord | None:
        result = await self._session.execute(
            sa.text(
                """
                SELECT event_id, event_type, aggregate_id, payload_json
                FROM event_outbox
                WHERE event_id = CAST(:event_id AS uuid)
                """
            ),
            {"event_id": trigger_event_id},
        )
        row = result.mappings().first()
        if row is None:
            return None
        return TriggerEventRecord(
            event_id=str(row["event_id"]),
            event_type=str(row["event_type"]),
            aggregate_id=str(row["aggregate_id"]) if row["aggregate_id"] else None,
            payload_json=row["payload_json"] or {},
        )

    async def resolve_refresh_targets(self, trigger_event_id: str) -> list[BundleRefreshTarget]:
        event = await self.load_trigger_event(trigger_event_id)
        if event is None:
            return []

        payload = event.payload_json

        if event.event_type == "candidate.bundle.refresh.v1":
            candidate_group_id = payload.get("candidate_group_id") or event.aggregate_id
            if not candidate_group_id:
                return []
            return [
                BundleRefreshTarget(
                    candidate_group_id=str(candidate_group_id),
                    trigger_event_id=event.event_id,
                    trigger_event_type=event.event_type,
                    trigger_artifact_id=str(payload.get("trigger_object_id")) if payload.get("trigger_object_id") else None,
                    trigger_snapshot_id=str(payload.get("snapshot_id")) if payload.get("snapshot_id") else None,
                )
            ]

        if event.event_type == "artifact.snapshot.updated.v1":
            artifact_id = payload.get("artifact_id") or event.aggregate_id
            snapshot_id = payload.get("snapshot_id")
            if not artifact_id:
                return []

            result = await self._session.execute(
                sa.text(
                    """
                    SELECT DISTINCT candidate_group_id
                    FROM candidate_group_members
                    WHERE artifact_id = CAST(:artifact_id AS uuid)
                    ORDER BY candidate_group_id
                    """
                ),
                {"artifact_id": str(artifact_id)},
            )
            return [
                BundleRefreshTarget(
                    candidate_group_id=str(row["candidate_group_id"]),
                    trigger_event_id=event.event_id,
                    trigger_event_type=event.event_type,
                    trigger_artifact_id=str(artifact_id),
                    trigger_snapshot_id=str(snapshot_id) if snapshot_id else None,
                )
                for row in result.mappings().all()
            ]

        return []

    async def load_candidate_group(self, candidate_group_id: str) -> CandidateGroupRecord | None:
        result = await self._session.execute(
            sa.text(
                """
                SELECT candidate_group_id, source_message_id, source_version_no,
                       initial_primary_artifact_id, current_primary_artifact_id,
                       proposal_status, current_bundle_id
                FROM candidate_group_proposals
                WHERE candidate_group_id = CAST(:candidate_group_id AS uuid)
                """
            ),
            {"candidate_group_id": candidate_group_id},
        )
        row = result.mappings().first()
        if row is None:
            return None
        return CandidateGroupRecord(
            candidate_group_id=str(row["candidate_group_id"]),
            source_message_id=str(row["source_message_id"]),
            source_version_no=int(row["source_version_no"]),
            initial_primary_artifact_id=str(row["initial_primary_artifact_id"]),
            current_primary_artifact_id=str(row["current_primary_artifact_id"]),
            proposal_status=str(row["proposal_status"]),
            current_bundle_id=str(row["current_bundle_id"]) if row["current_bundle_id"] else None,
        )

    async def load_candidate_members(self, candidate_group_id: str) -> list[CandidateMemberRecord]:
        result = await self._session.execute(
            sa.text(
                """
                SELECT cgm.artifact_id, ar.artifact_type, cgm.member_role, cgm.member_order
                FROM candidate_group_members cgm
                JOIN artifact_registry ar ON ar.artifact_id = cgm.artifact_id
                WHERE cgm.candidate_group_id = CAST(:candidate_group_id AS uuid)
                ORDER BY
                    CASE cgm.member_role
                        WHEN 'primary' THEN 0
                        WHEN 'supporting' THEN 1
                        WHEN 'inferred_anchor' THEN 2
                        ELSE 9
                    END,
                    cgm.member_order NULLS LAST,
                    cgm.artifact_id
                """
            ),
            {"candidate_group_id": candidate_group_id},
        )
        return [
            CandidateMemberRecord(
                artifact_id=str(row["artifact_id"]),
                artifact_type=str(row["artifact_type"]),
                member_role=str(row["member_role"]),
                member_order=row["member_order"],
            )
            for row in result.mappings().all()
        ]

    async def load_current_snapshots(self, artifact_ids: Iterable[str]) -> dict[str, SnapshotRecord]:
        artifact_ids = list(artifact_ids)
        if not artifact_ids:
            return {}
        result = await self._session.execute(
            sa.text(
                """
                SELECT ar.artifact_id, s.snapshot_id, s.provider, s.snapshot_type, s.status,
                       s.fetched_at, s.content_anchor, s.normalized_projection,
                       s.evidence_limitations, s.fetch_anomalies
                FROM artifact_registry ar
                JOIN artifact_snapshots s ON s.snapshot_id = ar.current_snapshot_id
                WHERE ar.artifact_id = ANY(CAST(:artifact_ids AS uuid[]))
                """
            ),
            {"artifact_ids": artifact_ids},
        )
        out: dict[str, SnapshotRecord] = {}
        for row in result.mappings().all():
            out[str(row["artifact_id"])] = SnapshotRecord(
                snapshot_id=str(row["snapshot_id"]),
                artifact_id=str(row["artifact_id"]),
                provider=str(row["provider"]),
                snapshot_type=str(row["snapshot_type"]),
                status=str(row["status"]),
                fetched_at=row["fetched_at"],
                content_anchor=str(row["content_anchor"]),
                normalized_projection=row["normalized_projection"],
                evidence_limitations=row["evidence_limitations"] or [],
                fetch_anomalies=row["fetch_anomalies"] or [],
            )
        return out

    async def load_source_message_text_surface(
        self,
        *,
        source_message_id: str,
        source_version_no: int,
    ) -> str | None:
        version_result = await self._session.execute(
            sa.text(
                """
                SELECT text_surface
                FROM source_message_versions
                WHERE source_message_id = CAST(:source_message_id AS uuid)
                  AND version_no = :source_version_no
                """
            ),
            {
                "source_message_id": source_message_id,
                "source_version_no": source_version_no,
            },
        )
        version_row = version_result.mappings().first()
        if version_row is not None and version_row["text_surface"]:
            return str(version_row["text_surface"])

        current_result = await self._session.execute(
            sa.text(
                """
                SELECT text_surface
                FROM source_messages
                WHERE source_message_id = CAST(:source_message_id AS uuid)
                """
            ),
            {"source_message_id": source_message_id},
        )
        current_row = current_result.mappings().first()
        if current_row is None:
            return None
        return str(current_row["text_surface"]) if current_row["text_surface"] else None

    async def ensure_text_idea_snapshot(self, draft: TextIdeaSnapshotDraft) -> tuple[str, str]:
        existing = await self._session.execute(
            sa.text(
                """
                SELECT snapshot_id, status
                FROM artifact_snapshots
                WHERE artifact_id = CAST(:artifact_id AS uuid)
                  AND provider = 'local_text_idea'
                  AND snapshot_type = 'text_idea'
                  AND content_anchor = :content_anchor
                ORDER BY fetched_at DESC
                LIMIT 1
                """
            ),
            {
                "artifact_id": draft.artifact_id,
                "content_anchor": draft.hash_surface,
            },
        )
        row = existing.mappings().first()
        if row is not None:
            return str(row["snapshot_id"]), str(row["status"])

        parent = await self._session.execute(
            sa.text(
                """
                INSERT INTO artifact_snapshots (
                    artifact_id, provider, snapshot_type, status, fetched_at,
                    content_anchor, auth_mode, normalized_projection,
                    raw_payload_ref, evidence_limitations, fetch_anomalies
                ) VALUES (
                    CAST(:artifact_id AS uuid),
                    'local_text_idea',
                    'text_idea',
                    CAST(:status AS snapshot_status_enum),
                    now(),
                    :content_anchor,
                    'local_text_idea',
                    NULL,
                    NULL,
                    CAST(:evidence_limitations AS jsonb),
                    CAST(:fetch_anomalies AS jsonb)
                )
                RETURNING snapshot_id
                """
            ),
            {
                "artifact_id": draft.artifact_id,
                "status": draft.status,
                "content_anchor": draft.hash_surface,
                "evidence_limitations": _jsonb_dumps(draft.evidence_limitations),
                "fetch_anomalies": _jsonb_dumps([]),
            },
        )
        snapshot_id = str(parent.scalar_one())

        await self._session.execute(
            sa.text(
                """
                INSERT INTO artifact_snapshot_text_idea (
                    snapshot_id, source_message_id, source_version_no,
                    hash_surface, display_surface, dev_context_signals_json
                ) VALUES (
                    CAST(:snapshot_id AS uuid),
                    CAST(:source_message_id AS uuid),
                    :source_version_no,
                    :hash_surface,
                    :display_surface,
                    CAST(:dev_context_signals_json AS jsonb)
                )
                """
            ),
            {
                "snapshot_id": snapshot_id,
                "source_message_id": draft.source_message_id,
                "source_version_no": draft.source_version_no,
                "hash_surface": draft.hash_surface,
                "display_surface": draft.display_surface,
                "dev_context_signals_json": _jsonb_dumps(draft.dev_context_signals_json),
            },
        )
        return snapshot_id, draft.status

    async def load_discovered_links(
        self,
        *,
        candidate_group_id: str,
        parent_artifact_ids: Iterable[str],
    ) -> list[DiscoveredLinkSummary]:
        parent_artifact_ids = list(parent_artifact_ids)
        if not parent_artifact_ids:
            return []
        result = await self._session.execute(
            sa.text(
                """
                SELECT observed_url, context_path, discovery_reason,
                       parent_artifact_id, parent_snapshot_id, created_at
                FROM discovered_url_observations
                WHERE parent_candidate_group_id = CAST(:candidate_group_id AS uuid)
                  AND parent_artifact_id = ANY(CAST(:artifact_ids AS uuid[]))
                ORDER BY created_at DESC
                """
            ),
            {
                "candidate_group_id": candidate_group_id,
                "artifact_ids": parent_artifact_ids,
            },
        )
        seen: set[tuple[str, str, str]] = set()
        out: list[DiscoveredLinkSummary] = []
        for row in result.mappings().all():
            key = (
                str(row["parent_artifact_id"]),
                str(row["observed_url"]),
                str(row["context_path"] or ""),
            )
            if key in seen:
                continue
            seen.add(key)
            out.append(
                DiscoveredLinkSummary(
                    observed_url=str(row["observed_url"]),
                    context_path=str(row["context_path"]) if row["context_path"] else None,
                    discovery_reason=str(row["discovery_reason"]),
                    parent_artifact_id=str(row["parent_artifact_id"]),
                    parent_snapshot_id=str(row["parent_snapshot_id"]) if row["parent_snapshot_id"] else None,
                )
            )
        out.sort(key=lambda item: (item.parent_artifact_id, item.context_path or "", item.observed_url))
        return out

    async def load_existing_bundle(
        self,
        *,
        candidate_group_id: str,
        bundle_profile_version: str,
        bundle_input_hash: str,
    ) -> ExistingBundleRecord | None:
        result = await self._session.execute(
            sa.text(
                """
                SELECT bundle_id, candidate_group_id, bundle_version,
                       bundle_profile_version, bundle_input_hash, ready_for_analysis
                FROM candidate_evidence_bundles
                WHERE candidate_group_id = CAST(:candidate_group_id AS uuid)
                  AND bundle_profile_version = :bundle_profile_version
                  AND bundle_input_hash = :bundle_input_hash
                LIMIT 1
                """
            ),
            {
                "candidate_group_id": candidate_group_id,
                "bundle_profile_version": bundle_profile_version,
                "bundle_input_hash": bundle_input_hash,
            },
        )
        row = result.mappings().first()
        if row is None:
            return None
        return ExistingBundleRecord(
            bundle_id=str(row["bundle_id"]),
            candidate_group_id=str(row["candidate_group_id"]),
            bundle_version=int(row["bundle_version"]),
            bundle_profile_version=str(row["bundle_profile_version"]),
            bundle_input_hash=str(row["bundle_input_hash"]),
            ready_for_analysis=bool(row["ready_for_analysis"]),
        )

    async def next_bundle_version(self, candidate_group_id: str) -> int:
        result = await self._session.execute(
            sa.text(
                """
                SELECT COALESCE(MAX(bundle_version), 0) + 1 AS next_version
                FROM candidate_evidence_bundles
                WHERE candidate_group_id = CAST(:candidate_group_id AS uuid)
                """
            ),
            {"candidate_group_id": candidate_group_id},
        )
        return int(result.scalar_one())

    async def count_reroot_events(self, candidate_group_id: str) -> int:
        result = await self._session.execute(
            sa.text(
                """
                SELECT COUNT(*)
                FROM candidate_reroot_events
                WHERE candidate_group_id = CAST(:candidate_group_id AS uuid)
                """
            ),
            {"candidate_group_id": candidate_group_id},
        )
        return int(result.scalar_one())

    async def append_reroot_event(
        self,
        *,
        candidate_group_id: str,
        from_artifact_id: str,
        to_artifact_id: str,
        reason_code: str,
        trigger_snapshot_id: str | None,
    ) -> None:
        await self._session.execute(
            sa.text(
                """
                INSERT INTO candidate_reroot_events (
                    candidate_group_id, from_artifact_id, to_artifact_id,
                    reason_code, trigger_snapshot_id, created_at
                ) VALUES (
                    CAST(:candidate_group_id AS uuid),
                    CAST(:from_artifact_id AS uuid),
                    CAST(:to_artifact_id AS uuid),
                    :reason_code,
                    CAST(:trigger_snapshot_id AS uuid),
                    now()
                )
                """
            ),
            {
                "candidate_group_id": candidate_group_id,
                "from_artifact_id": from_artifact_id,
                "to_artifact_id": to_artifact_id,
                "reason_code": reason_code,
                "trigger_snapshot_id": trigger_snapshot_id,
            },
        )

    async def update_current_primary(self, *, candidate_group_id: str, artifact_id: str) -> None:
        await self._session.execute(
            sa.text(
                """
                UPDATE candidate_group_proposals
                SET current_primary_artifact_id = CAST(:artifact_id AS uuid), updated_at = now()
                WHERE candidate_group_id = CAST(:candidate_group_id AS uuid)
                """
            ),
            {"candidate_group_id": candidate_group_id, "artifact_id": artifact_id},
        )

    async def append_bundle(self, *, draft: EvidenceBundleDraft, bundle_version: int) -> str:
        result = await self._session.execute(
            sa.text(
                """
                INSERT INTO candidate_evidence_bundles (
                    candidate_group_id,
                    initial_primary_artifact_id,
                    current_primary_artifact_id,
                    bundle_version,
                    bundle_profile_version,
                    bundle_input_hash,
                    reroot_count,
                    primary_summary,
                    supporting_summaries_json,
                    discovered_links_summary_json,
                    evidence_limitations,
                    ready_for_analysis,
                    token_budget_profile,
                    created_at
                ) VALUES (
                    CAST(:candidate_group_id AS uuid),
                    CAST(:initial_primary_artifact_id AS uuid),
                    CAST(:current_primary_artifact_id AS uuid),
                    :bundle_version,
                    :bundle_profile_version,
                    :bundle_input_hash,
                    :reroot_count,
                    CAST(:primary_summary AS jsonb),
                    CAST(:supporting_summaries_json AS jsonb),
                    CAST(:discovered_links_summary_json AS jsonb),
                    CAST(:evidence_limitations AS jsonb),
                    :ready_for_analysis,
                    :token_budget_profile,
                    now()
                )
                RETURNING bundle_id
                """
            ),
            {
                "candidate_group_id": draft.candidate_group_id,
                "initial_primary_artifact_id": draft.initial_primary_artifact_id,
                "current_primary_artifact_id": draft.current_primary_artifact_id,
                "bundle_version": bundle_version,
                "bundle_profile_version": draft.bundle_profile_version,
                "bundle_input_hash": draft.bundle_input_hash,
                "reroot_count": draft.reroot_count,
                "primary_summary": _jsonb_dumps(draft.primary_summary),
                "supporting_summaries_json": _jsonb_dumps(draft.supporting_summaries_json),
                "discovered_links_summary_json": _jsonb_dumps(draft.discovered_links_summary_json),
                "evidence_limitations": _jsonb_dumps(draft.evidence_limitations),
                "ready_for_analysis": draft.ready_for_analysis,
                "token_budget_profile": draft.token_budget_profile,
            },
        )
        bundle_id = str(result.scalar_one())

        for member in draft.members:
            await self._session.execute(
                sa.text(
                    """
                    INSERT INTO candidate_evidence_members (
                        candidate_evidence_member_id,
                        bundle_id,
                        artifact_id,
                        snapshot_id,
                        member_role,
                        member_order
                    ) VALUES (
                        gen_random_uuid(),
                        CAST(:bundle_id AS uuid),
                        CAST(:artifact_id AS uuid),
                        CAST(:snapshot_id AS uuid),
                        :member_role,
                        :member_order
                    )
                    """
                ),
                {
                    "bundle_id": bundle_id,
                    "artifact_id": member.artifact_id,
                    "snapshot_id": member.snapshot_id,
                    "member_role": member.member_role,
                    "member_order": member.member_order,
                },
            )
        return bundle_id

    async def update_current_bundle(self, *, candidate_group_id: str, bundle_id: str) -> None:
        await self._session.execute(
            sa.text(
                """
                UPDATE candidate_group_proposals
                SET current_bundle_id = CAST(:bundle_id AS uuid), updated_at = now()
                WHERE candidate_group_id = CAST(:candidate_group_id AS uuid)
                """
            ),
            {"candidate_group_id": candidate_group_id, "bundle_id": bundle_id},
        )

    async def insert_analysis_requested_outbox(
        self,
        *,
        candidate_group_id: str,
        bundle_id: str,
        judge_profile: str,
    ) -> None:
        await self._session.execute(
            sa.text(
                """
                INSERT INTO event_outbox (
                    event_type, aggregate_type, aggregate_id, dedupe_key,
                    payload_json, status, created_at
                ) VALUES (
                    'analysis.requested.v1',
                    'candidate_group',
                    CAST(:candidate_group_id AS uuid),
                    :dedupe_key,
                    CAST(:payload_json AS jsonb),
                    'pending'::outbox_status_enum,
                    now()
                )
                ON CONFLICT (dedupe_key) DO NOTHING
                """
            ),
            {
                "candidate_group_id": candidate_group_id,
                "dedupe_key": f"analysis-request:{candidate_group_id}:{bundle_id}",
                "payload_json": _jsonb_dumps(
                    {
                        "candidate_group_id": candidate_group_id,
                        "bundle_id": bundle_id,
                        "judge_profile": judge_profile,
                        "escalation_allowed": True,
                    }
                ),
            },
        )
```

---

## 6-3. `src/services/evidence_assembler/service.py` (updated)

```python
from __future__ import annotations

import hashlib
import json
from typing import Iterable

from .config import EvidenceAssemblerConfig
from .models import (
    BundleMemberDraft,
    BundleRefreshTarget,
    CandidateGroupRecord,
    CandidateMemberRecord,
    EvidenceBundleDraft,
    SnapshotRecord,
)
from .readiness import ReadinessEvaluator
from .repositories import EvidenceAssemblerRepository
from .reroot_rules import RerootRules
from .text_idea_builder import TextIdeaBuilder
from .token_budget import TokenBudgetProfiler


class EvidenceAssemblerService:
    def __init__(
        self,
        config: EvidenceAssemblerConfig,
        *,
        repository: EvidenceAssemblerRepository,
    ) -> None:
        self._config = config
        self._repository = repository
        self._reroot_rules = RerootRules()
        self._readiness = ReadinessEvaluator()
        self._text_idea_builder = TextIdeaBuilder()
        self._token_budget = TokenBudgetProfiler()

    async def handle_trigger_event(self, trigger_event_id: str) -> None:
        targets = await self._repository.resolve_refresh_targets(trigger_event_id)
        for target in targets:
            await self._refresh_one(target)

    async def _refresh_one(self, target: BundleRefreshTarget) -> None:
        candidate = await self._repository.load_candidate_group(target.candidate_group_id)
        if candidate is None:
            return

        members = await self._repository.load_candidate_members(candidate.candidate_group_id)
        if not members:
            return

        artifact_types = {member.artifact_id: member.artifact_type for member in members}
        snapshots = await self._repository.load_current_snapshots(artifact_types.keys())

        if self._config.enable_text_idea and self._should_materialize_text_idea(candidate, artifact_types, snapshots):
            text_surface = await self._repository.load_source_message_text_surface(
                source_message_id=candidate.source_message_id,
                source_version_no=candidate.source_version_no,
            )
            text_idea_artifact_id = self._text_idea_artifact_id(candidate)
            draft = self._text_idea_builder.build(
                artifact_id=text_idea_artifact_id,
                source_message_id=candidate.source_message_id,
                source_version_no=candidate.source_version_no,
                text_surface=text_surface,
            )
            if draft is not None:
                snapshot_id, status = await self._repository.ensure_text_idea_snapshot(draft)
                snapshots[text_idea_artifact_id] = SnapshotRecord(
                    snapshot_id=snapshot_id,
                    artifact_id=text_idea_artifact_id,
                    provider="local_text_idea",
                    snapshot_type="text_idea",
                    status=status,
                    fetched_at=self._now(),
                    content_anchor=draft.hash_surface,
                    normalized_projection={
                        "display_surface": draft.display_surface,
                        "dev_context_signals_json": draft.dev_context_signals_json,
                    },
                    evidence_limitations=draft.evidence_limitations,
                    fetch_anomalies=[],
                )
                artifact_types[text_idea_artifact_id] = "text_idea"
                if text_idea_artifact_id not in {m.artifact_id for m in members}:
                    members = list(members) + [
                        CandidateMemberRecord(
                            artifact_id=text_idea_artifact_id,
                            artifact_type="text_idea",
                            member_role="supporting",
                            member_order=None,
                        )
                    ]

        current_primary_artifact_id = candidate.current_primary_artifact_id
        if self._config.enable_reroot:
            decision = self._reroot_rules.decide(
                current_primary_artifact_id=current_primary_artifact_id,
                artifact_types=artifact_types,
                current_snapshots=snapshots,
            )
            if decision.changed:
                async with self._repository.transaction():
                    await self._repository.append_reroot_event(
                        candidate_group_id=candidate.candidate_group_id,
                        from_artifact_id=decision.from_artifact_id,
                        to_artifact_id=decision.to_artifact_id,
                        reason_code=decision.reason_code or "reroot",
                        trigger_snapshot_id=target.trigger_snapshot_id,
                    )
                    await self._repository.update_current_primary(
                        candidate_group_id=candidate.candidate_group_id,
                        artifact_id=decision.to_artifact_id,
                    )
                current_primary_artifact_id = decision.to_artifact_id

        primary_snapshot = snapshots.get(current_primary_artifact_id)
        supporting_snapshots = [
            snapshots[m.artifact_id]
            for m in members
            if m.artifact_id != current_primary_artifact_id and m.artifact_id in snapshots
        ]

        discovered_links = await self._repository.load_discovered_links(
            candidate_group_id=candidate.candidate_group_id,
            parent_artifact_ids=[m.artifact_id for m in members],
        )

        token_budget_profile = self._token_budget.choose(
            primary_snapshot=primary_snapshot,
            supporting_snapshot_count=len(supporting_snapshots),
            discovered_links_count=len(discovered_links),
        ) if primary_snapshot is not None else "small"

        evidence_limitations = self._collect_limitations(primary_snapshot, supporting_snapshots)
        ready_for_analysis = self._readiness.is_ready_for_analysis(
            primary_snapshot=primary_snapshot,
            evidence_limitations=evidence_limitations,
            token_budget_profile=token_budget_profile,
        )

        bundle_members = self._bundle_members(
            current_primary_artifact_id=current_primary_artifact_id,
            members=members,
            snapshots=snapshots,
        )
        if not bundle_members:
            return

        reroot_count = await self._repository.count_reroot_events(candidate.candidate_group_id)
        bundle_input_hash = self._bundle_input_hash(
            candidate_group_id=candidate.candidate_group_id,
            current_primary_artifact_id=current_primary_artifact_id,
            members=bundle_members,
            reroot_count=reroot_count,
            discovered_links=discovered_links,
        )

        existing_bundle = await self._repository.load_existing_bundle(
            candidate_group_id=candidate.candidate_group_id,
            bundle_profile_version=self._config.bundle_profile_version,
            bundle_input_hash=bundle_input_hash,
        )
        if existing_bundle is not None:
            if candidate.current_bundle_id != existing_bundle.bundle_id:
                async with self._repository.transaction():
                    await self._repository.update_current_bundle(
                        candidate_group_id=candidate.candidate_group_id,
                        bundle_id=existing_bundle.bundle_id,
                    )
            return

        bundle_draft = EvidenceBundleDraft(
            candidate_group_id=candidate.candidate_group_id,
            initial_primary_artifact_id=candidate.initial_primary_artifact_id,
            current_primary_artifact_id=current_primary_artifact_id,
            bundle_profile_version=self._config.bundle_profile_version,
            bundle_input_hash=bundle_input_hash,
            reroot_count=reroot_count,
            primary_summary=self._snapshot_summary(primary_snapshot),
            supporting_summaries_json=[self._snapshot_summary(item) for item in supporting_snapshots],
            discovered_links_summary_json=[
                {
                    "observed_url": item.observed_url,
                    "context_path": item.context_path,
                    "discovery_reason": item.discovery_reason,
                    "parent_artifact_id": item.parent_artifact_id,
                    "parent_snapshot_id": item.parent_snapshot_id,
                }
                for item in discovered_links
            ],
            evidence_limitations=evidence_limitations,
            ready_for_analysis=ready_for_analysis,
            token_budget_profile=token_budget_profile,
            members=bundle_members,
            judge_profile=self._judge_profile_for_primary(artifact_types.get(current_primary_artifact_id)),
        )

        async with self._repository.transaction():
            bundle_version = await self._repository.next_bundle_version(candidate.candidate_group_id)
            bundle_id = await self._repository.append_bundle(
                draft=bundle_draft,
                bundle_version=bundle_version,
            )
            await self._repository.update_current_bundle(
                candidate_group_id=candidate.candidate_group_id,
                bundle_id=bundle_id,
            )
            if bundle_draft.ready_for_analysis and bundle_draft.judge_profile:
                await self._repository.insert_analysis_requested_outbox(
                    candidate_group_id=candidate.candidate_group_id,
                    bundle_id=bundle_id,
                    judge_profile=bundle_draft.judge_profile,
                )

    def _should_materialize_text_idea(
        self,
        candidate: CandidateGroupRecord,
        artifact_types: dict[str, str],
        snapshots: dict[str, SnapshotRecord],
    ) -> bool:
        primary_type = artifact_types.get(candidate.current_primary_artifact_id)
        if primary_type == "text_idea":
            return True
        usable_external = any(
            snapshot.snapshot_type in {"github_repo", "github_gist", "x_post", "web_article"}
            and snapshot.status in {"ready", "partial_ready", "low_evidence"}
            for snapshot in snapshots.values()
        )
        return not usable_external

    def _text_idea_artifact_id(self, candidate: CandidateGroupRecord) -> str:
        seed = f"text-idea:{candidate.source_message_id}:{candidate.source_version_no}"
        digest = hashlib.sha256(seed.encode("utf-8")).hexdigest()[:32]
        return f"00000000-0000-0000-0000-{digest[:12]}{digest[12:24]}"[:36]

    def _bundle_members(
        self,
        *,
        current_primary_artifact_id: str,
        members: Iterable[CandidateMemberRecord],
        snapshots: dict[str, SnapshotRecord],
    ) -> list[BundleMemberDraft]:
        drafts: list[BundleMemberDraft] = []
        for member in members:
            snapshot = snapshots.get(member.artifact_id)
            if snapshot is None:
                continue
            role = "primary" if member.artifact_id == current_primary_artifact_id else "supporting"
            drafts.append(
                BundleMemberDraft(
                    artifact_id=member.artifact_id,
                    snapshot_id=snapshot.snapshot_id,
                    member_role=role,
                    member_order=member.member_order,
                )
            )
        drafts.sort(key=lambda item: (0 if item.member_role == "primary" else 1, item.member_order or 999999, item.artifact_id))
        return drafts

    def _collect_limitations(
        self,
        primary_snapshot: SnapshotRecord | None,
        supporting_snapshots: list[SnapshotRecord],
    ) -> list[str]:
        values: list[str] = []
        for snapshot in [primary_snapshot, *supporting_snapshots]:
            if snapshot is None:
                continue
            for item in snapshot.evidence_limitations or []:
                if item not in values:
                    values.append(item)
            if snapshot.status == "partial_ready" and "partial_ready" not in values:
                values.append("partial_ready")
            if snapshot.status == "low_evidence" and "low_evidence" not in values:
                values.append("low_evidence")
        return values

    def _snapshot_summary(self, snapshot: SnapshotRecord | None) -> dict[str, object]:
        if snapshot is None:
            return {"status": "missing"}
        projection = snapshot.normalized_projection or {}
        return {
            "artifact_id": snapshot.artifact_id,
            "snapshot_id": snapshot.snapshot_id,
            "provider": snapshot.provider,
            "snapshot_type": snapshot.snapshot_type,
            "status": snapshot.status,
            "content_anchor": snapshot.content_anchor,
            "headline": projection.get("title") or projection.get("description") or projection.get("display_surface"),
        }

    def _bundle_input_hash(
        self,
        *,
        candidate_group_id: str,
        current_primary_artifact_id: str,
        members: list[BundleMemberDraft],
        reroot_count: int,
        discovered_links: list,
    ) -> str:
        payload = {
            "candidate_group_id": candidate_group_id,
            "current_primary_artifact_id": current_primary_artifact_id,
            "members": [
                {
                    "artifact_id": item.artifact_id,
                    "snapshot_id": item.snapshot_id,
                    "member_role": item.member_role,
                    "member_order": item.member_order,
                }
                for item in members
            ],
            "reroot_count": reroot_count,
            "discovered_links": [
                {
                    "observed_url": item.observed_url,
                    "context_path": item.context_path,
                    "parent_artifact_id": item.parent_artifact_id,
                }
                for item in discovered_links
            ],
        }
        encoded = json.dumps(payload, ensure_ascii=False, sort_keys=True)
        return hashlib.sha256(encoded.encode("utf-8")).hexdigest()

    def _judge_profile_for_primary(self, artifact_type: str | None) -> str | None:
        if artifact_type in {"github_repo", "github_subpath", "github_repo_page", "github_gist"}:
            return "github_primary"
        if artifact_type == "x_post":
            return "x_primary"
        if artifact_type in {"web_article", "text_idea"}:
            return "text_idea_primary"
        return None

    @staticmethod
    def _now() -> datetime:
        return datetime.now(timezone.utc)
```

---

## 6-4. `src/services/evidence_assembler/worker.py` (tiny update)

```python
from __future__ import annotations

import asyncio
from dataclasses import dataclass

from .config import EvidenceAssemblerConfig
from .redis_streams import RedisStreamConsumer, StreamMessage
from .service import EvidenceAssemblerService


@dataclass(slots=True, frozen=True)
class WorkerBatchResult:
    processed: int = 0
    acked: int = 0


class EvidenceAssemblerWorker:
    def __init__(
        self,
        config: EvidenceAssemblerConfig,
        *,
        consumer: RedisStreamConsumer,
        service: EvidenceAssemblerService,
    ) -> None:
        self._config = config
        self._consumer = consumer
        self._service = service
        self._stop_event = asyncio.Event()

    async def run_forever(self) -> None:
        await self._consumer.ensure_group()
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
        for message in messages:
            processed += 1
            trigger_event_id = message.fields.get("trigger_event_id")
            if trigger_event_id:
                await self._service.handle_trigger_event(trigger_event_id)
            await self._consumer.ack(message.message_id)
            acked += 1
        return WorkerBatchResult(processed=processed, acked=acked)
```

---

## 7. 테스트 초안 포인트

### `tests/component/services/evidence_assembler/test_snapshot_updated_fanout.py`

검증:
- `artifact.snapshot.updated.v1` payload에 `candidate_group_id`가 없어도
- `candidate_group_members.artifact_id`를 기준으로 impacted candidate 두 개 이상을 fan-out refresh 하는지

### `tests/unit/services/evidence_assembler/test_existing_bundle_reuse.py`

검증:
- 같은 `bundle_input_hash`가 이미 존재하면
- 새 bundle row append 없이 existing bundle 재사용하는지
- duplicate `analysis.requested.v1`가 생기지 않는지

### `tests/component/services/evidence_assembler/test_text_idea_snapshot_reuse.py`

검증:
- 같은 source message/version/text_surface면
- `local_text_idea` snapshot이 중복 append되지 않는지

### `tests/unit/services/evidence_assembler/test_judge_profile_mapping.py`

검증:
- `web_article` primary가 `idea_primary`가 아니라 `text_idea_primary`로 고정되는지

### `tests/unit/services/evidence_assembler/test_discovered_link_filtering.py`

검증:
- stale observation이 current member artifact set 밖이면 bundle summary에서 빠지는지
- 같은 `(parent_artifact_id, observed_url, context_path)`는 newest-first 한 번만 들어가는지

### `tests/component/services/evidence_assembler/test_duplicate_trigger_no_new_bundle.py`

검증:
- 같은 snapshot update가 두 번 와도
- `candidate_evidence_bundles` 신규 row 수가 증가하지 않는지

---

## 8. 이번 단계가 구조를 지키는 이유

이번 hardening은 stage 5와 stage 6 사이 경계를 더 선명하게 만든다.

1. **snapshot update → candidate refresh fan-out** 은 Postgres membership 재조회로만 수행한다.  
   즉, Redis/event payload를 durable source처럼 쓰지 않는다.

2. **judge profile 명칭을 잠긴 계약에 다시 맞춘다.**  
   즉, `analysis-router`가 바로 붙을 수 있다.

3. **bundle / member write를 schema에 정확히 맞춘다.**  
   즉, append-only history 의미가 유지된다.

4. **새 artifact를 만들지 않는다.**  
   즉, assembler가 artifact identity 계층을 다시 침범하지 않는다.

5. **duplicate trigger를 analysis 폭주로 이어지게 하지 않는다.**  
   즉, current bundle reuse를 추가해 stage 6 진입 전 operational safety를 높인다.

---

## 9. 다음 단계

이 hardening이 닫히면 다음 구현 순서는 아래가 맞다.

1. `34_analysis_router_skeleton_and_code_draft_v0_1.md`
   - `analysis.requested.v1` consumer
   - judge profile/model 선택
   - escalation 여부 결정
   - re-enrich branch 판단

2. `35_judge_openai_skeleton_and_code_draft_v0_1.md`
3. `36_analysis_validator_skeleton_and_code_draft_v0_1.md`
4. `37_policy_engine_skeleton_and_code_draft_v0_1.md`
5. `38_notifier_telegram_skeleton_and_code_draft_v0_1.md`

즉, **stage 5는 여기서 operationally 닫고, 이제 stage 6 judge pipeline으로 넘어간다.**

---

## 최종 한 줄 결론

이번 단계의 최소-change 정답은 **`artifact.snapshot.updated.v1`를 candidate membership fan-out으로 재해석하고, current bundle reuse / text_idea snapshot reuse / discovered observation filtering / reroot edge-case hardening / schema-aligned bundle write를 추가해 `analysis-router` 직전의 `evidence-assembler` operational gap을 닫는 것**이다.


---

## Source file: `34_analysis_router_skeleton_and_code_draft_v0_1.md`

# 34단계: `analysis-router` 스켈레톤 + 실제 코드 초안 v0.1

## 0. 문서 목적

이 문서는 이미 잠긴 구조를 다시 설계하는 문서가 아니다.  
목적은 **stage 5 evidence layer가 33단계에서 operationally 닫힌 상태**를 전제로, stage 6 judge pipeline의 첫 서비스인 **`analysis-router`** 를 구현 가능한 수준으로 내리는 것이다.

이번 단계에서 고정하는 것은 아래 여섯 가지다.

1. `analysis.requested.v1` 소비 경계를 코드로 고정
2. thin Redis payload → `event_outbox` 재조회 → DB canonical row rehydration 경계를 고정
3. `analysis-router`의 좁은 책임을 **bundle readiness 확인 / profile 검증 / model 선택 / escalation gate / re-enrich branch / judge run 생성**으로 고정
4. `judge_runs` / `event_outbox`만 직접 쓰는 **service ownership** 을 고정
5. `judge.call.requested.v1` emit까지 닫아 `judge-openai`가 바로 붙을 수 있게 고정
6. `03_GitHub_AI_application_plan.md`의 적용 아이디어 중, 현재 단계에 넣어도 구조를 흔들지 않는 것과 지금 넣으면 안 되는 것을 명시적으로 분리

핵심 전제는 유지한다.

- `analysis-router`는 **LLM 호출기**가 아니다.
- `analysis-router`는 **policy engine**이 아니다.
- `analysis-router`는 **notifier**가 아니다.
- `analysis-router`는 **final verdict**를 계산하지 않는다.
- `analysis-router`는 **judge pipeline의 deterministic entry gate** 다.

---

## 1. 왜 지금 이 단계가 정확한 다음 단계인가

현재 소스 오브 트루스 기준 상태는 아래로 고정돼 있다.

- stage 5 evidence layer는 `gh-enricher`, `x-enricher`, `web-enricher`, `evidence-assembler`, `evidence-assembler integration hardening`까지 닫혀 있다.
- 다음 구현 순서는 `analysis-router` → `judge-openai` → `analysis-validator` → `policy-engine` → `notifier-telegram` 이다.
- execution contracts의 M5 judge pipeline 산출물도 동일하게 `analysis-router`부터 시작한다.

따라서 지금 collector / normalizer / enricher를 다시 여는 것은 순서상 후퇴고,  
**`analysis-router`가 정확한 다음 단계**다.

---

## 2. 새로 추가된 소스와 충돌 검토

이번 턴에서 새롭게 확인해야 하는 것은 `03_GitHub_AI_application_plan.md`다.  
이 문서는 외부 자산 적용 후보를 정리한 **적용 설계서**이지만, 현재 phase/순서를 잠그는 정본은 아니다.  
즉, 이 파일은 **advisory design note**이고, phase authority는 여전히 최신 README와 stage 문서다.

### 충돌 A — README v6 vs README v7

현재 프로젝트 소스에는 `README_replacement_consolidated_v0_6.md`와 `README_replacement_consolidated_v0_7.md`가 같이 있을 수 있다.  
둘은 최신 단계 인식이 다르다.

- v6: latest = 32, next = evidence-assembler hardening
- v7: latest = 33, next = analysis-router

최소-change 해석은 단순하다.

- **v7이 v6을 대체한다.**
- v6은 이력성 중간 산출물로 보고, 현재 phase authority로 사용하지 않는다.

### 충돌 B — application plan의 phase snapshot은 현재보다 한 단계 뒤처져 있다

`03_GitHub_AI_application_plan.md`는 작성 시점 기준으로  
“다음 구현 순서: `web-enricher → evidence-assembler`” 를 전제로 설명한다.

하지만 현재 phase authority는 이미 stage 33까지 닫혀 있고, 다음 순서를 `analysis-router`로 고정한다.

최소-change 해석:

- `03_GitHub_AI_application_plan.md`는 **적용 가능 자산 판단 문서**로만 사용
- **현재 phase/ordering authority는 아님**
- 즉, application plan의 좋은 아이디어는 가져오되, 현재 구현 순서를 덮어쓰지 않는다

### 충돌 C — Prompt Guard를 지금 어디에 넣을 것인가

application plan은 Prompt Guard를 아래 세 지점에 강하게 권장한다.

1. `web-enricher`
2. `x-enricher`
3. `judge-openai` 직전

하지만 지금 시점에서 `web-enricher` / `x-enricher`를 다시 열면,  
이미 닫힌 stage 5 evidence layer를 다시 흔들게 된다.

최소-change 해석:

- **이번 34단계에는 Prompt Guard를 runtime path로 넣지 않는다.**
- 대신 `analysis-router` 문서에서 **future pre-judge preflight insertion point** 만 예약한다.
- 실제 activation은 `judge-openai` 단계에서 붙이는 것이 가장 보수적이다.
- stage 5 source enricher retrofitting은 지금 하지 않는다.

### 충돌 D — AgentLinter / MemKraft / skill docs

이 셋은 runtime hot path가 아니라 **repo 운영 구조 / prompt discipline / ops sidecar** 성격이다.

따라서 최소-change 해석은 아래가 맞다.

- **AgentLinter**: 적용 가능, 하지만 `analysis-router` runtime 코드에 넣지 않는다
- **MemKraft**: ops/eval memory sidecar로만 가능, runtime DB truth로 넣지 않는다
- **Hermes/CrowClaw skill/playbook 패턴**: prompt/profile handbook 문서화에만 제한 적용

즉, 이 셋은 현재 34단계의 코드 설계 대상이 아니라  
**후속 repo hygiene / prompt asset 정리 단계** 대상이다.

---

## 3. `analysis-router`의 책임과 비책임

### 3-1. 반드시 하는 일

- `analysis.requested.v1` 소비
- `event_outbox` 기준 request rehydration
- `candidate_group_proposals.current_bundle_id`와 요청 bundle의 정합성 확인
- `candidate_evidence_bundles.ready_for_analysis` 확인
- `judge_profile` allowlist 검증
- model / reasoning effort / prompt version / schema version / policy version 선택
- escalation gate 적용
- `judge_runs` insert 또는 existing row reuse
- `judge.call.requested.v1` outbox emit
- 필요 시 `candidate.bundle.refresh.v1` 재요청 branch emit

### 3-2. 하면 안 되는 일

- LLM 직접 호출
- `judge_output_v1` 생성
- final verdict / delivery decision 계산
- notification render/send
- new artifact 생성
- reroot 결정 재수행
- raw snapshot fetch
- bundle 재조립
- Prompt Guard를 근거로 final suppress 확정

즉, 이 서비스는 **judge pipeline의 deterministic entry gate** 다.

---

## 4. 직접 소유하는 durable 경계

execution contracts 기준으로 `analysis-router`는 아래만 직접 쓴다.

- `judge_runs`
- `event_outbox`

읽는 것:

- `candidate_group_proposals`
- `candidate_evidence_bundles`
- `candidate_evidence_members`
- 필요 시 `artifact_snapshots`

즉, `analysis-router`는 **judge root row + downstream event** 만 만든다.  
`judge_outputs`, `analyses`, `notification_*`는 건드리지 않는다.

---

## 5. 입력/출력 계약

### 5-1. 입력 이벤트

허용 입력은 아래 하나로 좁게 고정한다.

- `analysis.requested.v1`

Redis Streams 메시지는 여전히 thin payload다.

```json
{
  "job_id": "<event_id>",
  "stage_name": "analysis_route",
  "root_object_type": "candidate_group",
  "root_object_id": "<candidate_group_id>",
  "idempotency_key": "<dedupe_key>",
  "pipeline_run_id": "",
  "not_before": "",
  "trigger_event_id": "<event_id>"
}
```

즉, consumer는 Redis 본문을 business source처럼 쓰지 않고,  
반드시 `trigger_event_id`로 `event_outbox`를 다시 조회한다.

### 5-2. 입력 payload 최소 필드

`analysis.requested.v1` payload는 아래를 믿는다.

- `candidate_group_id`
- `bundle_id`
- `judge_profile`
- `escalation_allowed`

하지만 이 값들은 **request hint** 일 뿐이다.  
최종 route는 여전히 PostgreSQL의 current row 기준으로 재검증한다.

### 5-3. 출력 이벤트

#### A. judge path
- `judge.call.requested.v1`

payload 최소 필드:

- `judge_run_id`
- `bundle_id`
- `model`
- `reasoning_effort`
- `prompt_version`
- `prompt_cache_key`

#### B. re-enrich path
- `candidate.bundle.refresh.v1`

payload 최소 필드:

- `candidate_group_id`
- `trigger_kind = analysis_router_recheck`
- `trigger_object_type = bundle`
- `trigger_object_id = bundle_id`
- `refresh_reason`

---

## 6. 이번 단계에서 고정할 핵심 라우팅 규칙

### 6-1. stale request 방지

`analysis.requested.v1`는 append-only history 위에서 생성된다.  
따라서 request가 늦게 소비될 수 있고, 그 사이 더 새로운 bundle이 current가 될 수 있다.

이번 단계의 규칙은 단순하다.

- 요청 payload의 `bundle_id`가 `candidate_group_proposals.current_bundle_id`와 다르면
- **stale request로 보고 no-op**
- re-enrich도 하지 않는다
- 더 최신 bundle용 `analysis.requested.v1`가 뒤에 올 것이기 때문이다

즉, analysis-router는 **stale bundle을 judge로 보내지 않는다.**

### 6-2. not-ready bundle 처리

아래면 judge로 보내면 안 된다.

- bundle row 없음
- `ready_for_analysis = false`
- `judge_profile` 없음
- bundle current pointer mismatch
- bundle member count = 0

이 경우:

- `candidate.bundle.refresh.v1`를 emit
- reason 예시:
  - `bundle_missing`
  - `bundle_not_ready`
  - `bundle_profile_missing`
  - `bundle_members_missing`

즉, 증거 부족 문제는 judge가 아니라 **bundle refresh** 로 되돌린다.

### 6-3. judge profile allowlist

허용 profile은 아래 셋만 고정한다.

- `github_primary`
- `x_primary`
- `text_idea_primary`

이 셋 밖이면 judge run을 만들지 않는다.

중요:

- `idea_primary`는 허용하지 않는다
- `web_primary`를 새로 만들지 않는다
- `03_GitHub_AI_application_plan.md`의 skill/playbook 아이디어는 profile handbook 확장으로만 흡수하고, profile enum 자체는 지금 늘리지 않는다

### 6-4. 기본 모델 선택

기본값:

- model = `gpt-5.4-mini`
- reasoning_effort = `low`

이 경로가 hot path다.

### 6-5. escalation gate

`ENABLE_MODEL_ESCALATION=false` 이면 무조건 기본 경로다.

켜져 있을 때도 아래를 모두 만족할 때만 승급을 허용한다.

- `analysis.requested.v1.escalation_allowed = true`
- bundle `reroot_count > 0`
  **또는**
- bundle supporting member count >= 3
  **또는**
- `token_budget_profile in {"large", "xlarge"}`

그때만:

- model = `gpt-5.4`
- reasoning_effort = `medium`

즉, 이번 단계의 승급 기준은 **해석 복잡도** 다.  
증거 부족을 승급으로 덮지 않는다.

### 6-6. prompt / schema / policy 선택

profile별 prompt version은 분리한다.

- `github_primary` → `judge_github_primary_v1`
- `x_primary` → `judge_x_primary_v1`
- `text_idea_primary` → `judge_text_idea_primary_v1`

공통:

- `schema_version = judge_output_v1`
- `policy_version = verdict_policy_v1`

### 6-7. prompt cache key

고정 규칙:

```text
judge:{profile}:{prompt_version}:{schema_version}:{policy_version}
```

예:

```text
judge:github_primary:judge_github_primary_v1:judge_output_v1:verdict_policy_v1
```

### 6-8. existing judge run reuse

`judge_runs`는 `(bundle_id, prompt_version, model, reasoning_effort)` unique다.

따라서 같은 조합의 judge run이 이미 있으면:

- 새 row를 만들지 않는다
- `judge.call.requested.v1`도 재emit하지 않는다

즉, analysis-router는 **duplicate request를 judge call 폭주로 연결하지 않는다.**

재판정은 나중에 replay path 책임이다.

---

## 7. application plan 적용 검토 — 이번 단계 결론

### 7-1. 지금 바로 적용 가능한 것

없다.  
정확히 말하면 **runtime hot path에는 없다.**

이유:

- AgentLinter는 repo hygiene/CI 자산이다
- MemKraft는 ops/eval memory sidecar다
- skill/playbook 패턴은 prompt asset 정리 자산이다
- Prompt Guard는 runtime 후보지만, 지금 34단계에서 activation하면 현재 contracts 밖의 quarantine/blocked flow를 새로 정의해야 한다

### 7-2. 지금 적용하면 구조를 흔드는 것

#### A. Prompt Guard를 `analysis-router`에서 hard block로 넣는 것
문제:
- 현재 contracts에는 `blocked_prompt_risk` 같은 공식 lifecycle이 없다
- final suppress는 policy-engine 책임인데 analysis-router가 판단을 선점하게 된다

#### B. `web-enricher` / `x-enricher`를 다시 열어 Prompt Guard를 retrofitting 하는 것
문제:
- stage 5 evidence layer를 다시 흔든다
- 지금 phase authority와 충돌한다

#### C. Hermes/CrowClaw runtime을 도입하는 것
문제:
- collector / outbox / normalizer / enrichers / assembler / judge / policy / notifier 경계를 흐린다

### 7-3. 최소-change 결론

- **이번 34단계에는 application plan을 runtime path에 넣지 않는다.**
- 대신 아래를 future-compatible decision으로만 남긴다.
  - Prompt Guard: `judge-openai` 직전 preflight insertion point로만 예약
  - AgentLinter: prompt/policy/README 정리 단계에서 적용
  - MemKraft: `ops-memory/` sidecar로만 적용
  - skill/playbook 패턴: profile handbook 문서화에만 적용

즉, application plan은 **현재 구조를 흔들지 않는 방향으로만 흡수** 한다.

---

## 8. 대상 파일 트리

```text
src/services/analysis_router/
  __init__.py
  config.py
  models.py
  repositories.py
  routing_policy.py
  service.py
  worker.py
  main.py

tests/
  unit/
    services/
      analysis_router/
        test_routing_policy.py
        test_prompt_cache_key.py
        test_unknown_profile_rejected.py
        test_escalation_gate.py
  component/
    services/
      analysis_router/
        test_worker_rehydrates_analysis_request.py
        test_not_ready_bundle_emits_refresh.py
        test_ready_bundle_creates_judge_run_and_outbox.py
        test_stale_request_noop.py
        test_existing_judge_run_reuse.py
```

---

## 9. 코드 초안

### 9-1. `src/services/analysis_router/__init__.py`

```python
from .config import AnalysisRouterConfig
from .service import AnalysisRouterService
from .worker import AnalysisRouterWorker

__all__ = [
    "AnalysisRouterConfig",
    "AnalysisRouterService",
    "AnalysisRouterWorker",
]
```

### 9-2. `src/services/analysis_router/config.py`

```python
from __future__ import annotations

import os
from dataclasses import dataclass


class AnalysisRouterConfigurationError(ValueError):
    pass


@dataclass(slots=True, frozen=True)
class AnalysisRouterConfig:
    app_env: str
    database_url: str
    redis_url: str

    queue_name: str
    consumer_group: str
    consumer_name: str
    batch_size: int
    block_ms: int

    enable_model_escalation: bool

    default_model: str
    escalation_model: str
    default_reasoning_effort: str
    escalation_reasoning_effort: str

    github_prompt_version: str
    x_prompt_version: str
    text_idea_prompt_version: str
    judge_schema_version: str
    policy_version: str

    log_level: str

    @classmethod
    def from_env(cls) -> "AnalysisRouterConfig":
        def _read(name: str, default: str = "") -> str:
            return os.getenv(name, default).strip()

        cfg = cls(
            app_env=_read("APP_ENV", "dev").lower(),
            database_url=_read("DATABASE_URL"),
            redis_url=_read("REDIS_URL"),
            queue_name=_read("ANALYSIS_ROUTER_QUEUE_NAME", "q.analysis.route"),
            consumer_group=_read("ANALYSIS_ROUTER_CONSUMER_GROUP", "analysis-router"),
            consumer_name=_read("ANALYSIS_ROUTER_CONSUMER_NAME", "analysis-router-1"),
            batch_size=int(_read("ANALYSIS_ROUTER_BATCH_SIZE", "20")),
            block_ms=int(_read("ANALYSIS_ROUTER_BLOCK_MS", "5000")),
            enable_model_escalation=_read("ENABLE_MODEL_ESCALATION", "false").lower() == "true",
            default_model=_read("JUDGE_DEFAULT_MODEL", "gpt-5.4-mini"),
            escalation_model=_read("JUDGE_ESCALATION_MODEL", "gpt-5.4"),
            default_reasoning_effort=_read("JUDGE_REASONING_EFFORT_DEFAULT", "low"),
            escalation_reasoning_effort=_read("JUDGE_REASONING_EFFORT_ESCALATION", "medium"),
            github_prompt_version=_read("JUDGE_PROMPT_VERSION_GITHUB", "judge_github_primary_v1"),
            x_prompt_version=_read("JUDGE_PROMPT_VERSION_X", "judge_x_primary_v1"),
            text_idea_prompt_version=_read("JUDGE_PROMPT_VERSION_TEXT_IDEA", "judge_text_idea_primary_v1"),
            judge_schema_version=_read("JUDGE_SCHEMA_VERSION", "judge_output_v1"),
            policy_version=_read("VERDICT_POLICY_VERSION", "verdict_policy_v1"),
            log_level=_read("LOG_LEVEL", "INFO").upper(),
        )
        cfg.validate()
        return cfg

    def validate(self) -> None:
        if not self.database_url:
            raise AnalysisRouterConfigurationError("DATABASE_URL is required")
        if not self.redis_url:
            raise AnalysisRouterConfigurationError("REDIS_URL is required")
        if not self.queue_name:
            raise AnalysisRouterConfigurationError("ANALYSIS_ROUTER_QUEUE_NAME must not be empty")
        if not self.consumer_group:
            raise AnalysisRouterConfigurationError("ANALYSIS_ROUTER_CONSUMER_GROUP must not be empty")
        if not self.consumer_name:
            raise AnalysisRouterConfigurationError("ANALYSIS_ROUTER_CONSUMER_NAME must not be empty")
        if self.batch_size <= 0 or self.batch_size > 100:
            raise AnalysisRouterConfigurationError("ANALYSIS_ROUTER_BATCH_SIZE must be between 1 and 100")
        if self.block_ms <= 0:
            raise AnalysisRouterConfigurationError("ANALYSIS_ROUTER_BLOCK_MS must be > 0")
        if not self.default_model:
            raise AnalysisRouterConfigurationError("JUDGE_DEFAULT_MODEL must not be empty")
        if not self.escalation_model:
            raise AnalysisRouterConfigurationError("JUDGE_ESCALATION_MODEL must not be empty")
```

### 9-3. `src/services/analysis_router/models.py`

```python
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Literal


JudgeProfile = Literal["github_primary", "x_primary", "text_idea_primary"]


@dataclass(slots=True, frozen=True)
class AnalysisRequestedJob:
    trigger_event_id: str
    event_type: str
    candidate_group_id: str
    bundle_id: str
    judge_profile: str | None
    escalation_allowed: bool


@dataclass(slots=True, frozen=True)
class CandidateRouteState:
    candidate_group_id: str
    current_bundle_id: str | None
    current_analysis_id: str | None


@dataclass(slots=True, frozen=True)
class BundleRouteRecord:
    bundle_id: str
    candidate_group_id: str
    bundle_profile_version: str
    reroot_count: int
    ready_for_analysis: bool
    token_budget_profile: str
    created_at: datetime


@dataclass(slots=True, frozen=True)
class BundleShapeStats:
    member_count: int
    supporting_count: int
    discovered_link_count: int


@dataclass(slots=True, frozen=True)
class JudgeRouteDecision:
    action: Literal["judge", "refresh", "noop"]
    judge_profile: JudgeProfile | None = None
    model: str | None = None
    reasoning_effort: str | None = None
    prompt_version: str | None = None
    schema_version: str | None = None
    policy_version: str | None = None
    prompt_cache_key: str | None = None
    refresh_reason: str | None = None
```

### 9-4. `src/services/analysis_router/repositories.py`

```python
from __future__ import annotations

import json
from contextlib import asynccontextmanager
from typing import Any

import sqlalchemy as sa
from sqlalchemy.ext.asyncio import AsyncSession

from .models import AnalysisRequestedJob, BundleRouteRecord, BundleShapeStats, CandidateRouteState


def _jsonb_dumps(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False)


class AnalysisRouterRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    @asynccontextmanager
    async def transaction(self):
        if self._session.in_transaction():
            yield self._session
            return
        async with self._session.begin():
            yield self._session

    async def load_job_by_trigger_event_id(self, trigger_event_id: str) -> AnalysisRequestedJob | None:
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
        if str(row["event_type"]) != "analysis.requested.v1":
            return None
        return AnalysisRequestedJob(
            trigger_event_id=str(row["event_id"]),
            event_type=str(row["event_type"]),
            candidate_group_id=str(payload["candidate_group_id"]),
            bundle_id=str(payload["bundle_id"]),
            judge_profile=str(payload["judge_profile"]) if payload.get("judge_profile") else None,
            escalation_allowed=bool(payload.get("escalation_allowed", False)),
        )

    async def load_candidate_route_state(self, candidate_group_id: str) -> CandidateRouteState | None:
        result = await self._session.execute(
            sa.text(
                """
                SELECT candidate_group_id, current_bundle_id, current_analysis_id
                FROM candidate_group_proposals
                WHERE candidate_group_id = CAST(:candidate_group_id AS uuid)
                """
            ),
            {"candidate_group_id": candidate_group_id},
        )
        row = result.mappings().first()
        if row is None:
            return None
        return CandidateRouteState(
            candidate_group_id=str(row["candidate_group_id"]),
            current_bundle_id=str(row["current_bundle_id"]) if row["current_bundle_id"] else None,
            current_analysis_id=str(row["current_analysis_id"]) if row["current_analysis_id"] else None,
        )

    async def load_bundle(self, bundle_id: str) -> BundleRouteRecord | None:
        result = await self._session.execute(
            sa.text(
                """
                SELECT bundle_id, candidate_group_id, bundle_profile_version,
                       reroot_count, ready_for_analysis,
                       token_budget_profile, created_at
                FROM candidate_evidence_bundles
                WHERE bundle_id = CAST(:bundle_id AS uuid)
                """
            ),
            {"bundle_id": bundle_id},
        )
        row = result.mappings().first()
        if row is None:
            return None
        return BundleRouteRecord(
            bundle_id=str(row["bundle_id"]),
            candidate_group_id=str(row["candidate_group_id"]),
            bundle_profile_version=str(row["bundle_profile_version"]),
            reroot_count=int(row["reroot_count"]),
            ready_for_analysis=bool(row["ready_for_analysis"]),
            token_budget_profile=str(row["token_budget_profile"]),
            created_at=row["created_at"],
        )

    async def load_bundle_shape_stats(self, bundle_id: str) -> BundleShapeStats:
        result = await self._session.execute(
            sa.text(
                """
                SELECT
                    COUNT(*) AS member_count,
                    COUNT(*) FILTER (WHERE member_role = 'supporting') AS supporting_count
                FROM candidate_evidence_members
                WHERE bundle_id = CAST(:bundle_id AS uuid)
                """
            ),
            {"bundle_id": bundle_id},
        )
        row = result.mappings().one()

        links = await self._session.execute(
            sa.text(
                """
                SELECT jsonb_array_length(COALESCE(discovered_links_summary_json, '[]'::jsonb)) AS discovered_link_count
                FROM candidate_evidence_bundles
                WHERE bundle_id = CAST(:bundle_id AS uuid)
                """
            ),
            {"bundle_id": bundle_id},
        )
        lrow = links.mappings().one()

        return BundleShapeStats(
            member_count=int(row["member_count"]),
            supporting_count=int(row["supporting_count"]),
            discovered_link_count=int(lrow["discovered_link_count"]),
        )

    async def load_existing_judge_run(
        self,
        *,
        bundle_id: str,
        prompt_version: str,
        model: str,
        reasoning_effort: str,
    ) -> str | None:
        result = await self._session.execute(
            sa.text(
                """
                SELECT judge_run_id
                FROM judge_runs
                WHERE bundle_id = CAST(:bundle_id AS uuid)
                  AND prompt_version = :prompt_version
                  AND model = :model
                  AND reasoning_effort = :reasoning_effort
                LIMIT 1
                """
            ),
            {
                "bundle_id": bundle_id,
                "prompt_version": prompt_version,
                "model": model,
                "reasoning_effort": reasoning_effort,
            },
        )
        value = result.scalar_one_or_none()
        return str(value) if value else None

    async def insert_judge_run(
        self,
        *,
        bundle_id: str,
        judge_profile: str,
        model: str,
        reasoning_effort: str,
        prompt_version: str,
        schema_version: str,
        policy_version: str,
        prompt_cache_key: str,
    ) -> str:
        result = await self._session.execute(
            sa.text(
                """
                INSERT INTO judge_runs (
                    judge_run_id,
                    bundle_id,
                    judge_profile,
                    model,
                    reasoning_effort,
                    prompt_version,
                    schema_version,
                    policy_version,
                    prompt_cache_key,
                    status,
                    started_at
                ) VALUES (
                    gen_random_uuid(),
                    CAST(:bundle_id AS uuid),
                    :judge_profile,
                    :model,
                    :reasoning_effort,
                    :prompt_version,
                    :schema_version,
                    :policy_version,
                    :prompt_cache_key,
                    'pending',
                    now()
                )
                RETURNING judge_run_id
                """
            ),
            {
                "bundle_id": bundle_id,
                "judge_profile": judge_profile,
                "model": model,
                "reasoning_effort": reasoning_effort,
                "prompt_version": prompt_version,
                "schema_version": schema_version,
                "policy_version": policy_version,
                "prompt_cache_key": prompt_cache_key,
            },
        )
        return str(result.scalar_one())

    async def insert_judge_call_requested_outbox(
        self,
        *,
        judge_run_id: str,
        bundle_id: str,
        model: str,
        reasoning_effort: str,
        prompt_version: str,
        prompt_cache_key: str,
    ) -> None:
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
                    'judge.call.requested.v1',
                    'judge_run',
                    CAST(:judge_run_id AS uuid),
                    :dedupe_key,
                    CAST(:payload_json AS jsonb),
                    'pending'::outbox_status_enum,
                    now()
                )
                ON CONFLICT (dedupe_key) DO NOTHING
                """
            ),
            {
                "judge_run_id": judge_run_id,
                "dedupe_key": f"judge-call:{judge_run_id}",
                "payload_json": _jsonb_dumps(
                    {
                        "judge_run_id": judge_run_id,
                        "bundle_id": bundle_id,
                        "model": model,
                        "reasoning_effort": reasoning_effort,
                        "prompt_version": prompt_version,
                        "prompt_cache_key": prompt_cache_key,
                    }
                ),
            },
        )

    async def insert_bundle_refresh_outbox(
        self,
        *,
        candidate_group_id: str,
        bundle_id: str,
        refresh_reason: str,
    ) -> None:
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
                    'candidate.bundle.refresh.v1',
                    'candidate_group',
                    CAST(:candidate_group_id AS uuid),
                    :dedupe_key,
                    CAST(:payload_json AS jsonb),
                    'pending'::outbox_status_enum,
                    now()
                )
                ON CONFLICT (dedupe_key) DO NOTHING
                """
            ),
            {
                "candidate_group_id": candidate_group_id,
                "dedupe_key": f"bundle-refresh:{candidate_group_id}:{bundle_id}:{refresh_reason}",
                "payload_json": _jsonb_dumps(
                    {
                        "candidate_group_id": candidate_group_id,
                        "trigger_kind": "analysis_router_recheck",
                        "trigger_object_type": "bundle",
                        "trigger_object_id": bundle_id,
                        "refresh_reason": refresh_reason,
                    }
                ),
            },
        )
```

### 9-5. `src/services/analysis_router/routing_policy.py`

```python
from __future__ import annotations

from .config import AnalysisRouterConfig
from .models import AnalysisRequestedJob, BundleRouteRecord, BundleShapeStats, JudgeRouteDecision


_ALLOWED_PROFILES = {
    "github_primary",
    "x_primary",
    "text_idea_primary",
}


class AnalysisRoutingPolicy:
    def __init__(self, config: AnalysisRouterConfig) -> None:
        self._config = config

    def decide(
        self,
        *,
        job: AnalysisRequestedJob,
        current_bundle_id: str | None,
        bundle: BundleRouteRecord | None,
        shape: BundleShapeStats | None,
    ) -> JudgeRouteDecision:
        if current_bundle_id is None or current_bundle_id != job.bundle_id:
            return JudgeRouteDecision(action="noop")

        if bundle is None:
            return JudgeRouteDecision(action="refresh", refresh_reason="bundle_missing")

        if not bundle.ready_for_analysis:
            return JudgeRouteDecision(action="refresh", refresh_reason="bundle_not_ready")

        if shape is None or shape.member_count <= 0:
            return JudgeRouteDecision(action="refresh", refresh_reason="bundle_members_missing")

        judge_profile = (job.judge_profile or "").strip()
        if judge_profile not in _ALLOWED_PROFILES:
            return JudgeRouteDecision(action="noop")

        prompt_version = self._prompt_version_for_profile(judge_profile)
        schema_version = self._config.judge_schema_version
        policy_version = self._config.policy_version
        prompt_cache_key = f"judge:{judge_profile}:{prompt_version}:{schema_version}:{policy_version}"

        use_escalation = (
            self._config.enable_model_escalation
            and job.escalation_allowed
            and (
                bundle.reroot_count > 0
                or shape.supporting_count >= 3
                or bundle.token_budget_profile in {"large", "xlarge"}
            )
        )

        if use_escalation:
            return JudgeRouteDecision(
                action="judge",
                judge_profile=judge_profile,  # type: ignore[arg-type]
                model=self._config.escalation_model,
                reasoning_effort=self._config.escalation_reasoning_effort,
                prompt_version=prompt_version,
                schema_version=schema_version,
                policy_version=policy_version,
                prompt_cache_key=prompt_cache_key,
            )

        return JudgeRouteDecision(
            action="judge",
            judge_profile=judge_profile,  # type: ignore[arg-type]
            model=self._config.default_model,
            reasoning_effort=self._config.default_reasoning_effort,
            prompt_version=prompt_version,
            schema_version=schema_version,
            policy_version=policy_version,
            prompt_cache_key=prompt_cache_key,
        )

    def _prompt_version_for_profile(self, profile: str) -> str:
        if profile == "github_primary":
            return self._config.github_prompt_version
        if profile == "x_primary":
            return self._config.x_prompt_version
        return self._config.text_idea_prompt_version
```

### 9-6. `src/services/analysis_router/service.py`

```python
from __future__ import annotations

import logging

from .config import AnalysisRouterConfig
from .repositories import AnalysisRouterRepository
from .routing_policy import AnalysisRoutingPolicy


class AnalysisRouterService:
    def __init__(
        self,
        config: AnalysisRouterConfig,
        *,
        repository: AnalysisRouterRepository,
        logger: logging.Logger | None = None,
    ) -> None:
        self._config = config
        self._repository = repository
        self._policy = AnalysisRoutingPolicy(config)
        self._logger = logger or logging.getLogger(__name__)

    async def handle_trigger_event(self, trigger_event_id: str) -> None:
        job = await self._repository.load_job_by_trigger_event_id(trigger_event_id)
        if job is None:
            return

        candidate_state = await self._repository.load_candidate_route_state(job.candidate_group_id)
        if candidate_state is None:
            return

        bundle = await self._repository.load_bundle(job.bundle_id)
        shape = await self._repository.load_bundle_shape_stats(job.bundle_id) if bundle is not None else None

        decision = self._policy.decide(
            job=job,
            current_bundle_id=candidate_state.current_bundle_id,
            bundle=bundle,
            shape=shape,
        )

        if decision.action == "noop":
            self._logger.info(
                "analysis_router_noop",
                extra={
                    "service": "analysis-router",
                    "event": "analysis_router_noop",
                    "candidate_group_id": job.candidate_group_id,
                    "bundle_id": job.bundle_id,
                },
            )
            return

        if decision.action == "refresh":
            async with self._repository.transaction():
                await self._repository.insert_bundle_refresh_outbox(
                    candidate_group_id=job.candidate_group_id,
                    bundle_id=job.bundle_id,
                    refresh_reason=decision.refresh_reason or "bundle_recheck",
                )
            return

        existing = await self._repository.load_existing_judge_run(
            bundle_id=job.bundle_id,
            prompt_version=decision.prompt_version or "",
            model=decision.model or "",
            reasoning_effort=decision.reasoning_effort or "",
        )
        if existing is not None:
            self._logger.info(
                "analysis_router_existing_judge_run_reused",
                extra={
                    "service": "analysis-router",
                    "event": "analysis_router_existing_judge_run_reused",
                    "judge_run_id": existing,
                    "bundle_id": job.bundle_id,
                },
            )
            return

        async with self._repository.transaction():
            judge_run_id = await self._repository.insert_judge_run(
                bundle_id=job.bundle_id,
                judge_profile=decision.judge_profile or "",
                model=decision.model or "",
                reasoning_effort=decision.reasoning_effort or "",
                prompt_version=decision.prompt_version or "",
                schema_version=decision.schema_version or "",
                policy_version=decision.policy_version or "",
                prompt_cache_key=decision.prompt_cache_key or "",
            )
            await self._repository.insert_judge_call_requested_outbox(
                judge_run_id=judge_run_id,
                bundle_id=job.bundle_id,
                model=decision.model or "",
                reasoning_effort=decision.reasoning_effort or "",
                prompt_version=decision.prompt_version or "",
                prompt_cache_key=decision.prompt_cache_key or "",
            )
```

### 9-7. `src/services/analysis_router/worker.py`

```python
from __future__ import annotations

import asyncio
from dataclasses import dataclass

from .config import AnalysisRouterConfig
from .service import AnalysisRouterService
from ..gh_enricher.redis_streams import RedisStreamConsumer


@dataclass(slots=True, frozen=True)
class WorkerBatchResult:
    processed: int = 0
    acked: int = 0


class AnalysisRouterWorker:
    def __init__(
        self,
        config: AnalysisRouterConfig,
        *,
        consumer: RedisStreamConsumer,
        service: AnalysisRouterService,
    ) -> None:
        self._config = config
        self._consumer = consumer
        self._service = service
        self._stop_event = asyncio.Event()

    async def run_forever(self) -> None:
        await self._consumer.ensure_group()
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
        for message in messages:
            processed += 1
            trigger_event_id = message.fields.get("trigger_event_id")
            if trigger_event_id:
                await self._service.handle_trigger_event(trigger_event_id)
            await self._consumer.ack(message.message_id)
            acked += 1
        return WorkerBatchResult(processed=processed, acked=acked)
```

### 9-8. `src/services/analysis_router/main.py`

```python
from __future__ import annotations

import asyncio
import logging
import sys

from redis.asyncio import Redis
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from .config import AnalysisRouterConfig
from .repositories import AnalysisRouterRepository
from .service import AnalysisRouterService
from .worker import AnalysisRouterWorker
from ..gh_enricher.redis_streams import RedisStreamConsumer


def _configure_logging(log_level: str) -> None:
    logging.basicConfig(
        level=getattr(logging, log_level, logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
        stream=sys.stdout,
    )


async def _run() -> int:
    config = AnalysisRouterConfig.from_env()
    _configure_logging(config.log_level)

    engine = create_async_engine(config.database_url, future=True)
    session_factory = async_sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)
    redis_client = Redis.from_url(config.redis_url, decode_responses=True)

    try:
        async with session_factory() as session:
            repository = AnalysisRouterRepository(session)
            service = AnalysisRouterService(config, repository=repository)
            consumer = RedisStreamConsumer(
                redis_client,
                queue_name=config.queue_name,
                consumer_group=config.consumer_group,
                consumer_name=config.consumer_name,
                block_ms=config.block_ms,
                batch_size=config.batch_size,
            )
            worker = AnalysisRouterWorker(
                config,
                consumer=consumer,
                service=service,
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

---

## 10. 테스트 포인트

### `tests/unit/services/analysis_router/test_routing_policy.py`
검증:
- unknown profile이면 `noop`
- not-ready bundle이면 `refresh`
- default는 `gpt-5.4-mini + low`
- escalation flag가 꺼져 있으면 항상 default
- reroot_count > 0 이고 escalation_allowed=true면 `gpt-5.4 + medium`

### `tests/unit/services/analysis_router/test_prompt_cache_key.py`
검증:
- `judge:{profile}:{prompt_version}:{schema_version}:{policy_version}` 형식 고정

### `tests/unit/services/analysis_router/test_unknown_profile_rejected.py`
검증:
- `idea_primary`를 더 이상 허용하지 않는지

### `tests/component/services/analysis_router/test_worker_rehydrates_analysis_request.py`
검증:
- Redis payload는 thin message
- `event_outbox` 기준으로 `analysis.requested.v1` rehydrate

### `tests/component/services/analysis_router/test_not_ready_bundle_emits_refresh.py`
검증:
- bundle not ready면 `candidate.bundle.refresh.v1` outbox insert

### `tests/component/services/analysis_router/test_ready_bundle_creates_judge_run_and_outbox.py`
검증:
- ready bundle이면 `judge_runs` insert
- `judge.call.requested.v1` outbox insert

### `tests/component/services/analysis_router/test_stale_request_noop.py`
검증:
- request bundle과 current bundle이 다르면 no-op

### `tests/component/services/analysis_router/test_existing_judge_run_reuse.py`
검증:
- 같은 `(bundle_id, prompt_version, model, reasoning_effort)` judge run이 있으면
- 새 judge run과 새 judge.call outbox를 만들지 않음

---

## 11. 이번 단계가 구조를 지키는 이유

1. `analysis-router`는 `judge_runs`와 `event_outbox`만 직접 쓴다.  
   즉, service ownership을 넘지 않는다.

2. 증거 부족 문제를 judge 승급으로 덮지 않고, `candidate.bundle.refresh.v1`로 되돌린다.  
   즉, stage 5와 stage 6 책임 분리가 유지된다.

3. stale bundle request를 no-op 처리한다.  
   즉, current pointer와 history가 섞이지 않는다.

4. Prompt Guard / AgentLinter / MemKraft를 지금 runtime hot path에 억지로 집어넣지 않는다.  
   즉, `03_GitHub_AI_application_plan.md`의 좋은 방향은 보존하되 current contracts는 흔들지 않는다.

5. `judge.call.requested.v1`까지만 emit한다.  
   즉, `judge-openai` / `analysis-validator` / `policy-engine` / `notifier-telegram` 경계를 침범하지 않는다.

---

## 12. 다음 단계

이 단계가 닫히면 다음 구현 순서는 그대로 아래다.

1. `35_judge_openai_skeleton_and_code_draft_v0_1.md`
2. `36_analysis_validator_skeleton_and_code_draft_v0_1.md`
3. `37_policy_engine_skeleton_and_code_draft_v0_1.md`
4. `38_notifier_telegram_skeleton_and_code_draft_v0_1.md`

즉, 이제 stage 6 judge pipeline의 첫 입구를 닫았고,  
다음은 실제 OpenAI Responses API 호출 경계를 붙이는 것이 맞다.

---

## 최종 한 줄 결론

이번 단계의 최소-change 정답은 **`analysis.requested.v1`를 rehydrate해 current bundle 정합성을 검증하고, not-ready/stale 경로는 `candidate.bundle.refresh.v1`로 되돌리며, ready bundle만 deterministic model/profile/escalation 정책으로 `judge_runs`와 `judge.call.requested.v1`까지 연결하는 `analysis-router` v0.1을 닫는 것**이다.


---

## Source file: `35_judge_openai_skeleton_and_code_draft_v0_1.md`


# 35단계: `judge-openai` 스켈레톤 + 실제 코드 초안 v0.1

## 0. 문서 목적

이 문서는 이미 잠긴 구조를 다시 설계하는 문서가 아니다.  
목적은 **34단계 `analysis-router`가 만들어낸 `judge.call.requested.v1`를 실제 OpenAI Responses API 호출로 연결하는 좁은 경계**를 구현 가능한 수준으로 내리는 것이다.

이번 단계에서 고정하는 것은 아래 일곱 가지다.

1. `judge.call.requested.v1` 소비 경계를 코드로 고정
2. thin Redis payload → `event_outbox` 재조회 → `judge_runs` / `candidate_evidence_bundles` 재hydrate 경계를 고정
3. `judge-openai`의 좁은 책임을 **Responses API 호출 / structured output 수신 / usage telemetry 기록 / `judge_outputs` append / `judge.output.ready.v1` emit** 으로 고정
4. `judge_runs`, `judge_outputs`, `event_outbox`만 직접 쓰는 **service ownership** 을 고정
5. stage 6 문서의 요구대로 **Structured Outputs + schema retry 1회** 를 judge-openai 내부 경계로 고정
6. `03_GitHub_AI_application_plan.md`의 Prompt Guard 아이디어를 **구조를 흔들지 않는 최소-change insertion point** 로만 반영
7. 다음 단계인 `analysis-validator`가 바로 붙을 수 있게 **refusal / structured output / telemetry handoff** 를 안정화

핵심 전제는 유지한다.

- `judge-openai`는 **analysis-router** 가 아니다.
- `judge-openai`는 **policy-engine** 이 아니다.
- `judge-openai`는 **notifier** 가 아니다.
- `judge-openai`는 **최종 verdict / delivery decision** 을 확정하지 않는다.
- `judge-openai`는 **`judge_output_v1`을 수집하는 OpenAI 호출 경계** 다.

---

## 1. 왜 지금 이 단계가 정확한 다음 단계인가

현재 소스 오브 트루스 기준 상태는 아래로 고정돼 있다.

- stage 5 evidence layer는 33단계에서 operationally 닫혔다.
- stage 6 judge pipeline의 첫 서비스인 `analysis-router`는 34단계에서 닫혔다.
- 최신 authoritative README는 다음 구현 순서를 **`judge-openai` → `analysis-validator` → `policy-engine` → `notifier-telegram`** 으로 고정한다.
- 34단계 문서도 다음 구현 순서를 그대로 `35_judge_openai_skeleton_and_code_draft_v0_1.md` 로 둔다.

즉, 지금 다시 collector / normalizer / enricher / assembler를 여는 것은 순서상 후퇴다.  
이제 붙여야 하는 것은 **실제 OpenAI 호출 경계인 `judge-openai`** 다.

---

## 2. 이번 단계에서 확인한 충돌과 최소-change 해석

### 충돌 A — current 소스에는 README v6 / v7 / v8이 함께 남아 있을 수 있다

현재 프로젝트 소스에는 아래 README 계열이 함께 존재할 수 있다.

- `README_replacement_consolidated_v0_6.md`
- `README_replacement_consolidated_v0_7.md`
- `README_replacement_consolidated_v0_8.md`

이 셋은 최신 단계 인식이 다르다.

- v6: latest = 32
- v7: latest = 33
- v8: latest = 34

### 최소-change 해석 A

- **v8이 phase authority** 다.
- v6 / v7은 이력성 중간본으로 본다.
- 이번 35단계 문서와 README 업데이트에서는 **v8을 이어받아 v9로만 승격** 한다.

즉, phase ordering은 최신 README 하나로 수렴시키고,  
오래된 README는 더 이상 authority로 쓰지 않는다.

---

### 충돌 B — stage 6 문서는 refusal을 validator가 처리하라고 하지만, 현재 이벤트 계약에는 refusal 전용 이벤트가 없다

현재 stage 6 문서는 아래를 동시에 요구한다.

- judge-openai는 Responses API + Structured Outputs를 사용
- refusal / schema failure / truncation도 처리해야 함
- 다음 단계인 validator가 refusal / schema failure를 분기해야 함

그런데 현재 이벤트 계약은 `judge.output.ready.v1` 하나만 있고,  
refusal 전용 outbox 이벤트는 없다.

### 최소-change 해석 B

이번 v0.1에서는 아래처럼 고정한다.

1. **structured output 성공**
   - `judge_outputs` row 생성
   - `judge.output.ready.v1` emit

2. **refusal**
   - `judge_outputs` row는 여전히 생성
   - 단, `payload_json`에는 structured verdict 대신 **refusal envelope** 를 저장
   - `judge_runs.refusal_detected = true`
   - `judge.output.ready.v1`는 그대로 emit
   - validator가 이 refusal envelope와 `refusal_detected`를 보고 분기

3. **transport failure / retry 초과 / final schema failure**
   - `judge_outputs` row 생성 안 함
   - `judge_runs.status`를 `failed_retryable` 또는 `failed_terminal`로만 끝냄
   - `judge.output.ready.v1` emit 안 함

즉, **validator가 볼 수 있는 refusal은 judge_output row로 넘기고**,  
**judge_output조차 없는 호출 실패는 judge-openai 내부 실패**로 남긴다.

이 해석이 현재 이벤트 계약과 stage 6 요구를 동시에 살리는 가장 작은 변경이다.

---

### 충돌 C — application plan은 Prompt Guard를 judge-openai 직전에 넣으라고 권하지만, 현재 durable schema에는 prompt risk 상태가 없다

`03_GitHub_AI_application_plan.md`는 Prompt Guard를 `judge-openai` 직전에 강하게 권장한다.  
하지만 현재 contracts에는 아래가 없다.

- `prompt_risk_level` durable 컬럼
- `requires_quarantine` 공식 lifecycle
- prompt-risk 전용 outbox 이벤트

즉, 지금 Prompt Guard를 **hard block / quarantine** 로 넣으면  
judge-openai가 policy-engine 책임을 침범하게 된다.

### 최소-change 해석 C

이번 v0.1에서는 Prompt Guard를 아래처럼만 반영한다.

- `judge-openai` 안에 **optional preflight hook** 을 둔다
- 기본 구현은 **No-op**
- 선택 구현은 **sanitize-only**
  - instruction-like 문자열 일부를 model context에서 중화
  - raw bundle / raw DB row overwrite는 금지
- preflight 결과는 **로그/메모리 수준**에서만 사용
- final suppress / quarantine / reroute는 하지 않는다

즉, application plan의 방향은 보존하지만  
**judge-openai v0.1은 여전히 contracts를 넘지 않는 최소 경계** 로 둔다.

---

### 충돌 D — judge-openai는 bundle 밖 정보를 찾아 나가면 안 되지만, implementation 편의상 raw snapshot 재조회를 유혹받기 쉽다

stage 6 정본은 judge 입력을 **`CandidateEvidenceBundle`로 제한** 한다.  
따라서 judge-openai가 raw snapshot이나 raw source_message를 다시 읽기 시작하면 stage 5/6 경계가 무너진다.

### 최소-change 해석 D

이번 v0.1에서는 judge-openai가 읽는 durable 입력을 아래로 고정한다.

- `judge_runs`
- `candidate_evidence_bundles`

필요 시 read-only로:
- `candidate_group_proposals`  
  - 단, pointer/candidate identity 확인 수준

금지:
- `artifact_snapshots` 재탐색
- `source_messages` 재탐색
- 외부 fetch
- tool call

즉, OpenAI에 보내는 실제 context는 **bundle row만으로 구성** 한다.

---

## 3. `judge-openai`의 책임과 비책임

### 3-1. 반드시 하는 일

- `judge.call.requested.v1` 소비
- `event_outbox` 기준 request rehydrate
- `judge_runs` / `candidate_evidence_bundles` 재조회
- profile별 prompt load/render
- optional preflight sanitize
- OpenAI Responses API 호출
- structured output 파싱
- schema retry 1회
- usage / latency / finish_reason / refusal_detected 기록
- `judge_outputs` append
- `judge.output.ready.v1` outbox emit

### 3-2. 하면 안 되는 일

- final verdict 확정
- delivery decision 확정
- 알림 렌더링/전송
- raw source rescan
- GitHub/X/web fetch
- candidate reroot 재계산
- policy override
- Prompt Guard 기반 hard suppress 결정

즉, 이 서비스는 **OpenAI 호출과 결과 수집 경계** 일 뿐이다.

---

## 4. 직접 소유하는 durable 경계

execution contracts 기준으로 `judge-openai`는 아래만 직접 쓴다.

- `judge_runs`
- `judge_outputs`
- `event_outbox`

읽는 것:

- `candidate_evidence_bundles`
- 필요 시 `candidate_group_proposals`

즉, `analyses`, `state_transitions`, `notification_*`는 건드리지 않는다.

---

## 5. 입력/출력 계약

### 5-1. 입력 이벤트

허용 입력은 아래 하나로 좁게 고정한다.

- `judge.call.requested.v1`

Redis Streams 메시지는 여전히 thin payload다.

```json
{
  "job_id": "<event_id>",
  "stage_name": "judge",
  "root_object_type": "judge_run",
  "root_object_id": "<judge_run_id>",
  "idempotency_key": "<dedupe_key>",
  "pipeline_run_id": "",
  "not_before": "",
  "trigger_event_id": "<event_id>"
}
```

즉, consumer는 Redis 본문을 business source처럼 쓰지 않고,  
반드시 `trigger_event_id`로 `event_outbox`를 다시 조회한다.

### 5-2. 입력 payload 최소 필드

`judge.call.requested.v1` payload는 아래를 믿는다.

- `judge_run_id`
- `bundle_id`
- `model`
- `reasoning_effort`
- `prompt_version`
- `prompt_cache_key`

하지만 이 값들은 **request hint** 일 뿐이다.  
최종 route는 여전히 `judge_runs` current row와 `bundle_id` 재조회로 재검증한다.

### 5-3. 출력 이벤트

허용 출력은 아래 하나다.

- `judge.output.ready.v1`

payload 최소 필드:

- `judge_run_id`
- `judge_output_id`
- `finish_reason`
- `refusal_detected`

즉, judge-openai는 여기서 멈추고  
다음 단계 `analysis-validator`가 이 이벤트를 소비한다.

---

## 6. 이번 단계에서 고정할 핵심 처리 규칙

### 6-1. stale / non-pending judge run no-op

judge-openai는 아래면 OpenAI를 호출하지 않는다.

- `judge_run` 없음
- `judge_run.status != pending`
- `judge_run.bundle_id != payload.bundle_id`

즉, `analysis-router`가 만든 deterministic entry gate를 judge-openai가 다시 흐리지 않는다.

### 6-2. bundle missing / malformed는 terminal failure

아래면 judge-openai는 OpenAI를 부르면 안 된다.

- bundle row 없음
- bundle candidate_group_id 없음
- prompt_version / model / reasoning_effort 누락
- bundle row가 비어 있어 context를 만들 수 없음

이 경우:

- `judge_runs.status = failed_terminal`
- `finish_reason = bundle_missing | bundle_invalid | prompt_missing`
- `judge_output` row 없음
- `judge.output.ready.v1` 없음

즉, 이건 validator가 아니라 **호출 전 불변식 위반** 이다.

### 6-3. model context는 bundle row만으로 만든다

judge-openai가 OpenAI에 보내는 context는 아래만 포함한다.

- `candidate_group_id`
- `bundle_id`
- `current_primary_artifact_id`
- `primary_summary`
- `supporting_summaries_json`
- `discovered_links_summary_json`
- `evidence_limitations`
- `token_budget_profile`

금지:
- raw snapshot 본문 전체
- raw source message 원문 전체
- 외부 검색
- tool call

즉, stage 6 문서가 잠근 **“bundle 밖으로 나가지 않는 judge”** 를 그대로 따른다.

### 6-4. Optional Prompt Guard preflight는 sanitize-only

`03_GitHub_AI_application_plan.md`를 현재 단계에 최소 반영하는 방식은 아래다.

- `ModelContextPreflight` 인터페이스 추가
- 기본값: `NoopModelContextPreflight`
- 선택값: `HeuristicSanitizingPreflight`
- 허용 동작:
  - obvious instruction-like line stripping
  - developer/user context text 정리
- 금지 동작:
  - judge run 차단
  - candidate suppress
  - quarantine 상태 생성
  - DB overwrite

즉, Prompt Guard는 **future-compatible hook** 이지,  
이번 v0.1의 lifecycle 변경 장치는 아니다.

### 6-5. Responses API + Structured Outputs strict

judge-openai는 아래 요청 구조를 고정한다.

- API: **Responses API**
- output format: **json_schema + strict**
- role split:
  - developer message: fixed rubric / hard rules / profile guidance
  - user message: bundle-derived context only

### 6-6. schema retry 1회

처리 규칙:

1. 첫 호출에서 valid structured payload를 얻으면 종료
2. structured payload가 비어 있거나 JSON parse 실패면
   - `schema_retry_count += 1`
   - **같은 model / 같은 prompt_version / 같은 bundle** 로 1회만 재시도
3. 두 번째도 실패면
   - `judge_runs.status = failed_terminal`
   - `finish_reason = schema_invalid_after_retry`
   - `judge_output` 없음

즉, stage 6 문서가 요구한 “schema retry 1회”를 judge-openai 안에서 닫는다.

### 6-7. refusal 처리

Responses API 호출 자체는 성공했지만 refusal이 감지되면:

- `judge_runs.status = succeeded`
- `judge_runs.refusal_detected = true`
- `judge_outputs.payload_json` 에 refusal envelope 저장
- `judge.output.ready.v1` emit

즉, refusal은 **호출 실패가 아니라 모델 결과의 한 유형** 으로 보고,  
validator가 후속 분기한다.

### 6-8. usage telemetry는 judge_runs에만 기록

`judge_runs`에 아래를 채운다.

- `input_tokens`
- `cached_input_tokens`
- `output_tokens`
- `reasoning_tokens`
- `latency_ms`
- `finish_reason`
- `refusal_detected`
- `schema_retry_count`

즉, OpenAI dashboard가 아니라 **PostgreSQL judge row가 실제 audit source** 다.

---

## 7. Prompt / schema / policy 선택 규칙

judge-openai는 새 정책을 만들지 않는다.  
`analysis-router`가 이미 결정한 값을 그대로 따른다.

- `judge_profile`
- `model`
- `reasoning_effort`
- `prompt_version`
- `schema_version`
- `policy_version`
- `prompt_cache_key`

즉, judge-openai는 “무엇을 쓸지 결정” 하지 않고,  
**결정된 judge run을 실행** 한다.

---

## 8. 대상 파일 트리

```text
src/services/judge_openai/
  __init__.py
  config.py
  models.py
  preflight.py
  prompt_library.py
  context_builder.py
  response_mapper.py
  openai_client.py
  repositories.py
  service.py
  worker.py
  main.py

tests/
  unit/
    services/
      judge_openai/
        test_prompt_library.py
        test_context_builder.py
        test_response_mapper_success.py
        test_response_mapper_refusal.py
        test_preflight_sanitize_only.py
  component/
    services/
      judge_openai/
        test_worker_rehydrates_judge_call.py
        test_success_writes_judge_output_and_outbox.py
        test_refusal_writes_envelope_and_outbox.py
        test_schema_retry_once_then_fail.py
        test_transport_failure_marks_retryable.py
```

---

## 9. 코드 초안

### 9-1. `src/services/judge_openai/__init__.py`

```python
from .config import JudgeOpenAIConfig
from .service import JudgeOpenAIService
from .worker import JudgeOpenAIWorker

__all__ = [
    "JudgeOpenAIConfig",
    "JudgeOpenAIService",
    "JudgeOpenAIWorker",
]
```

### 9-2. `src/services/judge_openai/config.py`

```python
from __future__ import annotations

import os
from dataclasses import dataclass


class JudgeOpenAIConfigurationError(ValueError):
    pass


@dataclass(slots=True, frozen=True)
class JudgeOpenAIConfig:
    app_env: str
    database_url: str
    redis_url: str

    queue_name: str
    consumer_group: str
    consumer_name: str
    batch_size: int
    block_ms: int

    openai_api_key: str
    openai_project: str | None
    request_timeout_sec: float
    max_output_tokens: int | None

    enable_prompt_guard_preflight: bool
    log_level: str

    @classmethod
    def from_env(cls) -> "JudgeOpenAIConfig":
        def _read(name: str, default: str = "") -> str:
            return os.getenv(name, default).strip()

        max_output_tokens_raw = _read("JUDGE_MAX_OUTPUT_TOKENS", "")

        cfg = cls(
            app_env=_read("APP_ENV", "dev").lower(),
            database_url=_read("DATABASE_URL"),
            redis_url=_read("REDIS_URL"),
            queue_name=_read("JUDGE_OPENAI_QUEUE_NAME", "q.analysis.judge"),
            consumer_group=_read("JUDGE_OPENAI_CONSUMER_GROUP", "judge-openai"),
            consumer_name=_read("JUDGE_OPENAI_CONSUMER_NAME", "judge-openai-1"),
            batch_size=int(_read("JUDGE_OPENAI_BATCH_SIZE", "10")),
            block_ms=int(_read("JUDGE_OPENAI_BLOCK_MS", "5000")),
            openai_api_key=_read("OPENAI_API_KEY"),
            openai_project=_read("OPENAI_PROJECT") or None,
            request_timeout_sec=float(_read("JUDGE_OPENAI_REQUEST_TIMEOUT_SEC", "60")),
            max_output_tokens=int(max_output_tokens_raw) if max_output_tokens_raw else None,
            enable_prompt_guard_preflight=_read("ENABLE_PROMPT_GUARD_PREFLIGHT", "false").lower() == "true",
            log_level=_read("LOG_LEVEL", "INFO").upper(),
        )
        cfg.validate()
        return cfg

    def validate(self) -> None:
        if not self.database_url:
            raise JudgeOpenAIConfigurationError("DATABASE_URL is required")
        if not self.redis_url:
            raise JudgeOpenAIConfigurationError("REDIS_URL is required")
        if not self.queue_name:
            raise JudgeOpenAIConfigurationError("JUDGE_OPENAI_QUEUE_NAME must not be empty")
        if not self.consumer_group:
            raise JudgeOpenAIConfigurationError("JUDGE_OPENAI_CONSUMER_GROUP must not be empty")
        if not self.consumer_name:
            raise JudgeOpenAIConfigurationError("JUDGE_OPENAI_CONSUMER_NAME must not be empty")
        if self.batch_size <= 0 or self.batch_size > 100:
            raise JudgeOpenAIConfigurationError("JUDGE_OPENAI_BATCH_SIZE must be between 1 and 100")
        if self.block_ms <= 0:
            raise JudgeOpenAIConfigurationError("JUDGE_OPENAI_BLOCK_MS must be > 0")
        if not self.openai_api_key:
            raise JudgeOpenAIConfigurationError("OPENAI_API_KEY is required")
        if self.request_timeout_sec <= 0:
            raise JudgeOpenAIConfigurationError("JUDGE_OPENAI_REQUEST_TIMEOUT_SEC must be > 0")
        if self.max_output_tokens is not None and self.max_output_tokens <= 0:
            raise JudgeOpenAIConfigurationError("JUDGE_MAX_OUTPUT_TOKENS must be > 0 when set")
```

### 9-3. `src/services/judge_openai/models.py`

```python
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Literal


JudgeRunStatus = Literal["pending", "running", "succeeded", "failed_retryable", "failed_terminal"]


@dataclass(slots=True, frozen=True)
class JudgeCallJob:
    trigger_event_id: str
    event_type: str
    judge_run_id: str
    bundle_id: str
    model: str
    reasoning_effort: str
    prompt_version: str
    prompt_cache_key: str | None


@dataclass(slots=True, frozen=True)
class JudgeRunRecord:
    judge_run_id: str
    bundle_id: str
    judge_profile: str
    model: str
    reasoning_effort: str
    prompt_version: str
    schema_version: str
    policy_version: str
    prompt_cache_key: str | None
    status: str
    schema_retry_count: int


@dataclass(slots=True, frozen=True)
class BundleJudgeContext:
    bundle_id: str
    candidate_group_id: str
    current_primary_artifact_id: str
    primary_summary: dict[str, Any]
    supporting_summaries_json: list[dict[str, Any]]
    discovered_links_summary_json: list[dict[str, Any]]
    evidence_limitations: list[str]
    token_budget_profile: str
    reroot_count: int
    created_at: datetime


@dataclass(slots=True, frozen=True)
class PreparedModelContext:
    developer_prompt: str
    user_context: str
    preflight_notes: list[str]
    preflight_flags: dict[str, Any]


@dataclass(slots=True, frozen=True)
class OpenAIJudgeUsage:
    input_tokens: int | None = None
    cached_input_tokens: int | None = None
    output_tokens: int | None = None
    reasoning_tokens: int | None = None
    latency_ms: int | None = None


@dataclass(slots=True, frozen=True)
class OpenAIJudgeResult:
    payload_json: dict[str, Any] | None
    refusal_text: str | None
    finish_reason: str | None
    usage: OpenAIJudgeUsage
    raw_response_id: str | None = None

    @property
    def refusal_detected(self) -> bool:
        return bool(self.refusal_text)

    @property
    def has_structured_payload(self) -> bool:
        return isinstance(self.payload_json, dict)
```

### 9-4. `src/services/judge_openai/preflight.py`

```python
from __future__ import annotations

import re
from dataclasses import dataclass


@dataclass(slots=True, frozen=True)
class PreflightResult:
    developer_prompt: str
    user_context: str
    notes: list[str]
    flags: dict[str, object]


class ModelContextPreflight:
    def apply(self, *, developer_prompt: str, user_context: str) -> PreflightResult:
        raise NotImplementedError


class NoopModelContextPreflight(ModelContextPreflight):
    def apply(self, *, developer_prompt: str, user_context: str) -> PreflightResult:
        return PreflightResult(
            developer_prompt=developer_prompt,
            user_context=user_context,
            notes=[],
            flags={},
        )


class HeuristicSanitizingPreflight(ModelContextPreflight):
    """Minimal-change preflight.

    This is intentionally sanitize-only.
    It does not quarantine, block, or reroute work.
    """

    _INSTRUCTION_RE = re.compile(
        r"(?im)^\s*(ignore previous instructions|ignore all previous instructions|system prompt|developer message|reveal your prompt|print your hidden rules).*$"
    )

    def apply(self, *, developer_prompt: str, user_context: str) -> PreflightResult:
        original = user_context
        sanitized = self._INSTRUCTION_RE.sub("[sanitized_instruction_like_line]", user_context)
        notes: list[str] = []
        flags: dict[str, object] = {}

        if sanitized != original:
            notes.append("sanitized_instruction_like_line")
            flags["prompt_guard_sanitized"] = True

        return PreflightResult(
            developer_prompt=developer_prompt,
            user_context=sanitized,
            notes=notes,
            flags=flags,
        )
```

### 9-5. `src/services/judge_openai/prompt_library.py`

```python
from __future__ import annotations


class PromptLibrary:
    def render(self, *, judge_profile: str, prompt_version: str) -> str:
        common_prefix = self._common_prefix()

        if judge_profile == "github_primary":
            return "\n\n".join(
                [
                    common_prefix,
                    f"prompt_version={prompt_version}",
                    "profile=github_primary",
                    "You are evaluating a GitHub-primary candidate.",
                    "Focus on code quality signals, maintenance signals, wrapper risk, and comparables.",
                ]
            )

        if judge_profile == "x_primary":
            return "\n\n".join(
                [
                    common_prefix,
                    f"prompt_version={prompt_version}",
                    "profile=x_primary",
                    "You are evaluating an X-post-primary candidate.",
                    "Focus on specificity, reproducibility signal, hype risk, and whether the linked artifact carries the real value.",
                ]
            )

        if judge_profile == "text_idea_primary":
            return "\n\n".join(
                [
                    common_prefix,
                    f"prompt_version={prompt_version}",
                    "profile=text_idea_primary",
                    "You are evaluating a text-idea-primary candidate.",
                    "Focus on procedural specificity, execution realism, anti-hype skepticism, and whether the idea is already common.",
                ]
            )

        raise ValueError(f"unsupported judge_profile: {judge_profile}")

    @staticmethod
    def _common_prefix() -> str:
        return "\n".join(
            [
                "You are the stage-6 judge for a precision-first GitHub/X catch-bot.",
                "You must return structured JSON only.",
                "Judge only from the provided evidence bundle.",
                "Do not browse, fetch, or assume facts outside the bundle.",
                "Negative-first: state why this may not be worth attention before positive framing.",
                "Never invent comparables or evidence.",
                "If evidence is insufficient, reflect that in evidence_strength, confidence, and limitations.",
                "Final verdict and delivery are not yours to decide; you only provide judge_output_v1 fields.",
            ]
        )
```

### 9-6. `src/services/judge_openai/context_builder.py`

```python
from __future__ import annotations

import json

from .models import BundleJudgeContext, PreparedModelContext
from .preflight import ModelContextPreflight


class JudgeContextBuilder:
    def __init__(self, *, preflight: ModelContextPreflight) -> None:
        self._preflight = preflight

    def build(
        self,
        *,
        developer_prompt: str,
        bundle: BundleJudgeContext,
    ) -> PreparedModelContext:
        user_context_payload = {
            "candidate_group_id": bundle.candidate_group_id,
            "bundle_id": bundle.bundle_id,
            "current_primary_artifact_id": bundle.current_primary_artifact_id,
            "primary_summary": bundle.primary_summary,
            "supporting_summaries": bundle.supporting_summaries_json,
            "discovered_links_summary": bundle.discovered_links_summary_json,
            "evidence_limitations": bundle.evidence_limitations,
            "token_budget_profile": bundle.token_budget_profile,
            "reroot_count": bundle.reroot_count,
        }

        user_context = json.dumps(
            user_context_payload,
            ensure_ascii=False,
            sort_keys=True,
            indent=2,
        )

        preflight_result = self._preflight.apply(
            developer_prompt=developer_prompt,
            user_context=user_context,
        )

        return PreparedModelContext(
            developer_prompt=preflight_result.developer_prompt,
            user_context=preflight_result.user_context,
            preflight_notes=preflight_result.notes,
            preflight_flags=preflight_result.flags,
        )
```

### 9-7. `src/services/judge_openai/response_mapper.py`

```python
from __future__ import annotations

import json
import time
from typing import Any

from .models import OpenAIJudgeResult, OpenAIJudgeUsage


class OpenAIResponseMapper:
    def parse(self, response: Any, *, started_monotonic: float) -> OpenAIJudgeResult:
        payload_json = None
        refusal_text = None

        output_text = getattr(response, "output_text", None)
        if isinstance(output_text, str) and output_text.strip():
            try:
                payload_json = json.loads(output_text)
            except json.JSONDecodeError:
                payload_json = None

        if payload_json is None:
            refusal_text = self._extract_refusal_text(response)

        usage = self._extract_usage(response, started_monotonic=started_monotonic)
        finish_reason = self._extract_finish_reason(response)
        raw_response_id = self._extract_response_id(response)

        return OpenAIJudgeResult(
            payload_json=payload_json,
            refusal_text=refusal_text,
            finish_reason=finish_reason,
            usage=usage,
            raw_response_id=raw_response_id,
        )

    def build_refusal_envelope(
        self,
        *,
        candidate_group_id: str,
        schema_version: str,
        refusal_text: str | None,
    ) -> dict[str, Any]:
        return {
            "judge_schema_version": schema_version,
            "candidate_group_id": candidate_group_id,
            "output_kind": "refusal",
            "refusal_text": refusal_text,
        }

    @staticmethod
    def _extract_usage(response: Any, *, started_monotonic: float) -> OpenAIJudgeUsage:
        usage = getattr(response, "usage", None)
        input_tokens = getattr(usage, "input_tokens", None) if usage is not None else None
        output_tokens = getattr(usage, "output_tokens", None) if usage is not None else None

        cached_input_tokens = None
        reasoning_tokens = None
        input_details = getattr(usage, "input_tokens_details", None) if usage is not None else None
        output_details = getattr(usage, "output_tokens_details", None) if usage is not None else None

        if input_details is not None:
            cached_input_tokens = getattr(input_details, "cached_tokens", None)
        if output_details is not None:
            reasoning_tokens = getattr(output_details, "reasoning_tokens", None)

        latency_ms = int((time.monotonic() - started_monotonic) * 1000)

        return OpenAIJudgeUsage(
            input_tokens=input_tokens,
            cached_input_tokens=cached_input_tokens,
            output_tokens=output_tokens,
            reasoning_tokens=reasoning_tokens,
            latency_ms=latency_ms,
        )

    @staticmethod
    def _extract_finish_reason(response: Any) -> str | None:
        status = getattr(response, "status", None)
        if isinstance(status, str) and status:
            return status
        incomplete_details = getattr(response, "incomplete_details", None)
        if incomplete_details is not None:
            reason = getattr(incomplete_details, "reason", None)
            if isinstance(reason, str) and reason:
                return reason
        return None

    @staticmethod
    def _extract_response_id(response: Any) -> str | None:
        value = getattr(response, "id", None)
        return str(value) if value else None

    def _extract_refusal_text(self, response: Any) -> str | None:
        output = getattr(response, "output", None)
        if not isinstance(output, list):
            return None

        texts: list[str] = []
        for item in output:
            item_type = getattr(item, "type", None) or (item.get("type") if isinstance(item, dict) else None)
            if item_type != "message":
                continue

            content = getattr(item, "content", None) or (item.get("content") if isinstance(item, dict) else None)
            if not isinstance(content, list):
                continue

            for block in content:
                block_type = getattr(block, "type", None) or (block.get("type") if isinstance(block, dict) else None)
                if block_type == "refusal":
                    text = getattr(block, "refusal", None) or (block.get("refusal") if isinstance(block, dict) else None)
                    if isinstance(text, str) and text.strip():
                        texts.append(text.strip())

        return "\n".join(texts) if texts else None
```

### 9-8. `src/services/judge_openai/openai_client.py`

```python
from __future__ import annotations

from typing import Any

from openai import AsyncOpenAI


class OpenAIJudgeClient:
    def __init__(
        self,
        *,
        api_key: str,
        project: str | None,
        timeout_sec: float,
    ) -> None:
        self._client = AsyncOpenAI(
            api_key=api_key,
            project=project,
            timeout=timeout_sec,
        )

    async def create_structured_response(
        self,
        *,
        model: str,
        reasoning_effort: str,
        developer_prompt: str,
        user_context: str,
        json_schema: dict[str, Any],
        max_output_tokens: int | None,
    ) -> Any:
        request: dict[str, Any] = {
            "model": model,
            "reasoning": {"effort": reasoning_effort},
            "input": [
                {
                    "role": "developer",
                    "content": [
                        {
                            "type": "input_text",
                            "text": developer_prompt,
                        }
                    ],
                },
                {
                    "role": "user",
                    "content": [
                        {
                            "type": "input_text",
                            "text": user_context,
                        }
                    ],
                },
            ],
            "text": {
                "format": {
                    "type": "json_schema",
                    "name": "judge_output_v1",
                    "strict": True,
                    "schema": json_schema,
                }
            },
        }

        if max_output_tokens is not None:
            request["max_output_tokens"] = max_output_tokens

        return await self._client.responses.create(**request)
```

### 9-9. `src/services/judge_openai/repositories.py`

```python
from __future__ import annotations

import json
from contextlib import asynccontextmanager
from typing import Any

import sqlalchemy as sa
from sqlalchemy.ext.asyncio import AsyncSession

from .models import BundleJudgeContext, JudgeCallJob, JudgeRunRecord, OpenAIJudgeUsage


def _jsonb_dumps(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False)


class JudgeOpenAIRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    @asynccontextmanager
    async def transaction(self):
        if self._session.in_transaction():
            yield self._session
            return
        async with self._session.begin():
            yield self._session

    async def load_job_by_trigger_event_id(self, trigger_event_id: str) -> JudgeCallJob | None:
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
        if row is None or str(row["event_type"]) != "judge.call.requested.v1":
            return None

        payload = row["payload_json"] or {}
        return JudgeCallJob(
            trigger_event_id=str(row["event_id"]),
            event_type=str(row["event_type"]),
            judge_run_id=str(payload["judge_run_id"]),
            bundle_id=str(payload["bundle_id"]),
            model=str(payload["model"]),
            reasoning_effort=str(payload["reasoning_effort"]),
            prompt_version=str(payload["prompt_version"]),
            prompt_cache_key=str(payload["prompt_cache_key"]) if payload.get("prompt_cache_key") else None,
        )

    async def load_judge_run(self, judge_run_id: str) -> JudgeRunRecord | None:
        result = await self._session.execute(
            sa.text(
                """
                SELECT judge_run_id, bundle_id, judge_profile, model, reasoning_effort,
                       prompt_version, schema_version, policy_version, prompt_cache_key,
                       status, schema_retry_count
                FROM judge_runs
                WHERE judge_run_id = CAST(:judge_run_id AS uuid)
                """
            ),
            {"judge_run_id": judge_run_id},
        )
        row = result.mappings().first()
        if row is None:
            return None
        return JudgeRunRecord(
            judge_run_id=str(row["judge_run_id"]),
            bundle_id=str(row["bundle_id"]),
            judge_profile=str(row["judge_profile"]),
            model=str(row["model"]),
            reasoning_effort=str(row["reasoning_effort"]),
            prompt_version=str(row["prompt_version"]),
            schema_version=str(row["schema_version"]),
            policy_version=str(row["policy_version"]),
            prompt_cache_key=str(row["prompt_cache_key"]) if row["prompt_cache_key"] else None,
            status=str(row["status"]),
            schema_retry_count=int(row["schema_retry_count"]),
        )

    async def load_bundle_context(self, bundle_id: str) -> BundleJudgeContext | None:
        result = await self._session.execute(
            sa.text(
                """
                SELECT bundle_id, candidate_group_id, current_primary_artifact_id,
                       primary_summary, supporting_summaries_json,
                       discovered_links_summary_json, evidence_limitations,
                       token_budget_profile, reroot_count, created_at
                FROM candidate_evidence_bundles
                WHERE bundle_id = CAST(:bundle_id AS uuid)
                """
            ),
            {"bundle_id": bundle_id},
        )
        row = result.mappings().first()
        if row is None:
            return None
        return BundleJudgeContext(
            bundle_id=str(row["bundle_id"]),
            candidate_group_id=str(row["candidate_group_id"]),
            current_primary_artifact_id=str(row["current_primary_artifact_id"]),
            primary_summary=row["primary_summary"] or {},
            supporting_summaries_json=row["supporting_summaries_json"] or [],
            discovered_links_summary_json=row["discovered_links_summary_json"] or [],
            evidence_limitations=row["evidence_limitations"] or [],
            token_budget_profile=str(row["token_budget_profile"]),
            reroot_count=int(row["reroot_count"]),
            created_at=row["created_at"],
        )

    async def mark_judge_run_running(self, judge_run_id: str) -> None:
        await self._session.execute(
            sa.text(
                """
                UPDATE judge_runs
                SET status = 'running',
                    started_at = COALESCE(started_at, now())
                WHERE judge_run_id = CAST(:judge_run_id AS uuid)
                """
            ),
            {"judge_run_id": judge_run_id},
        )

    async def increment_schema_retry_count(self, judge_run_id: str) -> None:
        await self._session.execute(
            sa.text(
                """
                UPDATE judge_runs
                SET schema_retry_count = schema_retry_count + 1
                WHERE judge_run_id = CAST(:judge_run_id AS uuid)
                """
            ),
            {"judge_run_id": judge_run_id},
        )

    async def finish_judge_run(
        self,
        *,
        judge_run_id: str,
        status: str,
        usage: OpenAIJudgeUsage | None,
        finish_reason: str | None,
        refusal_detected: bool,
    ) -> None:
        await self._session.execute(
            sa.text(
                """
                UPDATE judge_runs
                SET
                    status = :status,
                    input_tokens = :input_tokens,
                    cached_input_tokens = :cached_input_tokens,
                    output_tokens = :output_tokens,
                    reasoning_tokens = :reasoning_tokens,
                    latency_ms = :latency_ms,
                    finish_reason = :finish_reason,
                    refusal_detected = :refusal_detected,
                    finished_at = now()
                WHERE judge_run_id = CAST(:judge_run_id AS uuid)
                """
            ),
            {
                "judge_run_id": judge_run_id,
                "status": status,
                "input_tokens": usage.input_tokens if usage else None,
                "cached_input_tokens": usage.cached_input_tokens if usage else None,
                "output_tokens": usage.output_tokens if usage else None,
                "reasoning_tokens": usage.reasoning_tokens if usage else None,
                "latency_ms": usage.latency_ms if usage else None,
                "finish_reason": finish_reason,
                "refusal_detected": refusal_detected,
            },
        )

    async def insert_judge_output(
        self,
        *,
        judge_run_id: str,
        candidate_group_id: str,
        judge_schema_version: str,
        payload_json: dict[str, Any],
        model_proposed_verdict: str | None,
        model_confidence_band: str | None,
    ) -> str:
        result = await self._session.execute(
            sa.text(
                """
                INSERT INTO judge_outputs (
                    judge_output_id,
                    judge_run_id,
                    candidate_group_id,
                    judge_schema_version,
                    payload_json,
                    model_proposed_verdict,
                    model_confidence_band,
                    created_at
                ) VALUES (
                    gen_random_uuid(),
                    CAST(:judge_run_id AS uuid),
                    CAST(:candidate_group_id AS uuid),
                    :judge_schema_version,
                    CAST(:payload_json AS jsonb),
                    :model_proposed_verdict,
                    :model_confidence_band,
                    now()
                )
                RETURNING judge_output_id
                """
            ),
            {
                "judge_run_id": judge_run_id,
                "candidate_group_id": candidate_group_id,
                "judge_schema_version": judge_schema_version,
                "payload_json": _jsonb_dumps(payload_json),
                "model_proposed_verdict": model_proposed_verdict,
                "model_confidence_band": model_confidence_band,
            },
        )
        return str(result.scalar_one())

    async def insert_judge_output_ready_outbox(
        self,
        *,
        judge_run_id: str,
        judge_output_id: str,
        finish_reason: str | None,
        refusal_detected: bool,
    ) -> None:
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
                    'judge.output.ready.v1',
                    'judge_run',
                    CAST(:judge_run_id AS uuid),
                    :dedupe_key,
                    CAST(:payload_json AS jsonb),
                    'pending'::outbox_status_enum,
                    now()
                )
                ON CONFLICT (dedupe_key) DO NOTHING
                """
            ),
            {
                "judge_run_id": judge_run_id,
                "dedupe_key": f"judge:output_ready:{judge_run_id}:{judge_output_id}",
                "payload_json": _jsonb_dumps(
                    {
                        "judge_run_id": judge_run_id,
                        "judge_output_id": judge_output_id,
                        "finish_reason": finish_reason,
                        "refusal_detected": refusal_detected,
                    }
                ),
            },
        )
```

### 9-10. `src/services/judge_openai/service.py`

```python
from __future__ import annotations

import logging
import time

from .context_builder import JudgeContextBuilder
from .models import JudgeCallJob, OpenAIJudgeResult
from .openai_client import OpenAIJudgeClient
from .preflight import HeuristicSanitizingPreflight, NoopModelContextPreflight
from .prompt_library import PromptLibrary
from .repositories import JudgeOpenAIRepository
from .response_mapper import OpenAIResponseMapper


class JudgeOpenAIService:
    def __init__(
        self,
        config,
        *,
        repository: JudgeOpenAIRepository,
        openai_client: OpenAIJudgeClient,
        prompt_library: PromptLibrary,
        response_mapper: OpenAIResponseMapper,
        logger: logging.Logger | None = None,
    ) -> None:
        self._config = config
        self._repository = repository
        self._openai_client = openai_client
        self._prompt_library = prompt_library
        self._response_mapper = response_mapper
        self._logger = logger or logging.getLogger(__name__)

        preflight = (
            HeuristicSanitizingPreflight()
            if config.enable_prompt_guard_preflight
            else NoopModelContextPreflight()
        )
        self._context_builder = JudgeContextBuilder(preflight=preflight)

    async def rehydrate_job(self, trigger_event_id: str) -> JudgeCallJob | None:
        return await self._repository.load_job_by_trigger_event_id(trigger_event_id)

    async def handle_job(self, job: JudgeCallJob) -> None:
        judge_run = await self._repository.load_judge_run(job.judge_run_id)
        if judge_run is None:
            return
        if judge_run.status != "pending":
            return
        if judge_run.bundle_id != job.bundle_id:
            return

        bundle = await self._repository.load_bundle_context(judge_run.bundle_id)
        if bundle is None:
            async with self._repository.transaction():
                await self._repository.finish_judge_run(
                    judge_run_id=judge_run.judge_run_id,
                    status="failed_terminal",
                    usage=None,
                    finish_reason="bundle_missing",
                    refusal_detected=False,
                )
            return

        developer_prompt = self._prompt_library.render(
            judge_profile=judge_run.judge_profile,
            prompt_version=judge_run.prompt_version,
        )
        prepared = self._context_builder.build(
            developer_prompt=developer_prompt,
            bundle=bundle,
        )

        async with self._repository.transaction():
            await self._repository.mark_judge_run_running(judge_run.judge_run_id)

        call_result = await self._call_with_single_schema_retry(
            judge_run=judge_run,
            prepared=prepared,
        )

        if call_result is None:
            async with self._repository.transaction():
                await self._repository.finish_judge_run(
                    judge_run_id=judge_run.judge_run_id,
                    status="failed_terminal",
                    usage=None,
                    finish_reason="schema_invalid_after_retry",
                    refusal_detected=False,
                )
            return

        payload_json = call_result.payload_json
        if payload_json is None:
            payload_json = self._response_mapper.build_refusal_envelope(
                candidate_group_id=bundle.candidate_group_id,
                schema_version=judge_run.schema_version,
                refusal_text=call_result.refusal_text,
            )

        model_proposed_verdict = (
            payload_json.get("model_proposed_verdict")
            if isinstance(payload_json, dict)
            else None
        )
        model_confidence_band = (
            payload_json.get("model_confidence_band")
            if isinstance(payload_json, dict)
            else None
        )

        async with self._repository.transaction():
            judge_output_id = await self._repository.insert_judge_output(
                judge_run_id=judge_run.judge_run_id,
                candidate_group_id=bundle.candidate_group_id,
                judge_schema_version=judge_run.schema_version,
                payload_json=payload_json,
                model_proposed_verdict=model_proposed_verdict if isinstance(model_proposed_verdict, str) else None,
                model_confidence_band=model_confidence_band if isinstance(model_confidence_band, str) else None,
            )
            await self._repository.finish_judge_run(
                judge_run_id=judge_run.judge_run_id,
                status="succeeded",
                usage=call_result.usage,
                finish_reason=call_result.finish_reason,
                refusal_detected=call_result.refusal_detected,
            )
            await self._repository.insert_judge_output_ready_outbox(
                judge_run_id=judge_run.judge_run_id,
                judge_output_id=judge_output_id,
                finish_reason=call_result.finish_reason,
                refusal_detected=call_result.refusal_detected,
            )

    async def _call_with_single_schema_retry(self, *, judge_run, prepared) -> OpenAIJudgeResult | None:
        first = await self._call_once(judge_run=judge_run, prepared=prepared)
        if first.has_structured_payload or first.refusal_detected:
            return first

        async with self._repository.transaction():
            await self._repository.increment_schema_retry_count(judge_run.judge_run_id)

        second = await self._call_once(judge_run=judge_run, prepared=prepared)
        if second.has_structured_payload or second.refusal_detected:
            return second
        return None

    async def _call_once(self, *, judge_run, prepared) -> OpenAIJudgeResult:
        started = time.monotonic()
        response = await self._openai_client.create_structured_response(
            model=judge_run.model,
            reasoning_effort=judge_run.reasoning_effort,
            developer_prompt=prepared.developer_prompt,
            user_context=prepared.user_context,
            json_schema=self._judge_output_schema(),
            max_output_tokens=self._config.max_output_tokens,
        )
        return self._response_mapper.parse(response, started_monotonic=started)

    @staticmethod
    def _judge_output_schema() -> dict:
        return {
            "type": "object",
            "additionalProperties": False,
            "required": [
                "judge_schema_version",
                "candidate_group_id",
                "headline",
                "summary_one_line_ko",
                "skeptical_take_ko",
                "why_it_might_matter_ko",
                "comparables",
                "scores",
                "reason_codes",
                "red_flags_ko",
                "evidence_limitations_ko",
                "recommended_action_ko",
                "freshness_note_ko",
                "model_proposed_verdict",
                "model_confidence_band",
            ],
            "properties": {
                "judge_schema_version": {"type": "string"},
                "candidate_group_id": {"type": "string"},
                "headline": {"type": "string"},
                "summary_one_line_ko": {"type": "string"},
                "skeptical_take_ko": {"type": "string"},
                "why_it_might_matter_ko": {"type": "string"},
                "comparables": {
                    "type": "array",
                    "items": {"type": "string"},
                },
                "scores": {
                    "type": "object",
                    "additionalProperties": True,
                },
                "reason_codes": {
                    "type": "array",
                    "items": {"type": "string"},
                },
                "red_flags_ko": {
                    "type": "array",
                    "items": {"type": "string"},
                },
                "evidence_limitations_ko": {
                    "type": "array",
                    "items": {"type": "string"},
                },
                "recommended_action_ko": {"type": "string"},
                "freshness_note_ko": {"type": "string"},
                "model_proposed_verdict": {
                    "type": ["string", "null"],
                    "enum": ["inspect_now", "later", "skip", None],
                },
                "model_confidence_band": {
                    "type": ["string", "null"],
                    "enum": ["low", "medium", "high", None],
                },
            },
        }
```

### 9-11. `src/services/judge_openai/worker.py`

```python
from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass

from .config import JudgeOpenAIConfig
from .service import JudgeOpenAIService


@dataclass(slots=True, frozen=True)
class StreamMessage:
    stream: str
    message_id: str
    fields: dict[str, str]


class RedisStreamConsumerProtocol:
    async def ensure_group(self) -> None: ...
    async def read_batch(self) -> list[StreamMessage]: ...
    async def ack(self, message_id: str) -> None: ...


@dataclass(slots=True, frozen=True)
class WorkerBatchResult:
    processed: int = 0
    acked: int = 0


class JudgeOpenAIWorker:
    def __init__(
        self,
        config: JudgeOpenAIConfig,
        *,
        consumer: RedisStreamConsumerProtocol,
        service: JudgeOpenAIService,
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
            "judge_openai_worker_started",
            extra={
                "service": "judge-openai",
                "event": "judge_openai_worker_started",
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
        for message in messages:
            processed += 1
            await self._process_message(message)
            await self._consumer.ack(message.message_id)
            acked += 1
        return WorkerBatchResult(processed=processed, acked=acked)

    async def _process_message(self, message: StreamMessage) -> None:
        trigger_event_id = message.fields.get("trigger_event_id")
        if not trigger_event_id:
            self._logger.error(
                "judge_openai_stream_missing_trigger_event_id",
                extra={
                    "service": "judge-openai",
                    "event": "judge_openai_stream_missing_trigger_event_id",
                    "stream_message_id": message.message_id,
                },
            )
            return

        job = await self._service.rehydrate_job(trigger_event_id)
        if job is None:
            self._logger.warning(
                "judge_openai_missing_outbox_job",
                extra={
                    "service": "judge-openai",
                    "event": "judge_openai_missing_outbox_job",
                    "trigger_event_id": trigger_event_id,
                },
            )
            return

        await self._service.handle_job(job)
```

### 9-12. `src/services/judge_openai/main.py`

```python
from __future__ import annotations

import asyncio
import logging
import sys

from redis.asyncio import Redis
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from src.services.gh_enricher.redis_streams import RedisStreamConsumer  # reuse existing stream helper

from .config import JudgeOpenAIConfig
from .openai_client import OpenAIJudgeClient
from .prompt_library import PromptLibrary
from .repositories import JudgeOpenAIRepository
from .response_mapper import OpenAIResponseMapper
from .service import JudgeOpenAIService
from .worker import JudgeOpenAIWorker


def _configure_logging(log_level: str) -> None:
    logging.basicConfig(
        level=getattr(logging, log_level, logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
        stream=sys.stdout,
    )


async def _run() -> int:
    config = JudgeOpenAIConfig.from_env()
    _configure_logging(config.log_level)
    logger = logging.getLogger("judge_openai")

    engine = create_async_engine(config.database_url, future=True)
    session_factory = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    redis_client = Redis.from_url(config.redis_url, decode_responses=True)

    try:
        async with session_factory() as session:
            repository = JudgeOpenAIRepository(session)
            openai_client = OpenAIJudgeClient(
                api_key=config.openai_api_key,
                project=config.openai_project,
                timeout_sec=config.request_timeout_sec,
            )
            service = JudgeOpenAIService(
                config,
                repository=repository,
                openai_client=openai_client,
                prompt_library=PromptLibrary(),
                response_mapper=OpenAIResponseMapper(),
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
            worker = JudgeOpenAIWorker(
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

---

## 10. 테스트 초안 포인트

### `tests/unit/services/judge_openai/test_prompt_library.py`

검증:

- `github_primary` / `x_primary` / `text_idea_primary` 각각에서 prompt render가 되는지
- unsupported profile이면 예외가 나는지

### `tests/unit/services/judge_openai/test_context_builder.py`

검증:

- bundle row만으로 user context가 만들어지는지
- raw snapshot / raw source dependency가 없는지

### `tests/unit/services/judge_openai/test_response_mapper_success.py`

검증:

- `response.output_text` 가 valid JSON일 때 structured payload를 얻는지
- usage/latency가 채워지는지

### `tests/unit/services/judge_openai/test_response_mapper_refusal.py`

검증:

- structured payload가 비어 있어도 refusal text를 추출할 수 있는지
- refusal envelope 변환이 되는지

### `tests/unit/services/judge_openai/test_preflight_sanitize_only.py`

검증:

- obvious instruction-like line이 sanitize 되는지
- block/quarantine 없이 text만 바뀌는지

### `tests/component/services/judge_openai/test_worker_rehydrates_judge_call.py`

검증:

- Redis payload는 thin message
- `event_outbox` 기준으로 `judge.call.requested.v1` rehydrate

### `tests/component/services/judge_openai/test_success_writes_judge_output_and_outbox.py`

검증:

- structured output 성공
- `judge_outputs` insert
- `judge_runs.status = succeeded`
- `judge.output.ready.v1` outbox insert

### `tests/component/services/judge_openai/test_refusal_writes_envelope_and_outbox.py`

검증:

- refusal 발생
- refusal envelope `judge_outputs` insert
- `judge_runs.refusal_detected = true`
- `judge.output.ready.v1` emit

### `tests/component/services/judge_openai/test_schema_retry_once_then_fail.py`

검증:

- first parse 실패 → retry count 증가
- second parse도 실패 → `failed_terminal`
- `judge_output` / `judge.output.ready.v1` 없음

### `tests/component/services/judge_openai/test_transport_failure_marks_retryable.py`

검증:

- OpenAI transport 예외
- `judge_runs.status = failed_retryable`
- outbox emit 없음

---

## 11. 이번 단계가 구조를 지키는 이유

1. `judge-openai`는 `judge_runs`, `judge_outputs`, `event_outbox`만 직접 쓴다.  
   즉, service ownership을 넘지 않는다.

2. OpenAI 호출 입력을 **bundle row로만 제한** 한다.  
   즉, stage 5/6 책임 분리가 유지된다.

3. refusal은 validator가 볼 수 있게 envelope로 넘기고,  
   transport/schema 최종 실패는 judge-openai 내부 실패로 남긴다.  
   즉, current contracts를 최소 변경으로 연결한다.

4. Prompt Guard는 **sanitize-only optional hook** 으로만 둔다.  
   즉, application plan의 방향은 반영하되 current lifecycle은 흔들지 않는다.

5. `judge.output.ready.v1`까지만 emit한다.  
   즉, `analysis-validator` / `policy-engine` / `notifier-telegram` 경계를 침범하지 않는다.

---

## 12. 다음 단계

이 단계가 닫히면 다음 구현 순서는 그대로 아래다.

1. `36_analysis_validator_skeleton_and_code_draft_v0_1.md`
2. `37_policy_engine_skeleton_and_code_draft_v0_1.md`
3. `38_notifier_telegram_skeleton_and_code_draft_v0_1.md`

즉, 이제 stage 6 judge pipeline의 OpenAI 호출 경계를 닫았고,  
다음은 **LLM 출력 방화벽인 `analysis-validator`** 를 붙이는 것이 맞다.

---

## 최종 한 줄 결론

이번 단계의 최소-change 정답은 **`judge.call.requested.v1`를 rehydrate해 pending `judge_run`과 current bundle을 재검증하고, bundle row만으로 model context를 구성해 OpenAI Responses API + strict structured outputs를 호출하고, schema retry 1회 / refusal envelope / usage telemetry를 `judge_runs`와 `judge_outputs`에 기록한 뒤, `judge.output.ready.v1`로 validator에 넘기는 `judge-openai` v0.1을 닫는 것**이다.


---

## Source file: `36_analysis_validator_skeleton_and_code_draft_v0_1.md`

# 36단계: `analysis-validator` 스켈레톤 + 실제 코드 초안 v0.1

## 0. 문서 목적

이 문서는 이미 잠긴 구조를 다시 설계하는 문서가 아니다.  
목적은 **35단계 `judge-openai`가 만들어낸 `judge.output.ready.v1`를 받아, `judge_output_v1`을 정책 적용 직전의 안전한 입력으로 거르는 좁은 경계**를 구현 가능한 수준으로 내리는 것이다.

이번 단계에서 고정하는 것은 아래 일곱 가지다.

1. `judge.output.ready.v1` 소비 경계를 코드로 고정
2. thin Redis payload → `event_outbox` 재조회 → `judge_runs` / `judge_outputs` / `candidate_evidence_bundles` 재hydrate 경계를 고정
3. `analysis-validator`의 좁은 책임을 **schema 검증 / enum·길이·nullability 검증 / semantic validation / refusal 분기 / state transition 기록 / `analysis.policy.apply.v1` emit** 으로 고정
4. `judge_runs`, `state_transitions`, `event_outbox`만 직접 쓰는 **service ownership** 을 고정
5. `judge-openai`에서 이미 닫힌 **schema retry 1회 / refusal envelope / transport failure 분리** 를 그대로 이어받아, validator가 그 다음 분기만 담당하도록 고정
6. `03_GitHub_AI_application_plan.md`의 적용 아이디어를 검토하되, **Prompt Guard / AgentLinter / MemKraft / skill discipline을 runtime hot path에 새 lifecycle로 삽입하지 않는 최소-change 해석** 을 유지
7. 다음 단계인 `policy-engine`이 바로 붙을 수 있게 **`analysis.policy.apply.v1` handoff** 를 안정화

핵심 전제는 유지한다.

- `analysis-validator`는 **LLM 호출기**가 아니다.
- `analysis-validator`는 **policy-engine** 이 아니다.
- `analysis-validator`는 **notifier** 가 아니다.
- `analysis-validator`는 **최종 verdict / delivery decision** 을 계산하지 않는다.
- `analysis-validator`는 **LLM 출력 방화벽(LLM output firewall)** 이다.

---

## 1. 왜 지금 이 단계가 정확한 다음 단계인가

현재 authoritative README는 최신 진행 상태를 **35단계 `judge-openai`까지 완료**로 보고, 다음 구현 순서를 **`analysis-validator` → `policy-engine` → `notifier-telegram`** 으로 고정한다.  
또한 35단계 문서는 `judge-openai`가 `judge.output.ready.v1`까지만 emit하고, 그 다음 단계로 **validator가 refusal / structured output / telemetry handoff를 이어받아야 한다**고 잠갔다.

즉, 지금 다시 collector / normalizer / enricher / assembler / router / judge-openai를 여는 것은 순서상 후퇴다.  
이제 붙여야 하는 것은 **`judge_output_v1`을 정책 엔진 앞에서 걸러내는 `analysis-validator`** 다.

---

## 2. 이번 단계에서 확인한 충돌과 최소-change 해석

### 충돌 A — 현재 소스에는 README v6 / v7 / v8 / v9가 함께 존재할 수 있다

현재 소스는 이전 README 중간본들을 함께 포함할 수 있다. 최신 authoritative README는 이미 아래를 고정했다.

- latest = `35_judge_openai_skeleton_and_code_draft_v0_1.md`
- next = `36_analysis_validator_skeleton_and_code_draft_v0_1.md`
- `README_replacement_consolidated_v0_9.md`만 authoritative

### 최소-change 해석 A

- **v9만 phase authority** 로 사용한다.
- v6 / v7 / v8은 이력성 중간본으로 본다.
- 이번 36단계 문서와 README 업데이트에서는 **v9를 이어받아 v10으로만 승격** 한다.

즉, phase ordering은 최신 README 하나로 수렴시키고,  
오래된 README는 더 이상 authority로 쓰지 않는다.

---

### 충돌 B — 6단계 정본은 validator가 “필수 필드 보정 가능 여부 판정”까지 맡기지만, execution contracts는 validator의 직접 소유 테이블을 `judge_runs`, `state_transitions`, `event_outbox`로만 잠갔다

6단계 정본은 validator가 아래를 하라고 요구한다.

- JSON Schema 검증
- enum / 길이 / 비어있음 검증
- semantic validation
- policy reconciliation
- refusal / truncation / schema failure 처리

하지만 execution contracts의 service ownership은 `analysis-validator`가 직접 쓰는 durable 테이블을 아래로만 고정한다.

- `judge_runs`
- `state_transitions`
- `event_outbox`

즉, validator가 `judge_outputs`를 “고쳐서 다시 쓰는” 방식으로 들어가면 현재 ownership을 침범한다.

### 최소-change 해석 B

이번 v0.1에서는 아래처럼 고정한다.

1. validator는 **`judge_outputs`를 수정하지 않는다.**
2. validator는 **보정 가능한지 판단은 하되, 실제 durable mutation은 하지 않는다.**
3. 보정이 필요할 정도로 출력이 어긋나면:
   - `judge_runs.status`를 `failed_terminal` 또는 `failed_retryable`로 전환
   - `state_transitions`로 이유를 남김
   - downstream emit을 중단
4. 유효한 출력만 `analysis.policy.apply.v1`로 넘긴다.

즉, validator는 **append-only judge output 위에서 pass/fail을 판정하는 방화벽** 이고,  
**stored judge_output을 patch하는 repair service가 아니다.**

---

### 충돌 C — 35단계는 refusal을 `judge.output.ready.v1` 안의 refusal envelope로 넘기지만, 별도 refusal 이벤트는 없다

35단계는 아래를 잠갔다.

- structured output 성공 → `judge_outputs` 생성 + `judge.output.ready.v1`
- refusal → refusal envelope를 `judge_outputs.payload_json`에 저장 + `judge.output.ready.v1`
- transport failure / retry 초과 / final schema failure → `judge_output` 없음 + `judge.output.ready.v1` 없음

즉, validator는 **같은 입력 이벤트 안에서 “정상 structured output”과 “refusal envelope”를 구분** 해야 한다.

### 최소-change 해석 C

이번 v0.1에서는 아래처럼 고정한다.

1. `refusal_detected=true` 이거나 `payload_json.output_kind == refusal`
   - `analysis.policy.apply.v1`를 emit하지 않는다.
   - `state_transitions`에 `analysis_refused` 를 남긴다.
   - 사용자 전달은 기본 금지 상태로 둔다.
   - `judge_run.status`는 OpenAI 호출 성공 의미를 보존하기 위해 `succeeded`를 그대로 둔다.

2. structured payload
   - schema/business validation을 통과하면 `analysis.policy.apply.v1` emit
   - 통과하지 못하면 validator failure로 종료

즉, refusal은 **호출 실패가 아니라 모델 결과의 한 유형** 으로 보고,  
policy-engine으로 넘기지 않고 validator에서 terminal stop 한다.

---

### 충돌 D — truncation / incomplete 출력에 대해 stage 6은 retry를 말하지만, 현재 contracts에는 “bundle 축약 후 재judge” 자동 경로가 없다

6단계 정본은 truncation에 대해 아래를 말한다.

- output 길이 초과
- 1회만 재시도
- 필요 시 bundle 축약 또는 output budget 축소 검토

그러나 현재 35단계 judge-openai는 **schema retry 1회** 까지만 닫았고,  
현재 contracts에는 validator가 자동으로 “축약 bundle 재judge”를 만드는 공식 경로가 없다.

### 최소-change 해석 D

이번 v0.1에서는 아래처럼 고정한다.

1. judge-openai가 이미 final schema failure면 validator 이벤트 자체가 오지 않는다.
2. validator 이벤트가 왔는데도 `finish_reason`이 truncation/incomplete 성격이고 payload가 불안정하면:
   - `judge_runs.status = failed_retryable`
   - `state_transitions`에 `analysis_failed_truncation` 기록
   - `analysis.policy.apply.v1`는 emit하지 않음
3. 자동 bundle shrink / rejudge는 **지금 넣지 않는다.**
   - 그건 replay / maintenance / future bundle-reduction path가 생길 때 붙인다.

즉, 현재 단계는 **운영 가능하고 설명 가능한 stop state** 까지만 닫고,  
자동 축약 재시도는 후속 개선으로 미룬다.

---

### 충돌 E — application plan은 Prompt Guard / AgentLinter / MemKraft / skill discipline을 권장하지만, 36단계 runtime에 바로 집어넣으면 service ownership이 흔들린다

application plan의 방향은 맞다.

- Prompt Guard: judge 직전
- AgentLinter: repo hygiene / docs / prompts / policies
- MemKraft: ops-memory sidecar
- skill/playbook discipline: prompt handbook

하지만 이걸 36단계 runtime에 넣으면 아래 문제가 생긴다.

- `prompt_risk_level`, `requires_quarantine` 같은 durable lifecycle이 아직 없음
- validator가 policy-engine보다 먼저 suppress/quarantine를 결정하게 됨
- AgentLinter / MemKraft가 runtime hot path에 들어오며 책임이 섞임

### 최소-change 해석 E

이번 36단계에는 아래만 유지한다.

- **Prompt Guard**: 35단계 sanitize-only preflight를 그대로 전제만 한다. validator는 prompt-risk 새 lifecycle을 만들지 않는다.
- **AgentLinter**: `AGENTS.md`, `prompts/`, `policies/`, `README` 정리 단계에서만 적용
- **MemKraft**: `ops-memory/` sidecar로만 적용
- **skill/playbook discipline**: prompt/profile handbook으로만 적용

즉, application plan은 **runtime이 아니라 repo discipline / ops sidecar 방향으로만 계속 흡수** 한다.

---

## 3. `analysis-validator`의 책임과 비책임

### 3-1. 반드시 하는 일

- `judge.output.ready.v1` 소비
- `event_outbox` 기준 request rehydrate
- `judge_runs` / `judge_outputs` / `candidate_evidence_bundles` / 필요 시 `candidate_group_proposals` 재조회
- refusal envelope vs structured payload 구분
- JSON Schema 검증
- enum / 길이 / nullability / required field 검증
- semantic/business validation
- validator outcome에 따른 `judge_runs` status 조정
- `state_transitions` append
- 유효한 output만 `analysis.policy.apply.v1` emit

### 3-2. 하면 안 되는 일

- `judge_outputs` row 수정
- OpenAI 재호출
- final verdict / delivery decision 계산
- 알림 렌더링/전송
- raw source rescan
- GitHub/X/web fetch
- candidate reroot 재계산
- Prompt Guard 기반 hard quarantine/suppress 결정

즉, 이 서비스는 **LLM 출력 방화벽 + policy handoff gate** 다.

---

## 4. 직접 소유하는 durable 경계

execution contracts 기준으로 `analysis-validator`는 아래만 직접 쓴다.

- `judge_runs`
- `state_transitions`
- `event_outbox`

읽는 것:

- `judge_outputs`
- `candidate_evidence_bundles`
- 필요 시 `candidate_group_proposals`
- 필요 시 `artifact_registry` (primary artifact type 확인 수준)

즉, `analyses`, `notification_*`, `judge_outputs` mutation은 하지 않는다.

---

## 5. 입력/출력 계약

### 5-1. 입력 이벤트

허용 입력은 아래 하나로 좁게 고정한다.

- `judge.output.ready.v1`

Redis Streams 메시지는 여전히 thin payload다.

```json
{
  "job_id": "<event_id>",
  "stage_name": "analysis_validate",
  "root_object_type": "judge_run",
  "root_object_id": "<judge_run_id>",
  "idempotency_key": "<dedupe_key>",
  "pipeline_run_id": "",
  "not_before": "",
  "trigger_event_id": "<event_id>"
}
```

즉, consumer는 Redis 본문을 business source처럼 쓰지 않고,  
반드시 `trigger_event_id`로 `event_outbox`를 다시 조회한다.

### 5-2. 입력 payload 최소 필드

`judge.output.ready.v1` payload는 아래를 믿는다.

- `judge_run_id`
- `judge_output_id`
- `finish_reason`
- `refusal_detected`

하지만 이 값들은 **request hint** 일 뿐이다.  
최종 판정은 여전히 `judge_runs` / `judge_outputs` / `bundle` 재조회로 재검증한다.

### 5-3. 출력 이벤트

허용 출력은 아래 하나로 좁게 고정한다.

- `analysis.policy.apply.v1`

payload 최소 필드:

- `judge_run_id`
- `judge_output_id`
- `candidate_group_id`
- `bundle_id`

즉, validator는 여기서 멈추고  
다음 단계 `policy-engine`가 `analysis_v1`을 확정한다.

---

## 6. 이번 단계에서 고정할 핵심 처리 규칙

### 6-1. missing / mismatched run-output 조합은 terminal failure

아래면 validator는 downstream으로 보내면 안 된다.

- `judge_run` 없음
- `judge_output` 없음
- `judge_output.judge_run_id != payload.judge_run_id`
- `judge_run.bundle_id`에 해당하는 bundle row 없음
- `bundle.candidate_group_id != judge_output.candidate_group_id`

이 경우:

- `judge_runs.status = failed_terminal`
- `state_transitions`에 failure 기록
- `analysis.policy.apply.v1` emit 없음

즉, validator는 **judge-openai가 만든 durable 관계가 깨졌는지** 먼저 본다.

### 6-2. refusal envelope는 policy-engine으로 넘기지 않는다

아래 둘 중 하나면 refusal 경로다.

- event payload `refusal_detected = true`
- `judge_outputs.payload_json.output_kind == "refusal"`

이 경우:

- `judge_runs.status`는 `succeeded` 유지
- `state_transitions`에 `to_state = analysis_refused`, `reason_code = model_refusal`
- `analysis.policy.apply.v1` emit 없음

즉, refusal은 **LLM 호출 성공 + 정책 적용 비대상** 이다.

### 6-3. validator는 schema 검사만 하지 않는다

권장 순서는 아래다.

1. JSON Schema 검증
2. enum 검증
3. 길이 / 개수 검증
4. nullability / required field 검증
5. semantic validation
6. storage eligibility / forward 여부 판정

즉, validator는 **JSON parser가 아니라 LLM output firewall** 이다.

### 6-4. semantic validation은 “정책 전체 재계산”이 아니라 “명백한 자기모순 차단”까지만 한다

validator가 policy-engine 전체를 복제하면 경계가 흐려진다.  
따라서 이번 단계의 semantic rule은 **명백한 모순 차단** 으로만 제한한다.

권장 규칙:

- `skeptical_take_ko` 비어 있으면 invalid
- `reason_codes` 빈 배열이면 invalid
- `scores` 내부 0~100 범위 위반이면 invalid
- `model_proposed_verdict = inspect_now` 인데 `evidence_strength < 50` 이면 invalid
- primary가 GitHub 계열인데 `comparables`가 비어 있으면 invalid
- `model_confidence_band`는 `low|medium|high|null` 만 허용

즉, validator는 **정책을 확정하지 않고, 정책 적용 전에 obvious contradiction만 차단** 한다.

### 6-5. truncation / incomplete 출력은 retryable stop으로만 남긴다

`finish_reason`이 아래 계열이면 자동 policy handoff를 하지 않는다.

- `incomplete`
- `max_output_tokens`
- 기타 truncation 계열 reason

이 경우:

- `judge_runs.status = failed_retryable`
- `state_transitions`에 `to_state = analysis_failed_truncation`
- `analysis.policy.apply.v1` emit 없음

즉, 현재 단계에서는 **자동 rejudge가 아니라 retryable stop state** 까지만 닫는다.

### 6-6. valid structured payload만 policy-engine으로 넘긴다

아래를 모두 만족하면 downstream으로 넘긴다.

- refusal 아님
- schema valid
- semantic valid
- candidate/bundle identity 정합성 만족

그때만:

- `state_transitions`에 `to_state = analysis_validated`
- `analysis.policy.apply.v1` emit

즉, policy-engine은 **검증된 `judge_output_v1`만** 보게 된다.

---

## 7. application plan 적용 검토 — 이번 단계 결론

### 7-1. 지금 바로 runtime에 적용하는 것

없다.  
정확히 말하면 **새로운 runtime component로는 없다.**

이유:

- Prompt Guard hard block/quarantine는 contracts 밖 새 lifecycle을 요구한다.
- AgentLinter는 CI/repo hygiene 자산이다.
- MemKraft는 ops-memory sidecar다.
- skill/playbook discipline은 prompt asset 정리 자산이다.

### 7-2. 지금 적용하면 구조를 흔드는 것

#### A. validator가 prompt-risk를 근거로 suppress/quarantine를 결정하는 것
문제:
- policy-engine 책임을 선점한다.
- 현재 durable schema에 prompt-risk lifecycle이 없다.

#### B. validator가 judge_output를 patch해서 “수정된 정답”을 만드는 것
문제:
- `judge-openai` append-only 경계를 침범한다.
- execution contracts의 service ownership과 충돌한다.

#### C. MemKraft/AgentLinter를 worker hot path에 직접 연결하는 것
문제:
- runtime과 repo/ops discipline이 섞인다.

### 7-3. 최소-change 결론

- **이번 36단계에는 application plan을 runtime hot path에 새 책임으로 넣지 않는다.**
- 대신 아래를 future-compatible decision으로만 남긴다.
  - Prompt Guard: judge-openai sanitize-only preflight 유지
  - AgentLinter: `AGENTS.md` / `prompts/` / `policies/` 정리 단계에서 적용
  - MemKraft: `ops-memory/` sidecar로만 적용
  - skill/playbook discipline: profile handbook 문서화에만 적용

즉, application plan은 **repo hardening과 운영 기억 측면으로만 계속 흡수** 한다.

---

## 8. 대상 파일 트리

```text
src/services/analysis_validator/
  __init__.py
  config.py
  models.py
  schema_registry.py
  business_rules.py
  repositories.py
  service.py
  worker.py
  main.py

tests/
  unit/
    services/
      analysis_validator/
        test_schema_registry.py
        test_business_rules.py
        test_refusal_branch.py
        test_invalid_inspect_now_contradiction.py
  component/
    services/
      analysis_validator/
        test_worker_rehydrates_judge_output_ready.py
        test_valid_output_emits_policy_apply.py
        test_refusal_records_state_transition_only.py
        test_missing_judge_output_marks_terminal.py
        test_truncation_marks_retryable_without_emit.py
```

---

## 9. 코드 초안

### 9-1. `src/services/analysis_validator/__init__.py`

```python
from .config import AnalysisValidatorConfig
from .service import AnalysisValidatorService
from .worker import AnalysisValidatorWorker

__all__ = [
    "AnalysisValidatorConfig",
    "AnalysisValidatorService",
    "AnalysisValidatorWorker",
]
```

### 9-2. `src/services/analysis_validator/config.py`

```python
from __future__ import annotations

import os
from dataclasses import dataclass


class AnalysisValidatorConfigurationError(ValueError):
    pass


@dataclass(slots=True, frozen=True)
class AnalysisValidatorConfig:
    app_env: str
    database_url: str
    redis_url: str

    queue_name: str
    consumer_group: str
    consumer_name: str
    batch_size: int
    block_ms: int

    max_headline_chars: int
    max_summary_chars: int
    max_text_items: int
    log_level: str

    @classmethod
    def from_env(cls) -> "AnalysisValidatorConfig":
        def _read(name: str, default: str = "") -> str:
            return os.getenv(name, default).strip()

        cfg = cls(
            app_env=_read("APP_ENV", "dev").lower(),
            database_url=_read("DATABASE_URL"),
            redis_url=_read("REDIS_URL"),
            queue_name=_read("ANALYSIS_VALIDATOR_QUEUE_NAME", "q.analysis.validate"),
            consumer_group=_read("ANALYSIS_VALIDATOR_CONSUMER_GROUP", "analysis-validator"),
            consumer_name=_read("ANALYSIS_VALIDATOR_CONSUMER_NAME", "analysis-validator-1"),
            batch_size=int(_read("ANALYSIS_VALIDATOR_BATCH_SIZE", "20")),
            block_ms=int(_read("ANALYSIS_VALIDATOR_BLOCK_MS", "5000")),
            max_headline_chars=int(_read("ANALYSIS_VALIDATOR_MAX_HEADLINE_CHARS", "200")),
            max_summary_chars=int(_read("ANALYSIS_VALIDATOR_MAX_SUMMARY_CHARS", "1200")),
            max_text_items=int(_read("ANALYSIS_VALIDATOR_MAX_TEXT_ITEMS", "10")),
            log_level=_read("LOG_LEVEL", "INFO").upper(),
        )
        cfg.validate()
        return cfg

    def validate(self) -> None:
        if not self.database_url:
            raise AnalysisValidatorConfigurationError("DATABASE_URL is required")
        if not self.redis_url:
            raise AnalysisValidatorConfigurationError("REDIS_URL is required")
        if not self.queue_name:
            raise AnalysisValidatorConfigurationError("ANALYSIS_VALIDATOR_QUEUE_NAME must not be empty")
        if not self.consumer_group:
            raise AnalysisValidatorConfigurationError("ANALYSIS_VALIDATOR_CONSUMER_GROUP must not be empty")
        if not self.consumer_name:
            raise AnalysisValidatorConfigurationError("ANALYSIS_VALIDATOR_CONSUMER_NAME must not be empty")
        if self.batch_size <= 0 or self.batch_size > 100:
            raise AnalysisValidatorConfigurationError("ANALYSIS_VALIDATOR_BATCH_SIZE must be between 1 and 100")
        if self.block_ms <= 0:
            raise AnalysisValidatorConfigurationError("ANALYSIS_VALIDATOR_BLOCK_MS must be > 0")
        if self.max_headline_chars <= 0:
            raise AnalysisValidatorConfigurationError("ANALYSIS_VALIDATOR_MAX_HEADLINE_CHARS must be > 0")
        if self.max_summary_chars <= 0:
            raise AnalysisValidatorConfigurationError("ANALYSIS_VALIDATOR_MAX_SUMMARY_CHARS must be > 0")
        if self.max_text_items <= 0:
            raise AnalysisValidatorConfigurationError("ANALYSIS_VALIDATOR_MAX_TEXT_ITEMS must be > 0")
```

### 9-3. `src/services/analysis_validator/models.py`

```python
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Literal


ValidatorAction = Literal[
    "forward_policy",
    "refused",
    "failed_terminal",
    "failed_retryable",
    "noop",
]


@dataclass(slots=True, frozen=True)
class JudgeOutputReadyJob:
    trigger_event_id: str
    event_type: str
    judge_run_id: str
    judge_output_id: str
    finish_reason: str | None
    refusal_detected: bool


@dataclass(slots=True, frozen=True)
class JudgeRunValidationRecord:
    judge_run_id: str
    bundle_id: str
    judge_profile: str
    schema_version: str
    policy_version: str
    status: str
    finish_reason: str | None
    refusal_detected: bool


@dataclass(slots=True, frozen=True)
class JudgeOutputRecord:
    judge_output_id: str
    judge_run_id: str
    candidate_group_id: str
    judge_schema_version: str
    payload_json: dict[str, Any]
    model_proposed_verdict: str | None
    model_confidence_band: str | None
    created_at: datetime


@dataclass(slots=True, frozen=True)
class BundleValidationContext:
    bundle_id: str
    candidate_group_id: str
    current_primary_artifact_id: str
    current_primary_artifact_type: str
    created_at: datetime


@dataclass(slots=True, frozen=True)
class ValidationDecision:
    action: ValidatorAction
    reason_code: str | None = None
    transition_to_state: str | None = None
```

### 9-4. `src/services/analysis_validator/schema_registry.py`

```python
from __future__ import annotations

from typing import Any


class JudgeOutputSchemaRegistry:
    """Local mirror of judge_output_v1.

    Minimal-change rule:
    - validator re-checks the stored structured payload,
    - but does not mutate stored judge_outputs,
    - future extraction to shared contracts/ is allowed.
    """

    @staticmethod
    def schema() -> dict[str, Any]:
        return {
            "type": "object",
            "additionalProperties": False,
            "required": [
                "judge_schema_version",
                "candidate_group_id",
                "headline",
                "summary_one_line_ko",
                "skeptical_take_ko",
                "why_it_might_matter_ko",
                "comparables",
                "scores",
                "reason_codes",
                "red_flags_ko",
                "evidence_limitations_ko",
                "recommended_action_ko",
                "freshness_note_ko",
                "model_proposed_verdict",
                "model_confidence_band",
            ],
            "properties": {
                "judge_schema_version": {"type": "string"},
                "candidate_group_id": {"type": "string"},
                "headline": {"type": "string"},
                "summary_one_line_ko": {"type": "string"},
                "skeptical_take_ko": {"type": "string"},
                "why_it_might_matter_ko": {"type": "string"},
                "comparables": {"type": "array", "items": {"type": "string"}},
                "scores": {"type": "object", "additionalProperties": True},
                "reason_codes": {"type": "array", "items": {"type": "string"}},
                "red_flags_ko": {"type": "array", "items": {"type": "string"}},
                "evidence_limitations_ko": {"type": "array", "items": {"type": "string"}},
                "recommended_action_ko": {"type": "string"},
                "freshness_note_ko": {"type": "string"},
                "model_proposed_verdict": {
                    "type": ["string", "null"],
                    "enum": ["inspect_now", "later", "skip", None],
                },
                "model_confidence_band": {
                    "type": ["string", "null"],
                    "enum": ["low", "medium", "high", None],
                },
            },
        }
```

### 9-5. `src/services/analysis_validator/business_rules.py`

```python
from __future__ import annotations

from typing import Any

from .config import AnalysisValidatorConfig
from .models import BundleValidationContext, ValidationDecision


_GITHUB_PRIMARY_TYPES = {
    "github_repo",
    "github_subpath",
    "github_repo_page",
    "github_gist",
}


class JudgeOutputBusinessRules:
    def __init__(self, config: AnalysisValidatorConfig) -> None:
        self._config = config

    def validate(
        self,
        *,
        payload: dict[str, Any],
        bundle: BundleValidationContext,
        finish_reason: str | None,
        refusal_detected: bool,
    ) -> ValidationDecision:
        if refusal_detected or payload.get("output_kind") == "refusal":
            return ValidationDecision(
                action="refused",
                reason_code="model_refusal",
                transition_to_state="analysis_refused",
            )

        if finish_reason in {"incomplete", "max_output_tokens"}:
            return ValidationDecision(
                action="failed_retryable",
                reason_code="analysis_failed_truncation",
                transition_to_state="analysis_failed_truncation",
            )

        headline = str(payload.get("headline", ""))
        summary = str(payload.get("summary_one_line_ko", ""))
        skeptical_take = str(payload.get("skeptical_take_ko", ""))
        reason_codes = payload.get("reason_codes") or []
        comparables = payload.get("comparables") or []
        scores = payload.get("scores") or {}
        verdict = payload.get("model_proposed_verdict")

        if not skeptical_take.strip():
            return ValidationDecision(
                action="failed_terminal",
                reason_code="validator_missing_skeptical_take",
                transition_to_state="analysis_failed_semantic",
            )

        if len(headline) > self._config.max_headline_chars:
            return ValidationDecision(
                action="failed_terminal",
                reason_code="validator_headline_too_long",
                transition_to_state="analysis_failed_schema",
            )

        if len(summary) > self._config.max_summary_chars:
            return ValidationDecision(
                action="failed_terminal",
                reason_code="validator_summary_too_long",
                transition_to_state="analysis_failed_schema",
            )

        for key in ("comparables", "reason_codes", "red_flags_ko", "evidence_limitations_ko"):
            value = payload.get(key) or []
            if len(value) > self._config.max_text_items:
                return ValidationDecision(
                    action="failed_terminal",
                    reason_code=f"validator_{key}_too_many_items",
                    transition_to_state="analysis_failed_schema",
                )

        if not isinstance(reason_codes, list) or len(reason_codes) == 0:
            return ValidationDecision(
                action="failed_terminal",
                reason_code="validator_missing_reason_codes",
                transition_to_state="analysis_failed_semantic",
            )

        if bundle.current_primary_artifact_type in _GITHUB_PRIMARY_TYPES and len(comparables) == 0:
            return ValidationDecision(
                action="failed_terminal",
                reason_code="validator_missing_github_comparables",
                transition_to_state="analysis_failed_semantic",
            )

        evidence_strength = self._score(scores, "evidence_strength")
        confidence = self._score(scores, "confidence")
        hype_penalty = self._score(scores, "hype_penalty")

        if verdict == "inspect_now" and evidence_strength is not None and evidence_strength < 50:
            return ValidationDecision(
                action="failed_terminal",
                reason_code="validator_inspect_now_evidence_too_low",
                transition_to_state="analysis_failed_semantic",
            )

        if verdict == "inspect_now" and confidence is not None and confidence < 60:
            return ValidationDecision(
                action="failed_terminal",
                reason_code="validator_inspect_now_confidence_too_low",
                transition_to_state="analysis_failed_semantic",
            )

        if verdict == "inspect_now" and hype_penalty is not None and hype_penalty >= 70:
            return ValidationDecision(
                action="failed_terminal",
                reason_code="validator_inspect_now_hype_too_high",
                transition_to_state="analysis_failed_semantic",
            )

        return ValidationDecision(
            action="forward_policy",
            reason_code="validator_passed",
            transition_to_state="analysis_validated",
        )

    @staticmethod
    def _score(scores: dict[str, Any], key: str) -> int | None:
        value = scores.get(key)
        if value is None:
            return None
        if not isinstance(value, (int, float)):
            raise ValueError(f"score {key} must be numeric")
        value_int = int(value)
        if value_int < 0 or value_int > 100:
            raise ValueError(f"score {key} must be between 0 and 100")
        return value_int
```

### 9-6. `src/services/analysis_validator/repositories.py`

```python
from __future__ import annotations

import json
from contextlib import asynccontextmanager
from typing import Any

import sqlalchemy as sa
from sqlalchemy.ext.asyncio import AsyncSession

from .models import (
    BundleValidationContext,
    JudgeOutputReadyJob,
    JudgeOutputRecord,
    JudgeRunValidationRecord,
)


def _jsonb_dumps(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False)


class AnalysisValidatorRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    @asynccontextmanager
    async def transaction(self):
        if self._session.in_transaction():
            yield self._session
            return
        async with self._session.begin():
            yield self._session

    async def load_job_by_trigger_event_id(self, trigger_event_id: str) -> JudgeOutputReadyJob | None:
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
        if row is None or str(row["event_type"]) != "judge.output.ready.v1":
            return None

        payload = row["payload_json"] or {}
        return JudgeOutputReadyJob(
            trigger_event_id=str(row["event_id"]),
            event_type=str(row["event_type"]),
            judge_run_id=str(payload["judge_run_id"]),
            judge_output_id=str(payload["judge_output_id"]),
            finish_reason=str(payload["finish_reason"]) if payload.get("finish_reason") else None,
            refusal_detected=bool(payload.get("refusal_detected", False)),
        )

    async def load_judge_run(self, judge_run_id: str) -> JudgeRunValidationRecord | None:
        result = await self._session.execute(
            sa.text(
                """
                SELECT judge_run_id, bundle_id, judge_profile, schema_version,
                       policy_version, status, finish_reason, refusal_detected
                FROM judge_runs
                WHERE judge_run_id = CAST(:judge_run_id AS uuid)
                """
            ),
            {"judge_run_id": judge_run_id},
        )
        row = result.mappings().first()
        if row is None:
            return None
        return JudgeRunValidationRecord(
            judge_run_id=str(row["judge_run_id"]),
            bundle_id=str(row["bundle_id"]),
            judge_profile=str(row["judge_profile"]),
            schema_version=str(row["schema_version"]),
            policy_version=str(row["policy_version"]),
            status=str(row["status"]),
            finish_reason=str(row["finish_reason"]) if row["finish_reason"] else None,
            refusal_detected=bool(row["refusal_detected"]),
        )

    async def load_judge_output(self, judge_output_id: str) -> JudgeOutputRecord | None:
        result = await self._session.execute(
            sa.text(
                """
                SELECT judge_output_id, judge_run_id, candidate_group_id,
                       judge_schema_version, payload_json,
                       model_proposed_verdict, model_confidence_band, created_at
                FROM judge_outputs
                WHERE judge_output_id = CAST(:judge_output_id AS uuid)
                """
            ),
            {"judge_output_id": judge_output_id},
        )
        row = result.mappings().first()
        if row is None:
            return None
        return JudgeOutputRecord(
            judge_output_id=str(row["judge_output_id"]),
            judge_run_id=str(row["judge_run_id"]),
            candidate_group_id=str(row["candidate_group_id"]),
            judge_schema_version=str(row["judge_schema_version"]),
            payload_json=row["payload_json"] or {},
            model_proposed_verdict=str(row["model_proposed_verdict"]) if row["model_proposed_verdict"] else None,
            model_confidence_band=str(row["model_confidence_band"]) if row["model_confidence_band"] else None,
            created_at=row["created_at"],
        )

    async def load_bundle_context(self, bundle_id: str) -> BundleValidationContext | None:
        result = await self._session.execute(
            sa.text(
                """
                SELECT ceb.bundle_id,
                       ceb.candidate_group_id,
                       ceb.current_primary_artifact_id,
                       ar.artifact_type AS current_primary_artifact_type,
                       ceb.created_at
                FROM candidate_evidence_bundles ceb
                JOIN artifact_registry ar ON ar.artifact_id = ceb.current_primary_artifact_id
                WHERE ceb.bundle_id = CAST(:bundle_id AS uuid)
                """
            ),
            {"bundle_id": bundle_id},
        )
        row = result.mappings().first()
        if row is None:
            return None
        return BundleValidationContext(
            bundle_id=str(row["bundle_id"]),
            candidate_group_id=str(row["candidate_group_id"]),
            current_primary_artifact_id=str(row["current_primary_artifact_id"]),
            current_primary_artifact_type=str(row["current_primary_artifact_type"]),
            created_at=row["created_at"],
        )

    async def update_judge_run_status(
        self,
        *,
        judge_run_id: str,
        status: str,
        finish_reason: str | None = None,
    ) -> None:
        await self._session.execute(
            sa.text(
                """
                UPDATE judge_runs
                SET
                    status = :status,
                    finish_reason = COALESCE(:finish_reason, finish_reason),
                    finished_at = COALESCE(finished_at, now())
                WHERE judge_run_id = CAST(:judge_run_id AS uuid)
                """
            ),
            {
                "judge_run_id": judge_run_id,
                "status": status,
                "finish_reason": finish_reason,
            },
        )

    async def insert_state_transition(
        self,
        *,
        object_type: str,
        object_id: str,
        from_state: str | None,
        to_state: str,
        reason_code: str | None,
    ) -> None:
        await self._session.execute(
            sa.text(
                """
                INSERT INTO state_transitions (
                    state_transition_id,
                    object_type,
                    object_id,
                    from_state,
                    to_state,
                    reason_code,
                    created_at
                ) VALUES (
                    gen_random_uuid(),
                    :object_type,
                    CAST(:object_id AS uuid),
                    :from_state,
                    :to_state,
                    :reason_code,
                    now()
                )
                """
            ),
            {
                "object_type": object_type,
                "object_id": object_id,
                "from_state": from_state,
                "to_state": to_state,
                "reason_code": reason_code,
            },
        )

    async def insert_analysis_policy_apply_outbox(
        self,
        *,
        judge_run_id: str,
        judge_output_id: str,
        candidate_group_id: str,
        bundle_id: str,
    ) -> None:
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
                    'analysis.policy.apply.v1',
                    'judge_run',
                    CAST(:judge_run_id AS uuid),
                    :dedupe_key,
                    CAST(:payload_json AS jsonb),
                    'pending'::outbox_status_enum,
                    now()
                )
                ON CONFLICT (dedupe_key) DO NOTHING
                """
            ),
            {
                "judge_run_id": judge_run_id,
                "dedupe_key": f"analysis-policy:{judge_run_id}:{judge_output_id}",
                "payload_json": _jsonb_dumps(
                    {
                        "judge_run_id": judge_run_id,
                        "judge_output_id": judge_output_id,
                        "candidate_group_id": candidate_group_id,
                        "bundle_id": bundle_id,
                    }
                ),
            },
        )
```

### 9-7. `src/services/analysis_validator/service.py`

```python
from __future__ import annotations

import logging

from jsonschema import Draft202012Validator

from .business_rules import JudgeOutputBusinessRules
from .models import JudgeOutputReadyJob
from .repositories import AnalysisValidatorRepository
from .schema_registry import JudgeOutputSchemaRegistry


class AnalysisValidatorService:
    def __init__(
        self,
        config,
        *,
        repository: AnalysisValidatorRepository,
        logger: logging.Logger | None = None,
    ) -> None:
        self._config = config
        self._repository = repository
        self._logger = logger or logging.getLogger(__name__)
        self._schema_validator = Draft202012Validator(JudgeOutputSchemaRegistry.schema())
        self._business_rules = JudgeOutputBusinessRules(config)

    async def rehydrate_job(self, trigger_event_id: str) -> JudgeOutputReadyJob | None:
        return await self._repository.load_job_by_trigger_event_id(trigger_event_id)

    async def handle_job(self, job: JudgeOutputReadyJob) -> None:
        judge_run = await self._repository.load_judge_run(job.judge_run_id)
        if judge_run is None:
            return

        judge_output = await self._repository.load_judge_output(job.judge_output_id)
        if judge_output is None:
            async with self._repository.transaction():
                await self._repository.update_judge_run_status(
                    judge_run_id=job.judge_run_id,
                    status="failed_terminal",
                    finish_reason="validator_missing_judge_output",
                )
                await self._repository.insert_state_transition(
                    object_type="judge_run",
                    object_id=job.judge_run_id,
                    from_state=judge_run.status,
                    to_state="analysis_failed_missing_output",
                    reason_code="validator_missing_judge_output",
                )
            return

        if judge_output.judge_run_id != job.judge_run_id:
            async with self._repository.transaction():
                await self._repository.update_judge_run_status(
                    judge_run_id=job.judge_run_id,
                    status="failed_terminal",
                    finish_reason="validator_judge_output_mismatch",
                )
                await self._repository.insert_state_transition(
                    object_type="judge_run",
                    object_id=job.judge_run_id,
                    from_state=judge_run.status,
                    to_state="analysis_failed_schema",
                    reason_code="validator_judge_output_mismatch",
                )
            return

        bundle = await self._repository.load_bundle_context(judge_run.bundle_id)
        if bundle is None or bundle.candidate_group_id != judge_output.candidate_group_id:
            async with self._repository.transaction():
                await self._repository.update_judge_run_status(
                    judge_run_id=job.judge_run_id,
                    status="failed_terminal",
                    finish_reason="validator_bundle_identity_mismatch",
                )
                await self._repository.insert_state_transition(
                    object_type="judge_run",
                    object_id=job.judge_run_id,
                    from_state=judge_run.status,
                    to_state="analysis_failed_schema",
                    reason_code="validator_bundle_identity_mismatch",
                )
            return

        payload = judge_output.payload_json or {}

        # refusal branch first
        decision = self._business_rules.validate(
            payload=payload,
            bundle=bundle,
            finish_reason=job.finish_reason or judge_run.finish_reason,
            refusal_detected=job.refusal_detected or judge_run.refusal_detected,
        )
        if decision.action == "refused":
            async with self._repository.transaction():
                await self._repository.insert_state_transition(
                    object_type="judge_run",
                    object_id=job.judge_run_id,
                    from_state=judge_run.status,
                    to_state=decision.transition_to_state or "analysis_refused",
                    reason_code=decision.reason_code,
                )
            return

        # strict schema branch for structured payloads only
        errors = sorted(self._schema_validator.iter_errors(payload), key=lambda e: list(e.path))
        if errors:
            async with self._repository.transaction():
                await self._repository.update_judge_run_status(
                    judge_run_id=job.judge_run_id,
                    status="failed_terminal",
                    finish_reason="validator_schema_invalid",
                )
                await self._repository.insert_state_transition(
                    object_type="judge_run",
                    object_id=job.judge_run_id,
                    from_state=judge_run.status,
                    to_state="analysis_failed_schema",
                    reason_code="validator_schema_invalid",
                )
            return

        try:
            decision = self._business_rules.validate(
                payload=payload,
                bundle=bundle,
                finish_reason=job.finish_reason or judge_run.finish_reason,
                refusal_detected=False,
            )
        except ValueError as exc:
            async with self._repository.transaction():
                await self._repository.update_judge_run_status(
                    judge_run_id=job.judge_run_id,
                    status="failed_terminal",
                    finish_reason=str(exc),
                )
                await self._repository.insert_state_transition(
                    object_type="judge_run",
                    object_id=job.judge_run_id,
                    from_state=judge_run.status,
                    to_state="analysis_failed_semantic",
                    reason_code="validator_score_range_invalid",
                )
            return

        if decision.action == "failed_retryable":
            async with self._repository.transaction():
                await self._repository.update_judge_run_status(
                    judge_run_id=job.judge_run_id,
                    status="failed_retryable",
                    finish_reason=decision.reason_code,
                )
                await self._repository.insert_state_transition(
                    object_type="judge_run",
                    object_id=job.judge_run_id,
                    from_state=judge_run.status,
                    to_state=decision.transition_to_state or "analysis_failed_truncation",
                    reason_code=decision.reason_code,
                )
            return

        if decision.action == "failed_terminal":
            async with self._repository.transaction():
                await self._repository.update_judge_run_status(
                    judge_run_id=job.judge_run_id,
                    status="failed_terminal",
                    finish_reason=decision.reason_code,
                )
                await self._repository.insert_state_transition(
                    object_type="judge_run",
                    object_id=job.judge_run_id,
                    from_state=judge_run.status,
                    to_state=decision.transition_to_state or "analysis_failed_semantic",
                    reason_code=decision.reason_code,
                )
            return

        async with self._repository.transaction():
            await self._repository.insert_state_transition(
                object_type="judge_run",
                object_id=job.judge_run_id,
                from_state=judge_run.status,
                to_state=decision.transition_to_state or "analysis_validated",
                reason_code=decision.reason_code,
            )
            await self._repository.insert_analysis_policy_apply_outbox(
                judge_run_id=job.judge_run_id,
                judge_output_id=job.judge_output_id,
                candidate_group_id=judge_output.candidate_group_id,
                bundle_id=judge_run.bundle_id,
            )
```

### 9-8. `src/services/analysis_validator/worker.py`

```python
from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass

from ..gh_enricher.redis_streams import RedisStreamConsumer
from .config import AnalysisValidatorConfig
from .service import AnalysisValidatorService


@dataclass(slots=True, frozen=True)
class WorkerBatchResult:
    processed: int = 0
    acked: int = 0


class AnalysisValidatorWorker:
    def __init__(
        self,
        config: AnalysisValidatorConfig,
        *,
        consumer: RedisStreamConsumer,
        service: AnalysisValidatorService,
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
            "analysis_validator_worker_started",
            extra={
                "service": "analysis-validator",
                "event": "analysis_validator_worker_started",
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
        for message in messages:
            processed += 1
            trigger_event_id = message.fields.get("trigger_event_id")
            if trigger_event_id:
                job = await self._service.rehydrate_job(trigger_event_id)
                if job is not None:
                    await self._service.handle_job(job)
            await self._consumer.ack(message.message_id)
            acked += 1
        return WorkerBatchResult(processed=processed, acked=acked)
```

### 9-9. `src/services/analysis_validator/main.py`

```python
from __future__ import annotations

import asyncio
import logging
import sys

from redis.asyncio import Redis
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from ..gh_enricher.redis_streams import RedisStreamConsumer
from .config import AnalysisValidatorConfig
from .repositories import AnalysisValidatorRepository
from .service import AnalysisValidatorService
from .worker import AnalysisValidatorWorker


def _configure_logging(log_level: str) -> None:
    logging.basicConfig(
        level=getattr(logging, log_level, logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
        stream=sys.stdout,
    )


async def _run() -> int:
    config = AnalysisValidatorConfig.from_env()
    _configure_logging(config.log_level)
    logger = logging.getLogger("analysis_validator")

    engine = create_async_engine(config.database_url, future=True)
    session_factory = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    redis_client = Redis.from_url(config.redis_url, decode_responses=True)

    try:
        async with session_factory() as session:
            repository = AnalysisValidatorRepository(session)
            service = AnalysisValidatorService(
                config,
                repository=repository,
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
            worker = AnalysisValidatorWorker(
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

---

## 10. 테스트 초안 포인트

### `tests/unit/services/analysis_validator/test_schema_registry.py`

검증:

- `judge_output_v1` required field가 유지되는지
- `model_proposed_verdict` / `model_confidence_band` enum이 고정되는지

### `tests/unit/services/analysis_validator/test_business_rules.py`

검증:

- GitHub primary인데 comparables가 비면 terminal invalid
- `inspect_now` + `evidence_strength < 50`이면 terminal invalid
- `skeptical_take_ko` 비어 있으면 invalid
- 정상 payload면 `forward_policy`

### `tests/unit/services/analysis_validator/test_refusal_branch.py`

검증:

- refusal envelope이면 `analysis_refused`
- downstream emit이 없는지

### `tests/unit/services/analysis_validator/test_invalid_inspect_now_contradiction.py`

검증:

- `inspect_now`인데 `confidence < 60` / `hype_penalty >= 70` 같은 obvious contradiction을 막는지

### `tests/component/services/analysis_validator/test_worker_rehydrates_judge_output_ready.py`

검증:

- Redis payload는 thin message
- `event_outbox` 기준으로 `judge.output.ready.v1` rehydrate

### `tests/component/services/analysis_validator/test_valid_output_emits_policy_apply.py`

검증:

- valid payload면 `analysis.policy.apply.v1` outbox insert
- `state_transitions`에 `analysis_validated` 기록

### `tests/component/services/analysis_validator/test_refusal_records_state_transition_only.py`

검증:

- refusal이면 `analysis_refused` state transition만 남고
- `analysis.policy.apply.v1`는 emit하지 않는지

### `tests/component/services/analysis_validator/test_missing_judge_output_marks_terminal.py`

검증:

- output row 없으면 `failed_terminal`
- `analysis_failed_missing_output` transition 기록

### `tests/component/services/analysis_validator/test_truncation_marks_retryable_without_emit.py`

검증:

- truncation/incomplete면 `failed_retryable`
- `analysis.policy.apply.v1` emit 없음

---

## 11. 이번 단계가 구조를 지키는 이유

1. `analysis-validator`는 `judge_runs`, `state_transitions`, `event_outbox`만 직접 쓴다.  
   즉, service ownership을 넘지 않는다.

2. validator는 `judge_outputs`를 **수정하지 않고 pass/fail만 판정** 한다.  
   즉, judge-openai의 append-only 경계가 유지된다.

3. refusal은 policy-engine으로 넘기지 않고 validator에서 stop 한다.  
   즉, current event 계약을 바꾸지 않으면서 stage 6 요구를 만족한다.

4. Prompt Guard / AgentLinter / MemKraft를 runtime hot path에 새 lifecycle로 넣지 않는다.  
   즉, `03_GitHub_AI_application_plan.md`의 좋은 방향은 보존하되 current contracts는 흔들지 않는다.

5. `analysis.policy.apply.v1`까지만 emit한다.  
   즉, `policy-engine` / `notifier-telegram` 경계를 침범하지 않는다.

---

## 12. 다음 단계

이 단계가 닫히면 다음 구현 순서는 그대로 아래다.

1. `37_policy_engine_skeleton_and_code_draft_v0_1.md`
2. `38_notifier_telegram_skeleton_and_code_draft_v0_1.md`

즉, 이제 stage 6 judge pipeline의 출력 방화벽을 닫았고,  
다음은 **최종 verdict / delivery decision을 확정하는 `policy-engine`** 을 붙이는 것이 맞다.

---

## 최종 한 줄 결론

이번 단계의 최소-change 정답은 **`judge.output.ready.v1`를 rehydrate해 `judge_run` / `judge_output` / current bundle identity를 재검증하고, refusal은 `analysis_refused`로 종료하고, structured payload는 schema·semantic·obvious contradiction 검증을 거친 뒤 유효한 경우에만 `analysis.policy.apply.v1`로 넘기는 `analysis-validator` v0.1을 닫는 것** 이다.


---

## Source file: `37_policy_engine_skeleton_and_code_draft_v0_1.md`

# 37단계: `policy-engine` 스켈레톤 + 실제 코드 초안 v0.1

## 0. 문서 목적

이 문서는 이미 잠긴 구조를 다시 설계하는 문서가 아니다.  
목적은 **36단계 `analysis-validator`가 만들어낸 `analysis.policy.apply.v1`를 받아, 최종 `analysis_v1`과 notifier handoff intent를 계산하는 좁은 경계**를 구현 가능한 수준으로 내리는 것이다.

이번 단계에서 고정하는 것은 아래 여덟 가지다.

1. `analysis.policy.apply.v1` 소비 경계를 코드로 고정
2. thin Redis payload → `event_outbox` 재조회 → `judge_runs` / `judge_outputs` / `candidate_evidence_bundles` / `candidate_group_proposals` 재hydrate 경계를 고정
3. `policy-engine`의 좁은 책임을 **deterministic verdict 재계산 / deterministic delivery decision 계산 / `analyses` append / `state_transitions` 기록 / notifier handoff event emit** 으로 고정
4. `analyses`, `state_transitions`, `event_outbox`만 직접 쓰는 **service ownership** 을 고정
5. stage 0 / stage 6 / stage 7에서 잠근 규칙대로 **모델 제안 verdict와 최종 policy verdict를 분리** 하고 `policy_reconciled_flag`로 차이를 기록
6. notifier 소유권을 깨지 않기 위해, `notification_plans` row는 아직 쓰지 않고 **`notification.plan.created.v1`를 plan-intent event로 해석** 하는 최소-change bridge를 고정
7. `03_GitHub_AI_application_plan.md`의 외부 자산 제안을 검토하되, **Prompt Guard / AgentLinter / MemKraft / skill discipline을 deterministic hot path 판단식 안에 넣지 않는 최소-change 해석** 을 유지
8. 다음 단계인 `notifier-telegram`이 바로 붙을 수 있게 **notification plan intent payload** 를 안정화

핵심 전제는 유지한다.

- `policy-engine`는 **LLM 호출기**가 아니다.
- `policy-engine`는 **validator** 가 아니다.
- `policy-engine`는 **notifier** 가 아니다.
- `policy-engine`는 **최종 `analysis_v1`의 verdict / delivery decision 확정기** 다.
- `policy-engine`는 **deterministic 해야 한다.**

---

## 1. 왜 지금 이 단계가 정확한 다음 단계인가

현재 프로젝트 소스의 최신 README와 방금 추가된 36단계 문서는 stage 6 judge pipeline이 `analysis-router` → `judge-openai` → `analysis-validator` 순으로 닫혔고, **다음 구현 순서가 `policy-engine` → `notifier-telegram`** 임을 고정한다.  
또한 6단계 정본은 **모델은 `judge_output_v1`만 만들고, 최종 `verdict`와 `delivery_decision`은 deterministic policy engine이 계산** 해야 한다고 잠갔다.  
즉, 지금 다시 collector / normalizer / enricher / assembler / judge 계층을 여는 것은 순서상 후퇴고, 이제 붙여야 하는 것은 **최종 Analysis 확정기인 `policy-engine`** 이다.

---

## 2. 이번 단계에서 확인한 충돌과 최소-change 해석

### 충돌 A — 현재 소스에는 README v6 / v7 / v8 / v9 / v10이 함께 존재할 수 있다

현재 프로젝트 소스에는 이전 README 중간본들이 함께 남아 있을 수 있다.

- v6: latest = 32
- v7: latest = 33
- v8: latest = 34
- v9: latest = 35
- v10: latest = 36

### 최소-change 해석 A

- **v10만 phase authority** 로 사용한다.
- v6 / v7 / v8 / v9은 이력성 중간본으로 본다.
- 이번 37단계 문서와 README 업데이트에서는 **v10을 이어받아 v11로만 승격** 한다.

즉, phase ordering은 최신 README 하나로 수렴시키고, 오래된 README는 더 이상 authority로 쓰지 않는다.

---

### 충돌 B — schema에는 `candidate_group_proposals.current_analysis_id` pointer가 있지만, execution contracts의 service ownership에는 policy-engine의 candidate aggregate write가 명시돼 있지 않다

migration 정본은 `candidate_group_proposals.current_analysis_id` FK patch까지 열어두었다.  
하지만 execution contracts의 service ownership 표는 `policy-engine`의 직접 소유 테이블을 아래로만 잠갔다.

- `analyses`
- `state_transitions`
- `event_outbox`

즉, 이번 단계에서 `policy-engine`이 `candidate_group_proposals.current_analysis_id`까지 건드리기 시작하면 현재 ownership을 확장하게 된다.

### 최소-change 해석 B

이번 v0.1에서는 아래처럼 고정한다.

1. `policy-engine`는 **`analyses` append-only row만** 쓴다.
2. `current_analysis_id` pointer는 **이번 단계에서 업데이트하지 않는다.**
3. notifier handoff와 downstream 진행은 **outbox payload만으로** 이어간다.
4. `current_analysis_id` pointer maintenance는 필요성이 확인되면 **후속 hardening 턴** 에 좁게 닫는다.

이 해석의 장점은 다음과 같다.

- execution contracts의 service ownership을 그대로 존중한다.
- `analysis_v1` durable truth는 이미 `analyses` 테이블에 남는다.
- notifier 연결에 필요한 정보는 outbox event payload로 충분하다.

즉, 현재 단계는 **pointer mutation 없이 final analysis append-only history를 먼저 닫는 것** 이 더 보수적이다.

---

### 충돌 C — event contract는 `notification.plan.created.v1`를 delivery 계열로 잠갔지만, stage 7과 service ownership은 `notifier-telegram`이 `notification_plans`를 직접 쓰도록 잠갔다

현재 잠긴 문서들을 그대로 놓고 보면 작은 비틀림이 있다.

- event contract에는 `notification.plan.created.v1`가 있고, payload 최소 필드에 `notification_plan_id`가 포함된다.
- stage 7 문서는 notifier 입력을 `analysis_v1 + delivery_policy_applied` 로 본다.
- execution contracts의 service ownership은 `notification_plans`를 **notifier-telegram** 이 직접 쓰도록 잠갔다.

즉, `policy-engine`이 `notification_plans` row까지 써버리면 ownership을 침범하고, 반대로 아무 event도 안 만들면 notifier queue를 붙일 수 없다.

### 최소-change 해석 C

이번 v0.1에서는 아래처럼 고정한다.

1. `policy-engine`은 **`notification_plans` row를 쓰지 않는다.**
2. 대신 `notification.plan.created.v1`를 **plan-intent event** 로 해석한다.
3. `policy-engine`이 `notification_plan_id`를 미리 할당해 outbox payload에 넣는다.
4. 다음 단계 `notifier-telegram`은 이 event를 소비해 **해당 ID로 `notification_plans` row를 생성** 한다.

즉, event 이름은 다소 어색하지만,

- event contract 유지
- queue routing 유지
- notifier ownership 유지

를 동시에 만족시키는 가장 작은 변경은 이 bridge 해석이다.

---

### 충돌 D — `analysis.policy.apply.v1`가 늦게 소비되면 더 최신 bundle이 current가 되었을 수 있다

33단계 이후 pipeline은 append-only history + current pointer 구조다. 따라서 아래가 가능하다.

1. bundle A로 judge 완료
2. 그 사이 더 최신 bundle B가 current로 승격
3. 늦게 도착한 `analysis.policy.apply.v1`가 bundle A 기준으로 policy 적용 시도

이 상태에서 stale analysis까지 final analysis로 쓰면 history/current가 다시 섞인다.

### 최소-change 해석 D

이번 단계에서는 아래처럼 고정한다.

- payload의 `bundle_id`가 `candidate_group_proposals.current_bundle_id`와 다르면 **stale policy request** 로 보고 no-op
- stale request에 대해 `analyses` row를 새로 만들지 않는다
- notifier handoff도 만들지 않는다

즉, `policy-engine`도 `analysis-router`와 같은 보수성으로 **current pointer를 우선** 본다.

---

### 충돌 E — application plan의 외부 자산 제안을 `policy-engine`에 넣으면 deterministic boundary가 흔들린다

application plan은 Prompt Guard, AgentLinter, MemKraft, skill/playbook discipline을 권장한다. 방향은 맞다. 하지만 `policy-engine`은 **최종 verdict / delivery decision을 deterministic하게 고정하는 계층** 이다. 여기에 아래를 넣으면 구조가 흔들린다.

- Prompt Guard hard block / quarantine
- MemKraft runtime retrieval
- AgentLinter runtime enforcement
- self-learning / auto-skill loop

### 최소-change 해석 E

이번 37단계에는 아래만 유지한다.

- **Prompt Guard**: 35단계 sanitize-only preflight까지만 인정
- **AgentLinter**: `AGENTS.md`, `prompts/`, `policies/`, `README` 정리 단계에서만 적용
- **MemKraft**: `ops-memory/` sidecar로만 적용
- **skill/playbook discipline**: prompt/profile handbook 문서화에만 적용

즉, `policy-engine`는 **외부 자산 흡수 지점이 아니라 deterministic 집행 지점** 으로 남는다.

---

## 3. `policy-engine`의 책임과 비책임

### 3-1. 반드시 하는 일

- `analysis.policy.apply.v1` 소비
- `event_outbox` 기준 request rehydrate
- `judge_runs` / `judge_outputs` / `candidate_evidence_bundles` / `candidate_group_proposals` 재조회
- stage 0 verdict 규칙에 따른 deterministic verdict 재계산
- stage 7 default delivery 규칙에 따른 deterministic delivery decision 계산
- `analyses` append
- `policy_reconciled_flag` 계산
- `state_transitions` append
- non-suppress 결과에 대해 `notification.plan.created.v1` outbox emit

### 3-2. 하면 안 되는 일

- LLM 재호출
- `judge_outputs` 수정
- `candidate_evidence_bundles` 수정
- `candidate_group_proposals.current_analysis_id` 수정
- notification render/send
- Prompt Guard 기반 block/quarantine 결정
- MemKraft/AgentLinter runtime 호출

즉, 이 서비스는 **최종 Analysis 확정기 + notifier handoff gate** 다.

---

## 4. 직접 소유하는 durable 경계

execution contracts 기준으로 `policy-engine`는 아래만 직접 쓴다.

- `analyses`
- `state_transitions`
- `event_outbox`

읽는 것:

- `judge_runs`
- `judge_outputs`
- `candidate_evidence_bundles`
- `candidate_group_proposals`

즉, `notification_plans`, `notification_renders`, `notification_delivery_records`는 건드리지 않는다.

---

## 5. 입력/출력 계약

### 5-1. 입력 이벤트

허용 입력은 아래 하나로 좁게 고정한다.

- `analysis.policy.apply.v1`

Redis Streams 메시지는 여전히 thin payload다.

```json
{
  "job_id": "<event_id>",
  "stage_name": "analysis_policy",
  "root_object_type": "judge_run",
  "root_object_id": "<judge_run_id>",
  "idempotency_key": "<dedupe_key>",
  "pipeline_run_id": "",
  "not_before": "",
  "trigger_event_id": "<event_id>"
}
```

즉, consumer는 Redis 본문을 business source처럼 쓰지 않고, 반드시 `trigger_event_id`로 `event_outbox`를 다시 조회한다.

### 5-2. 입력 payload 최소 필드

`analysis.policy.apply.v1` payload는 아래를 믿는다.

- `judge_run_id`
- `judge_output_id`
- `candidate_group_id`
- `bundle_id`

하지만 이 값들은 **request hint** 일 뿐이고, 최종 판정은 여전히 DB 재조회로 재검증한다.

### 5-3. 출력 이벤트

허용 출력은 아래 하나로 좁게 고정한다.

- `notification.plan.created.v1`

단, 아래를 만족할 때만 emit 한다.

- final `delivery_decision != suppress`
- `ENABLE_NOTIFICATION_SEND=true`

payload 최소 필드:

- `notification_plan_id`
- `analysis_id`
- `target_chat_id`
- `send_after`

실제 v0.1 payload에는 다음도 같이 싣는다.

- `candidate_group_id`
- `delivery_decision`
- `urgency_profile`
- `render_profile`
- `dedupe_subject_key`
- `material_change_hash`
- `target_thread_id`
- `suppress_reason_code`

즉, 다음 단계 notifier는 이 payload로 durable `notification_plans` row를 만들 수 있다.

---

## 6. 이번 단계에서 고정할 핵심 처리 규칙

### 6-1. missing / mismatched context는 terminal no-op 또는 policy stop

아래면 `policy-engine`는 analysis를 만들면 안 된다.

- `judge_run` 없음
- `judge_output` 없음
- `judge_output.judge_run_id != payload.judge_run_id`
- `judge_output.candidate_group_id != payload.candidate_group_id`
- `bundle` 없음
- `bundle.candidate_group_id != payload.candidate_group_id`

이 경우:

- `analyses` row 없음
- notifier handoff 없음
- `state_transitions`에 failure reason만 남김

즉, validator를 통과했더라도 마지막 deterministic identity check는 한 번 더 한다.

### 6-2. stale bundle request는 no-op

아래면 stale request다.

- `candidate_group_proposals.current_bundle_id != payload.bundle_id`

이 경우:

- analysis append 안 함
- notification handoff 안 함
- `state_transitions`에 `analysis_policy_stale_bundle`만 기록 가능

즉, final analysis도 **current bundle 우선** 이다.

### 6-3. existing analysis reuse

`analyses`는 migration 정본에서 아래 unique를 가진다.

- `(judge_output_id, policy_version, delivery_policy_version)`

따라서 같은 조합의 row가 이미 있으면:

- 새 `analyses` row를 만들지 않는다
- 새 `notification.plan.created.v1`도 만들지 않는다

즉, duplicate validator event를 final analysis 폭주로 연결하지 않는다.

### 6-4. verdict는 deterministic하게 재계산한다

기본 규칙은 stage 0을 그대로 따른다.

#### `inspect_now`
모두 만족:

- `practical_usefulness >= 70`
- `evidence_strength >= 50`
- `confidence >= 60`
- `hype_penalty < 70`

그리고 추가로 하나 만족:

- GitHub primary: `code_quality >= 65`
- X/text primary: `specificity >= 60`

#### `later`
아래를 모두 만족하고 inspect_now는 아님:

- `practical_usefulness >= 45`
- `evidence_strength >= 30`
- `confidence >= 35`

#### `skip`
그 외 전부.

또한 아래 보정은 유지한다.

- `evidence_strength < 50` 이면 `inspect_now` 금지
- primary가 GitHub 계열인데 `code_quality`가 없거나 낮으면 `inspect_now` 금지
- primary가 X/text 계열인데 `specificity`가 없거나 낮으면 `inspect_now` 금지

즉, `policy-engine`는 **모델 제안 verdict를 참고할 수는 있어도, 최종 판단은 score rule로 다시 계산** 한다.

### 6-5. delivery decision은 stage 7 default를 따른다

#### `inspect_now`
- `delivery_decision = send_now`
- `urgency_profile = high`

#### `later`
- `ENABLE_LATER_DELIVERY=true` 이면 `delivery_decision = send_now`
- `urgency_profile = normal_silent`
- `ENABLE_LATER_DELIVERY=false` 이면 `delivery_decision = suppress`

#### `skip`
- `delivery_decision = suppress`
- `urgency_profile = suppressed`

즉, digest path는 구조상 남겨두되 **v0.1 기본 경로는 아니다.**

### 6-6. policy reconciliation은 명시적으로 남긴다

아래면 `policy_reconciled_flag = false` 다.

- `judge_output.model_proposed_verdict != final verdict`

이 경우 `reason_codes_json`에는 아래를 추가한다.

- `policy_overrode_model_verdict`

즉, model drift와 policy threshold 차이를 나중에 측정할 수 있게 한다.

### 6-7. suppress도 analysis는 남긴다

`delivery_decision = suppress` 라도 아래는 남긴다.

- `analyses` row
- `state_transitions`

남기지 않는 것:

- `notification.plan.created.v1`

즉, **알림을 안 보낸 것도 analysis 결과의 일부** 다.

### 6-8. notification plan intent는 policy-engine이 직접 plan row를 쓰지 않고 outbox로만 넘긴다

non-suppress 결과일 때만:

1. `notification_plan_id = uuid4()` 미리 생성
2. `material_change_hash` 계산
3. `notification.plan.created.v1` outbox insert

여기서 `material_change_hash`는 아래를 해시한다.

- `candidate_group_id`
- `verdict`
- `delivery_decision`
- `urgency_profile`
- `reason_codes_json`
- `recommended_action_ko`
- `freshness_note_ko`

즉, notifier는 이 intent payload를 durable `notification_plan`으로 구체화하면 된다.

---

## 7. application plan 적용 검토 — 이번 단계 결론

### 7-1. 지금 바로 runtime에 적용하는 것

없다.  
정확히 말하면 **새로운 runtime component로는 없다.**

이유:

- Prompt Guard는 35단계 sanitize-only preflight로 이미 최소 삽입됨
- AgentLinter는 repo hygiene / CI 자산이다
- MemKraft는 ops-memory sidecar다
- skill/playbook discipline은 prompt/policy handbook 자산이다

### 7-2. 지금 적용하면 구조를 흔드는 것

#### A. policy-engine에 prompt-risk / memory retrieval / external heuristic를 넣는 것
문제:
- deterministic boundary가 깨진다.
- 최종 verdict가 replayable하지 않게 된다.

#### B. policy-engine이 notifier ownership까지 침범하는 것
문제:
- `notification_plans` row를 직접 쓰면 execution contracts를 넘는다.

#### C. self-learning / auto-skill loop를 policy threshold 계산에 넣는 것
문제:
- precision-first 시스템이 drift에 취약해진다.

### 7-3. 최소-change 결론

- **이번 37단계에는 application plan을 runtime hot path에 새 책임으로 넣지 않는다.**
- 대신 아래를 future-compatible decision으로만 남긴다.
  - Prompt Guard: judge-openai sanitize-only preflight 유지
  - AgentLinter: `AGENTS.md` / `prompts/` / `policies/` / `README` 정리 단계에서 적용
  - MemKraft: `ops-memory/` sidecar로만 적용
  - skill/playbook discipline: prompt/profile handbook 문서화에만 적용

즉, `policy-engine`는 **repo hardening 흡수 지점이 아니라 deterministic policy 집행 지점** 으로 남는다.

---

## 8. 대상 파일 트리

```text
src/services/policy_engine/
  __init__.py
  config.py
  models.py
  verdict_policy.py
  delivery_policy.py
  notification_intent.py
  repositories.py
  service.py
  worker.py
  main.py

tests/
  unit/
    services/
      policy_engine/
        test_verdict_policy_github.py
        test_verdict_policy_text_like.py
        test_delivery_policy_default_mapping.py
        test_policy_reconciled_flag.py
        test_notification_intent_hash.py
  component/
    services/
      policy_engine/
        test_worker_rehydrates_policy_apply.py
        test_valid_output_writes_analysis_and_notification_intent.py
        test_suppress_writes_analysis_without_notification_intent.py
        test_existing_analysis_reuse.py
        test_stale_bundle_request_noop.py
```

---

## 9. 코드 초안

### 9-1. `src/services/policy_engine/__init__.py`

```python
from .config import PolicyEngineConfig
from .service import PolicyEngineService
from .worker import PolicyEngineWorker

__all__ = [
    "PolicyEngineConfig",
    "PolicyEngineService",
    "PolicyEngineWorker",
]
```

### 9-2. `src/services/policy_engine/config.py`

```python
from __future__ import annotations

import os
from dataclasses import dataclass


class PolicyEngineConfigurationError(ValueError):
    pass


@dataclass(slots=True, frozen=True)
class PolicyEngineConfig:
    app_env: str
    database_url: str
    redis_url: str

    queue_name: str
    consumer_group: str
    consumer_name: str
    batch_size: int
    block_ms: int

    policy_version: str
    delivery_policy_version: str

    operator_chat_id: int
    debug_chat_id: int | None
    digest_chat_id: int | None

    enable_later_delivery: bool
    enable_silent_later: bool
    enable_notification_send: bool

    render_profile_high: str
    render_profile_normal: str
    log_level: str

    @classmethod
    def from_env(cls) -> "PolicyEngineConfig":
        def _read(name: str, default: str = "") -> str:
            return os.getenv(name, default).strip()

        operator_chat_raw = _read("TELEGRAM_OPERATOR_CHAT_ID")
        debug_chat_raw = _read("TELEGRAM_DEBUG_CHAT_ID")
        digest_chat_raw = _read("TELEGRAM_DIGEST_CHAT_ID")

        cfg = cls(
            app_env=_read("APP_ENV", "dev").lower(),
            database_url=_read("DATABASE_URL"),
            redis_url=_read("REDIS_URL"),
            queue_name=_read("POLICY_ENGINE_QUEUE_NAME", "q.analysis.policy"),
            consumer_group=_read("POLICY_ENGINE_CONSUMER_GROUP", "policy-engine"),
            consumer_name=_read("POLICY_ENGINE_CONSUMER_NAME", "policy-engine-1"),
            batch_size=int(_read("POLICY_ENGINE_BATCH_SIZE", "20")),
            block_ms=int(_read("POLICY_ENGINE_BLOCK_MS", "5000")),
            policy_version=_read("VERDICT_POLICY_VERSION", "verdict_policy_v1"),
            delivery_policy_version=_read("DELIVERY_POLICY_VERSION", "delivery_policy_v1"),
            operator_chat_id=int(operator_chat_raw) if operator_chat_raw else 0,
            debug_chat_id=int(debug_chat_raw) if debug_chat_raw else None,
            digest_chat_id=int(digest_chat_raw) if digest_chat_raw else None,
            enable_later_delivery=_read("ENABLE_LATER_DELIVERY", "true").lower() == "true",
            enable_silent_later=_read("ENABLE_SILENT_LATER", "true").lower() == "true",
            enable_notification_send=_read("ENABLE_NOTIFICATION_SEND", "true").lower() == "true",
            render_profile_high=_read("NOTIFY_RENDER_PROFILE_HIGH", "telegram_single_alert_high_v1"),
            render_profile_normal=_read("NOTIFY_RENDER_PROFILE_NORMAL", "telegram_single_alert_normal_v1"),
            log_level=_read("LOG_LEVEL", "INFO").upper(),
        )
        cfg.validate()
        return cfg

    def validate(self) -> None:
        if not self.database_url:
            raise PolicyEngineConfigurationError("DATABASE_URL is required")
        if not self.redis_url:
            raise PolicyEngineConfigurationError("REDIS_URL is required")
        if not self.queue_name:
            raise PolicyEngineConfigurationError("POLICY_ENGINE_QUEUE_NAME must not be empty")
        if not self.consumer_group:
            raise PolicyEngineConfigurationError("POLICY_ENGINE_CONSUMER_GROUP must not be empty")
        if not self.consumer_name:
            raise PolicyEngineConfigurationError("POLICY_ENGINE_CONSUMER_NAME must not be empty")
        if self.batch_size <= 0 or self.batch_size > 100:
            raise PolicyEngineConfigurationError("POLICY_ENGINE_BATCH_SIZE must be between 1 and 100")
        if self.block_ms <= 0:
            raise PolicyEngineConfigurationError("POLICY_ENGINE_BLOCK_MS must be > 0")
        if self.operator_chat_id == 0:
            raise PolicyEngineConfigurationError("TELEGRAM_OPERATOR_CHAT_ID is required")
        if not self.policy_version:
            raise PolicyEngineConfigurationError("VERDICT_POLICY_VERSION must not be empty")
        if not self.delivery_policy_version:
            raise PolicyEngineConfigurationError("DELIVERY_POLICY_VERSION must not be empty")
```

### 9-3. `src/services/policy_engine/models.py`

```python
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Literal


Verdict = Literal["inspect_now", "later", "skip"]
DeliveryDecision = Literal["send_now", "send_digest", "suppress"]
UrgencyProfile = Literal["high", "normal_silent", "digest", "suppressed"]


@dataclass(slots=True, frozen=True)
class AnalysisPolicyJob:
    trigger_event_id: str
    event_type: str
    judge_run_id: str
    judge_output_id: str
    candidate_group_id: str
    bundle_id: str


@dataclass(slots=True, frozen=True)
class CandidatePolicyContext:
    candidate_group_id: str
    current_bundle_id: str | None
    current_analysis_id: str | None


@dataclass(slots=True, frozen=True)
class JudgeRunPolicyContext:
    judge_run_id: str
    bundle_id: str
    prompt_version: str
    policy_version: str
    status: str


@dataclass(slots=True, frozen=True)
class JudgeOutputPolicyContext:
    judge_output_id: str
    judge_run_id: str
    candidate_group_id: str
    payload_json: dict[str, Any]
    model_proposed_verdict: str | None
    model_confidence_band: str | None
    created_at: datetime


@dataclass(slots=True, frozen=True)
class BundlePolicyContext:
    bundle_id: str
    candidate_group_id: str
    current_primary_artifact_id: str
    current_primary_artifact_type: str
    created_at: datetime


@dataclass(slots=True, frozen=True)
class PolicyEvaluation:
    verdict: Verdict
    delivery_decision: DeliveryDecision
    urgency_profile: UrgencyProfile
    reason_codes: list[str]
    policy_reconciled_flag: bool


@dataclass(slots=True, frozen=True)
class AnalysisDraft:
    candidate_group_id: str
    judge_output_id: str
    schema_version: str
    policy_version: str
    prompt_version: str
    delivery_policy_version: str
    verdict: Verdict
    delivery_decision: DeliveryDecision
    scores_json: dict[str, Any]
    reason_codes_json: list[str]
    evidence_limitations_ko: str | None
    recommended_action_ko: str | None
    freshness_note_ko: str | None
    model_proposed_verdict: str | None
    policy_reconciled_flag: bool


@dataclass(slots=True, frozen=True)
class ExistingAnalysisRecord:
    analysis_id: str
    judge_output_id: str
    policy_version: str
    delivery_policy_version: str


@dataclass(slots=True, frozen=True)
class NotificationPlanIntent:
    notification_plan_id: str
    analysis_id: str
    candidate_group_id: str
    delivery_decision: DeliveryDecision
    urgency_profile: UrgencyProfile
    target_chat_id: int
    target_thread_id: int | None
    render_profile: str
    dedupe_subject_key: str
    material_change_hash: str
    send_after: str
    suppress_reason_code: str | None = None
```

### 9-4. `src/services/policy_engine/verdict_policy.py`

```python
from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from .models import Verdict


_GITHUB_PRIMARY_TYPES = {
    "github_repo",
    "github_subpath",
    "github_repo_page",
    "github_gist",
}

_TEXT_LIKE_PRIMARY_TYPES = {
    "x_post",
    "web_article",
    "text_idea",
}


@dataclass(slots=True, frozen=True)
class VerdictDecision:
    verdict: Verdict
    reason_codes: list[str]


class VerdictPolicy:
    def evaluate(
        self,
        *,
        scores: dict[str, Any],
        current_primary_artifact_type: str,
    ) -> VerdictDecision:
        practical = self._score(scores, "practical_usefulness")
        evidence = self._score(scores, "evidence_strength")
        confidence = self._score(scores, "confidence")
        hype = self._score(scores, "hype_penalty")
        code_quality = self._score(scores, "code_quality")
        specificity = self._score(scores, "specificity")

        if (
            practical >= 70
            and evidence >= 50
            and confidence >= 60
            and hype < 70
            and self._primary_gate(
                artifact_type=current_primary_artifact_type,
                code_quality=code_quality,
                specificity=specificity,
            )
        ):
            return VerdictDecision(
                verdict="inspect_now",
                reason_codes=["policy_threshold_inspect_now"],
            )

        if practical >= 45 and evidence >= 30 and confidence >= 35:
            return VerdictDecision(
                verdict="later",
                reason_codes=["policy_threshold_later"],
            )

        return VerdictDecision(
            verdict="skip",
            reason_codes=["policy_threshold_skip"],
        )

    def _primary_gate(
        self,
        *,
        artifact_type: str,
        code_quality: int,
        specificity: int,
    ) -> bool:
        if artifact_type in _GITHUB_PRIMARY_TYPES:
            return code_quality >= 65
        if artifact_type in _TEXT_LIKE_PRIMARY_TYPES:
            return specificity >= 60
        return False

    @staticmethod
    def _score(scores: dict[str, Any], key: str) -> int:
        value = scores.get(key)
        if isinstance(value, bool):
            return 0
        if isinstance(value, int):
            return value
        if isinstance(value, float):
            return int(value)
        return 0
```

### 9-5. `src/services/policy_engine/delivery_policy.py`

```python
from __future__ import annotations

from dataclasses import dataclass

from .models import DeliveryDecision, UrgencyProfile, Verdict


@dataclass(slots=True, frozen=True)
class DeliveryDecisionResult:
    delivery_decision: DeliveryDecision
    urgency_profile: UrgencyProfile
    suppress_reason_code: str | None = None


class DeliveryPolicy:
    def __init__(
        self,
        *,
        enable_later_delivery: bool,
        enable_silent_later: bool,
    ) -> None:
        self._enable_later_delivery = enable_later_delivery
        self._enable_silent_later = enable_silent_later

    def evaluate(self, *, verdict: Verdict) -> DeliveryDecisionResult:
        if verdict == "inspect_now":
            return DeliveryDecisionResult(
                delivery_decision="send_now",
                urgency_profile="high",
            )

        if verdict == "later":
            if not self._enable_later_delivery:
                return DeliveryDecisionResult(
                    delivery_decision="suppress",
                    urgency_profile="suppressed",
                    suppress_reason_code="later_delivery_disabled",
                )
            return DeliveryDecisionResult(
                delivery_decision="send_now",
                urgency_profile="normal_silent" if self._enable_silent_later else "normal_silent",
            )

        return DeliveryDecisionResult(
            delivery_decision="suppress",
            urgency_profile="suppressed",
            suppress_reason_code="verdict_skip",
        )
```

### 9-6. `src/services/policy_engine/notification_intent.py`

```python
from __future__ import annotations

import hashlib
import json
from dataclasses import asdict
from datetime import datetime, timezone
from uuid import uuid4

from .config import PolicyEngineConfig
from .models import AnalysisDraft, NotificationPlanIntent, PolicyEvaluation


class NotificationIntentBuilder:
    def __init__(self, *, config: PolicyEngineConfig) -> None:
        self._config = config

    def build(
        self,
        *,
        analysis_id: str,
        candidate_group_id: str,
        analysis: AnalysisDraft,
        evaluation: PolicyEvaluation,
    ) -> NotificationPlanIntent | None:
        if analysis.delivery_decision == "suppress":
            return None
        if not self._config.enable_notification_send:
            return None

        render_profile = (
            self._config.render_profile_high
            if evaluation.urgency_profile == "high"
            else self._config.render_profile_normal
        )

        material_change_hash = self._material_change_hash(
            candidate_group_id=candidate_group_id,
            verdict=analysis.verdict,
            delivery_decision=analysis.delivery_decision,
            urgency_profile=evaluation.urgency_profile,
            reason_codes=analysis.reason_codes_json,
            recommended_action_ko=analysis.recommended_action_ko,
            freshness_note_ko=analysis.freshness_note_ko,
        )

        return NotificationPlanIntent(
            notification_plan_id=str(uuid4()),
            analysis_id=analysis_id,
            candidate_group_id=candidate_group_id,
            delivery_decision=analysis.delivery_decision,
            urgency_profile=evaluation.urgency_profile,
            target_chat_id=self._config.operator_chat_id,
            target_thread_id=None,
            render_profile=render_profile,
            dedupe_subject_key=candidate_group_id,
            material_change_hash=material_change_hash,
            send_after=datetime.now(timezone.utc).isoformat(),
            suppress_reason_code=None,
        )

    @staticmethod
    def _material_change_hash(
        *,
        candidate_group_id: str,
        verdict: str,
        delivery_decision: str,
        urgency_profile: str,
        reason_codes: list[str],
        recommended_action_ko: str | None,
        freshness_note_ko: str | None,
    ) -> str:
        payload = {
            "candidate_group_id": candidate_group_id,
            "verdict": verdict,
            "delivery_decision": delivery_decision,
            "urgency_profile": urgency_profile,
            "reason_codes": reason_codes,
            "recommended_action_ko": recommended_action_ko,
            "freshness_note_ko": freshness_note_ko,
        }
        encoded = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        return hashlib.sha256(encoded.encode("utf-8")).hexdigest()
```

### 9-7. `src/services/policy_engine/repositories.py`

```python
from __future__ import annotations

import json
from contextlib import asynccontextmanager
from typing import Any

import sqlalchemy as sa
from sqlalchemy.ext.asyncio import AsyncSession

from .models import (
    AnalysisDraft,
    AnalysisPolicyJob,
    BundlePolicyContext,
    CandidatePolicyContext,
    ExistingAnalysisRecord,
    JudgeOutputPolicyContext,
    JudgeRunPolicyContext,
    NotificationPlanIntent,
)


def _jsonb_dumps(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False)


class PolicyEngineRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    @asynccontextmanager
    async def transaction(self):
        if self._session.in_transaction():
            yield self._session
            return
        async with self._session.begin():
            yield self._session

    async def load_job_by_trigger_event_id(self, trigger_event_id: str) -> AnalysisPolicyJob | None:
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
        if row is None or str(row["event_type"]) != "analysis.policy.apply.v1":
            return None

        payload = row["payload_json"] or {}
        return AnalysisPolicyJob(
            trigger_event_id=str(row["event_id"]),
            event_type=str(row["event_type"]),
            judge_run_id=str(payload["judge_run_id"]),
            judge_output_id=str(payload["judge_output_id"]),
            candidate_group_id=str(payload["candidate_group_id"]),
            bundle_id=str(payload["bundle_id"]),
        )

    async def load_candidate_context(self, candidate_group_id: str) -> CandidatePolicyContext | None:
        result = await self._session.execute(
            sa.text(
                """
                SELECT candidate_group_id, current_bundle_id, current_analysis_id
                FROM candidate_group_proposals
                WHERE candidate_group_id = CAST(:candidate_group_id AS uuid)
                """
            ),
            {"candidate_group_id": candidate_group_id},
        )
        row = result.mappings().first()
        if row is None:
            return None
        return CandidatePolicyContext(
            candidate_group_id=str(row["candidate_group_id"]),
            current_bundle_id=str(row["current_bundle_id"]) if row["current_bundle_id"] else None,
            current_analysis_id=str(row["current_analysis_id"]) if row["current_analysis_id"] else None,
        )

    async def load_judge_run(self, judge_run_id: str) -> JudgeRunPolicyContext | None:
        result = await self._session.execute(
            sa.text(
                """
                SELECT judge_run_id, bundle_id, prompt_version, policy_version, status
                FROM judge_runs
                WHERE judge_run_id = CAST(:judge_run_id AS uuid)
                """
            ),
            {"judge_run_id": judge_run_id},
        )
        row = result.mappings().first()
        if row is None:
            return None
        return JudgeRunPolicyContext(
            judge_run_id=str(row["judge_run_id"]),
            bundle_id=str(row["bundle_id"]),
            prompt_version=str(row["prompt_version"]),
            policy_version=str(row["policy_version"]),
            status=str(row["status"]),
        )

    async def load_judge_output(self, judge_output_id: str) -> JudgeOutputPolicyContext | None:
        result = await self._session.execute(
            sa.text(
                """
                SELECT judge_output_id, judge_run_id, candidate_group_id,
                       payload_json, model_proposed_verdict, model_confidence_band, created_at
                FROM judge_outputs
                WHERE judge_output_id = CAST(:judge_output_id AS uuid)
                """
            ),
            {"judge_output_id": judge_output_id},
        )
        row = result.mappings().first()
        if row is None:
            return None
        return JudgeOutputPolicyContext(
            judge_output_id=str(row["judge_output_id"]),
            judge_run_id=str(row["judge_run_id"]),
            candidate_group_id=str(row["candidate_group_id"]),
            payload_json=row["payload_json"] or {},
            model_proposed_verdict=str(row["model_proposed_verdict"]) if row["model_proposed_verdict"] else None,
            model_confidence_band=str(row["model_confidence_band"]) if row["model_confidence_band"] else None,
            created_at=row["created_at"],
        )

    async def load_bundle_context(self, bundle_id: str) -> BundlePolicyContext | None:
        result = await self._session.execute(
            sa.text(
                """
                SELECT ceb.bundle_id,
                       ceb.candidate_group_id,
                       ceb.current_primary_artifact_id,
                       ar.artifact_type AS current_primary_artifact_type,
                       ceb.created_at
                FROM candidate_evidence_bundles ceb
                JOIN artifact_registry ar
                  ON ar.artifact_id = ceb.current_primary_artifact_id
                WHERE ceb.bundle_id = CAST(:bundle_id AS uuid)
                """
            ),
            {"bundle_id": bundle_id},
        )
        row = result.mappings().first()
        if row is None:
            return None
        return BundlePolicyContext(
            bundle_id=str(row["bundle_id"]),
            candidate_group_id=str(row["candidate_group_id"]),
            current_primary_artifact_id=str(row["current_primary_artifact_id"]),
            current_primary_artifact_type=str(row["current_primary_artifact_type"]),
            created_at=row["created_at"],
        )

    async def load_existing_analysis(
        self,
        *,
        judge_output_id: str,
        policy_version: str,
        delivery_policy_version: str,
    ) -> ExistingAnalysisRecord | None:
        result = await self._session.execute(
            sa.text(
                """
                SELECT analysis_id, judge_output_id, policy_version, delivery_policy_version
                FROM analyses
                WHERE judge_output_id = CAST(:judge_output_id AS uuid)
                  AND policy_version = :policy_version
                  AND delivery_policy_version = :delivery_policy_version
                """
            ),
            {
                "judge_output_id": judge_output_id,
                "policy_version": policy_version,
                "delivery_policy_version": delivery_policy_version,
            },
        )
        row = result.mappings().first()
        if row is None:
            return None
        return ExistingAnalysisRecord(
            analysis_id=str(row["analysis_id"]),
            judge_output_id=str(row["judge_output_id"]),
            policy_version=str(row["policy_version"]),
            delivery_policy_version=str(row["delivery_policy_version"]),
        )

    async def insert_analysis(self, draft: AnalysisDraft) -> str:
        result = await self._session.execute(
            sa.text(
                """
                INSERT INTO analyses (
                    analysis_id,
                    candidate_group_id,
                    judge_output_id,
                    schema_version,
                    policy_version,
                    prompt_version,
                    delivery_policy_version,
                    verdict,
                    delivery_decision,
                    scores_json,
                    reason_codes_json,
                    evidence_limitations_ko,
                    recommended_action_ko,
                    freshness_note_ko,
                    model_proposed_verdict,
                    policy_reconciled_flag,
                    created_at
                ) VALUES (
                    gen_random_uuid(),
                    CAST(:candidate_group_id AS uuid),
                    CAST(:judge_output_id AS uuid),
                    :schema_version,
                    :policy_version,
                    :prompt_version,
                    :delivery_policy_version,
                    CAST(:verdict AS verdict_enum),
                    CAST(:delivery_decision AS delivery_decision_enum),
                    CAST(:scores_json AS jsonb),
                    CAST(:reason_codes_json AS jsonb),
                    :evidence_limitations_ko,
                    :recommended_action_ko,
                    :freshness_note_ko,
                    CAST(:model_proposed_verdict AS verdict_enum),
                    :policy_reconciled_flag,
                    now()
                )
                ON CONFLICT (judge_output_id, policy_version, delivery_policy_version)
                DO NOTHING
                RETURNING analysis_id
                """
            ),
            {
                "candidate_group_id": draft.candidate_group_id,
                "judge_output_id": draft.judge_output_id,
                "schema_version": draft.schema_version,
                "policy_version": draft.policy_version,
                "prompt_version": draft.prompt_version,
                "delivery_policy_version": draft.delivery_policy_version,
                "verdict": draft.verdict,
                "delivery_decision": draft.delivery_decision,
                "scores_json": _jsonb_dumps(draft.scores_json),
                "reason_codes_json": _jsonb_dumps(draft.reason_codes_json),
                "evidence_limitations_ko": draft.evidence_limitations_ko,
                "recommended_action_ko": draft.recommended_action_ko,
                "freshness_note_ko": draft.freshness_note_ko,
                "model_proposed_verdict": draft.model_proposed_verdict,
                "policy_reconciled_flag": draft.policy_reconciled_flag,
            },
        )
        analysis_id = result.scalar_one_or_none()
        if analysis_id is None:
            existing = await self.load_existing_analysis(
                judge_output_id=draft.judge_output_id,
                policy_version=draft.policy_version,
                delivery_policy_version=draft.delivery_policy_version,
            )
            if existing is None:
                raise RuntimeError("analysis insert conflicted but existing row was not found")
            return existing.analysis_id
        return str(analysis_id)

    async def insert_state_transition(
        self,
        *,
        object_type: str,
        object_id: str,
        from_state: str | None,
        to_state: str,
        reason_code: str,
    ) -> None:
        await self._session.execute(
            sa.text(
                """
                INSERT INTO state_transitions (
                    state_transition_id,
                    object_type,
                    object_id,
                    from_state,
                    to_state,
                    reason_code,
                    created_at
                ) VALUES (
                    gen_random_uuid(),
                    :object_type,
                    CAST(:object_id AS uuid),
                    :from_state,
                    :to_state,
                    :reason_code,
                    now()
                )
                """
            ),
            {
                "object_type": object_type,
                "object_id": object_id,
                "from_state": from_state,
                "to_state": to_state,
                "reason_code": reason_code,
            },
        )

    async def insert_notification_plan_created_outbox(self, intent: NotificationPlanIntent) -> None:
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
                "analysis_id": intent.analysis_id,
                "dedupe_key": f"notify_plan:{intent.analysis_id}:{intent.target_chat_id}:{intent.material_change_hash}",
                "payload_json": _jsonb_dumps(
                    {
                        "notification_plan_id": intent.notification_plan_id,
                        "analysis_id": intent.analysis_id,
                        "candidate_group_id": intent.candidate_group_id,
                        "delivery_decision": intent.delivery_decision,
                        "urgency_profile": intent.urgency_profile,
                        "target_chat_id": intent.target_chat_id,
                        "target_thread_id": intent.target_thread_id,
                        "render_profile": intent.render_profile,
                        "dedupe_subject_key": intent.dedupe_subject_key,
                        "material_change_hash": intent.material_change_hash,
                        "send_after": intent.send_after,
                        "suppress_reason_code": intent.suppress_reason_code,
                    }
                ),
            },
        )
```

### 9-8. `src/services/policy_engine/service.py`

```python
from __future__ import annotations

import logging

from .config import PolicyEngineConfig
from .delivery_policy import DeliveryPolicy
from .models import AnalysisDraft, AnalysisPolicyJob, PolicyEvaluation
from .notification_intent import NotificationIntentBuilder
from .repositories import PolicyEngineRepository
from .verdict_policy import VerdictPolicy


class PolicyEngineService:
    def __init__(
        self,
        config: PolicyEngineConfig,
        *,
        repository: PolicyEngineRepository,
        verdict_policy: VerdictPolicy,
        delivery_policy: DeliveryPolicy,
        notification_intent_builder: NotificationIntentBuilder,
        logger: logging.Logger | None = None,
    ) -> None:
        self._config = config
        self._repository = repository
        self._verdict_policy = verdict_policy
        self._delivery_policy = delivery_policy
        self._notification_intent_builder = notification_intent_builder
        self._logger = logger or logging.getLogger(__name__)

    async def rehydrate_job(self, trigger_event_id: str) -> AnalysisPolicyJob | None:
        return await self._repository.load_job_by_trigger_event_id(trigger_event_id)

    async def handle_job(self, job: AnalysisPolicyJob) -> None:
        candidate = await self._repository.load_candidate_context(job.candidate_group_id)
        if candidate is None:
            return
        if candidate.current_bundle_id != job.bundle_id:
            async with self._repository.transaction():
                await self._repository.insert_state_transition(
                    object_type="candidate_group",
                    object_id=job.candidate_group_id,
                    from_state="analysis_validated",
                    to_state="analysis_policy_stale_bundle",
                    reason_code="policy_stale_bundle_request",
                )
            return

        judge_run = await self._repository.load_judge_run(job.judge_run_id)
        judge_output = await self._repository.load_judge_output(job.judge_output_id)
        bundle = await self._repository.load_bundle_context(job.bundle_id)
        if judge_run is None or judge_output is None or bundle is None:
            async with self._repository.transaction():
                await self._repository.insert_state_transition(
                    object_type="candidate_group",
                    object_id=job.candidate_group_id,
                    from_state="analysis_validated",
                    to_state="analysis_policy_failed",
                    reason_code="policy_missing_context",
                )
            return

        if judge_output.judge_run_id != job.judge_run_id:
            async with self._repository.transaction():
                await self._repository.insert_state_transition(
                    object_type="candidate_group",
                    object_id=job.candidate_group_id,
                    from_state="analysis_validated",
                    to_state="analysis_policy_failed",
                    reason_code="policy_judge_output_mismatch",
                )
            return

        if bundle.candidate_group_id != job.candidate_group_id or judge_output.candidate_group_id != job.candidate_group_id:
            async with self._repository.transaction():
                await self._repository.insert_state_transition(
                    object_type="candidate_group",
                    object_id=job.candidate_group_id,
                    from_state="analysis_validated",
                    to_state="analysis_policy_failed",
                    reason_code="policy_candidate_identity_mismatch",
                )
            return

        existing = await self._repository.load_existing_analysis(
            judge_output_id=job.judge_output_id,
            policy_version=self._config.policy_version,
            delivery_policy_version=self._config.delivery_policy_version,
        )
        if existing is not None:
            return

        payload = judge_output.payload_json or {}
        scores = payload.get("scores") if isinstance(payload.get("scores"), dict) else {}
        verdict_decision = self._verdict_policy.evaluate(
            scores=scores,
            current_primary_artifact_type=bundle.current_primary_artifact_type,
        )
        delivery_decision = self._delivery_policy.evaluate(verdict=verdict_decision.verdict)

        reason_codes = [
            *(payload.get("reason_codes") if isinstance(payload.get("reason_codes"), list) else []),
            *verdict_decision.reason_codes,
        ]

        policy_reconciled_flag = True
        model_proposed_verdict = judge_output.model_proposed_verdict
        if model_proposed_verdict and model_proposed_verdict != verdict_decision.verdict:
            policy_reconciled_flag = False
            reason_codes.append("policy_overrode_model_verdict")

        analysis = AnalysisDraft(
            candidate_group_id=job.candidate_group_id,
            judge_output_id=job.judge_output_id,
            schema_version="analysis_v1",
            policy_version=self._config.policy_version,
            prompt_version=judge_run.prompt_version,
            delivery_policy_version=self._config.delivery_policy_version,
            verdict=verdict_decision.verdict,
            delivery_decision=delivery_decision.delivery_decision,
            scores_json=scores,
            reason_codes_json=reason_codes,
            evidence_limitations_ko=payload.get("evidence_limitations_ko"),
            recommended_action_ko=payload.get("recommended_action_ko"),
            freshness_note_ko=payload.get("freshness_note_ko"),
            model_proposed_verdict=model_proposed_verdict,
            policy_reconciled_flag=policy_reconciled_flag,
        )

        async with self._repository.transaction():
            analysis_id = await self._repository.insert_analysis(analysis)
            await self._repository.insert_state_transition(
                object_type="analysis",
                object_id=analysis_id,
                from_state="analysis_validated",
                to_state="analysis_finalized" if analysis.delivery_decision != "suppress" else "analysis_suppressed",
                reason_code=f"policy_applied:{analysis.verdict}:{analysis.delivery_decision}",
            )

            intent = self._notification_intent_builder.build(
                analysis_id=analysis_id,
                candidate_group_id=job.candidate_group_id,
                analysis=analysis,
                evaluation=PolicyEvaluation(
                    verdict=analysis.verdict,
                    delivery_decision=analysis.delivery_decision,
                    urgency_profile=delivery_decision.urgency_profile,
                    reason_codes=reason_codes,
                    policy_reconciled_flag=policy_reconciled_flag,
                ),
            )
            if intent is not None:
                await self._repository.insert_notification_plan_created_outbox(intent)
```

### 9-9. `src/services/policy_engine/worker.py`

```python
from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass

from ..gh_enricher.redis_streams import RedisStreamConsumer
from .config import PolicyEngineConfig
from .service import PolicyEngineService


@dataclass(slots=True, frozen=True)
class WorkerBatchResult:
    processed: int = 0
    acked: int = 0


class PolicyEngineWorker:
    def __init__(
        self,
        config: PolicyEngineConfig,
        *,
        consumer: RedisStreamConsumer,
        service: PolicyEngineService,
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
            "policy_engine_worker_started",
            extra={
                "service": "policy-engine",
                "event": "policy_engine_worker_started",
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
        for message in messages:
            processed += 1
            trigger_event_id = message.fields.get("trigger_event_id")
            if trigger_event_id:
                job = await self._service.rehydrate_job(trigger_event_id)
                if job is not None:
                    await self._service.handle_job(job)
            await self._consumer.ack(message.message_id)
            acked += 1
        return WorkerBatchResult(processed=processed, acked=acked)
```

### 9-10. `src/services/policy_engine/main.py`

```python
from __future__ import annotations

import asyncio
import logging
import sys

from redis.asyncio import Redis
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from ..gh_enricher.redis_streams import RedisStreamConsumer
from .config import PolicyEngineConfig
from .delivery_policy import DeliveryPolicy
from .notification_intent import NotificationIntentBuilder
from .repositories import PolicyEngineRepository
from .service import PolicyEngineService
from .verdict_policy import VerdictPolicy
from .worker import PolicyEngineWorker


def _configure_logging(log_level: str) -> None:
    logging.basicConfig(
        level=getattr(logging, log_level, logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
        stream=sys.stdout,
    )


async def _run() -> int:
    config = PolicyEngineConfig.from_env()
    _configure_logging(config.log_level)
    logger = logging.getLogger("policy_engine")

    engine = create_async_engine(config.database_url, future=True)
    session_factory = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    redis_client = Redis.from_url(config.redis_url, decode_responses=True)

    try:
        async with session_factory() as session:
            repository = PolicyEngineRepository(session)
            service = PolicyEngineService(
                config,
                repository=repository,
                verdict_policy=VerdictPolicy(),
                delivery_policy=DeliveryPolicy(
                    enable_later_delivery=config.enable_later_delivery,
                    enable_silent_later=config.enable_silent_later,
                ),
                notification_intent_builder=NotificationIntentBuilder(config=config),
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
            worker = PolicyEngineWorker(
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

---

## 10. 테스트 초안 포인트

### `tests/unit/services/policy_engine/test_verdict_policy_github.py`

검증:

- GitHub primary에서 `inspect_now` 조건이 모두 충족되면 `inspect_now`
- `code_quality < 65`면 inspect_now 금지
- `evidence_strength < 50`면 inspect_now 금지

### `tests/unit/services/policy_engine/test_verdict_policy_text_like.py`

검증:

- `x_post` / `web_article` / `text_idea` primary에서 `specificity >= 60`이면 inspect_now 가능
- `specificity < 60`이면 inspect_now 금지
- 기본 later/skip 분기가 안정적인지

### `tests/unit/services/policy_engine/test_delivery_policy_default_mapping.py`

검증:

- `inspect_now -> send_now/high`
- `later -> send_now/normal_silent` (default flags)
- `skip -> suppress/suppressed`
- `ENABLE_LATER_DELIVERY=false`면 `later -> suppress`

### `tests/unit/services/policy_engine/test_policy_reconciled_flag.py`

검증:

- model proposed verdict와 policy verdict가 같으면 `policy_reconciled_flag=true`
- 다르면 `false` + `policy_overrode_model_verdict` 추가

### `tests/unit/services/policy_engine/test_notification_intent_hash.py`

검증:

- 같은 analysis material이면 `material_change_hash`가 안정적인지
- `analysis_id`가 달라도 material이 같으면 hash가 같게 유지되는지

### `tests/component/services/policy_engine/test_worker_rehydrates_policy_apply.py`

검증:

- Redis payload는 thin message
- `event_outbox` 기준으로 `analysis.policy.apply.v1` rehydrate

### `tests/component/services/policy_engine/test_valid_output_writes_analysis_and_notification_intent.py`

검증:

- valid structured payload면 `analyses` insert
- `notification.plan.created.v1` outbox insert
- `state_transitions` append

### `tests/component/services/policy_engine/test_suppress_writes_analysis_without_notification_intent.py`

검증:

- final `delivery_decision = suppress` 이면
- `analyses` row는 남고
- `notification.plan.created.v1`는 생기지 않는지

### `tests/component/services/policy_engine/test_existing_analysis_reuse.py`

검증:

- 동일 `(judge_output_id, policy_version, delivery_policy_version)` existing row가 있으면
- 새 analysis와 새 notification intent가 생기지 않는지

### `tests/component/services/policy_engine/test_stale_bundle_request_noop.py`

검증:

- payload.bundle_id와 `candidate_group_proposals.current_bundle_id`가 다르면
- analysis append / notification intent 없이 종료되는지

---

## 11. 이번 단계가 구조를 지키는 이유

1. `policy-engine`는 `analyses`, `state_transitions`, `event_outbox`만 직접 쓴다.  
   즉, service ownership을 넘지 않는다.

2. final verdict와 delivery는 **judge output에서 deterministic 재계산** 한다.  
   즉, stage 0과 stage 6의 분리가 유지된다.

3. `notification_plans` row를 직접 쓰지 않고 plan-intent event만 만든다.  
   즉, stage 7 notifier ownership을 침범하지 않는다.

4. stale bundle request를 no-op 처리한다.  
   즉, current pointer와 history가 다시 섞이지 않는다.

5. Prompt Guard / AgentLinter / MemKraft / skill discipline을 deterministic hot path에 넣지 않는다.  
   즉, application plan의 좋은 방향은 보존하되 current contracts는 흔들지 않는다.

---

## 12. 다음 단계

이 단계가 닫히면 다음 구현 순서는 하나다.

1. `38_notifier_telegram_skeleton_and_code_draft_v0_1.md`

즉, 이제 stage 6 judge pipeline은 operationally 닫혔고, 다음은 **delivery layer 본체인 `notifier-telegram`** 을 붙이는 것이 맞다.

---

## 최종 한 줄 결론

이번 단계의 최소-change 정답은 **`analysis.policy.apply.v1`를 rehydrate해 current bundle 정합성과 judge/output identity를 재검증하고, stage 0 verdict 규칙과 stage 7 delivery 기본 규칙으로 final `analysis_v1`을 deterministic하게 계산해 `analyses`에 append하고, non-suppress 결과만 `notification.plan.created.v1` plan-intent event로 넘기는 `policy-engine` v0.1을 닫는 것** 이다.


---

## Source file: `38_notifier_telegram_skeleton_and_code_draft_v0_1.md`

# 38단계: `notifier-telegram` 스켈레톤 + 실제 코드 초안 v0.1

## 0. 문서 목적

이 문서는 이미 잠긴 구조를 다시 설계하는 문서가 아니다.  
목적은 **37단계 `policy-engine`이 만들어낸 `notification.plan.created.v1` plan-intent event를 받아, 실제 Telegram 전송 경계로 연결하는 좁은 delivery 계층**을 구현 가능한 수준으로 내리는 것이다.

이번 단계에서 고정하는 것은 아래 여덟 가지다.

1. `notification.plan.created.v1` 소비 경계를 코드로 고정
2. thin Redis payload → `event_outbox` 재조회 → `analyses` / `judge_outputs` / `candidate_group_proposals` / `artifact_registry` / `source_messages` rehydrate 경계를 고정
3. `notifier-telegram`의 좁은 책임을 **plan concretization / render / send-or-edit / delivery record append / `notification.delivery.result.v1` emit** 으로 고정
4. `notification_plans`, `notification_renders`, `notification_delivery_records`, `state_transitions`, `event_outbox`만 직접 쓰는 **service ownership** 을 고정
5. 37단계가 plan row 대신 **plan-intent event만 emit** 하도록 둔 최소-change bridge를, notifier에서 `notification_plans` 실제 row 생성으로 닫음
6. 7단계 전달 정책 문서가 잠근 **text message + inline keyboard + explicit entities + preview disabled + single-shot send / material edit only** 규칙을 구현 경계로 고정
7. `03_GitHub_AI_application_plan.md`의 외부 자산 제안을 검토하되, **Prompt Guard / AgentLinter / MemKraft / skill discipline을 notifier runtime hot path에 새 lifecycle로 삽입하지 않는 최소-change 해석** 을 유지
8. 현재까지 잠긴 구현 체인을 **delivery layer까지 닫고**, 다음 턴에서 consolidation 또는 acceptance hardening으로 넘어갈 수 있게 current contracts를 안정화

핵심 전제는 유지한다.

- `notifier-telegram`은 **policy-engine** 이 아니다.
- `notifier-telegram`은 **judge** 가 아니다.
- `notifier-telegram`은 **collector / normalizer / enricher** 가 아니다.
- `notifier-telegram`은 **최종 verdict / delivery decision을 다시 계산하지 않는다.**
- `notifier-telegram`은 **Analysis → Notification 변환 + Telegram delivery 경계** 다.

---

## 1. 왜 지금 이 단계가 정확한 다음 단계인가

현재 authoritative README는 최신 진행 상태를 **37단계 `policy-engine`까지 완료**로 보고, 다음 구현 순서를 **`38_notifier_telegram_skeleton_and_code_draft_v0_1.md`** 하나로 고정한다.  
또한 37단계 문서는 `policy-engine`이 `notification_plans` row를 직접 쓰지 않고, 대신 **`notification.plan.created.v1`를 plan-intent event로 emit** 하도록 잠갔다. 즉, 그 bridge를 실제 delivery row와 Telegram 전송으로 닫는 다음 단계는 `notifier-telegram`이 맞다.

즉, 지금 다시 collector / normalizer / enricher / assembler / judge / policy 계층을 여는 것은 순서상 후퇴다.  
이제 붙여야 하는 것은 **stage 7 delivery layer 본체인 `notifier-telegram`** 이다.

---

## 2. 이번 단계에서 확인한 충돌과 최소-change 해석

### 충돌 A — 현재 소스에는 README v6 ~ v11이 함께 존재할 수 있다

현재 프로젝트 소스에는 이전 README 중간본들이 함께 남아 있을 수 있다.

- v6: latest = 32
- v7: latest = 33
- v8: latest = 34
- v9: latest = 35
- v10: latest = 36
- v11: latest = 37

### 최소-change 해석 A

- **v11만 phase authority** 로 사용한다.
- v6 ~ v10은 이력성 중간본으로 본다.
- 이번 38단계 문서와 README 업데이트에서는 **v11을 이어받아 v12로만 승격** 한다.

즉, phase ordering은 최신 README 하나로 수렴시키고, 오래된 README는 더 이상 authority로 쓰지 않는다.

---

### 충돌 B — 37단계는 `notification.plan.created.v1`를 emit하지만, 실제 `notification_plans` row는 아직 없다

37단계는 notifier ownership을 지키기 위해 아래처럼 잠갔다.

- `policy-engine`는 `notification_plans` row를 직접 쓰지 않는다.
- 대신 `notification.plan.created.v1`를 **plan-intent event** 로 emit한다.
- payload에는 `notification_plan_id`, `analysis_id`, `target_chat_id`, `send_after`와 richer intent fields가 포함된다.

즉, 현재 이벤트 계약 이름만 보면 plan이 이미 만들어진 것처럼 보이지만, 실제 durable row는 아직 없다.

### 최소-change 해석 B

이번 v0.1에서는 아래처럼 고정한다.

1. `notifier-telegram`이 `notification.plan.created.v1`를 rehydrate 한다.
2. event payload의 `notification_plan_id`를 **그대로 사용해 `notification_plans` row를 생성** 한다.
3. 이후 render / send / edit / delivery record를 이어서 처리한다.

즉, event 이름은 유지하되 실제 ownership은 notifier가 가진다.  
이 해석이 event contract, queue routing, notifier ownership을 동시에 살리는 가장 작은 변경이다.

---

### 충돌 C — `analyses` row에는 최종 verdict/delivery는 있지만, headline/summary/skeptical_take 등 렌더용 필드가 전부 있지 않다

현재 schema와 단계 문서를 그대로 따르면서 생기는 작은 비틀림이 있다.

- `analyses`는 최종 `analysis_v1` durable truth다.
- 하지만 headline, `summary_one_line_ko`, `skeptical_take_ko`, comparables, red flags 같은 **표면 렌더용 텍스트는 주로 `judge_outputs.payload_json`** 에 있다.

따라서 notifier가 `analyses`만 읽으면 stage 7 템플릿을 충분히 채우기 어렵다.

### 최소-change 해석 C

이번 v0.1에서는 아래처럼 고정한다.

1. **최종 verdict / delivery / urgency / suppress 여부는 `analyses`를 기준** 으로 본다.
2. **headline / summary / skeptical_take / comparables / red_flags는 `judge_outputs.payload_json`을 보조 렌더 source** 로 사용한다.
3. notifier는 이 두 소스를 **의미를 바꾸지 않고 조합만** 한다.
4. notifier는 `judge_output`을 수정하거나 재판정하지 않는다.

즉, `analysis_v1`의 집행 결과와 `judge_output_v1`의 표현 텍스트를 분리한 기존 구조를 그대로 살린다.

---

### 충돌 D — stage 7 정본은 notifier 입력을 `analysis_v1 + delivery_policy_applied`로 설명하지만, 현재 runtime bridge는 `analysis + plan-intent event` 다

정본 7단계 문서는 notifier 입력을 개념적으로 아래처럼 설명한다.

- `analysis_v1`
- `delivery_policy_applied`

반면 현재 구현 체인은 아래처럼 잠겼다.

- `policy-engine`가 `analyses` row append
- 이어서 `notification.plan.created.v1` plan-intent event emit

### 최소-change 해석 D

이번 v0.1에서는 아래처럼 고정한다.

- **개념적 입력**: `analysis_v1 + delivery policy intent`
- **실제 runtime 입력**: `notification.plan.created.v1`
- **durable rehydration source**:
  - `analyses`
  - `judge_outputs`
  - `candidate_group_proposals`
  - `artifact_registry`
  - `source_messages`

즉, 현재 runtime bridge는 정본의 의미를 깨는 것이 아니라, **그 의미를 event-driven 실행형태로 구체화한 것**으로 해석한다.

---

### 충돌 E — application plan의 외부 자산 제안을 notifier runtime에 바로 넣으면 presentation boundary가 흔들린다

application plan의 방향은 맞다.

- Prompt Guard
- AgentLinter
- MemKraft
- skill/playbook discipline

하지만 이걸 notifier runtime에 넣으면 아래 문제가 생긴다.

- Prompt Guard가 message send 차단/격리 lifecycle을 새로 만들 수 있음
- AgentLinter가 runtime 로직처럼 섞일 수 있음
- MemKraft가 render/send hot path에 들어와 deterministic delivery를 흔들 수 있음

### 최소-change 해석 E

이번 38단계에는 아래만 유지한다.

- **Prompt Guard**: judge-openai sanitize-only preflight까지만 인정, notifier는 새 risk lifecycle을 만들지 않음
- **AgentLinter**: repo/CI hygiene 자산으로만 유지
- **MemKraft**: ops-memory sidecar로만 유지
- **skill/playbook discipline**: prompt/profile handbook 문서화 자산으로만 유지

즉, `notifier-telegram`은 **presentation/delivery boundary** 로만 남는다.

---

## 3. `notifier-telegram`의 책임과 비책임

### 3-1. 반드시 하는 일

- `notification.plan.created.v1` 소비
- `event_outbox` 기준 request rehydrate
- `notification_plans` durable row concretize
- `analyses` / `judge_outputs` / `candidate_group_proposals` / `artifact_registry` / `source_messages` 재조회
- explicit entity 중심 render 생성
- inline keyboard 구성
- `sendMessage` 또는 `editMessageText` 결정
- `notification_renders` append
- `notification_delivery_records` append
- `state_transitions` append
- `notification.delivery.result.v1` outbox emit

### 3-2. 하면 안 되는 일

- verdict 재계산
- delivery decision override
- judge output 수정
- comparables / skeptical_take 임의 재작성
- raw source rescan을 통한 새 candidate 해석
- Prompt Guard 기반 차단/격리 상태 생성
- inbound command plane 도입
- digest planner를 본 턴에 새로 설계·활성화

즉, 이 서비스는 **의미를 바꾸지 않는 render + delivery 계층** 이다.

---

## 4. 직접 소유하는 durable 경계

execution contracts 기준으로 `notifier-telegram`은 아래만 직접 쓴다.

- `notification_plans`
- `notification_renders`
- `notification_delivery_records`
- `state_transitions`
- `event_outbox`

읽는 것:

- `analyses`
- `judge_outputs`
- `candidate_group_proposals`
- `artifact_registry`
- `source_messages`

즉, `analyses`, `judge_outputs`, `candidate_group_proposals`는 **read-only render source** 로만 사용한다.

---

## 5. 입력/출력 계약

### 5-1. 입력 이벤트

허용 입력은 아래 하나로 좁게 고정한다.

- `notification.plan.created.v1`

Redis Streams 메시지는 여전히 thin payload다.

```json
{
  "job_id": "<event_id>",
  "stage_name": "notify",
  "root_object_type": "analysis",
  "root_object_id": "<analysis_id>",
  "idempotency_key": "<dedupe_key>",
  "pipeline_run_id": "",
  "not_before": "",
  "trigger_event_id": "<event_id>"
}
```

즉, consumer는 Redis 본문을 business source처럼 쓰지 않고, 반드시 `trigger_event_id`로 `event_outbox`를 다시 조회한다.

### 5-2. 입력 payload 최소 필드

`notification.plan.created.v1` payload는 아래를 믿는다.

- `notification_plan_id`
- `analysis_id`
- `target_chat_id`
- `send_after`

실제 v0.1 payload에는 아래도 포함된다.

- `candidate_group_id`
- `delivery_decision`
- `urgency_profile`
- `render_profile`
- `dedupe_subject_key`
- `material_change_hash`
- `target_thread_id`
- `suppress_reason_code`

하지만 이 값들은 **request hint + durable concretization seed** 일 뿐이다. 최종 send/edit 판단은 DB 재조회와 기존 delivery history까지 확인해 재검증한다.

### 5-3. 출력 이벤트

허용 출력은 아래 하나로 좁게 고정한다.

- `notification.delivery.result.v1`

payload 최소 필드:

- `notification_plan_id`
- `delivery_status`
- `telegram_chat_id`
- `telegram_message_id`

실제 v0.1 payload에는 아래도 같이 싣는다.

- `notification_delivery_record_id`
- `attempt_count`
- `transport_error_code`
- `transport_error_class`
- `edited`

즉, maintenance / observability / replay가 이후 이 durable transport 결과를 따라갈 수 있다.

---

## 6. 이번 단계에서 고정할 핵심 처리 규칙

### 6-1. missing / mismatched intent는 terminal stop

아래면 notifier는 render/send를 시도하면 안 된다.

- `event_outbox` row 없음
- `analysis` row 없음
- `analysis_id != payload.analysis_id`
- `candidate_group_id != payload.candidate_group_id`
- `delivery_decision = suppress`
- `target_chat_id` 없음

이 경우:

- `notification_plans` row 생성 안 함 또는 `suppressed` 상태로만 남김
- `notification_renders` / `notification_delivery_records` 없음
- `state_transitions`에 failure/suppress reason만 남김

즉, notifier는 **policy-engine이 이미 suppress했거나 identity가 깨진 입력을 강제로 보내지 않는다.**

### 6-2. plan concretization은 notifier가 수행한다

`notification.plan.created.v1`를 받으면 아래 순서로 고정한다.

1. payload의 `notification_plan_id`를 그대로 사용
2. 같은 `notification_plan_id`가 있으면 existing row reuse
3. 없으면 `notification_plans` insert
4. unique `(analysis_id, target_chat_id, material_change_hash)`와 충돌하면 existing row reuse

즉, notifier는 **plan-intent를 durable plan row로 concretize** 하는 첫 실제 소유자다.

### 6-3. render source는 `analysis_v1` 우선, `judge_output_v1` 보조

render 시 의미 계층을 아래처럼 고정한다.

#### final truth
- `analyses.verdict`
- `analyses.delivery_decision`
- `analyses.reason_codes_json`
- `analyses.evidence_limitations_ko`
- `analyses.recommended_action_ko`
- `analyses.freshness_note_ko`

#### render text supplement
- `judge_outputs.payload_json.headline`
- `judge_outputs.payload_json.summary_one_line_ko`
- `judge_outputs.payload_json.skeptical_take_ko`
- `judge_outputs.payload_json.comparables`
- `judge_outputs.payload_json.red_flags_ko`
- `judge_outputs.payload_json.model_confidence_band`

즉, notifier는 **final verdict는 analysis에서, human-facing phrasing은 judge_output에서** 가져오되 둘을 다시 재판정하지 않는다.

### 6-4. text message + inline keyboard + explicit entities를 기본으로 고정한다

v0.1 기본은 아래다.

- Telegram API: `sendMessage`
- 수정 시: `editMessageText`
- format: **explicit entities 우선**
- `parse_mode`: fallback only
- `link_preview_options.is_disabled = true`
- `protect_content = false`
- `allow_paid_broadcast = false`

버튼 기본 구성:

1행:
- `원문 Telegram`
- `Primary Link`

2행:
- primary가 GitHub/X일 때 source-specific button 1개
- supporting link가 있으면 `Supporting`

즉, 링크는 가능한 한 본문이 아니라 버튼으로 빼서 triage note 가독성을 유지한다.

### 6-5. single-shot send가 기본, edit는 예외다

기본:

- plan 생성
- render 생성
- **최종본 1회 전송**

edit 허용 조건:

- 같은 `dedupe_subject_key`
- 최근 sent/edited message가 있음
- 새 `material_change_hash`가 이전 것과 다름
- 아래 둘 중 하나:
  - observation count 갱신처럼 같은 메시지를 계속 쓰는 편이 유리함
  - wording이 아니라 실제 내용이 material하게 바뀜

new message 강제 조건:

- `later -> inspect_now`
- primary reroot 발생
- primary artifact canonical subject 자체가 바뀜
- 기존 메시지가 너무 오래되어 문맥 단절이 큼

즉, **edit는 예외이고, single-shot send가 기본** 이다.

### 6-6. `disable_notification`은 urgency profile에서만 결정한다

- `urgency_profile = high` → `disable_notification = false`
- `urgency_profile = normal_silent` → `disable_notification = true`
- `urgency_profile = digest` → v0.1 기본 비활성
- `urgency_profile = suppressed` → send 없음

즉, notifier는 urgency profile을 **표현/transport 옵션으로만** 사용한다.

### 6-7. digest는 구조만 남기고 v0.1 기본 경로에서는 비활성이다

현재 정책엔 digest enum이 있지만, v0.1 기본 경로는 immediate send다.  
따라서 이번 단계에서는 아래처럼 고정한다.

- `delivery_decision = send_digest` 가 오면 **새 digest planner를 만들지 않는다.**
- `state_transitions`에 `notification_digest_deferred`만 남긴다.
- 기본 send/edit는 수행하지 않는다.

즉, digest path는 **구조는 유지하되 runtime activation은 보류** 한다.

### 6-8. `dedupe_subject_key`와 `material_change_hash`는 notifier가 재계산하지 않는다

두 값은 policy-engine이 이미 계산해 event payload에 실었다.  
notifier는 이 값을 그대로 durable row에 저장하고, send vs edit vs no-op 판단에만 사용한다.

즉,

- subject key = 같은 알림 주체 통합
- material change hash = 재전송/수정 여부 판단

이며, notifier는 둘을 **소비만** 한다.

### 6-9. render length budget은 deterministic하게 자른다

Telegram `sendMessage`는 4096자 제한이 있으므로, 렌더러는 아래 우선순위로 절단한다.

권장 예산:

- headline: 짧게
- summary_one_line: 1문장
- skeptical_take: 1~2문장
- comparables: 최대 2~3개
- red_flags: 최대 2개
- limitations: 1개 우선

절단 우선순위:

1. limitations 축소
2. comparables 개수 축소
3. why_it_might_matter 축소
4. red_flags 축소
5. skeptical_take는 마지막까지 유지

즉, 이 봇의 핵심 가치인 **냉정 평가와 추천 행동** 을 끝까지 살린다.

### 6-10. 실패 처리는 render bug와 transport glitch를 분리한다

#### retryable
- Telegram API 일시 오류
- flood control / timeout / transient network

#### terminal
- invalid chat_id
- bot access 상실
- entity builder/render bug
- policy상 보내면 안 되는 입력

핵심 규칙:

- render bug는 `failed_terminal`
- transport glitch는 `failed_retryable`
- 동일 `render_hash`에 대한 무한 재시도 금지
- 모든 send/edit 결과는 `notification_delivery_records`로 남김

### 6-11. outbound-only 원칙은 유지한다

이번 v0.1에서는 아래를 하지 않는다.

- `getUpdates`
- webhook
- callback query handling
- inbound admin command plane

즉, notifier는 **outbound-only delivery service** 로만 남는다.

---

## 7. application plan 적용 검토 — 이번 단계 결론

### 7-1. 지금 바로 runtime에 적용하는 것

없다.  
정확히 말하면 **새로운 runtime component로는 없다.**

이유:

- Prompt Guard는 35단계 sanitize-only preflight까지만 최소 삽입됨
- AgentLinter는 repo hygiene / CI 자산이다
- MemKraft는 ops-memory sidecar다
- skill/playbook discipline은 prompt/policy handbook 자산이다

### 7-2. 지금 적용하면 구조를 흔드는 것

#### A. notifier가 prompt-risk / memory retrieval / external heuristic를 근거로 send를 막는 것
문제:
- final delivery decision은 policy-engine 책임인데 notifier가 다시 판단하게 된다.

#### B. MemKraft / AgentLinter를 render/send hot path에 연결하는 것
문제:
- runtime과 repo/ops discipline이 섞인다.

#### C. notifier가 external summarizer처럼 문장을 다시 만들어 의미를 바꾸는 것
문제:
- presentation boundary가 무너진다.

### 7-3. 최소-change 결론

- **이번 38단계에는 application plan을 runtime hot path에 새 책임으로 넣지 않는다.**
- 대신 아래를 future-compatible decision으로만 남긴다.
  - Prompt Guard: judge-openai sanitize-only preflight 유지
  - AgentLinter: `AGENTS.md` / `prompts/` / `policies/` / `README` 정리 단계에서 적용
  - MemKraft: `ops-memory/` sidecar로만 적용
  - skill/playbook discipline: prompt/profile handbook 문서화에만 적용

즉, `notifier-telegram`은 **repo hardening 흡수 지점이 아니라 presentation/delivery 집행 지점** 으로 남는다.

---

## 8. 대상 파일 트리

```text
src/services/notifier_telegram/
  __init__.py
  config.py
  models.py
  entity_builder.py
  keyboard_builder.py
  renderer.py
  telegram_client.py
  repositories.py
  service.py
  worker.py
  main.py

tests/
  unit/
    services/
      notifier_telegram/
        test_renderer_single_alert.py
        test_entity_builder.py
        test_keyboard_builder.py
        test_disable_notification_mapping.py
        test_material_change_edit_decision.py
  component/
    services/
      notifier_telegram/
        test_worker_rehydrates_notification_plan_intent.py
        test_plan_intent_concretizes_notification_plan.py
        test_send_now_writes_render_and_delivery_record.py
        test_silent_later_sets_disable_notification_true.py
        test_existing_subject_recent_message_edits_on_material_change.py
        test_suppress_or_digest_path_no_send.py
        test_transport_failure_marks_retryable.py
```

---

## 9. 코드 초안

### 9-1. `src/services/notifier_telegram/__init__.py`

```python
from .config import NotifierTelegramConfig
from .service import NotifierTelegramService
from .worker import NotifierTelegramWorker

__all__ = [
    "NotifierTelegramConfig",
    "NotifierTelegramService",
    "NotifierTelegramWorker",
]
```

### 9-2. `src/services/notifier_telegram/config.py`

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

### 9-3. `src/services/notifier_telegram/models.py`

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
class AnalysisRecord:
    analysis_id: str
    candidate_group_id: str
    judge_output_id: str
    verdict: str
    delivery_decision: str
    reason_codes_json: list[str]
    evidence_limitations_ko: str | None
    recommended_action_ko: str | None
    freshness_note_ko: str | None
    model_proposed_verdict: str | None
    policy_reconciled_flag: bool
    created_at: datetime


@dataclass(slots=True, frozen=True)
class JudgeOutputRenderFields:
    judge_output_id: str
    headline: str | None
    summary_one_line_ko: str | None
    skeptical_take_ko: str | None
    why_it_might_matter_ko: str | None
    comparables: list[str]
    red_flags_ko: list[str]
    model_confidence_band: str | None
    payload_json: dict[str, Any]


@dataclass(slots=True, frozen=True)
class CandidateRenderContext:
    candidate_group_id: str
    source_message_id: str
    source_message_link: str | None
    current_primary_artifact_id: str
    primary_canonical_url: str | None
    primary_artifact_type: str | None
    supporting_urls: list[str]


@dataclass(slots=True, frozen=True)
class NotificationPlanDraft:
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
    status: str


@dataclass(slots=True, frozen=True)
class NotificationRenderDraft:
    notification_plan_id: str
    message_text: str
    entities_json: list[dict[str, Any]]
    link_preview_options_json: dict[str, Any]
    reply_markup_json: dict[str, Any] | None
    disable_notification: bool
    protect_content: bool
    parse_strategy: str
    render_hash: str


@dataclass(slots=True, frozen=True)
class DeliveryAction:
    mode: Literal["send", "edit", "noop"]
    existing_message_id: int | None = None


@dataclass(slots=True, frozen=True)
class DeliveryResult:
    delivery_status: str
    telegram_chat_id: int | None
    telegram_message_id: int | None
    attempt_count: int
    transport_error_code: str | None = None
    transport_error_class: str | None = None
    telegram_response_json: dict[str, Any] | None = None
```

### 9-4. `src/services/notifier_telegram/entity_builder.py`

```python
from __future__ import annotations

from dataclasses import dataclass


@dataclass(slots=True, frozen=True)
class EntityBuildResult:
    text: str
    entities: list[dict]


class TelegramEntityBuilder:
    """Build explicit Telegram entities.

    v0.1 keeps formatting minimal and deterministic:
    - bold headline / verdict line
    - plain paragraphs for the rest
    - no MarkdownV2 escaping dependence
    """

    def build(self, *, lines: list[str]) -> EntityBuildResult:
        text = "\n".join(lines).strip()
        entities: list[dict] = []
        cursor = 0
        for idx, line in enumerate(lines):
            if idx in {0, 2}:  # badge/source line and verdict line
                entities.append(
                    {
                        "type": "bold",
                        "offset": cursor,
                        "length": len(line),
                    }
                )
            cursor += len(line) + 1
        return EntityBuildResult(text=text, entities=entities)
```

### 9-5. `src/services/notifier_telegram/keyboard_builder.py`

```python
from __future__ import annotations


class InlineKeyboardBuilder:
    def build(
        self,
        *,
        telegram_link: str | None,
        primary_link: str | None,
        primary_artifact_type: str | None,
        supporting_urls: list[str],
    ) -> dict | None:
        rows: list[list[dict]] = []

        first_row: list[dict] = []
        if telegram_link:
            first_row.append({"text": "원문 Telegram", "url": telegram_link})
        if primary_link:
            first_row.append({"text": "Primary Link", "url": primary_link})
        if first_row:
            rows.append(first_row)

        second_row: list[dict] = []
        if primary_link and primary_artifact_type in {"github_repo", "github_subpath", "github_repo_page", "github_gist"}:
            second_row.append({"text": "GitHub", "url": primary_link})
        elif primary_link and primary_artifact_type == "x_post":
            second_row.append({"text": "X", "url": primary_link})

        for url in supporting_urls[:1]:
            second_row.append({"text": "Supporting", "url": url})
        if second_row:
            rows.append(second_row)

        if not rows:
            return None
        return {"inline_keyboard": rows}
```

### 9-6. `src/services/notifier_telegram/renderer.py`

```python
from __future__ import annotations

import hashlib
from dataclasses import dataclass

from .entity_builder import TelegramEntityBuilder
from .models import AnalysisRecord, CandidateRenderContext, JudgeOutputRenderFields, NotificationRenderDraft
from .keyboard_builder import InlineKeyboardBuilder


@dataclass(slots=True, frozen=True)
class RenderInput:
    analysis: AnalysisRecord
    judge_output: JudgeOutputRenderFields
    candidate: CandidateRenderContext


class NotificationRenderer:
    def __init__(
        self,
        *,
        entity_builder: TelegramEntityBuilder,
        keyboard_builder: InlineKeyboardBuilder,
        max_message_chars: int,
    ) -> None:
        self._entity_builder = entity_builder
        self._keyboard_builder = keyboard_builder
        self._max_message_chars = max_message_chars

    def render(self, *, notification_plan_id: str, payload: RenderInput) -> NotificationRenderDraft:
        analysis = payload.analysis
        judge = payload.judge_output
        candidate = payload.candidate

        badge = self._badge(analysis.verdict, candidate.primary_artifact_type)
        headline = judge.headline or self._fallback_headline(candidate)
        summary = judge.summary_one_line_ko or "요약 정보 부족"
        skeptical = judge.skeptical_take_ko or "냉정 평가 정보 부족"
        comparables = ", ".join(judge.comparables[:3]) if judge.comparables else "비교 정보 부족"
        risks = "; ".join(judge.red_flags_ko[:2]) if judge.red_flags_ko else (analysis.evidence_limitations_ko or "리스크 정보 부족")
        action = analysis.recommended_action_ko or "추가 확인 필요"
        confidence = judge.model_confidence_band or "unknown"

        lines = [
            badge,
            f"제목: {headline}",
            f"판정: {analysis.verdict} | confidence {confidence}",
            f"한줄 요약: {summary}",
            f"냉정 평가: {skeptical}",
            f"기존 도구 대비: {comparables}",
            f"리스크: {risks}",
            f"추천 행동: {action}",
        ]
        if analysis.freshness_note_ko:
            lines.append(f"신선도 메모: {analysis.freshness_note_ko}")

        built = self._entity_builder.build(lines=lines)
        text = built.text
        if len(text) > self._max_message_chars:
            text = text[: self._max_message_chars - 1] + "…"
            built = self._entity_builder.build(lines=text.split("\n"))

        reply_markup = self._keyboard_builder.build(
            telegram_link=candidate.source_message_link,
            primary_link=candidate.primary_canonical_url,
            primary_artifact_type=candidate.primary_artifact_type,
            supporting_urls=candidate.supporting_urls,
        )

        disable_notification = analysis.delivery_decision == "send_now" and analysis.verdict == "later"
        if analysis.delivery_decision == "send_now" and analysis.verdict == "inspect_now":
            disable_notification = False

        render_hash = hashlib.sha256(
            (built.text + str(reply_markup) + str(disable_notification)).encode("utf-8")
        ).hexdigest()

        return NotificationRenderDraft(
            notification_plan_id=notification_plan_id,
            message_text=built.text,
            entities_json=built.entities,
            link_preview_options_json={"is_disabled": True},
            reply_markup_json=reply_markup,
            disable_notification=disable_notification,
            protect_content=False,
            parse_strategy="entities",
            render_hash=render_hash,
        )

    @staticmethod
    def _badge(verdict: str, artifact_type: str | None) -> str:
        severity = "HIGH" if verdict == "inspect_now" else "MID"
        source = {
            "github_repo": "GitHub",
            "github_subpath": "GitHub",
            "github_repo_page": "GitHub",
            "github_gist": "GitHub",
            "x_post": "X",
            "text_idea": "Idea",
            "web_article": "Idea",
        }.get(artifact_type or "", "Idea")
        return f"[{severity}] [{source}]"

    @staticmethod
    def _fallback_headline(candidate: CandidateRenderContext) -> str:
        return candidate.primary_canonical_url or f"candidate:{candidate.candidate_group_id}"
```

### 9-7. `src/services/notifier_telegram/telegram_client.py`

```python
from __future__ import annotations

import httpx


class TelegramTransportRetryableError(Exception):
    pass


class TelegramTransportTerminalError(Exception):
    pass


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
        async with httpx.AsyncClient(timeout=self._timeout_sec) as client:
            response = await client.post(url, json=payload)
        if response.status_code >= 500:
            raise TelegramTransportRetryableError(response.text)
        if response.status_code >= 400:
            raise TelegramTransportTerminalError(response.text)
        data = response.json()
        if not data.get("ok", False):
            description = str(data.get("description", "telegram request failed"))
            # simple heuristic: flood/timeouts retryable, bad chat terminal
            if "Too Many Requests" in description or "timed out" in description:
                raise TelegramTransportRetryableError(description)
            raise TelegramTransportTerminalError(description)
        return data
```

### 9-8. `src/services/notifier_telegram/repositories.py`

```python
from __future__ import annotations

import json
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from typing import Any

import sqlalchemy as sa
from sqlalchemy.ext.asyncio import AsyncSession

from .models import (
    AnalysisRecord,
    CandidateRenderContext,
    JudgeOutputRenderFields,
    NotificationIntentJob,
    NotificationPlanDraft,
    NotificationRenderDraft,
)


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
        if row is None:
            return None
        payload = row["payload_json"] or {}
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
            send_after=payload.get("send_after"),
            suppress_reason_code=str(payload.get("suppress_reason_code")) if payload.get("suppress_reason_code") else None,
        )

    async def load_analysis(self, analysis_id: str) -> AnalysisRecord | None:
        result = await self._session.execute(
            sa.text(
                """
                SELECT analysis_id, candidate_group_id, judge_output_id,
                       verdict, delivery_decision, reason_codes_json,
                       evidence_limitations_ko, recommended_action_ko,
                       freshness_note_ko, model_proposed_verdict,
                       policy_reconciled_flag, created_at
                FROM analyses
                WHERE analysis_id = CAST(:analysis_id AS uuid)
                """
            ),
            {"analysis_id": analysis_id},
        )
        row = result.mappings().first()
        if row is None:
            return None
        return AnalysisRecord(
            analysis_id=str(row["analysis_id"]),
            candidate_group_id=str(row["candidate_group_id"]),
            judge_output_id=str(row["judge_output_id"]),
            verdict=str(row["verdict"]),
            delivery_decision=str(row["delivery_decision"]),
            reason_codes_json=row["reason_codes_json"] or [],
            evidence_limitations_ko=row["evidence_limitations_ko"],
            recommended_action_ko=row["recommended_action_ko"],
            freshness_note_ko=row["freshness_note_ko"],
            model_proposed_verdict=row["model_proposed_verdict"],
            policy_reconciled_flag=bool(row["policy_reconciled_flag"]),
            created_at=row["created_at"],
        )

    async def load_judge_output_render_fields(self, judge_output_id: str) -> JudgeOutputRenderFields | None:
        result = await self._session.execute(
            sa.text(
                """
                SELECT judge_output_id, payload_json
                FROM judge_outputs
                WHERE judge_output_id = CAST(:judge_output_id AS uuid)
                """
            ),
            {"judge_output_id": judge_output_id},
        )
        row = result.mappings().first()
        if row is None:
            return None
        payload = row["payload_json"] or {}
        return JudgeOutputRenderFields(
            judge_output_id=str(row["judge_output_id"]),
            headline=payload.get("headline"),
            summary_one_line_ko=payload.get("summary_one_line_ko"),
            skeptical_take_ko=payload.get("skeptical_take_ko"),
            why_it_might_matter_ko=payload.get("why_it_might_matter_ko"),
            comparables=payload.get("comparables") or [],
            red_flags_ko=payload.get("red_flags_ko") or [],
            model_confidence_band=payload.get("model_confidence_band"),
            payload_json=payload,
        )

    async def load_candidate_render_context(self, candidate_group_id: str) -> CandidateRenderContext | None:
        result = await self._session.execute(
            sa.text(
                """
                SELECT cgp.candidate_group_id,
                       cgp.source_message_id,
                       sm.message_link AS source_message_link,
                       cgp.current_primary_artifact_id,
                       ar.canonical_url AS primary_canonical_url,
                       ar.artifact_type AS primary_artifact_type
                FROM candidate_group_proposals cgp
                JOIN source_messages sm ON sm.source_message_id = cgp.source_message_id
                LEFT JOIN artifact_registry ar ON ar.artifact_id = cgp.current_primary_artifact_id
                WHERE cgp.candidate_group_id = CAST(:candidate_group_id AS uuid)
                """
            ),
            {"candidate_group_id": candidate_group_id},
        )
        row = result.mappings().first()
        if row is None:
            return None

        supporting = await self._session.execute(
            sa.text(
                """
                SELECT ar.canonical_url
                FROM candidate_group_members cgm
                JOIN artifact_registry ar ON ar.artifact_id = cgm.artifact_id
                WHERE cgm.candidate_group_id = CAST(:candidate_group_id AS uuid)
                  AND cgm.member_role <> 'primary'
                  AND ar.canonical_url IS NOT NULL
                ORDER BY cgm.member_order NULLS LAST, ar.canonical_url
                LIMIT 3
                """
            ),
            {"candidate_group_id": candidate_group_id},
        )
        return CandidateRenderContext(
            candidate_group_id=str(row["candidate_group_id"]),
            source_message_id=str(row["source_message_id"]),
            source_message_link=row["source_message_link"],
            current_primary_artifact_id=str(row["current_primary_artifact_id"]),
            primary_canonical_url=row["primary_canonical_url"],
            primary_artifact_type=row["primary_artifact_type"],
            supporting_urls=[str(r["canonical_url"]) for r in supporting.mappings().all() if r["canonical_url"]],
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

    async def insert_notification_render(self, draft: NotificationRenderDraft) -> str:
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
        row = result.scalar_one_or_none()
        return str(row) if row else ""

    async def load_recent_delivery_for_subject(self, *, dedupe_subject_key: str, target_chat_id: int, within_minutes: int):
        result = await self._session.execute(
            sa.text(
                """
                SELECT ndr.telegram_message_id, ndr.telegram_chat_id, np.material_change_hash, np.created_at
                FROM notification_plans np
                JOIN notification_delivery_records ndr ON ndr.notification_plan_id = np.notification_plan_id
                WHERE np.dedupe_subject_key = :dedupe_subject_key
                  AND np.target_chat_id = :target_chat_id
                  AND ndr.delivery_status IN ('sent', 'edited')
                  AND np.created_at >= :threshold
                ORDER BY np.created_at DESC
                LIMIT 1
                """
            ),
            {
                "dedupe_subject_key": dedupe_subject_key,
                "target_chat_id": target_chat_id,
                "threshold": datetime.now(timezone.utc) - timedelta(minutes=within_minutes),
            },
        )
        return result.mappings().first()

    async def insert_delivery_record(self, *, notification_plan_id: str, delivery_status: str, telegram_chat_id: int | None, telegram_message_id: int | None, attempt_count: int, transport_error_code: str | None, transport_error_class: str | None, telegram_response_json: dict | None) -> str:
        result = await self._session.execute(
            sa.text(
                """
                INSERT INTO notification_delivery_records (
                    notification_delivery_record_id,
                    notification_plan_id,
                    telegram_chat_id,
                    telegram_message_id,
                    delivery_status,
                    sent_at,
                    edited_at,
                    attempt_count,
                    transport_error_code,
                    transport_error_class,
                    telegram_response_json
                ) VALUES (
                    gen_random_uuid(),
                    CAST(:notification_plan_id AS uuid),
                    :telegram_chat_id,
                    :telegram_message_id,
                    CAST(:delivery_status AS notification_status_enum),
                    CASE WHEN :delivery_status = 'sent' THEN now() ELSE NULL END,
                    CASE WHEN :delivery_status = 'edited' THEN now() ELSE NULL END,
                    :attempt_count,
                    :transport_error_code,
                    :transport_error_class,
                    CAST(:telegram_response_json AS jsonb)
                )
                RETURNING notification_delivery_record_id
                """
            ),
            {
                "notification_plan_id": notification_plan_id,
                "telegram_chat_id": telegram_chat_id,
                "telegram_message_id": telegram_message_id,
                "delivery_status": delivery_status,
                "attempt_count": attempt_count,
                "transport_error_code": transport_error_code,
                "transport_error_class": transport_error_class,
                "telegram_response_json": _jsonb_dumps(telegram_response_json),
            },
        )
        return str(result.scalar_one())

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

    async def insert_state_transition(self, *, object_type: str, object_id: str, from_state: str | None, to_state: str, reason_code: str) -> None:
        await self._session.execute(
            sa.text(
                """
                INSERT INTO state_transitions (
                    state_transition_id,
                    object_type,
                    object_id,
                    from_state,
                    to_state,
                    reason_code,
                    created_at
                ) VALUES (
                    gen_random_uuid(),
                    :object_type,
                    CAST(:object_id AS uuid),
                    :from_state,
                    :to_state,
                    :reason_code,
                    now()
                )
                """
            ),
            {
                "object_type": object_type,
                "object_id": object_id,
                "from_state": from_state,
                "to_state": to_state,
                "reason_code": reason_code,
            },
        )

    async def insert_delivery_result_outbox(self, *, notification_plan_id: str, delivery_status: str, telegram_chat_id: int | None, telegram_message_id: int | None, notification_delivery_record_id: str, attempt_count: int, transport_error_code: str | None, transport_error_class: str | None) -> None:
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
                    'notification.delivery.result.v1',
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
                "notification_plan_id": notification_plan_id,
                "dedupe_key": f"notification:delivery_result:{notification_plan_id}:{attempt_count}:{delivery_status}",
                "payload_json": _jsonb_dumps(
                    {
                        "notification_plan_id": notification_plan_id,
                        "notification_delivery_record_id": notification_delivery_record_id,
                        "delivery_status": delivery_status,
                        "telegram_chat_id": telegram_chat_id,
                        "telegram_message_id": telegram_message_id,
                        "attempt_count": attempt_count,
                        "transport_error_code": transport_error_code,
                        "transport_error_class": transport_error_class,
                    }
                ),
            },
        )
```

### 9-9. `src/services/notifier_telegram/service.py`

```python
from __future__ import annotations

import logging

from .models import DeliveryAction, DeliveryResult, NotificationPlanDraft
from .renderer import NotificationRenderer, RenderInput
from .telegram_client import TelegramBotClient, TelegramTransportRetryableError, TelegramTransportTerminalError


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
        if analysis is None or analysis.candidate_group_id != intent.candidate_group_id:
            return
        if analysis.delivery_decision == "suppress":
            return

        judge_output = await self._repository.load_judge_output_render_fields(analysis.judge_output_id)
        candidate = await self._repository.load_candidate_render_context(intent.candidate_group_id)
        if judge_output is None or candidate is None:
            return

        async with self._repository.transaction():
            existing_plan = await self._repository.load_notification_plan(intent.notification_plan_id)
            if existing_plan is None:
                material_existing = await self._repository.load_existing_plan_by_material(
                    analysis_id=intent.analysis_id,
                    target_chat_id=intent.target_chat_id,
                    material_change_hash=intent.material_change_hash,
                )
                if material_existing is None:
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
                    intent = type(intent)(**{**intent.__dict__, "notification_plan_id": str(material_existing["notification_plan_id"])})

            render = self._renderer.render(
                notification_plan_id=intent.notification_plan_id,
                payload=RenderInput(
                    analysis=analysis,
                    judge_output=judge_output,
                    candidate=candidate,
                ),
            )
            await self._repository.insert_notification_render(render)
            await self._repository.update_plan_status(notification_plan_id=intent.notification_plan_id, status="rendered")

        action = await self._decide_delivery_action(intent=intent)
        result = await self._perform_delivery(intent=intent, render=render, action=action)

        async with self._repository.transaction():
            await self._repository.update_plan_status(notification_plan_id=intent.notification_plan_id, status=result.delivery_status)
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
                from_state="rendered",
                to_state=result.delivery_status,
                reason_code="telegram_delivery_result",
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

    async def _decide_delivery_action(self, *, intent) -> DeliveryAction:
        if intent.delivery_decision == "send_digest" and not self._config.enable_digest_runtime:
            return DeliveryAction(mode="noop")

        recent = await self._repository.load_recent_delivery_for_subject(
            dedupe_subject_key=intent.dedupe_subject_key,
            target_chat_id=intent.target_chat_id,
            within_minutes=self._config.edit_window_minutes,
        )
        if recent is None:
            return DeliveryAction(mode="send")

        previous_hash = str(recent["material_change_hash"])
        if previous_hash == intent.material_change_hash:
            return DeliveryAction(mode="noop")

        # conservative edit-only path: same subject, recent message, same candidate intent
        return DeliveryAction(
            mode="edit",
            existing_message_id=int(recent["telegram_message_id"]),
        )

    async def _perform_delivery(self, *, intent, render, action: DeliveryAction) -> DeliveryResult:
        if action.mode == "noop":
            return DeliveryResult(
                delivery_status="suppressed" if intent.delivery_decision == "send_digest" else "edited",
                telegram_chat_id=intent.target_chat_id,
                telegram_message_id=action.existing_message_id,
                attempt_count=0,
                telegram_response_json=None,
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
        except TelegramTransportRetryableError as exc:
            return DeliveryResult(
                delivery_status="failed_retryable",
                telegram_chat_id=intent.target_chat_id,
                telegram_message_id=action.existing_message_id,
                attempt_count=1,
                transport_error_code="telegram_retryable",
                transport_error_class=type(exc).__name__,
            )
        except TelegramTransportTerminalError as exc:
            return DeliveryResult(
                delivery_status="failed_terminal",
                telegram_chat_id=intent.target_chat_id,
                telegram_message_id=action.existing_message_id,
                attempt_count=1,
                transport_error_code="telegram_terminal",
                transport_error_class=type(exc).__name__,
            )
```

### 9-10. `src/services/notifier_telegram/worker.py`

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

### 9-11. `src/services/notifier_telegram/main.py`

```python
from __future__ import annotations

import asyncio
import logging

from redis.asyncio import Redis
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from .config import NotifierTelegramConfig
from .entity_builder import TelegramEntityBuilder
from .keyboard_builder import InlineKeyboardBuilder
from .renderer import NotificationRenderer
from .repositories import NotifierTelegramRepository
from .service import NotifierTelegramService
from .telegram_client import TelegramBotClient
from .worker import NotifierTelegramWorker
# redis stream consumer implementation is intentionally reused from the same Redis Streams pattern used by prior services.


def _build_logger(level: str) -> logging.Logger:
    logging.basicConfig(level=getattr(logging, level, logging.INFO))
    return logging.getLogger("notifier-telegram")


async def _run() -> int:
    config = NotifierTelegramConfig.from_env()
    logger = _build_logger(config.log_level)

    engine = create_async_engine(config.database_url, future=True)
    session_factory = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    redis_client = Redis.from_url(config.redis_url, decode_responses=True)

    # same stream primitive pattern as earlier services; implementation intentionally omitted here.
    from src.services.web_enricher.redis_streams import RedisStreamConsumer  # minimal-change reuse pattern

    try:
        async with session_factory() as session:
            repository = NotifierTelegramRepository(session)
            renderer = NotificationRenderer(
                entity_builder=TelegramEntityBuilder(),
                keyboard_builder=InlineKeyboardBuilder(),
                max_message_chars=config.max_message_chars,
            )
            telegram_client = TelegramBotClient(
                base_url=config.telegram_api_base_url,
                bot_token=config.telegram_bot_token,
                timeout_sec=config.request_timeout_sec,
            )
            consumer = RedisStreamConsumer(
                redis_client,
                queue_name=config.queue_name,
                consumer_group=config.consumer_group,
                consumer_name=config.consumer_name,
                block_ms=config.block_ms,
                batch_size=config.batch_size,
            )
            service = NotifierTelegramService(
                config,
                repository=repository,
                renderer=renderer,
                telegram_client=telegram_client,
                logger=logger,
            )
            worker = NotifierTelegramWorker(config, consumer=consumer, service=service, logger=logger)
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

---

## 10. 테스트 초안 포인트

### `tests/unit/services/notifier_telegram/test_renderer_single_alert.py`

검증:
- `analysis + judge_output` 조합으로 single-alert 템플릿이 생성되는지
- first 3 lines 안에 badge / 제목 / verdict가 들어가는지

### `tests/unit/services/notifier_telegram/test_entity_builder.py`

검증:
- explicit entities가 deterministic하게 생성되는지
- parse_mode에 의존하지 않는지

### `tests/unit/services/notifier_telegram/test_keyboard_builder.py`

검증:
- Telegram 원문 / Primary Link / Supporting 버튼이 기대 순서로 생성되는지
- supporting link가 없을 때 row 생략이 되는지

### `tests/unit/services/notifier_telegram/test_disable_notification_mapping.py`

검증:
- `inspect_now -> disable_notification=false`
- `later/send_now -> disable_notification=true`

### `tests/unit/services/notifier_telegram/test_material_change_edit_decision.py`

검증:
- 같은 subject / 최근 delivery / material hash 변경이면 edit path를 고르는지
- 같은 material hash면 noop이 되는지

### `tests/component/services/notifier_telegram/test_worker_rehydrates_notification_plan_intent.py`

검증:
- Redis payload는 thin message
- `event_outbox` 기준으로 `notification.plan.created.v1` rehydrate

### `tests/component/services/notifier_telegram/test_plan_intent_concretizes_notification_plan.py`

검증:
- `notification_plans` row가 notifier ownership으로 생성되는지
- payload의 `notification_plan_id`가 그대로 durable row에 반영되는지

### `tests/component/services/notifier_telegram/test_send_now_writes_render_and_delivery_record.py`

검증:
- send path 성공
- `notification_renders` insert
- `notification_delivery_records` insert
- `notification.delivery.result.v1` outbox insert

### `tests/component/services/notifier_telegram/test_silent_later_sets_disable_notification_true.py`

검증:
- later/send_now 경로에서 silent option이 적용되는지

### `tests/component/services/notifier_telegram/test_existing_subject_recent_message_edits_on_material_change.py`

검증:
- same subject recent message 존재
- material change hash 변경
- `editMessageText` path 사용
- delivery status = `edited`

### `tests/component/services/notifier_telegram/test_suppress_or_digest_path_no_send.py`

검증:
- suppress 입력은 send 안 함
- digest disabled 상태에서는 Telegram API 호출 없이 stop 하는지

### `tests/component/services/notifier_telegram/test_transport_failure_marks_retryable.py`

검증:
- Telegram transient error 발생
- delivery status = `failed_retryable`
- result outbox / state transition 남는지

---

## 11. 이번 단계가 구조를 지키는 이유

1. `notifier-telegram`은 `notification_plans`, `notification_renders`, `notification_delivery_records`, `state_transitions`, `event_outbox`만 직접 쓴다.  
   즉, service ownership을 넘지 않는다.

2. final verdict와 delivery decision을 다시 계산하지 않는다.  
   즉, stage 0 / stage 6 / stage 7 경계가 유지된다.

3. `policy-engine`가 만든 plan-intent event를 notifier가 concrete durable rows로 바꾼다.  
   즉, 37단계 bridge가 자연스럽게 닫힌다.

4. render는 `analysis_v1`을 우선하고 `judge_output_v1`을 보조 source로만 사용한다.  
   즉, 의미를 바꾸지 않는 presentation 계층이 유지된다.

5. text message + inline keyboard + explicit entities + preview disabled + single-shot send / material edit only 를 유지한다.  
   즉, 7단계 전달 정책 정본을 현재 runtime contract로 그대로 내린다.

6. Prompt Guard / AgentLinter / MemKraft / skill discipline을 notifier runtime hot path에 넣지 않는다.  
   즉, application plan의 좋은 방향은 보존하되 current contracts는 흔들지 않는다.

---

## 12. 다음 단계

이 단계가 닫히면 현재 잠긴 33~38 구현 체인은 delivery layer까지 일단 닫힌다.  
다음 안전한 작업은 새 구조를 다시 설계하는 것이 아니라 아래 둘 중 하나다.

1. **`09_analysis_pipeline_stage33_stage38_bundle_v0_1.md` 통합 번들 생성**
2. **`notifier-telegram` acceptance / integration hardening**
   - queue retry / backoff
   - duplicate delivery guard
   - edit-vs-new heuristic test 보강
   - end-to-end compose wiring / dry-run

즉, 다음은 **구조 확장보다 consolidation 또는 delivery hardening** 이 맞다.

---

## 최종 한 줄 결론

이번 단계의 최소-change 정답은 **`notification.plan.created.v1` plan-intent를 rehydrate해 notifier ownership으로 `notification_plans`를 concretize하고, `analysis_v1`을 final truth로, `judge_output_v1`을 보조 렌더 source로 사용해 explicit-entity text message + inline keyboard를 single-shot send 또는 material edit로 전달하고, 그 결과를 `notification_delivery_records`와 `notification.delivery.result.v1`로 남기는 `notifier-telegram` v0.1을 닫는 것**이다.


---
