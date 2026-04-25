# 08 enrichers assembler stage29 stage32 bundle v0 1
이 문서는 프로젝트 소스 파일 수 제한(40개)을 피하기 위한 **통합 번들**이다.
구조 변경 문서가 아니며, 아래 원본 문서를 **순서와 내용을 보존한 채** 묶는다.

## 포함 원본 파일
- `29_gh_enricher_skeleton_and_code_draft_v0_1.md`
- `30_x_enricher_skeleton_and_code_draft_v0_1.md`
- `31_web_enricher_skeleton_and_code_draft_v0_1.md`
- `32_evidence_assembler_skeleton_and_code_draft_v0_1.md`

## 통합 원칙
- 아키텍처 불변식은 유지한다.
- 원본 파일명은 아래 섹션 제목으로 보존한다.
- 충돌 시 해석 우선순위는 기존과 동일하게 정본 단계 문서 → 실행 계약 → migration 정본 → 구현 초안이다.


---

## Source file: `29_gh_enricher_skeleton_and_code_draft_v0_1.md`

# 29단계: `gh-enricher` 스켈레톤 + 실제 코드 초안 v0.1

## 0. 문서 목적

이 문서는 프로젝트 소스의 README/정본 단계 문서, `05_stage5_external_enrichers.md`, `11_stage11_execution_contracts_v0_1.md`, `12_migration_spec_0001_0004_v0_1.md`, `26_outbox_relay_skeleton_and_code_draft_v0_1.md`, `27_router_normalizer_skeleton_and_code_draft_v0_1.md`, `28_router_normalizer_consumer_integration_hardening_v0_1.md`까지의 구현 흐름을 바탕으로,  
**`gh-enricher`의 첫 구현 묶음**을 실제 코드 초안 수준으로 내리는 문서다.

이번 단계의 목적은 여섯 가지다.

1. `q.artifact.enrich.github` Redis Streams를 소비하는 **GitHub 전용 enrichment 경계**를 코드로 고정
2. `artifact.enrich.requested.v1` thin payload를 기준으로, `event_outbox`에서 다시 **`ArtifactEnrichmentJob`** 을 복원하는 rehydration 경계를 고정
3. GitHub App 인증, anonymous degraded fallback, repo/subpath/repo_page/gist별 fetch 계획을 **최소-change 구현**으로 고정
4. `artifact_enrichment_runs`, `artifact_snapshots`, `artifact_snapshot_github_repo`, `artifact_snapshot_github_file_samples`, `discovered_url_observations`, `event_outbox`에 대한 **gh-enricher 전용 DB 경계**를 코드로 고정
5. `artifact.snapshot.updated.v1` outbox emit까지 닫아, 다음 단계의 `x-enricher` / `web-enricher` / `evidence-assembler`가 같은 패턴으로 이어질 수 있게 고정
6. 이 모든 것을 넣어도 `gh-enricher`가 여전히 **비-LLM evidence 수집기**로만 남도록 고정

핵심 전제:

- `gh-enricher`는 **판단기**가 아니다.
- `gh-enricher`는 **candidate mutation 계층**이 아니다.
- `gh-enricher`는 **reroot를 확정하지 않는다.**
- `gh-enricher`는 **공식 API 우선 + 얕은 evidence sampling** 경계다.
- `gh-enricher`는 `router-normalizer`의 canonicalization 규칙을 **재정의하지 않는다.**

---

## 1. 왜 지금 이 단계가 정확한 다음 단계인가

28단계 문서는 `router-normalizer` deterministic boundary를 operational hardening까지 닫은 뒤, **다음 구현 순서가 `gh-enricher` → `x-enricher` → `web-enricher` → `evidence-assembler`** 라고 명시했다.  
즉, 현재 단계에서 collector나 normalizer를 다시 열어보는 것은 구조상 후퇴고, 바로 stage 5의 첫 source enricher 본체로 넘어가는 것이 맞다.

또한 5단계 정본은 enrichers를 아래처럼 분리했다.

- `gh-enricher`
- `x-enricher`
- `web-enricher`
- `evidence-assembler`

그리고 11단계 실행 계약은 `gh-enricher`가 직접 쓰는 durable 테이블과 허용 secret을 이미 고정했다.

- 직접 쓰는 durable 테이블:
  - `artifact_enrichment_runs`
  - `artifact_snapshots`
  - `artifact_snapshot_github_repo`
  - `artifact_snapshot_github_file_samples`
  - `discovered_url_observations`
  - `event_outbox`
- 허용 secret:
  - **GitHub App만**

즉, 지금은 stage 5를 다시 설계할 단계가 아니라,  
**이미 잠긴 GitHub evidence boundary를 첫 runnable package로 내리는 단계**다.

---

## 2. 이번 단계에서 발견되는 작은 충돌과 최소-change 해석

이번 단계에는 작은 충돌이 하나 있다.

### 충돌 지점

정본 문서들은 `gh-enricher` 범위에 `github_gist`를 포함하고 있다.  
그런데 `0003_enrichment_bundles` 스키마에서 source-specific child table은 사실상 `artifact_snapshot_github_repo` 하나뿐이고, 이 테이블은 `repo_full_name`을 강하게 전제한다.

즉, 문서상 범위는:

- `github_repo`
- `github_subpath`
- `github_repo_page`
- `github_gist`

인데, child snapshot schema는 repo 중심이다.

### 최소-change 해석

이번 v0.1에서는 아래 해석이 가장 보수적이다.

1. `github_repo`, `github_subpath`, `github_repo_page`  
   → parent snapshot + `artifact_snapshot_github_repo` child row + file samples

2. `github_gist`  
   → parent `artifact_snapshots` row는 생성  
   → `snapshot_type = github_gist`  
   → gist-specific detail은 `normalized_projection` JSON에 저장  
   → `artifact_snapshot_github_repo` child row는 **생성하지 않음**

이 해석의 장점은 다음이다.

- schema patch 없이 현재 migration 정본을 그대로 사용 가능
- `gh-enricher` 범위에서 gist를 완전히 배제하지 않음
- repo 중심 child schema를 억지로 gist에 맞추지 않음
- 뒤 단계에서 gist 전용 child table이 필요해질 경우 자연스럽게 확장 가능

중요:

- 이건 architecture change가 아니다.
- 이건 **현재 스키마와 현재 범위를 동시에 살리는 최소-change 해석**이다.

---

## 3. 범위와 비범위

### 3-1. 포함 범위

- Redis Streams consumer group bootstrap
- `trigger_event_id` 기반 `ArtifactEnrichmentJob` 재구성
- `artifact_registry` rehydration
- GitHub App token provider
- anonymous degraded fallback
- repo identity/meta fetch
- tree/shape fetch
- role-based file sampling
- release summary fetch
- README / sampled file excerpt 기반 discovered URL extraction
- snapshot insert + current pointer update
- `artifact.snapshot.updated.v1` outbox emit
- 최소 unit/component tests

### 3-2. 제외 범위

- full clone 기본 전략
- tarball/archive fallback의 실제 구현
- candidate reroot 확정
- bundle assembly
- judge / notifier / policy 연결
- multi-consumer reclaim/DLQ hardening
- GitHub discussion/issue/pull 본문 deep fetch
- gist 전용 child snapshot table 추가 migration

즉, 이번 문서는 **first runnable GitHub evidence collector**를 닫되,  
scope는 stage 5 GitHub 경계 안으로 유지한다.

---

## 4. 대상 파일 트리

```text
src/services/gh_enricher/
  __init__.py
  config.py
  models.py
  github_app_auth.py
  github_client.py
  fetch_planner.py
  file_sampler.py
  url_discovery.py
  repositories.py
  redis_streams.py
  service.py
  worker.py
  main.py

tests/
  unit/
    services/
      gh_enricher/
        test_fetch_planner.py
        test_file_sampler.py
        test_url_discovery.py
        test_gist_minimal_change_resolution.py
  component/
    services/
      gh_enricher/
        test_worker_rehydrates_job_from_event_outbox.py
        test_repo_snapshot_write_and_outbox_emit.py
        test_existing_current_snapshot_standard_refresh_short_circuit.py
```

원칙:

- GitHub 관련 구현은 `src/services/gh_enricher/` 아래로만 모은다.
- shared canonicalization은 이미 stage 4 / stage 11에서 잠겼으므로, gh-enricher 내부에 새 canonicalizer를 만들지 않는다.
- discovered link는 observation만 남기고, artifact mutation은 뒤 단계로 넘긴다.

---

## 5. 이번 단계에서 고정할 구현 규칙

## 5-1. Redis payload는 계속 얇게 유지한다

Redis Streams 메시지는 여전히 outbox-relay가 싣는 최소 필드만 믿는다.

- `job_id`
- `stage_name`
- `root_object_type`
- `root_object_id`
- `idempotency_key`
- `pipeline_run_id`
- `not_before`
- `trigger_event_id`

따라서 gh-enricher worker도 **Redis 본문에서 business payload를 읽지 않는다.**
반드시 `trigger_event_id`로 `event_outbox`를 다시 조회해 enrichment job을 복원한다.

즉, 이 단계에서도 durable truth는 Postgres다.

---

## 5-2. 입력 계약은 `artifact.enrich.requested.v1`만 받는다

기본 입력 이벤트는 다음 하나다.

- `artifact.enrich.requested.v1`

이벤트 payload 최소 필드는 이미 잠겨 있다.

- `candidate_group_id`
- `artifact_id`
- `artifact_type`
- `provider_route`
- `refresh_mode`
- `depth_budget`

gh-enricher는 다음을 반드시 검증한다.

1. `provider_route == github`
2. `artifact_type in {github_repo, github_subpath, github_repo_page, github_gist}`
3. `artifact_id`가 `artifact_registry`에 존재

이 세 가지 중 하나라도 깨지면 worker는 GitHub fetch를 시도하면 안 된다.

---

## 5-3. auth mode는 두 단계다

### 기본
- `auth_mode = app_installation`

### degraded fallback
- 인증 장애가 있을 때
- 대상이 public repo로 처리 가능할 때만
- `auth_mode = anonymous_degraded`

금지:
- private 전제 fetch
- token 영구 저장
- GitHub App private key를 다른 서비스로 넘기기

즉, GitHub App은 gh-enricher 내부에서만 사용한다.

---

## 5-4. fetch 전략은 3계층으로 고정한다

### 계층 1. identity/meta
가장 먼저 읽는다.

- repo identity
- default branch
- archived/fork/template
- description
- homepage
- license
- topics
- pushed_at
- stars/watchers/forks/open issues 같은 약한 운영 신호

### 계층 2. tree/shape
다음에 구조를 읽는다.

- contents API or tree API
- recursive tree가 너무 크면 truncated anomaly를 기록
- key path / tests / CI / examples / docs 후보 path를 구조적으로 확보

### 계층 3. evidence file sampling
마지막에 역할 기반 샘플링을 한다.

우선순위:

1. `README*`
2. manifest
3. lockfile
4. CI
5. tests
6. examples/demo
7. docs
8. entrypoint candidate
9. config
10. release metadata

즉, `gh-enricher`는 crawler가 아니라  
**repo evidence sampler**다.

---

## 5-5. archive fallback은 hook만 남기고 기본 구현은 defer한다

5단계 정본은 archive fallback을 허용했지만 기본 전략으로 두지 않았다.  
이번 v0.1도 그 해석을 유지한다.

- interface hook은 둔다
- 기본 path는 metadata + tree + contents + releases
- archive/tarball은 아직 실제 구현하지 않음
- archive가 필요한 상황은 `fetch_anomalies`에 남긴다

이건 기능 누락이 아니라,  
**scope explosion을 막기 위한 보수적 구현 순서**다.

---

## 5-6. repo/subpath/page/gist별 처리 원칙

### `github_repo`
- full repo evidence path
- repo child row 생성
- file samples 생성 가능
- 가장 표준적인 path

### `github_subpath`
- 상위 repo anchor로 fetch
- child row는 repo snapshot으로 저장
- `normalized_projection`에 `focus_path`, `resolved_ref`, `focus_kind=subpath` 보강
- file samples는 subpath 중심 우선순위 조정 가능

### `github_repo_page`
- 상위 repo anchor로 fetch
- `normalized_projection`에 `page_path`, `focus_kind=repo_page` 보강
- issue/pull/release 본문 deep fetch는 v0.1에서 하지 않음
- 기본은 repo anchor evidence + page context marker

### `github_gist`
- parent snapshot only
- gist metadata / files / language mix / truncated 여부를 `normalized_projection`에 저장
- child repo row는 생성하지 않음
- file sample row도 기본 생성하지 않음

---

## 5-7. discovered links는 observation만 남긴다

README나 docs, sampled file excerpt에서 링크를 발견할 수 있다.  
하지만 `gh-enricher`가 새 artifact를 직접 만들면 stage 4 규칙과 충돌한다.

따라서 이번 단계의 규칙은 고정한다.

- URL 발견
- `discovered_url_observations`에 저장
- discovery reason 기록
- depth remaining 기록
- **새 artifact 생성 안 함**
- **candidate member 수정 안 함**
- **reroot 확정 안 함**

즉, gh-enricher는 supporting discovery signal만 남긴다.

---

## 5-8. 출력 경계는 세 단계다

### 1) `artifact_enrichment_runs`
요청/시도/종료 상태 기록

### 2) `artifact_snapshots` + source-specific projection
append-only snapshot 저장

### 3) `artifact.snapshot.updated.v1`
후단에게 snapshot availability 알림

중요:
- snapshot write 성공 후에만 current pointer 업데이트
- current pointer 업데이트 성공 후에만 outbox emit
- 이 세 단계는 가능하면 한 transaction 경계로 묶는다

---

## 5-9. status 모델은 partial success를 기본 전제로 둔다

권장 상태 사용:

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

예시:

- repo meta + tree + README 확보, tests/examples 부족  
  → `partial_ready`

- gist metadata는 읽혔지만 파일 content excerpt 없음  
  → `partial_ready`

- GitHub 429 / abuse / secondary limit  
  → `rate_limited`

- artifact type mismatch or impossible locator  
  → `unsupported` or `failed_permanent`

---

## 5-10. idempotency key는 현재 snapshot 입력 상태를 기준으로 만든다

8단계 observability 문서가 enrich idempotency 예시를 잠갔다.

- `enrich:{artifact_id}:{profile}:{snapshot_input_hash}`

이번 단계에서는 GitHub용으로 아래처럼 해석한다.

```text
enrich:github:{artifact_id}:{snapshot_input_hash}
```

`snapshot_input_hash`는 최소 아래를 섞는다.

- `artifact_id`
- `artifact_type`
- `refresh_mode`
- `depth_budget`
- `current_snapshot_id`
- `current_status`

이렇게 하면,

- current snapshot이 없는 중복 요청은 dedupe
- snapshot이 갱신되면 새 enrichment run 허용
- refresh mode가 다르면 별도 run 허용

---

## 5-11. existing current snapshot reuse는 보수적으로 허용한다

현재 snapshot이 이미 있고, 다음을 만족하면 short-circuit 가능하다.

- `refresh_mode == standard`
- `current_snapshot_id is not None`
- `current_status in {ready, partial_ready}`

이 경우:

1. 새 API fetch는 건너뛴다.
2. 새 snapshot row는 만들지 않는다.
3. 대신 enrichment run은 남길 수 있다.
4. `artifact.snapshot.updated.v1`는 **현재 snapshot_id** 기준으로 다시 emit 가능하다.

이 경로는 같은 artifact를 여러 candidate가 공유할 때 비용을 줄이는 데 유용하다.

---

## 5-12. q.artifact.enrich.github worker는 ack-first가 아니라 state-first다

이번 단계는 retry/reclaim hardening 전 단계이므로, worker는 아래를 고정한다.

1. job rehydrate
2. enrichment run row 생성
3. fetch / snapshot write / outbox emit
4. 최종 상태를 Postgres에 기록
5. 그 후 stream ack

즉, Redis ack보다 Postgres state가 먼저다.

다만 v0.1에서는 retry budget / reclaim을 아직 닫지 않으므로,  
**실패도 durable row에 먼저 남기고 ack하는 보수적 worker**로 두는 편이 안전하다.

이 해석은 다음과 같다.

- Redis는 short-lived execution state
- 실패 원천 기록은 Postgres
- 재시도는 뒤 단계 maintenance/replay가 담당

---

## 6. 코드 초안

## 6-1. `src/services/gh_enricher/__init__.py`

```python
from .config import GhEnricherConfig
from .service import GhEnricherService
from .worker import GhEnricherWorker

__all__ = [
    "GhEnricherConfig",
    "GhEnricherService",
    "GhEnricherWorker",
]
```

---

## 6-2. `src/services/gh_enricher/config.py`

```python
from __future__ import annotations

import os
from dataclasses import dataclass


class GhEnricherConfigurationError(ValueError):
    pass


@dataclass(slots=True, frozen=True)
class GhEnricherConfig:
    app_env: str
    database_url: str
    redis_url: str

    queue_name: str
    consumer_group: str
    consumer_name: str
    batch_size: int
    block_ms: int

    github_api_base_url: str
    github_app_id: str | None
    github_installation_id: str | None
    github_private_key: str | None

    request_timeout_sec: float
    sample_max_files: int
    sample_excerpt_chars: int
    max_file_bytes: int
    stale_after_sec: int

    log_level: str

    @classmethod
    def from_env(cls) -> "GhEnricherConfig":
        def _read(name: str, default: str = "") -> str:
            return os.getenv(name, default).strip()

        cfg = cls(
            app_env=_read("APP_ENV", "dev").lower(),
            database_url=_read("DATABASE_URL"),
            redis_url=_read("REDIS_URL"),
            queue_name=_read("GH_ENRICHER_QUEUE_NAME", "q.artifact.enrich.github"),
            consumer_group=_read("GH_ENRICHER_CONSUMER_GROUP", "gh-enricher"),
            consumer_name=_read("GH_ENRICHER_CONSUMER_NAME", "gh-enricher-1"),
            batch_size=int(_read("GH_ENRICHER_BATCH_SIZE", "10")),
            block_ms=int(_read("GH_ENRICHER_BLOCK_MS", "5000")),
            github_api_base_url=_read("GITHUB_API_BASE_URL", "https://api.github.com"),
            github_app_id=_read("GITHUB_APP_ID") or None,
            github_installation_id=_read("GITHUB_INSTALLATION_ID") or None,
            github_private_key=_read("GITHUB_PRIVATE_KEY") or None,
            request_timeout_sec=float(_read("GH_ENRICHER_REQUEST_TIMEOUT_SEC", "10")),
            sample_max_files=int(_read("GH_ENRICHER_SAMPLE_MAX_FILES", "20")),
            sample_excerpt_chars=int(_read("GH_ENRICHER_SAMPLE_EXCERPT_CHARS", "1200")),
            max_file_bytes=int(_read("GH_ENRICHER_MAX_FILE_BYTES", "131072")),
            stale_after_sec=int(_read("GH_ENRICHER_STALE_AFTER_SEC", "21600")),
            log_level=_read("LOG_LEVEL", "INFO").upper(),
        )
        cfg.validate()
        return cfg

    def validate(self) -> None:
        if not self.database_url:
            raise GhEnricherConfigurationError("DATABASE_URL is required")
        if not self.redis_url:
            raise GhEnricherConfigurationError("REDIS_URL is required")
        if not self.queue_name:
            raise GhEnricherConfigurationError("GH_ENRICHER_QUEUE_NAME must not be empty")
        if not self.consumer_group:
            raise GhEnricherConfigurationError("GH_ENRICHER_CONSUMER_GROUP must not be empty")
        if not self.consumer_name:
            raise GhEnricherConfigurationError("GH_ENRICHER_CONSUMER_NAME must not be empty")
        if self.batch_size <= 0 or self.batch_size > 100:
            raise GhEnricherConfigurationError("GH_ENRICHER_BATCH_SIZE must be between 1 and 100")
        if self.block_ms <= 0:
            raise GhEnricherConfigurationError("GH_ENRICHER_BLOCK_MS must be > 0")
        if self.request_timeout_sec <= 0:
            raise GhEnricherConfigurationError("GH_ENRICHER_REQUEST_TIMEOUT_SEC must be > 0")
        if self.sample_max_files <= 0:
            raise GhEnricherConfigurationError("GH_ENRICHER_SAMPLE_MAX_FILES must be > 0")
        if self.sample_excerpt_chars <= 0:
            raise GhEnricherConfigurationError("GH_ENRICHER_SAMPLE_EXCERPT_CHARS must be > 0")
        if self.max_file_bytes <= 0:
            raise GhEnricherConfigurationError("GH_ENRICHER_MAX_FILE_BYTES must be > 0")
        if self.stale_after_sec <= 0:
            raise GhEnricherConfigurationError("GH_ENRICHER_STALE_AFTER_SEC must be > 0")
```

---

## 6-3. `src/services/gh_enricher/models.py`

```python
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Literal


ArtifactType = Literal[
    "github_repo",
    "github_subpath",
    "github_repo_page",
    "github_gist",
]

SnapshotStatus = Literal[
    "pending",
    "fetching",
    "ready",
    "partial_ready",
    "failed_transient",
    "failed_permanent",
    "rate_limited",
    "access_denied",
    "unsupported",
    "low_evidence",
]

AuthMode = Literal["app_installation", "anonymous_degraded"]


@dataclass(slots=True, frozen=True)
class ArtifactEnrichmentJob:
    trigger_event_id: str
    event_type: str
    candidate_group_id: str
    artifact_id: str
    artifact_type: ArtifactType
    provider_route: str
    refresh_mode: str
    depth_budget: int


@dataclass(slots=True, frozen=True)
class ArtifactRecord:
    artifact_id: str
    artifact_type: ArtifactType
    canonical_id: str
    canonical_url: str | None
    normalized_host: str | None
    artifact_key_json: dict[str, Any] | None
    current_snapshot_id: str | None
    current_status: str | None


@dataclass(slots=True, frozen=True)
class CurrentSnapshotRef:
    snapshot_id: str
    status: str
    fetched_at: datetime
    content_anchor: str
    normalized_projection: dict[str, Any] | None = None


@dataclass(slots=True, frozen=True)
class GitHubArtifactLocator:
    artifact_type: ArtifactType
    owner: str | None = None
    repo: str | None = None
    ref: str | None = None
    path: str | None = None
    page_path: str | None = None
    gist_id: str | None = None


@dataclass(slots=True, frozen=True)
class GitHubFileSample:
    path: str
    role: str
    size_bytes: int | None
    content_hash: str | None
    excerpt: str | None
    raw_blob_ref: str | None = None


@dataclass(slots=True, frozen=True)
class GitHubRepoProjection:
    repo_full_name: str
    default_branch: str | None
    resolved_ref: str | None
    content_anchor_commit_sha: str | None
    repo_flags_json: dict[str, Any] | None
    license_spdx: str | None
    topics_json: list[str] | None
    readme_excerpt: str | None
    detected_build_systems_json: list[str] | None
    detected_languages_json: list[str] | None
    key_paths_json: list[str] | None
    test_paths_json: list[str] | None
    ci_paths_json: list[str] | None
    examples_paths_json: list[str] | None
    docs_paths_json: list[str] | None
    release_summary_json: dict[str, Any] | None
    normalized_projection: dict[str, Any] | None = None
    sampled_files: list[GitHubFileSample] = field(default_factory=list)


@dataclass(slots=True, frozen=True)
class DiscoveredUrlObservationDraft:
    parent_candidate_group_id: str
    parent_artifact_id: str
    observed_url: str
    context_path: str
    discovery_reason: str
    depth_remaining: int = 0


@dataclass(slots=True, frozen=True)
class SnapshotWritePlan:
    snapshot_type: str
    status: SnapshotStatus
    content_anchor: str
    auth_mode: AuthMode
    normalized_projection: dict[str, Any] | None
    raw_payload_ref: str | None
    evidence_limitations: list[str]
    fetch_anomalies: list[str]
    repo_child: GitHubRepoProjection | None = None
    discovered_urls: list[DiscoveredUrlObservationDraft] = field(default_factory=list)


@dataclass(slots=True, frozen=True)
class EnrichmentResult:
    artifact_id: str
    snapshot_id: str | None
    status: SnapshotStatus
    content_anchor: str | None
    emitted_snapshot_updated: bool
```

---

## 6-4. `src/services/gh_enricher/github_app_auth.py`

```python
from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Any

import httpx
import jwt  # PyJWT


@dataclass(slots=True)
class GitHubInstallationToken:
    token: str
    expires_at_epoch: int


class GitHubAppTokenProvider:
    def __init__(
        self,
        *,
        app_id: str,
        installation_id: str,
        private_key_pem: str,
        api_base_url: str,
        timeout_sec: float,
    ) -> None:
        self._app_id = app_id
        self._installation_id = installation_id
        self._private_key_pem = private_key_pem
        self._api_base_url = api_base_url.rstrip("/")
        self._timeout_sec = timeout_sec
        self._cached: GitHubInstallationToken | None = None

    async def get_token(self) -> str:
        now = int(time.time())
        if self._cached is not None and now < self._cached.expires_at_epoch - 60:
            return self._cached.token

        app_jwt = self._build_app_jwt(now)
        async with httpx.AsyncClient(timeout=self._timeout_sec) as client:
            response = await client.post(
                f"{self._api_base_url}/app/installations/{self._installation_id}/access_tokens",
                headers={
                    "Authorization": f"Bearer {app_jwt}",
                    "Accept": "application/vnd.github+json",
                },
            )
            response.raise_for_status()
            payload = response.json()

        expires_at = payload.get("expires_at")
        expires_at_epoch = self._iso_to_epoch(expires_at)
        token = str(payload["token"])
        self._cached = GitHubInstallationToken(token=token, expires_at_epoch=expires_at_epoch)
        return token

    def _build_app_jwt(self, now: int) -> str:
        payload = {
            "iat": now - 60,
            "exp": now + 540,
            "iss": self._app_id,
        }
        encoded = jwt.encode(payload, self._private_key_pem, algorithm="RS256")
        return encoded if isinstance(encoded, str) else encoded.decode("utf-8")

    @staticmethod
    def _iso_to_epoch(value: Any) -> int:
        if not isinstance(value, str) or not value:
            return int(time.time()) + 300
        from datetime import datetime

        return int(datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp())
```

---

## 6-5. `src/services/gh_enricher/github_client.py`

```python
from __future__ import annotations

from typing import Any

import httpx


class GitHubClientError(Exception):
    pass


class GitHubRateLimitedError(GitHubClientError):
    pass


class GitHubAccessDeniedError(GitHubClientError):
    pass


class GitHubNotFoundError(GitHubClientError):
    pass


class GitHubClient:
    def __init__(
        self,
        *,
        api_base_url: str,
        timeout_sec: float,
        token_provider=None,
    ) -> None:
        self._api_base_url = api_base_url.rstrip("/")
        self._timeout_sec = timeout_sec
        self._token_provider = token_provider

    async def get_repo(self, owner: str, repo: str, *, auth_mode: str) -> dict[str, Any]:
        return await self._get_json(f"/repos/{owner}/{repo}", auth_mode=auth_mode)

    async def get_tree(self, owner: str, repo: str, ref: str, *, recursive: bool, auth_mode: str) -> dict[str, Any]:
        suffix = "?recursive=1" if recursive else ""
        return await self._get_json(f"/repos/{owner}/{repo}/git/trees/{ref}{suffix}", auth_mode=auth_mode)

    async def get_contents(self, owner: str, repo: str, path: str, *, ref: str | None, auth_mode: str) -> dict[str, Any] | list[dict[str, Any]]:
        ref_suffix = f"?ref={ref}" if ref else ""
        return await self._get_json(f"/repos/{owner}/{repo}/contents/{path}{ref_suffix}", auth_mode=auth_mode)

    async def get_releases(self, owner: str, repo: str, *, auth_mode: str) -> list[dict[str, Any]]:
        payload = await self._get_json(f"/repos/{owner}/{repo}/releases", auth_mode=auth_mode)
        return payload if isinstance(payload, list) else []

    async def get_default_branch_head(self, owner: str, repo: str, default_branch: str, *, auth_mode: str) -> dict[str, Any]:
        return await self._get_json(f"/repos/{owner}/{repo}/commits/{default_branch}", auth_mode=auth_mode)

    async def get_gist(self, gist_id: str, *, auth_mode: str) -> dict[str, Any]:
        return await self._get_json(f"/gists/{gist_id}", auth_mode=auth_mode)

    async def get_text_download(self, url: str) -> str:
        async with httpx.AsyncClient(timeout=self._timeout_sec, follow_redirects=True) as client:
            response = await client.get(url, headers={"Accept": "text/plain"})
            response.raise_for_status()
            return response.text

    async def _get_json(self, path: str, *, auth_mode: str) -> Any:
        headers = {"Accept": "application/vnd.github+json"}
        token = None
        if auth_mode == "app_installation" and self._token_provider is not None:
            token = await self._token_provider.get_token()
            headers["Authorization"] = f"Bearer {token}"

        async with httpx.AsyncClient(
            base_url=self._api_base_url,
            timeout=self._timeout_sec,
            follow_redirects=True,
        ) as client:
            response = await client.get(path, headers=headers)

        if response.status_code == 404:
            raise GitHubNotFoundError(response.text)
        if response.status_code in {401, 403}:
            body = response.text.upper()
            if "RATE LIMIT" in body or response.headers.get("x-ratelimit-remaining") == "0":
                raise GitHubRateLimitedError(response.text)
            raise GitHubAccessDeniedError(response.text)
        if response.status_code >= 500:
            raise GitHubClientError(response.text)
        response.raise_for_status()
        return response.json()
```

---

## 6-6. `src/services/gh_enricher/fetch_planner.py`

```python
from __future__ import annotations

from urllib.parse import urlparse

from .models import ArtifactRecord, GitHubArtifactLocator


class GitHubFetchPlanner:
    def build_locator(self, artifact: ArtifactRecord) -> GitHubArtifactLocator:
        key = artifact.artifact_key_json or {}
        artifact_type = artifact.artifact_type

        if artifact_type == "github_gist":
            gist_id = key.get("gist_id")
            if gist_id:
                return GitHubArtifactLocator(artifact_type="github_gist", gist_id=str(gist_id))
            raise ValueError("github_gist artifact missing gist_id")

        owner = key.get("owner")
        repo = key.get("repo")
        if owner and repo:
            return GitHubArtifactLocator(
                artifact_type=artifact_type,
                owner=str(owner),
                repo=str(repo),
                ref=self._maybe_str(key.get("ref")),
                path=self._maybe_str(key.get("path")),
                page_path=self._maybe_str(key.get("page_path")),
            )

        # Fallback to canonical_url parse for repo artifact
        parsed = urlparse(artifact.canonical_url or "")
        parts = [p for p in parsed.path.split("/") if p]
        if len(parts) >= 2:
            return GitHubArtifactLocator(
                artifact_type=artifact_type,
                owner=parts[0],
                repo=parts[1].removesuffix(".git"),
            )

        raise ValueError(f"unable to derive github locator for artifact_id={artifact.artifact_id}")

    @staticmethod
    def _maybe_str(value) -> str | None:
        if value is None:
            return None
        value = str(value).strip()
        return value or None
```

---

## 6-7. `src/services/gh_enricher/file_sampler.py`

```python
from __future__ import annotations

import base64
import hashlib
from dataclasses import dataclass
from typing import Any

from .models import GitHubFileSample


@dataclass(slots=True, frozen=True)
class CandidatePath:
    path: str
    role: str


class GitHubFileSampler:
    _ROLE_ORDER = [
        ("README", ("README", "README.md", "README.rst")),
        ("manifest", ("package.json", "pyproject.toml", "requirements.txt", "requirements-dev.txt", "Cargo.toml", "go.mod", "pom.xml")),
        ("lockfile", ("package-lock.json", "pnpm-lock.yaml", "yarn.lock", "poetry.lock", "Cargo.lock")),
        ("ci", (".github/workflows/",)),
        ("tests", ("tests/", "test/", "__tests__/")),
        ("examples", ("examples/", "example/", "demo/")),
        ("docs", ("docs/", "doc/")),
        ("entrypoint", ("main.py", "src/main.py", "app.py", "cli.py")),
        ("config", (".env.example", "config/", "settings/")),
    ]

    def select_paths(self, tree_entries: list[dict[str, Any]], *, max_files: int) -> list[CandidatePath]:
        normalized_paths = [str(entry.get("path", "")) for entry in tree_entries if entry.get("type") == "blob"]
        seen: set[str] = set()
        selected: list[CandidatePath] = []

        for role, prefixes in self._ROLE_ORDER:
            for path in normalized_paths:
                if path in seen:
                    continue
                if any(path == prefix or path.startswith(prefix) for prefix in prefixes):
                    selected.append(CandidatePath(path=path, role=role))
                    seen.add(path)
                    if len(selected) >= max_files:
                        return selected
        return selected[:max_files]

    def build_sample(
        self,
        *,
        path: str,
        role: str,
        raw_text: str,
        size_bytes: int | None,
        excerpt_chars: int,
    ) -> GitHubFileSample:
        excerpt = raw_text[:excerpt_chars] if raw_text else None
        content_hash = hashlib.sha256(raw_text.encode("utf-8")).hexdigest() if raw_text else None
        return GitHubFileSample(
            path=path,
            role=role,
            size_bytes=size_bytes,
            content_hash=content_hash,
            excerpt=excerpt,
            raw_blob_ref=None,
        )

    @staticmethod
    def decode_contents_response(payload: dict[str, Any]) -> str:
        if payload.get("encoding") == "base64" and isinstance(payload.get("content"), str):
            return base64.b64decode(payload["content"]).decode("utf-8", errors="replace")
        return ""
```

---

## 6-8. `src/services/gh_enricher/url_discovery.py`

```python
from __future__ import annotations

import re

from .models import DiscoveredUrlObservationDraft, GitHubRepoProjection


_URL_RE = re.compile(r"https?://[^\s<>()\[\]{}\"']+")


class GitHubUrlDiscovery:
    def discover(
        self,
        *,
        candidate_group_id: str,
        parent_artifact_id: str,
        repo_projection: GitHubRepoProjection | None,
    ) -> list[DiscoveredUrlObservationDraft]:
        if repo_projection is None:
            return []

        results: list[DiscoveredUrlObservationDraft] = []

        def add(url: str, context_path: str, reason: str) -> None:
            results.append(
                DiscoveredUrlObservationDraft(
                    parent_candidate_group_id=candidate_group_id,
                    parent_artifact_id=parent_artifact_id,
                    observed_url=url,
                    context_path=context_path,
                    discovery_reason=reason,
                    depth_remaining=0,
                )
            )

        if repo_projection.readme_excerpt:
            for idx, match in enumerate(_URL_RE.findall(repo_projection.readme_excerpt)):
                add(match, f"readme_excerpt.url[{idx}]", "github_readme_embedded_link")

        for sample in repo_projection.sampled_files:
            if not sample.excerpt:
                continue
            for idx, match in enumerate(_URL_RE.findall(sample.excerpt)):
                add(match, f"sampled_files[{sample.path}].url[{idx}]", f"github_sample_{sample.role}_embedded_link")

        return results
```

---

## 6-9. `src/services/gh_enricher/repositories.py`

```python
from __future__ import annotations

import json
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Any, AsyncIterator

import sqlalchemy as sa
from sqlalchemy.ext.asyncio import AsyncSession

from .models import (
    ArtifactEnrichmentJob,
    ArtifactRecord,
    CurrentSnapshotRef,
    DiscoveredUrlObservationDraft,
    GitHubFileSample,
    GitHubRepoProjection,
    SnapshotWritePlan,
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


class GhEnricherRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    @asynccontextmanager
    async def transaction(self) -> AsyncIterator[AsyncSession]:
        if self._session.in_transaction():
            yield self._session
            return
        async with self._session.begin():
            yield self._session

    async def load_job_by_trigger_event_id(self, trigger_event_id: str) -> ArtifactEnrichmentJob | None:
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
        return ArtifactEnrichmentJob(
            trigger_event_id=str(row["event_id"]),
            event_type=str(row["event_type"]),
            candidate_group_id=str(payload["candidate_group_id"]),
            artifact_id=str(payload["artifact_id"]),
            artifact_type=str(payload["artifact_type"]),
            provider_route=str(payload["provider_route"]),
            refresh_mode=str(payload["refresh_mode"]),
            depth_budget=int(payload["depth_budget"]),
        )

    async def load_artifact(self, artifact_id: str) -> ArtifactRecord | None:
        result = await self._session.execute(
            sa.text(
                """
                SELECT
                    artifact_id,
                    artifact_type,
                    canonical_id,
                    canonical_url,
                    normalized_host,
                    artifact_key_json,
                    current_snapshot_id,
                    current_status
                FROM artifact_registry
                WHERE artifact_id = CAST(:artifact_id AS uuid)
                """
            ),
            {"artifact_id": artifact_id},
        )
        row = result.mappings().first()
        if row is None:
            return None

        return ArtifactRecord(
            artifact_id=str(row["artifact_id"]),
            artifact_type=str(row["artifact_type"]),
            canonical_id=str(row["canonical_id"]),
            canonical_url=row["canonical_url"],
            normalized_host=row["normalized_host"],
            artifact_key_json=row["artifact_key_json"],
            current_snapshot_id=str(row["current_snapshot_id"]) if row["current_snapshot_id"] else None,
            current_status=str(row["current_status"]) if row["current_status"] else None,
        )

    async def load_current_snapshot(self, snapshot_id: str | None) -> CurrentSnapshotRef | None:
        if snapshot_id is None:
            return None
        result = await self._session.execute(
            sa.text(
                """
                SELECT snapshot_id, status, fetched_at, content_anchor, normalized_projection
                FROM artifact_snapshots
                WHERE snapshot_id = CAST(:snapshot_id AS uuid)
                """
            ),
            {"snapshot_id": snapshot_id},
        )
        row = result.mappings().first()
        if row is None:
            return None
        return CurrentSnapshotRef(
            snapshot_id=str(row["snapshot_id"]),
            status=str(row["status"]),
            fetched_at=row["fetched_at"],
            content_anchor=str(row["content_anchor"]),
            normalized_projection=row["normalized_projection"],
        )

    async def insert_enrichment_run_if_absent(
        self,
        *,
        artifact_id: str,
        provider: str,
        refresh_mode: str,
        depth_budget: int,
        status: str,
        job_idempotency_key: str,
        content_anchor: str | None = None,
    ) -> str | None:
        result = await self._session.execute(
            sa.text(
                """
                INSERT INTO artifact_enrichment_runs (
                    artifact_id,
                    provider,
                    refresh_mode,
                    depth_budget,
                    status,
                    content_anchor,
                    job_idempotency_key,
                    requested_at
                )
                VALUES (
                    CAST(:artifact_id AS uuid),
                    :provider,
                    :refresh_mode,
                    :depth_budget,
                    CAST(:status AS snapshot_status_enum),
                    :content_anchor,
                    :job_idempotency_key,
                    now()
                )
                ON CONFLICT (job_idempotency_key) DO NOTHING
                RETURNING artifact_enrichment_run_id
                """
            ),
            {
                "artifact_id": artifact_id,
                "provider": provider,
                "refresh_mode": refresh_mode,
                "depth_budget": depth_budget,
                "status": status,
                "content_anchor": content_anchor,
                "job_idempotency_key": job_idempotency_key,
            },
        )
        row = result.scalar_one_or_none()
        return str(row) if row else None

    async def mark_enrichment_run_started(self, run_id: str) -> None:
        await self._session.execute(
            sa.text(
                """
                UPDATE artifact_enrichment_runs
                SET status = 'fetching'::snapshot_status_enum,
                    started_at = now()
                WHERE artifact_enrichment_run_id = CAST(:run_id AS uuid)
                """
            ),
            {"run_id": run_id},
        )

    async def mark_enrichment_run_finished(
        self,
        *,
        run_id: str,
        status: str,
        content_anchor: str | None,
    ) -> None:
        await self._session.execute(
            sa.text(
                """
                UPDATE artifact_enrichment_runs
                SET status = CAST(:status AS snapshot_status_enum),
                    content_anchor = :content_anchor,
                    finished_at = now()
                WHERE artifact_enrichment_run_id = CAST(:run_id AS uuid)
                """
            ),
            {
                "run_id": run_id,
                "status": status,
                "content_anchor": content_anchor,
            },
        )

    async def insert_snapshot(self, *, artifact_id: str, provider: str, plan: SnapshotWritePlan) -> str:
        result = await self._session.execute(
            sa.text(
                """
                INSERT INTO artifact_snapshots (
                    artifact_id,
                    provider,
                    snapshot_type,
                    status,
                    fetched_at,
                    content_anchor,
                    auth_mode,
                    normalized_projection,
                    raw_payload_ref,
                    evidence_limitations,
                    fetch_anomalies
                )
                VALUES (
                    CAST(:artifact_id AS uuid),
                    :provider,
                    :snapshot_type,
                    CAST(:status AS snapshot_status_enum),
                    now(),
                    :content_anchor,
                    :auth_mode,
                    CAST(:normalized_projection AS jsonb),
                    :raw_payload_ref,
                    CAST(:evidence_limitations AS jsonb),
                    CAST(:fetch_anomalies AS jsonb)
                )
                RETURNING snapshot_id
                """
            ),
            {
                "artifact_id": artifact_id,
                "provider": provider,
                "snapshot_type": plan.snapshot_type,
                "status": plan.status,
                "content_anchor": plan.content_anchor,
                "auth_mode": plan.auth_mode,
                "normalized_projection": _jsonb_dumps(plan.normalized_projection),
                "raw_payload_ref": plan.raw_payload_ref,
                "evidence_limitations": _jsonb_dumps(plan.evidence_limitations),
                "fetch_anomalies": _jsonb_dumps(plan.fetch_anomalies),
            },
        )
        return str(result.scalar_one())

    async def insert_github_repo_child(self, *, snapshot_id: str, repo: GitHubRepoProjection) -> None:
        await self._session.execute(
            sa.text(
                """
                INSERT INTO artifact_snapshot_github_repo (
                    snapshot_id,
                    repo_full_name,
                    default_branch,
                    resolved_ref,
                    content_anchor_commit_sha,
                    repo_flags_json,
                    license_spdx,
                    topics_json,
                    readme_excerpt,
                    detected_build_systems_json,
                    detected_languages_json,
                    key_paths_json,
                    test_paths_json,
                    ci_paths_json,
                    examples_paths_json,
                    docs_paths_json,
                    release_summary_json
                )
                VALUES (
                    CAST(:snapshot_id AS uuid),
                    :repo_full_name,
                    :default_branch,
                    :resolved_ref,
                    :content_anchor_commit_sha,
                    CAST(:repo_flags_json AS jsonb),
                    :license_spdx,
                    CAST(:topics_json AS jsonb),
                    :readme_excerpt,
                    CAST(:detected_build_systems_json AS jsonb),
                    CAST(:detected_languages_json AS jsonb),
                    CAST(:key_paths_json AS jsonb),
                    CAST(:test_paths_json AS jsonb),
                    CAST(:ci_paths_json AS jsonb),
                    CAST(:examples_paths_json AS jsonb),
                    CAST(:docs_paths_json AS jsonb),
                    CAST(:release_summary_json AS jsonb)
                )
                """
            ),
            {
                "snapshot_id": snapshot_id,
                "repo_full_name": repo.repo_full_name,
                "default_branch": repo.default_branch,
                "resolved_ref": repo.resolved_ref,
                "content_anchor_commit_sha": repo.content_anchor_commit_sha,
                "repo_flags_json": _jsonb_dumps(repo.repo_flags_json),
                "license_spdx": repo.license_spdx,
                "topics_json": _jsonb_dumps(repo.topics_json),
                "readme_excerpt": repo.readme_excerpt,
                "detected_build_systems_json": _jsonb_dumps(repo.detected_build_systems_json),
                "detected_languages_json": _jsonb_dumps(repo.detected_languages_json),
                "key_paths_json": _jsonb_dumps(repo.key_paths_json),
                "test_paths_json": _jsonb_dumps(repo.test_paths_json),
                "ci_paths_json": _jsonb_dumps(repo.ci_paths_json),
                "examples_paths_json": _jsonb_dumps(repo.examples_paths_json),
                "docs_paths_json": _jsonb_dumps(repo.docs_paths_json),
                "release_summary_json": _jsonb_dumps(repo.release_summary_json),
            },
        )

    async def insert_github_file_sample(self, *, snapshot_id: str, sample: GitHubFileSample) -> None:
        await self._session.execute(
            sa.text(
                """
                INSERT INTO artifact_snapshot_github_file_samples (
                    file_sample_id,
                    snapshot_id,
                    path,
                    role,
                    size_bytes,
                    content_hash,
                    excerpt,
                    raw_blob_ref
                )
                VALUES (
                    gen_random_uuid(),
                    CAST(:snapshot_id AS uuid),
                    :path,
                    :role,
                    :size_bytes,
                    :content_hash,
                    :excerpt,
                    :raw_blob_ref
                )
                """
            ),
            {
                "snapshot_id": snapshot_id,
                "path": sample.path,
                "role": sample.role,
                "size_bytes": sample.size_bytes,
                "content_hash": sample.content_hash,
                "excerpt": sample.excerpt,
                "raw_blob_ref": sample.raw_blob_ref,
            },
        )

    async def insert_discovered_url(self, *, snapshot_id: str, draft: DiscoveredUrlObservationDraft, parent_artifact_id: str) -> None:
        await self._session.execute(
            sa.text(
                """
                INSERT INTO discovered_url_observations (
                    discovered_url_observation_id,
                    parent_candidate_group_id,
                    parent_artifact_id,
                    parent_snapshot_id,
                    observed_url,
                    context_path,
                    discovery_reason,
                    depth_remaining,
                    created_at
                )
                VALUES (
                    gen_random_uuid(),
                    CAST(:parent_candidate_group_id AS uuid),
                    CAST(:parent_artifact_id AS uuid),
                    CAST(:parent_snapshot_id AS uuid),
                    :observed_url,
                    :context_path,
                    :discovery_reason,
                    :depth_remaining,
                    now()
                )
                """
            ),
            {
                "parent_candidate_group_id": draft.parent_candidate_group_id,
                "parent_artifact_id": parent_artifact_id,
                "parent_snapshot_id": snapshot_id,
                "observed_url": draft.observed_url,
                "context_path": draft.context_path,
                "discovery_reason": draft.discovery_reason,
                "depth_remaining": draft.depth_remaining,
            },
        )

    async def update_artifact_current_snapshot(self, *, artifact_id: str, snapshot_id: str, status: str) -> None:
        await self._session.execute(
            sa.text(
                """
                UPDATE artifact_registry
                SET
                    current_snapshot_id = CAST(:snapshot_id AS uuid),
                    current_status = CAST(:status AS snapshot_status_enum),
                    updated_at = now()
                WHERE artifact_id = CAST(:artifact_id AS uuid)
                """
            ),
            {
                "artifact_id": artifact_id,
                "snapshot_id": snapshot_id,
                "status": status,
            },
        )

    async def insert_snapshot_updated_outbox(
        self,
        *,
        artifact_id: str,
        snapshot_id: str,
        status: str,
        content_anchor: str,
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
                )
                VALUES (
                    gen_random_uuid(),
                    'artifact.snapshot.updated.v1',
                    'artifact',
                    CAST(:artifact_id AS uuid),
                    :dedupe_key,
                    CAST(:payload_json AS jsonb),
                    'pending'::outbox_status_enum,
                    now()
                )
                ON CONFLICT (dedupe_key) DO NOTHING
                """
            ),
            {
                "artifact_id": artifact_id,
                "dedupe_key": f"artifact:snapshot_updated:{artifact_id}:{snapshot_id}",
                "payload_json": _jsonb_dumps(
                    {
                        "artifact_id": artifact_id,
                        "snapshot_id": snapshot_id,
                        "provider": "github",
                        "status": status,
                        "content_anchor": content_anchor,
                    }
                ),
            },
        )
```

---

## 6-10. `src/services/gh_enricher/redis_streams.py`

```python
from __future__ import annotations

from dataclasses import dataclass

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

## 6-11. `src/services/gh_enricher/service.py`

```python
from __future__ import annotations

import hashlib
import logging

from .fetch_planner import GitHubFetchPlanner
from .file_sampler import GitHubFileSampler
from .github_client import (
    GitHubAccessDeniedError,
    GitHubClient,
    GitHubNotFoundError,
    GitHubRateLimitedError,
)
from .models import (
    ArtifactEnrichmentJob,
    ArtifactRecord,
    EnrichmentResult,
    GitHubRepoProjection,
    SnapshotWritePlan,
)
from .repositories import GhEnricherRepository
from .url_discovery import GitHubUrlDiscovery


class GhEnricherService:
    def __init__(
        self,
        config,
        *,
        repository: GhEnricherRepository,
        github_client: GitHubClient,
        fetch_planner: GitHubFetchPlanner,
        file_sampler: GitHubFileSampler,
        url_discovery: GitHubUrlDiscovery,
        logger: logging.Logger | None = None,
    ) -> None:
        self._config = config
        self._repository = repository
        self._github_client = github_client
        self._fetch_planner = fetch_planner
        self._file_sampler = file_sampler
        self._url_discovery = url_discovery
        self._logger = logger or logging.getLogger(__name__)

    async def rehydrate_job(self, trigger_event_id: str) -> ArtifactEnrichmentJob | None:
        return await self._repository.load_job_by_trigger_event_id(trigger_event_id)

    async def handle_job(self, job: ArtifactEnrichmentJob) -> EnrichmentResult:
        if job.provider_route != "github":
            return EnrichmentResult(
                artifact_id=job.artifact_id,
                snapshot_id=None,
                status="unsupported",
                content_anchor=None,
                emitted_snapshot_updated=False,
            )

        artifact = await self._repository.load_artifact(job.artifact_id)
        if artifact is None:
            return EnrichmentResult(
                artifact_id=job.artifact_id,
                snapshot_id=None,
                status="failed_permanent",
                content_anchor=None,
                emitted_snapshot_updated=False,
            )

        current_snapshot = await self._repository.load_current_snapshot(artifact.current_snapshot_id)
        if self._should_short_circuit(job=job, artifact=artifact, current_snapshot=current_snapshot):
            async with self._repository.transaction():
                await self._repository.insert_snapshot_updated_outbox(
                    artifact_id=artifact.artifact_id,
                    snapshot_id=current_snapshot.snapshot_id,
                    status=str(current_snapshot.status),
                    content_anchor=current_snapshot.content_anchor,
                )
            return EnrichmentResult(
                artifact_id=artifact.artifact_id,
                snapshot_id=current_snapshot.snapshot_id,
                status=str(current_snapshot.status),
                content_anchor=current_snapshot.content_anchor,
                emitted_snapshot_updated=True,
            )

        snapshot_input_hash = self._build_snapshot_input_hash(job=job, artifact=artifact, current_snapshot=current_snapshot)
        run_id = await self._repository.insert_enrichment_run_if_absent(
            artifact_id=artifact.artifact_id,
            provider="github",
            refresh_mode=job.refresh_mode,
            depth_budget=job.depth_budget,
            status="pending",
            job_idempotency_key=f"enrich:github:{artifact.artifact_id}:{snapshot_input_hash}",
            content_anchor=None,
        )
        if run_id is None:
            return EnrichmentResult(
                artifact_id=artifact.artifact_id,
                snapshot_id=current_snapshot.snapshot_id if current_snapshot else None,
                status=str(current_snapshot.status) if current_snapshot else "pending",
                content_anchor=current_snapshot.content_anchor if current_snapshot else None,
                emitted_snapshot_updated=False,
            )

        async with self._repository.transaction():
            await self._repository.mark_enrichment_run_started(run_id)

        try:
            plan = await self._build_snapshot_plan(job=job, artifact=artifact)
        except GitHubRateLimitedError:
            await self._repository.mark_enrichment_run_finished(run_id=run_id, status="rate_limited", content_anchor=None)
            return EnrichmentResult(
                artifact_id=artifact.artifact_id,
                snapshot_id=None,
                status="rate_limited",
                content_anchor=None,
                emitted_snapshot_updated=False,
            )
        except GitHubAccessDeniedError:
            await self._repository.mark_enrichment_run_finished(run_id=run_id, status="access_denied", content_anchor=None)
            return EnrichmentResult(
                artifact_id=artifact.artifact_id,
                snapshot_id=None,
                status="access_denied",
                content_anchor=None,
                emitted_snapshot_updated=False,
            )
        except GitHubNotFoundError:
            await self._repository.mark_enrichment_run_finished(run_id=run_id, status="failed_permanent", content_anchor=None)
            return EnrichmentResult(
                artifact_id=artifact.artifact_id,
                snapshot_id=None,
                status="failed_permanent",
                content_anchor=None,
                emitted_snapshot_updated=False,
            )
        except Exception:
            await self._repository.mark_enrichment_run_finished(run_id=run_id, status="failed_transient", content_anchor=None)
            raise

        async with self._repository.transaction():
            snapshot_id = await self._repository.insert_snapshot(
                artifact_id=artifact.artifact_id,
                provider="github",
                plan=plan,
            )
            if plan.repo_child is not None:
                await self._repository.insert_github_repo_child(snapshot_id=snapshot_id, repo=plan.repo_child)
                for sample in plan.repo_child.sampled_files:
                    await self._repository.insert_github_file_sample(snapshot_id=snapshot_id, sample=sample)

            for draft in plan.discovered_urls:
                await self._repository.insert_discovered_url(
                    snapshot_id=snapshot_id,
                    draft=draft,
                    parent_artifact_id=artifact.artifact_id,
                )

            await self._repository.update_artifact_current_snapshot(
                artifact_id=artifact.artifact_id,
                snapshot_id=snapshot_id,
                status=plan.status,
            )
            await self._repository.insert_snapshot_updated_outbox(
                artifact_id=artifact.artifact_id,
                snapshot_id=snapshot_id,
                status=plan.status,
                content_anchor=plan.content_anchor,
            )
            await self._repository.mark_enrichment_run_finished(
                run_id=run_id,
                status=plan.status,
                content_anchor=plan.content_anchor,
            )

        return EnrichmentResult(
            artifact_id=artifact.artifact_id,
            snapshot_id=snapshot_id,
            status=plan.status,
            content_anchor=plan.content_anchor,
            emitted_snapshot_updated=True,
        )

    def _should_short_circuit(self, *, job, artifact, current_snapshot) -> bool:
        if current_snapshot is None:
            return False
        if job.refresh_mode != "standard":
            return False
        return str(artifact.current_status) in {"ready", "partial_ready"}

    def _build_snapshot_input_hash(self, *, job, artifact, current_snapshot) -> str:
        raw = "|".join(
            [
                artifact.artifact_id,
                artifact.artifact_type,
                job.refresh_mode,
                str(job.depth_budget),
                current_snapshot.snapshot_id if current_snapshot else "none",
                current_snapshot.status if current_snapshot else "none",
            ]
        )
        return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:16]

    async def _build_snapshot_plan(self, *, job: ArtifactEnrichmentJob, artifact: ArtifactRecord) -> SnapshotWritePlan:
        locator = self._fetch_planner.build_locator(artifact)
        auth_mode = "app_installation"

        if locator.artifact_type == "github_gist":
            gist_payload = await self._github_client.get_gist(locator.gist_id, auth_mode=auth_mode)
            files = gist_payload.get("files") or {}
            normalized_projection = {
                "gist_id": locator.gist_id,
                "description": gist_payload.get("description"),
                "public": gist_payload.get("public"),
                "truncated": any(bool(file_obj.get("truncated")) for file_obj in files.values() if isinstance(file_obj, dict)),
                "file_names": sorted(files.keys()),
                "language_set": sorted(
                    {str(file_obj.get("language")) for file_obj in files.values() if isinstance(file_obj, dict) and file_obj.get("language")}
                ),
                "owner_login": ((gist_payload.get("owner") or {}).get("login") if isinstance(gist_payload.get("owner"), dict) else None),
            }
            return SnapshotWritePlan(
                snapshot_type="github_gist",
                status="partial_ready",
                content_anchor=f"gist:{locator.gist_id}",
                auth_mode=auth_mode,
                normalized_projection=normalized_projection,
                raw_payload_ref=None,
                evidence_limitations=["gist child snapshot schema is deferred; using parent normalized_projection only"],
                fetch_anomalies=[],
                repo_child=None,
                discovered_urls=[],
            )

        repo_payload = await self._github_client.get_repo(locator.owner, locator.repo, auth_mode=auth_mode)
        default_branch = repo_payload.get("default_branch")
        head_payload = await self._github_client.get_default_branch_head(locator.owner, locator.repo, default_branch, auth_mode=auth_mode)
        commit_sha = (((head_payload.get("sha")) or "") if isinstance(head_payload, dict) else "") or None
        tree_payload = await self._github_client.get_tree(locator.owner, locator.repo, commit_sha or default_branch, recursive=True, auth_mode=auth_mode)

        tree_entries = tree_payload.get("tree") if isinstance(tree_payload, dict) else []
        if not isinstance(tree_entries, list):
            tree_entries = []

        selected_paths = self._file_sampler.select_paths(tree_entries, max_files=self._config.sample_max_files)
        sampled_files = []
        readme_excerpt = None
        for candidate in selected_paths:
            try:
                contents_payload = await self._github_client.get_contents(
                    locator.owner,
                    locator.repo,
                    candidate.path,
                    ref=commit_sha or default_branch,
                    auth_mode=auth_mode,
                )
            except Exception:
                continue
            if not isinstance(contents_payload, dict):
                continue
            raw_text = self._file_sampler.decode_contents_response(contents_payload)
            if len(raw_text.encode("utf-8")) > self._config.max_file_bytes:
                raw_text = raw_text[: self._config.sample_excerpt_chars]
            sample = self._file_sampler.build_sample(
                path=candidate.path,
                role=candidate.role,
                raw_text=raw_text,
                size_bytes=contents_payload.get("size"),
                excerpt_chars=self._config.sample_excerpt_chars,
            )
            sampled_files.append(sample)
            if candidate.role == "README" and readme_excerpt is None:
                readme_excerpt = sample.excerpt

        releases = await self._github_client.get_releases(locator.owner, locator.repo, auth_mode=auth_mode)
        release_summary = {
            "release_count_recent": len(releases[:10]),
            "latest_release_published_at": releases[0].get("published_at") if releases else None,
            "has_release_assets": bool(releases and releases[0].get("assets")),
            "release_asset_download_count_topk": sorted(
                [
                    asset.get("download_count", 0)
                    for release in releases[:3]
                    for asset in (release.get("assets") or [])
                    if isinstance(asset, dict)
                ],
                reverse=True,
            )[:5],
            "has_prerelease_pattern": any(bool(release.get("prerelease")) for release in releases[:5]),
        }

        repo_projection = GitHubRepoProjection(
            repo_full_name=str(repo_payload.get("full_name") or f"{locator.owner}/{locator.repo}"),
            default_branch=default_branch,
            resolved_ref=commit_sha or default_branch,
            content_anchor_commit_sha=commit_sha,
            repo_flags_json={
                "archived": bool(repo_payload.get("archived", False)),
                "fork": bool(repo_payload.get("fork", False)),
                "template": bool(repo_payload.get("is_template", False)),
            },
            license_spdx=((repo_payload.get("license") or {}).get("spdx_id") if isinstance(repo_payload.get("license"), dict) else None),
            topics_json=repo_payload.get("topics") or None,
            readme_excerpt=readme_excerpt,
            detected_build_systems_json=self._detect_build_systems(sampled_files),
            detected_languages_json=sorted(
                [str(k) for k in (repo_payload.get("language") and [repo_payload.get("language")] or []) if k]
            ) or None,
            key_paths_json=self._paths_by_role(sampled_files, {"README", "manifest", "entrypoint", "config"}),
            test_paths_json=self._paths_by_role(sampled_files, {"tests"}),
            ci_paths_json=self._paths_by_role(sampled_files, {"ci"}),
            examples_paths_json=self._paths_by_role(sampled_files, {"examples"}),
            docs_paths_json=self._paths_by_role(sampled_files, {"docs"}),
            release_summary_json=release_summary,
            normalized_projection={
                "artifact_type": locator.artifact_type,
                "focus_path": locator.path,
                "page_path": locator.page_path,
                "repo_homepage": repo_payload.get("homepage"),
                "description": repo_payload.get("description"),
                "pushed_at": repo_payload.get("pushed_at"),
                "stars": repo_payload.get("stargazers_count"),
                "watchers": repo_payload.get("subscribers_count"),
                "forks": repo_payload.get("forks_count"),
                "open_issues": repo_payload.get("open_issues_count"),
                "tree_truncated": bool(tree_payload.get("truncated", False)),
            },
            sampled_files=sampled_files,
        )

        discovered = self._url_discovery.discover(
            candidate_group_id=job.candidate_group_id,
            parent_artifact_id=artifact.artifact_id,
            repo_projection=repo_projection,
        )

        anomalies = []
        if bool(tree_payload.get("truncated", False)):
            anomalies.append("git_tree_truncated")
        evidence_limitations = []
        if readme_excerpt is None:
            evidence_limitations.append("readme_excerpt_missing")
        if not sampled_files:
            evidence_limitations.append("sampled_files_missing")

        status = "ready" if readme_excerpt and sampled_files else "partial_ready"
        return SnapshotWritePlan(
            snapshot_type="github_repo",
            status=status,
            content_anchor=f"commit:{commit_sha}" if commit_sha else f"branch:{default_branch}",
            auth_mode=auth_mode,
            normalized_projection=repo_projection.normalized_projection,
            raw_payload_ref=None,
            evidence_limitations=evidence_limitations,
            fetch_anomalies=anomalies,
            repo_child=repo_projection,
            discovered_urls=discovered,
        )

    @staticmethod
    def _detect_build_systems(samples) -> list[str] | None:
        mapping = {
            "package.json": "node",
            "pyproject.toml": "python",
            "requirements.txt": "python",
            "Cargo.toml": "rust",
            "go.mod": "go",
            "pom.xml": "java",
        }
        results = []
        for sample in samples:
            name = sample.path.split("/")[-1]
            if name in mapping and mapping[name] not in results:
                results.append(mapping[name])
        return results or None

    @staticmethod
    def _paths_by_role(samples, roles: set[str]) -> list[str] | None:
        values = [sample.path for sample in samples if sample.role in roles]
        return values or None
```

---

## 6-12. `src/services/gh_enricher/worker.py`

```python
from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass

from .config import GhEnricherConfig
from .redis_streams import RedisStreamConsumer, StreamMessage
from .service import GhEnricherService


@dataclass(slots=True, frozen=True)
class WorkerBatchResult:
    processed: int = 0
    acked: int = 0


class GhEnricherWorker:
    def __init__(
        self,
        config: GhEnricherConfig,
        *,
        consumer: RedisStreamConsumer,
        service: GhEnricherService,
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
            "gh_enricher_worker_started",
            extra={
                "service": "gh-enricher",
                "event": "gh_enricher_worker_started",
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
                "gh_enricher_stream_missing_trigger_event_id",
                extra={
                    "service": "gh-enricher",
                    "event": "gh_enricher_stream_missing_trigger_event_id",
                    "stream_message_id": message.message_id,
                },
            )
            return

        job = await self._service.rehydrate_job(trigger_event_id)
        if job is None:
            self._logger.warning(
                "gh_enricher_missing_outbox_job",
                extra={
                    "service": "gh-enricher",
                    "event": "gh_enricher_missing_outbox_job",
                    "trigger_event_id": trigger_event_id,
                },
            )
            return

        await self._service.handle_job(job)
```

---

## 6-13. `src/services/gh_enricher/main.py`

```python
from __future__ import annotations

import asyncio
import logging
import sys

from redis.asyncio import Redis
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from .config import GhEnricherConfig
from .fetch_planner import GitHubFetchPlanner
from .file_sampler import GitHubFileSampler
from .github_app_auth import GitHubAppTokenProvider
from .github_client import GitHubClient
from .redis_streams import RedisStreamConsumer
from .repositories import GhEnricherRepository
from .service import GhEnricherService
from .url_discovery import GitHubUrlDiscovery
from .worker import GhEnricherWorker


def _configure_logging(log_level: str) -> None:
    logging.basicConfig(
        level=getattr(logging, log_level, logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
        stream=sys.stdout,
    )


async def _run() -> int:
    config = GhEnricherConfig.from_env()
    _configure_logging(config.log_level)
    logger = logging.getLogger("gh_enricher")

    engine = create_async_engine(config.database_url, future=True)
    session_factory = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    redis_client = Redis.from_url(config.redis_url, decode_responses=True)

    token_provider = None
    if config.github_app_id and config.github_installation_id and config.github_private_key:
        token_provider = GitHubAppTokenProvider(
            app_id=config.github_app_id,
            installation_id=config.github_installation_id,
            private_key_pem=config.github_private_key,
            api_base_url=config.github_api_base_url,
            timeout_sec=config.request_timeout_sec,
        )

    try:
        async with session_factory() as session:
            repository = GhEnricherRepository(session)
            github_client = GitHubClient(
                api_base_url=config.github_api_base_url,
                timeout_sec=config.request_timeout_sec,
                token_provider=token_provider,
            )
            service = GhEnricherService(
                config,
                repository=repository,
                github_client=github_client,
                fetch_planner=GitHubFetchPlanner(),
                file_sampler=GitHubFileSampler(),
                url_discovery=GitHubUrlDiscovery(),
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
            worker = GhEnricherWorker(
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

## 7. 테스트 초안 포인트

### `tests/unit/services/gh_enricher/test_fetch_planner.py`

검증:
- `github_repo` artifact_key_json → owner/repo locator
- `github_subpath` → owner/repo/ref/path locator
- `github_repo_page` → owner/repo/page_path locator
- `github_gist` → gist_id locator

### `tests/unit/services/gh_enricher/test_file_sampler.py`

검증:
- tree entries에 README/manifest/tests/examples/docs/ci path가 섞여 있을 때
- role-based 우선순위대로 `sample_max_files`까지만 선택되는지
- README와 manifest가 우선 확보되는지

### `tests/unit/services/gh_enricher/test_url_discovery.py`

검증:
- README excerpt와 sampled file excerpt에 URL이 있을 때
- `DiscoveredUrlObservationDraft`가 생성되는지
- discovery reason과 context_path가 보존되는지

### `tests/unit/services/gh_enricher/test_gist_minimal_change_resolution.py`

검증:
- `github_gist` artifact일 때
- parent snapshot plan은 생성되지만
- `repo_child is None`
- child table insert path가 호출되지 않는지

### `tests/component/services/gh_enricher/test_worker_rehydrates_job_from_event_outbox.py`

검증:
- Redis Streams message에는 `trigger_event_id`만 있음
- `event_outbox.payload_json`에서 `ArtifactEnrichmentJob` 복원
- service 호출 후 ack 수행

### `tests/component/services/gh_enricher/test_repo_snapshot_write_and_outbox_emit.py`

검증:
- repo snapshot write 성공
- `artifact_registry.current_snapshot_id/current_status` 갱신
- `artifact.snapshot.updated.v1` outbox insert

### `tests/component/services/gh_enricher/test_existing_current_snapshot_standard_refresh_short_circuit.py`

검증:
- current snapshot 존재
- refresh_mode = standard
- 새 API fetch 없이 current snapshot 기준 outbox emit 가능

---

## 8. 이번 단계가 구조를 지키는 이유

이 문서는 stage 5 GitHub evidence collector만 구현한다.

하지 않는 것:
- 새 canonicalization 규칙 정의
- candidate group mutation
- reroot 확정
- bundle 생성
- judge 호출
- notifier 연결

즉, 책임은 여전히 다음 경계 안에 있다.

```text
Artifact / CandidateGroup proposal
  ↓
gh-enricher
  ↓
artifact_snapshot + discovered_url_observation + artifact.snapshot.updated.v1
```

이건 5단계 정본과 11단계 실행 계약이 잠근 구조를 그대로 따른다.

---

## 9. 다음 단계

이 단계가 끝나면 다음 구현 순서는 아래가 맞다.

1. `x-enricher`
2. `web-enricher`
3. `evidence-assembler`

즉, 아직 `analysis-router`나 `judge-openai`로 가면 안 된다.  
stage 5 source enricher 3종과 assembler를 먼저 닫아야 한다.

---

## 최종 한 줄 결론

이번 단계의 최소-change 정답은 **`artifact.enrich.requested.v1`를 rehydrate해 GitHub App 기반 얕은 evidence 수집을 수행하고, repo/subpath/repo_page는 repo child snapshot으로, gist는 parent snapshot only로 처리하며, discovered links는 observation만 남기고 `artifact.snapshot.updated.v1`까지 emit하는 `gh-enricher` v0.1을 먼저 닫는 것**이다.


---

## Source file: `30_x_enricher_skeleton_and_code_draft_v0_1.md`

# 30단계: `x-enricher` 스켈레톤 + 실제 코드 초안 v0.1

## 0. 문서 목적

이 문서는 프로젝트 소스의 README/정본 단계 문서, `05_stage5_external_enrichers.md`, `11_stage11_execution_contracts_v0_1.md`, `12_migration_spec_0001_0004_v0_1.md`, 그리고 `28_router_normalizer_consumer_integration_hardening_v0_1.md`, `29_gh_enricher_skeleton_and_code_draft_v0_1.md`까지의 구현 흐름을 바탕으로,  
**`x-enricher`의 첫 구현 묶음**을 실제 코드 초안 수준으로 내리는 문서다.

이번 단계의 목적은 여섯 가지다.

1. `q.artifact.enrich.x` Redis Streams를 소비하는 **X 전용 enrichment 경계**를 코드로 고정
2. `artifact.enrich.requested.v1` thin payload를 기준으로, `event_outbox`에서 다시 **`ArtifactEnrichmentJob`** 을 복원하는 rehydration 경계를 고정
3. X Bearer Token 기반 공식 API 호출, root post + referenced post + author/media one-hop context 수집을 **최소-change 구현**으로 고정
4. `artifact_enrichment_runs`, `artifact_snapshots`, `artifact_snapshot_x_post`, `discovered_url_observations`, `event_outbox`에 대한 **x-enricher 전용 DB 경계**를 코드로 고정
5. `artifact.snapshot.updated.v1` outbox emit까지 닫아, 다음 단계의 `web-enricher` / `evidence-assembler`가 같은 패턴으로 이어질 수 있게 고정
6. 이 모든 것을 넣어도 `x-enricher`가 여전히 **비-LLM evidence 수집기**로만 남도록 고정

핵심 전제:

- `x-enricher`는 **판단기**가 아니다.
- `x-enricher`는 **candidate mutation 계층**이 아니다.
- `x-enricher`는 **reroot를 확정하지 않는다.**
- `x-enricher`는 **공식 API 우선 + 얕은 문맥 수집** 경계다.
- `x-enricher`는 `router-normalizer`의 canonicalization 규칙을 **재정의하지 않는다.**

---

## 1. 왜 지금 이 단계가 정확한 다음 단계인가

`README_minimal_update_v0_2.md`는 현재 구현 상태를 아래처럼 고정했다.

- `router-normalizer` consumer/integration hardening 완료
- `gh-enricher` v0.1 초안 완료
- 다음 구현 순서:
  - **`x-enricher`**
  - `web-enricher`
  - `evidence-assembler`

즉, 지금 시점에서 collector, outbox-relay, router-normalizer, gh-enricher를 다시 열어보는 것은 순서상 후퇴다.  
이제 stage 5의 두 번째 source enricher 본체인 **X evidence boundary**로 들어가는 것이 맞다.

또한 5단계 정본은 enrichers를 다음처럼 분리했다.

- `gh-enricher`
- `x-enricher`
- `web-enricher`
- `evidence-assembler`

그리고 11단계 실행 계약은 `x-enricher`가 직접 쓰는 durable 테이블과 허용 secret을 이미 고정했다.

- 직접 쓰는 durable 테이블:
  - `artifact_enrichment_runs`
  - `artifact_snapshots`
  - `artifact_snapshot_x_post`
  - `discovered_url_observations`
  - `event_outbox`
- 허용 secret:
  - **X bearer token만**

즉, 지금은 stage 5를 다시 설계하는 단계가 아니라,  
**이미 잠긴 X evidence boundary를 첫 runnable package로 내리는 단계**다.

---

## 2. 이번 단계에서 발견되는 작은 충돌과 최소-change 해석

이번 단계에는 작은 충돌이 두 개 있다.

### 충돌 지점 A — 내부 도메인은 `x_post`, 외부 API 파라미터는 여전히 `tweet.*`

정본 문서와 스키마는 내부 도메인 이름을 `x_post`, `post_id`로 통일했다.  
그런데 X 공식 API 파라미터와 expansions 이름은 아직도 `tweet.fields`, `referenced_tweets`, `edit_history_tweet_ids` 같은 명칭을 사용한다.

즉,

- **내부 계약**: post / x_post
- **외부 공식 API**: tweet / referenced_tweets

### 최소-change 해석 A

이번 v0.1에서는 아래 해석이 가장 보수적이다.

1. 외부 HTTP 요청과 응답 파싱에서는 **공식 API 명칭**을 그대로 쓴다.
2. 서비스 내부 dataclass / snapshot write에서는 **`x_post` 도메인 명칭**을 유지한다.
3. 즉, 네이밍 변환은 `response_mapper.py` 한 지점에서만 수행한다.

이렇게 해야:

- external API 변경 추적은 한 곳에 모이고
- 내부 아키텍처 용어는 흔들리지 않는다.

---

### 충돌 지점 B — referenced post/context 요구에 비해 child table은 root-post 중심이다

정본 5단계는 X fetch에서 다음을 허용했다.

- 본문 post
- 직접 referenced_tweets
- 필요 시 author
- media summary

그런데 `artifact_snapshot_x_post` child table은 아래처럼 **root snapshot 중심**이다.

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

즉, referenced post들의 **상세 구조 전체를 child row로 정규화할 자리는 없다.**

### 최소-change 해석 B

이번 v0.1에서는 아래 해석이 가장 안전하다.

1. `artifact_snapshot_x_post` child row에는 **root post 기준 핵심 필드**만 저장한다.
2. referenced post / expanded user / media의 richer structure는
   - parent `artifact_snapshots.normalized_projection`
   - 또는 child row의 JSON 필드
   에 **inline 구조화**해서 넣는다.
3. 별도 child table 추가나 migration patch는 지금 하지 않는다.

이렇게 하면 migration을 건드리지 않고도 5단계 정본의 맥락 보강 요구를 수용할 수 있다.

---

## 3. 범위와 비범위

### 3-1. 포함 범위

- Redis Streams consumer group bootstrap
- `event_outbox` 기반 `ArtifactEnrichmentJob` 복원
- `artifact_registry` 재조회
- X API GET `/2/tweets` 기반 root post 조회
- one-hop referenced post, author, media expansions
- discovered link extraction
- `artifact_enrichment_runs` / `artifact_snapshots` / `artifact_snapshot_x_post` write
- `artifact.snapshot.updated.v1` outbox emit
- 최소 단위 tests

### 3-2. 제외 범위

- HTML scraping fallback
- search / timeline / full conversation crawl
- OCR / image understanding
- reroot 확정
- bundle assembly
- OpenAI judge / policy / notifier
- multi-consumer reclaim hardening
- usage/budget control plane 전체 구현

즉, 이번 문서는 **실제 소비 가능한 X evidence worker**를 닫되, 그 범위를 stage 5 경계 안으로 제한한다.

---

## 4. 대상 파일 트리

```text
src/services/x_enricher/
  __init__.py
  config.py
  models.py
  x_api_client.py
  response_mapper.py
  url_discovery.py
  repositories.py
  redis_streams.py
  service.py
  worker.py
  main.py

tests/
  unit/
    services/
      x_enricher/
        test_response_mapper.py
        test_url_discovery.py
        test_content_anchor_computation.py
        test_reference_depth_budget.py
  component/
    services/
      x_enricher/
        test_worker_rehydrates_job_from_event_outbox.py
        test_x_snapshot_write_and_outbox_emit.py
        test_partial_ready_on_reference_loss.py
```

원칙:

- X 전용 코드는 `src/services/x_enricher/` 아래로만 모은다.
- X API 응답 파싱, 내부 도메인 모델 변환, URL discovery는 서비스 안에서 닫는다.
- canonicalization 규칙은 **공유 canonicalizer**를 재사용하되, 여기서 새 규칙을 만들지 않는다.

---

## 5. 이번 단계에서 고정할 구현 규칙

## 5-1. Redis payload는 계속 얇게 유지한다

`outbox-relay`가 Redis Streams에 싣는 메시지는 이미 최소 필드로 잠겨 있다.

- `job_id`
- `stage_name`
- `root_object_type`
- `root_object_id`
- `idempotency_key`
- `pipeline_run_id`
- `not_before`
- `trigger_event_id`

따라서 `x-enricher` consumer는 Redis 본문에서 business payload를 기대하면 안 된다.  
**반드시 `trigger_event_id`로 `event_outbox`를 다시 조회해 `ArtifactEnrichmentJob`을 복원**해야 한다.

이 규칙이 중요한 이유는,
Redis를 durable source처럼 취급하지 않기 위해서다.

---

## 5-2. 입력 계약은 `artifact.enrich.requested.v1`만 받는다

이번 단계의 `x-enricher`는 입력 이벤트를 좁게 제한한다.

허용 입력:

- `artifact.enrich.requested.v1`
  - `provider_route = "x"`

무시 또는 reject:

- `provider_route != "x"`
- `artifact.snapshot.updated.v1`
- `candidate.bundle.refresh.v1`
- 기타 maintenance/replay 이벤트

이렇게 해야 source-specific fetcher 책임이 선명하게 유지된다.

---

## 5-3. auth mode는 단일 Bearer Token만 둔다

`gh-enricher`는 anonymous degraded fallback을 둘 수 있었지만,
`x-enricher`는 그렇게 두면 구조가 흐려진다.

이번 단계의 기본 원칙은 아래처럼 잠근다.

### 기본
- `auth_mode = bearer_app_only`
- 런타임 secret:
  - `X_BEARER_TOKEN`

### 의도적으로 두지 않는 것
- anonymous fallback
- HTML scrape fallback
- user-context OAuth
- browser/session cookie fallback

즉, X는 **공식 API가 실패하면 partial/failure 상태로 남기고 끝내는 쪽**이 더 보수적이다.

---

## 5-4. fetch 경로는 `/2/tweets` 1회 호출 + expansions 기반으로 고정한다

이번 단계의 X evidence 수집은 **검색기**가 아니라 **post lookup**이다.

기본 경로:

1. `ArtifactEnrichmentJob.artifact_type == x_post`
2. `artifact_registry.canonical_id = x:post:{id}`에서 `post_id` 추출
3. GET `/2/tweets?ids={post_id}`
4. 필요 expansions / fields를 명시적으로 요청
5. root post + referenced post + user/media includes를 한 번에 파싱

권장 요청 필드:

- `tweet.fields`
  - `author_id`
  - `conversation_id`
  - `created_at`
  - `edit_history_tweet_ids`
  - `entities`
  - `lang`
  - `note_tweet`
  - `possibly_sensitive`
  - `public_metrics`
  - `referenced_tweets`
  - `attachments`
- `expansions`
  - `author_id`
  - `attachments.media_keys`
  - `referenced_tweets.id`
  - `referenced_tweets.id.author_id`
  - `referenced_tweets.id.attachments.media_keys`
  - `edit_history_tweet_ids`
- `user.fields`
  - `id`
  - `username`
  - `name`
  - `verified`
  - `created_at`
  - `public_metrics`
- `media.fields`
  - `media_key`
  - `type`
  - `preview_image_url`
  - `url`
  - `alt_text`
  - `duration_ms`
  - `width`
  - `height`
  - `public_metrics`

중요:
- 이것은 **1회 lookup + expansions** 경로다.
- 추가 search/timeline 호출을 붙이지 않는다.

---

## 5-5. depth budget은 여전히 1이다

5단계 정본은 X fetch depth를 제한하라고 잠갔다.

이번 단계의 기본 depth budget:

- `depth_budget = 1`

허용:
- root post
- direct `referenced_tweets`
- direct author
- direct media

금지:
- conversation 전체 재귀
- referenced_tweets의 referenced_tweets 재귀
- user profile/timeline 확장
- search 기반 주변 문맥 수집

즉, 이번 worker는 **링크가 가리키는 글의 직접 맥락만 보강**한다.

---

## 5-6. content anchor는 `post_id:last_edit_history_id`로 고정한다

X 쪽 freshness 기준은 애매하게 두면 안 된다.

이번 단계의 strong content anchor는 아래처럼 둔다.

```text
xpost:{post_id}:{latest_edit_history_tweet_id}
```

예시:
- root post ID = `1881234567890123456`
- `edit_history_tweet_ids = ["1881234567890123456", "1881234567890999999"]`
- content anchor =
  - `xpost:1881234567890123456:1881234567890999999`

보조 정보:
- fetched_at
- referenced_post_ids set hash
- discovered_links hash

하지만 snapshot dedupe 핵심은 **`post_id + latest_edit_history_id`** 다.

---

## 5-7. discovered links는 observation만 남긴다

X post 본문과 referenced post 본문에는 GitHub / X / 일반 article 링크가 숨어 있을 수 있다.

하지만 `x-enricher`가 여기서 새 candidate를 직접 만들면 구조가 찢어진다.  
따라서 이번 단계의 규칙은 단순하다.

1. `entities.urls` 기반 discovered links를 추출한다.
2. 공유 canonicalizer를 재사용한다.
3. `discovered_url_observations`에 **observation만 남긴다.**
4. supporting artifact 연결이나 reroot 확정은 여기서 하지 않는다.

즉,
`x-enricher`는 “이런 링크를 봤다”까지만 말한다.

---

## 5-8. 출력 경계는 세 단계다

### 1) `artifact_enrichment_runs`
- job 시작
- status transition
- content anchor 기록
- idempotency key 기록

### 2) `artifact_snapshots` + `artifact_snapshot_x_post`
- parent snapshot row
- X child projection row
- discovered URL observation append

### 3) `artifact.snapshot.updated.v1`
- 다음 단계 assembler / maintenance가 재조회할 수 있는 얇은 outbox event

즉,
이번 단계의 끝은 **snapshot append + thin event emit**이다.

---

## 5-9. partial success를 기본 전제로 둔다

X API는 항상 완전하지 않다.

예를 들어:

- root post는 왔지만 referenced post 하나가 누락
- media include 일부 없음
- user include 일부 없음
- 200 응답이지만 `errors` 배열 동반

따라서 이번 단계의 기본 상태 모델은 아래처럼 둔다.

- `ready`
- `partial_ready`
- `failed_transient`
- `failed_permanent`
- `rate_limited`
- `access_denied`
- `low_evidence`

규칙 예시:

- root post OK + 일부 include 누락
  - `partial_ready`
- root post 없음 + 404류
  - `failed_permanent`
- quota/credit/rate-limit
  - `rate_limited`
- root post는 왔지만 text/media/context 매우 빈약
  - `low_evidence`

중요:
- `partial_ready`는 예외가 아니라 **정상적인 상태**다.

---

## 5-10. idempotency key는 job 입력과 content anchor를 분리한다

이번 worker에서 혼동하면 안 되는 것:

- job idempotency
- snapshot dedupe

권장 job idempotency key:

```text
enrich:{artifact_id}:x:{refresh_mode}:{depth_budget}
```

권장 snapshot uniqueness key는 스키마가 이미 잠근다.

- `(artifact_id, provider, content_anchor, snapshot_type)`

즉,
- 같은 job이 다시 들어와도 run row는 재사용/중복 억제 가능
- 같은 content anchor면 snapshot append는 건너뛸 수 있다

---

## 5-11. current snapshot reuse는 보수적으로 허용한다

이번 단계의 최소-change 최적화는 아래만 둔다.

1. API fetch는 수행한다.
2. new content anchor 계산
3. `artifact_registry.current_snapshot_id`가 가리키는 current snapshot의
   - provider == `x`
   - snapshot_type == `x_post`
   - content_anchor == new content anchor
   이면
4. child/parent snapshot 새 append 없이 run만 종료 가능

이 최적화는 **비용 절감용**이지, correctness 대체제가 아니다.  
API fetch 이전 short-circuit는 두지 않는다.

---

## 5-12. q.artifact.enrich.x worker는 ack-first가 아니라 state-first다

consumer는 다음 순서를 지킨다.

1. Redis stream read
2. `trigger_event_id`로 `event_outbox` 재조회
3. `ArtifactEnrichmentJob` 복원
4. DB transaction 안에서
   - enrichment run row 시작
   - snapshot / discovered links / outbox write
5. commit 성공
6. stream ack

즉,
**ack는 state write 뒤**에 온다.

---

## 6. 코드 초안

## 6-1. `src/services/x_enricher/__init__.py`

```python
from .config import XEnricherConfig
from .service import XEnricherService

__all__ = [
    "XEnricherConfig",
    "XEnricherService",
]
```

---

## 6-2. `src/services/x_enricher/config.py`

```python
from __future__ import annotations

import os
from dataclasses import dataclass


class XEnricherConfigurationError(ValueError):
    pass


@dataclass(slots=True, frozen=True)
class XEnricherConfig:
    app_env: str
    database_url: str
    redis_url: str
    queue_name: str
    consumer_group: str
    consumer_name: str
    batch_size: int
    block_ms: int

    x_base_url: str
    x_bearer_token: str
    request_timeout_ms: int
    request_max_ids: int
    depth_budget_default: int

    provider_name: str
    snapshot_type: str
    log_level: str

    @classmethod
    def from_env(cls) -> "XEnricherConfig":
        database_url = os.getenv("DATABASE_URL", "").strip()
        redis_url = os.getenv("REDIS_URL", "").strip()
        bearer_token = os.getenv("X_BEARER_TOKEN", "").strip()

        if not database_url:
            raise XEnricherConfigurationError("DATABASE_URL is required")
        if not redis_url:
            raise XEnricherConfigurationError("REDIS_URL is required")
        if not bearer_token:
            raise XEnricherConfigurationError("X_BEARER_TOKEN is required")

        cfg = cls(
            app_env=os.getenv("APP_ENV", "dev").strip().lower(),
            database_url=database_url,
            redis_url=redis_url,
            queue_name=os.getenv("X_ENRICHER_QUEUE_NAME", "q.artifact.enrich.x").strip(),
            consumer_group=os.getenv("X_ENRICHER_CONSUMER_GROUP", "x-enricher").strip(),
            consumer_name=os.getenv("X_ENRICHER_CONSUMER_NAME", "x-enricher-1").strip(),
            batch_size=int(os.getenv("X_ENRICHER_BATCH_SIZE", "20")),
            block_ms=int(os.getenv("X_ENRICHER_BLOCK_MS", "5000")),
            x_base_url=os.getenv("X_BASE_URL", "https://api.x.com").strip().rstrip("/"),
            x_bearer_token=bearer_token,
            request_timeout_ms=int(os.getenv("X_REQUEST_TIMEOUT_MS", "8000")),
            request_max_ids=int(os.getenv("X_REQUEST_MAX_IDS", "100")),
            depth_budget_default=int(os.getenv("X_DEPTH_BUDGET_DEFAULT", "1")),
            provider_name="x",
            snapshot_type="x_post",
            log_level=os.getenv("LOG_LEVEL", "INFO").strip().upper(),
        )
        cfg.validate()
        return cfg

    def validate(self) -> None:
        if self.batch_size <= 0 or self.batch_size > 100:
            raise XEnricherConfigurationError("X_ENRICHER_BATCH_SIZE must be between 1 and 100")
        if self.block_ms <= 0:
            raise XEnricherConfigurationError("X_ENRICHER_BLOCK_MS must be > 0")
        if self.request_timeout_ms <= 0:
            raise XEnricherConfigurationError("X_REQUEST_TIMEOUT_MS must be > 0")
        if self.request_max_ids <= 0 or self.request_max_ids > 100:
            raise XEnricherConfigurationError("X_REQUEST_MAX_IDS must be between 1 and 100")
        if self.depth_budget_default != 1:
            raise XEnricherConfigurationError("v0.1 only supports X_DEPTH_BUDGET_DEFAULT=1")
        if not self.queue_name:
            raise XEnricherConfigurationError("X_ENRICHER_QUEUE_NAME must not be empty")
        if not self.consumer_group:
            raise XEnricherConfigurationError("X_ENRICHER_CONSUMER_GROUP must not be empty")
        if not self.consumer_name:
            raise XEnricherConfigurationError("X_ENRICHER_CONSUMER_NAME must not be empty")
        if not self.x_base_url.startswith("https://"):
            raise XEnricherConfigurationError("X_BASE_URL must start with https://")
        if self.provider_name != "x":
            raise XEnricherConfigurationError("provider_name must be x")
        if self.snapshot_type != "x_post":
            raise XEnricherConfigurationError("snapshot_type must be x_post")
```

---

## 6-3. `src/services/x_enricher/models.py`

```python
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any


@dataclass(slots=True, frozen=True)
class ArtifactEnrichmentJob:
    job_id: str
    candidate_group_id: str
    artifact_id: str
    artifact_type: str
    provider_route: str
    refresh_mode: str
    depth_budget: int
    requested_at: datetime


@dataclass(slots=True, frozen=True)
class XApiRequestProfile:
    tweet_fields: tuple[str, ...]
    expansions: tuple[str, ...]
    user_fields: tuple[str, ...]
    media_fields: tuple[str, ...]


@dataclass(slots=True, frozen=True)
class XUserSummary:
    user_id: str
    username: str | None
    name: str | None
    verified: bool | None
    created_at: str | None
    public_metrics: dict[str, Any] | None = None


@dataclass(slots=True, frozen=True)
class XMediaSummary:
    media_key: str
    media_type: str | None
    preview_image_url: str | None
    url: str | None
    alt_text: str | None
    width: int | None
    height: int | None
    duration_ms: int | None
    public_metrics: dict[str, Any] | None = None


@dataclass(slots=True, frozen=True)
class XReferencedPostSummary:
    post_id: str
    relation_type: str | None
    author_id: str | None
    text_excerpt: str | None


@dataclass(slots=True, frozen=True)
class XPostSnapshotDraft:
    artifact_id: str
    post_id: str
    content_anchor_post_version: str
    author_summary_json: dict[str, Any] | None
    text_full: str | None
    text_excerpt: str | None
    conversation_id: str | None
    referenced_post_ids_json: list[str]
    discovered_links_json: list[dict[str, Any]]
    media_summary_json: list[dict[str, Any]]
    metrics_summary_json: dict[str, Any] | None
    normalized_projection: dict[str, Any] | None
    evidence_limitations: list[str] = field(default_factory=list)
    fetch_anomalies: list[str] = field(default_factory=list)
    status: str = "ready"
```

---

## 6-4. `src/services/x_enricher/x_api_client.py`

```python
from __future__ import annotations

from typing import Any

import httpx

from .config import XEnricherConfig
from .models import XApiRequestProfile


JsonDict = dict[str, Any]


class XApiClient:
    def __init__(self, config: XEnricherConfig) -> None:
        self._config = config
        self._client = httpx.AsyncClient(
            base_url=self._config.x_base_url,
            timeout=self._config.request_timeout_ms / 1000.0,
            headers={
                "Authorization": f"Bearer {self._config.x_bearer_token}",
                "Accept": "application/json",
                "User-Agent": "catchbot-x-enricher/0.1",
            },
        )

    async def close(self) -> None:
        await self._client.aclose()

    def default_request_profile(self) -> XApiRequestProfile:
        return XApiRequestProfile(
            tweet_fields=(
                "author_id",
                "attachments",
                "conversation_id",
                "created_at",
                "edit_history_tweet_ids",
                "entities",
                "lang",
                "note_tweet",
                "possibly_sensitive",
                "public_metrics",
                "referenced_tweets",
            ),
            expansions=(
                "author_id",
                "attachments.media_keys",
                "referenced_tweets.id",
                "referenced_tweets.id.author_id",
                "referenced_tweets.id.attachments.media_keys",
                "edit_history_tweet_ids",
            ),
            user_fields=(
                "id",
                "username",
                "name",
                "verified",
                "created_at",
                "public_metrics",
            ),
            media_fields=(
                "media_key",
                "type",
                "preview_image_url",
                "url",
                "alt_text",
                "width",
                "height",
                "duration_ms",
                "public_metrics",
            ),
        )

    async def get_posts_by_ids(
        self,
        *,
        post_ids: list[str],
        profile: XApiRequestProfile,
    ) -> JsonDict:
        if not post_ids:
            raise ValueError("post_ids must not be empty")
        if len(post_ids) > self._config.request_max_ids:
            raise ValueError(f"post_ids exceeds max {self._config.request_max_ids}")

        response = await self._client.get(
            "/2/tweets",
            params={
                "ids": ",".join(post_ids),
                "tweet.fields": ",".join(profile.tweet_fields),
                "expansions": ",".join(profile.expansions),
                "user.fields": ",".join(profile.user_fields),
                "media.fields": ",".join(profile.media_fields),
            },
        )
        payload: JsonDict = response.json() if response.content else {}

        if response.status_code == 200:
            payload["_transport_status_code"] = 200
            return payload

        payload["_transport_status_code"] = response.status_code
        payload["_transport_headers"] = dict(response.headers)
        return payload
```

---

## 6-5. `src/services/x_enricher/response_mapper.py`

```python
from __future__ import annotations

from typing import Any

from .models import XPostSnapshotDraft


JsonDict = dict[str, Any]


class XResponseMapper:
    def map_post_lookup_response(
        self,
        *,
        artifact_id: str,
        requested_post_id: str,
        payload: JsonDict,
    ) -> XPostSnapshotDraft:
        data = payload.get("data") or []
        includes = payload.get("includes") or {}
        errors = payload.get("errors") or []

        root_post = None
        for item in data:
            if isinstance(item, dict) and str(item.get("id")) == requested_post_id:
                root_post = item
                break

        if root_post is None:
            status_code = int(payload.get("_transport_status_code", 0) or 0)
            if status_code == 429:
                return XPostSnapshotDraft(
                    artifact_id=artifact_id,
                    post_id=requested_post_id,
                    content_anchor_post_version=f"xpost:{requested_post_id}:missing",
                    author_summary_json=None,
                    text_full=None,
                    text_excerpt=None,
                    conversation_id=None,
                    referenced_post_ids_json=[],
                    discovered_links_json=[],
                    media_summary_json=[],
                    metrics_summary_json=None,
                    normalized_projection={"errors": errors},
                    evidence_limitations=["x_root_post_unavailable"],
                    fetch_anomalies=["rate_limited"],
                    status="rate_limited",
                )
            return XPostSnapshotDraft(
                artifact_id=artifact_id,
                post_id=requested_post_id,
                content_anchor_post_version=f"xpost:{requested_post_id}:missing",
                author_summary_json=None,
                text_full=None,
                text_excerpt=None,
                conversation_id=None,
                referenced_post_ids_json=[],
                discovered_links_json=[],
                media_summary_json=[],
                metrics_summary_json=None,
                normalized_projection={"errors": errors},
                evidence_limitations=["x_root_post_unavailable"],
                fetch_anomalies=["root_post_missing"],
                status="failed_permanent",
            )

        users_by_id = {
            str(u.get("id")): u
            for u in includes.get("users", [])
            if isinstance(u, dict) and u.get("id") is not None
        }
        media_by_key = {
            str(m.get("media_key")): m
            for m in includes.get("media", [])
            if isinstance(m, dict) and m.get("media_key") is not None
        }
        posts_by_id = {
            str(p.get("id")): p
            for p in data
            if isinstance(p, dict) and p.get("id") is not None
        }

        author_id = self._as_str(root_post.get("author_id"))
        author = users_by_id.get(author_id) if author_id else None
        author_summary = None
        if author is not None:
            author_summary = {
                "user_id": self._as_str(author.get("id")),
                "username": self._as_str(author.get("username")),
                "name": self._as_str(author.get("name")),
                "verified": author.get("verified"),
                "created_at": self._as_str(author.get("created_at")),
                "public_metrics": author.get("public_metrics"),
            }

        edit_ids = root_post.get("edit_history_tweet_ids") or []
        latest_edit_id = self._as_str(edit_ids[-1]) if isinstance(edit_ids, list) and edit_ids else requested_post_id
        content_anchor = f"xpost:{requested_post_id}:{latest_edit_id}"

        text_full = self._post_text(root_post)
        text_excerpt = text_full[:500] if text_full else None
        referenced_items = root_post.get("referenced_tweets") or []
        referenced_post_ids: list[str] = []
        referenced_summaries: list[dict[str, Any]] = []
        for ref in referenced_items:
            if not isinstance(ref, dict):
                continue
            ref_id = self._as_str(ref.get("id"))
            if not ref_id:
                continue
            referenced_post_ids.append(ref_id)
            ref_post = posts_by_id.get(ref_id)
            referenced_summaries.append(
                {
                    "post_id": ref_id,
                    "relation_type": self._as_str(ref.get("type")),
                    "author_id": self._as_str(ref_post.get("author_id")) if ref_post else None,
                    "text_excerpt": (self._post_text(ref_post)[:280] if ref_post else None),
                }
            )

        media_keys = []
        attachments = root_post.get("attachments") or {}
        if isinstance(attachments, dict):
            raw_media_keys = attachments.get("media_keys") or []
            if isinstance(raw_media_keys, list):
                media_keys = [str(x) for x in raw_media_keys if x]

        media_summary = []
        for key in media_keys:
            media = media_by_key.get(key)
            if not media:
                continue
            media_summary.append(
                {
                    "media_key": self._as_str(media.get("media_key")),
                    "media_type": self._as_str(media.get("type")),
                    "preview_image_url": self._as_str(media.get("preview_image_url")),
                    "url": self._as_str(media.get("url")),
                    "alt_text": self._as_str(media.get("alt_text")),
                    "width": media.get("width"),
                    "height": media.get("height"),
                    "duration_ms": media.get("duration_ms"),
                    "public_metrics": media.get("public_metrics"),
                }
            )

        metrics = root_post.get("public_metrics")
        fetch_anomalies: list[str] = []
        evidence_limitations: list[str] = []
        status = "ready"

        if errors:
            fetch_anomalies.append("partial_errors_present")
            status = "partial_ready"
        if author is None:
            evidence_limitations.append("x_author_summary_missing")
            status = "partial_ready" if status == "ready" else status
        if referenced_items and not referenced_summaries:
            evidence_limitations.append("x_referenced_posts_missing")
            status = "partial_ready" if status == "ready" else status
        if not text_full:
            evidence_limitations.append("x_text_missing")
            status = "low_evidence"
        if not media_summary and media_keys:
            evidence_limitations.append("x_media_summary_missing")
            status = "partial_ready" if status == "ready" else status

        normalized_projection = {
            "root_post": root_post,
            "referenced_post_summaries": referenced_summaries,
            "includes_errors": errors,
            "edit_history_tweet_ids": edit_ids,
        }

        return XPostSnapshotDraft(
            artifact_id=artifact_id,
            post_id=requested_post_id,
            content_anchor_post_version=content_anchor,
            author_summary_json=author_summary,
            text_full=text_full,
            text_excerpt=text_excerpt,
            conversation_id=self._as_str(root_post.get("conversation_id")),
            referenced_post_ids_json=referenced_post_ids,
            discovered_links_json=[],
            media_summary_json=media_summary,
            metrics_summary_json=metrics if isinstance(metrics, dict) else None,
            normalized_projection=normalized_projection,
            evidence_limitations=evidence_limitations,
            fetch_anomalies=fetch_anomalies,
            status=status,
        )

    def _post_text(self, post: JsonDict | None) -> str | None:
        if not isinstance(post, dict):
            return None
        note_tweet = post.get("note_tweet") or {}
        if isinstance(note_tweet, dict):
            note_text = note_tweet.get("text")
            if isinstance(note_text, str) and note_text.strip():
                return note_text.strip()
        text = post.get("text")
        if isinstance(text, str) and text.strip():
            return text.strip()
        return None

    @staticmethod
    def _as_str(value: Any) -> str | None:
        if value is None:
            return None
        value_str = str(value).strip()
        return value_str or None
```

---

## 6-6. `src/services/x_enricher/url_discovery.py`

```python
from __future__ import annotations

from typing import Any

from src.services.router_normalizer.canonicalizer import Canonicalizer
from src.services.router_normalizer.models import ObservedUrl


class XUrlDiscovery:
    def __init__(self) -> None:
        self._canonicalizer = Canonicalizer()

    def extract_and_canonicalize(
        self,
        *,
        x_snapshot_projection: dict[str, Any] | None,
    ) -> tuple[list[dict[str, Any]], list[ObservedUrl]]:
        if not x_snapshot_projection:
            return [], []

        observed: list[ObservedUrl] = []

        root_post = x_snapshot_projection.get("root_post") or {}
        referenced_summaries = x_snapshot_projection.get("referenced_post_summaries") or []

        for idx, url in enumerate(self._extract_urls_from_post(root_post)):
            observed.append(
                ObservedUrl(
                    observed_url=url,
                    source_kind="x_entities",
                    context_path=f"root_post.entities.urls[{idx}]",
                )
            )

        for ref_idx, ref in enumerate(referenced_summaries):
            if not isinstance(ref, dict):
                continue
            ref_post = ref.get("raw_post") or {}
            for url_idx, url in enumerate(self._extract_urls_from_post(ref_post)):
                observed.append(
                    ObservedUrl(
                        observed_url=url,
                        source_kind="x_entities",
                        context_path=f"referenced_posts[{ref_idx}].entities.urls[{url_idx}]",
                    )
                )

        _, normalized = self._canonicalizer.canonicalize_many(observed)
        discovered_links_json = [
            {
                "observed_url": item.observed_url,
                "normalized_url": item.normalized_url,
                "resolved_url": item.resolved_url,
                "canonical_url": item.canonical_url,
                "classification": item.classification,
                "context_path": item.context_path,
                "source_kind": item.source_kind,
            }
            for item in normalized
        ]
        return discovered_links_json, normalized

    def _extract_urls_from_post(self, post: dict[str, Any]) -> list[str]:
        entities = post.get("entities") or {}
        if not isinstance(entities, dict):
            return []
        urls = entities.get("urls") or []
        results: list[str] = []
        for entry in urls:
            if not isinstance(entry, dict):
                continue
            expanded = entry.get("expanded_url")
            display = entry.get("url")
            candidate = expanded if isinstance(expanded, str) and expanded.strip() else display
            if isinstance(candidate, str) and candidate.startswith(("http://", "https://")):
                results.append(candidate.strip())
        return results
```

---

## 6-7. `src/services/x_enricher/repositories.py`

```python
from __future__ import annotations

import json
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Any, AsyncIterator, Mapping

import sqlalchemy as sa
from sqlalchemy.ext.asyncio import AsyncSession

from .models import ArtifactEnrichmentJob, XPostSnapshotDraft


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


class XEnricherRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    @asynccontextmanager
    async def transaction(self) -> AsyncIterator[AsyncSession]:
        if self._session.in_transaction():
            yield self._session
            return
        async with self._session.begin():
            yield self._session

    async def load_job_by_trigger_event_id(self, trigger_event_id: str) -> ArtifactEnrichmentJob | None:
        result = await self._session.execute(
            sa.text(
                """
                SELECT event_id, payload_json
                FROM event_outbox
                WHERE event_id = CAST(:event_id AS uuid)
                  AND event_type = 'artifact.enrich.requested.v1'
                """
            ),
            {"event_id": trigger_event_id},
        )
        row = result.mappings().first()
        if row is None:
            return None
        payload = row["payload_json"] or {}
        if payload.get("provider_route") != "x":
            return None

        return ArtifactEnrichmentJob(
            job_id=str(row["event_id"]),
            candidate_group_id=str(payload["candidate_group_id"]),
            artifact_id=str(payload["artifact_id"]),
            artifact_type=str(payload["artifact_type"]),
            provider_route=str(payload["provider_route"]),
            refresh_mode=str(payload.get("refresh_mode", "standard")),
            depth_budget=int(payload.get("depth_budget", 1)),
            requested_at=datetime.fromisoformat(str(payload["requested_at"])),
        )

    async def load_artifact_registry_row(self, artifact_id: str) -> Mapping[str, Any] | None:
        result = await self._session.execute(
            sa.text(
                """
                SELECT *
                FROM artifact_registry
                WHERE artifact_id = CAST(:artifact_id AS uuid)
                """
            ),
            {"artifact_id": artifact_id},
        )
        return result.mappings().first()

    async def load_current_snapshot_for_artifact(self, artifact_id: str) -> Mapping[str, Any] | None:
        result = await self._session.execute(
            sa.text(
                """
                SELECT s.*
                FROM artifact_registry a
                JOIN artifact_snapshots s
                  ON s.snapshot_id = a.current_snapshot_id
                WHERE a.artifact_id = CAST(:artifact_id AS uuid)
                """
            ),
            {"artifact_id": artifact_id},
        )
        return result.mappings().first()

    async def begin_enrichment_run(
        self,
        *,
        artifact_id: str,
        provider: str,
        refresh_mode: str,
        depth_budget: int,
        job_idempotency_key: str,
    ) -> str:
        result = await self._session.execute(
            sa.text(
                """
                INSERT INTO artifact_enrichment_runs (
                    artifact_id,
                    provider,
                    refresh_mode,
                    depth_budget,
                    status,
                    job_idempotency_key,
                    requested_at,
                    started_at
                )
                VALUES (
                    CAST(:artifact_id AS uuid),
                    :provider,
                    :refresh_mode,
                    :depth_budget,
                    'fetching'::snapshot_status_enum,
                    :job_idempotency_key,
                    now(),
                    now()
                )
                ON CONFLICT (job_idempotency_key)
                DO UPDATE SET started_at = EXCLUDED.started_at
                RETURNING artifact_enrichment_run_id
                """
            ),
            {
                "artifact_id": artifact_id,
                "provider": provider,
                "refresh_mode": refresh_mode,
                "depth_budget": depth_budget,
                "job_idempotency_key": job_idempotency_key,
            },
        )
        return str(result.scalar_one())

    async def finish_enrichment_run(
        self,
        *,
        artifact_enrichment_run_id: str,
        status: str,
        content_anchor: str | None,
    ) -> None:
        await self._session.execute(
            sa.text(
                """
                UPDATE artifact_enrichment_runs
                SET
                    status = CAST(:status AS snapshot_status_enum),
                    content_anchor = :content_anchor,
                    finished_at = now()
                WHERE artifact_enrichment_run_id = CAST(:run_id AS uuid)
                """
            ),
            {
                "run_id": artifact_enrichment_run_id,
                "status": status,
                "content_anchor": content_anchor,
            },
        )

    async def insert_snapshot(self, draft: XPostSnapshotDraft) -> str:
        result = await self._session.execute(
            sa.text(
                """
                INSERT INTO artifact_snapshots (
                    artifact_id,
                    provider,
                    snapshot_type,
                    status,
                    fetched_at,
                    content_anchor,
                    auth_mode,
                    normalized_projection,
                    raw_payload_ref,
                    evidence_limitations,
                    fetch_anomalies
                )
                VALUES (
                    CAST(:artifact_id AS uuid),
                    'x',
                    'x_post',
                    CAST(:status AS snapshot_status_enum),
                    now(),
                    :content_anchor,
                    'bearer_app_only',
                    CAST(:normalized_projection AS jsonb),
                    NULL,
                    CAST(:evidence_limitations AS jsonb),
                    CAST(:fetch_anomalies AS jsonb)
                )
                ON CONFLICT (artifact_id, provider, content_anchor, snapshot_type)
                DO UPDATE SET
                    status = EXCLUDED.status
                RETURNING snapshot_id
                """
            ),
            {
                "artifact_id": draft.artifact_id,
                "status": draft.status,
                "content_anchor": draft.content_anchor_post_version,
                "normalized_projection": _jsonb_dumps(draft.normalized_projection),
                "evidence_limitations": _jsonb_dumps(draft.evidence_limitations),
                "fetch_anomalies": _jsonb_dumps(draft.fetch_anomalies),
            },
        )
        return str(result.scalar_one())

    async def upsert_snapshot_x_post(
        self,
        *,
        snapshot_id: str,
        draft: XPostSnapshotDraft,
    ) -> None:
        await self._session.execute(
            sa.text(
                """
                INSERT INTO artifact_snapshot_x_post (
                    snapshot_id,
                    post_id,
                    content_anchor_post_version,
                    author_summary_json,
                    text_full,
                    text_excerpt,
                    conversation_id,
                    referenced_post_ids_json,
                    discovered_links_json,
                    media_summary_json,
                    metrics_summary_json
                )
                VALUES (
                    CAST(:snapshot_id AS uuid),
                    :post_id,
                    :content_anchor_post_version,
                    CAST(:author_summary_json AS jsonb),
                    :text_full,
                    :text_excerpt,
                    :conversation_id,
                    CAST(:referenced_post_ids_json AS jsonb),
                    CAST(:discovered_links_json AS jsonb),
                    CAST(:media_summary_json AS jsonb),
                    CAST(:metrics_summary_json AS jsonb)
                )
                ON CONFLICT (snapshot_id)
                DO UPDATE SET
                    author_summary_json = EXCLUDED.author_summary_json,
                    text_full = EXCLUDED.text_full,
                    text_excerpt = EXCLUDED.text_excerpt,
                    conversation_id = EXCLUDED.conversation_id,
                    referenced_post_ids_json = EXCLUDED.referenced_post_ids_json,
                    discovered_links_json = EXCLUDED.discovered_links_json,
                    media_summary_json = EXCLUDED.media_summary_json,
                    metrics_summary_json = EXCLUDED.metrics_summary_json
                """
            ),
            {
                "snapshot_id": snapshot_id,
                "post_id": draft.post_id,
                "content_anchor_post_version": draft.content_anchor_post_version,
                "author_summary_json": _jsonb_dumps(draft.author_summary_json),
                "text_full": draft.text_full,
                "text_excerpt": draft.text_excerpt,
                "conversation_id": draft.conversation_id,
                "referenced_post_ids_json": _jsonb_dumps(draft.referenced_post_ids_json),
                "discovered_links_json": _jsonb_dumps(draft.discovered_links_json),
                "media_summary_json": _jsonb_dumps(draft.media_summary_json),
                "metrics_summary_json": _jsonb_dumps(draft.metrics_summary_json),
            },
        )

    async def update_artifact_current_snapshot(
        self,
        *,
        artifact_id: str,
        snapshot_id: str,
        status: str,
    ) -> None:
        await self._session.execute(
            sa.text(
                """
                UPDATE artifact_registry
                SET
                    current_snapshot_id = CAST(:snapshot_id AS uuid),
                    current_status = CAST(:status AS snapshot_status_enum),
                    updated_at = now()
                WHERE artifact_id = CAST(:artifact_id AS uuid)
                """
            ),
            {
                "artifact_id": artifact_id,
                "snapshot_id": snapshot_id,
                "status": status,
            },
        )

    async def insert_discovered_url_observation(
        self,
        *,
        parent_candidate_group_id: str,
        parent_artifact_id: str,
        parent_snapshot_id: str,
        observed_url: str,
        context_path: str | None,
        discovery_reason: str,
        depth_remaining: int,
    ) -> None:
        await self._session.execute(
            sa.text(
                """
                INSERT INTO discovered_url_observations (
                    parent_candidate_group_id,
                    parent_artifact_id,
                    parent_snapshot_id,
                    observed_url,
                    context_path,
                    discovery_reason,
                    depth_remaining,
                    created_at
                )
                VALUES (
                    CAST(:parent_candidate_group_id AS uuid),
                    CAST(:parent_artifact_id AS uuid),
                    CAST(:parent_snapshot_id AS uuid),
                    :observed_url,
                    :context_path,
                    :discovery_reason,
                    :depth_remaining,
                    now()
                )
                """
            ),
            {
                "parent_candidate_group_id": parent_candidate_group_id,
                "parent_artifact_id": parent_artifact_id,
                "parent_snapshot_id": parent_snapshot_id,
                "observed_url": observed_url,
                "context_path": context_path,
                "discovery_reason": discovery_reason,
                "depth_remaining": depth_remaining,
            },
        )

    async def insert_outbox_event(
        self,
        *,
        event_type: str,
        aggregate_type: str,
        aggregate_id: str,
        dedupe_key: str,
        payload_json: dict[str, Any],
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
                    :event_type,
                    :aggregate_type,
                    CAST(:aggregate_id AS uuid),
                    :dedupe_key,
                    CAST(:payload_json AS jsonb),
                    'pending'::outbox_status_enum,
                    now()
                )
                ON CONFLICT (dedupe_key) DO NOTHING
                """
            ),
            {
                "event_type": event_type,
                "aggregate_type": aggregate_type,
                "aggregate_id": aggregate_id,
                "dedupe_key": dedupe_key,
                "payload_json": _jsonb_dumps(payload_json),
            },
        )
```

---

## 6-8. `src/services/x_enricher/redis_streams.py`

```python
from __future__ import annotations

from dataclasses import dataclass

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

## 6-9. `src/services/x_enricher/service.py`

```python
from __future__ import annotations

from datetime import timezone
from typing import Any

from .config import XEnricherConfig
from .models import ArtifactEnrichmentJob, XPostSnapshotDraft
from .repositories import XEnricherRepository
from .response_mapper import XResponseMapper
from .url_discovery import XUrlDiscovery
from .x_api_client import XApiClient


class XEnricherService:
    def __init__(
        self,
        config: XEnricherConfig,
        *,
        repository: XEnricherRepository,
        api_client: XApiClient,
        response_mapper: XResponseMapper,
        url_discovery: XUrlDiscovery,
    ) -> None:
        self._config = config
        self._repository = repository
        self._api_client = api_client
        self._response_mapper = response_mapper
        self._url_discovery = url_discovery

    async def rehydrate_job(self, trigger_event_id: str) -> ArtifactEnrichmentJob | None:
        return await self._repository.load_job_by_trigger_event_id(trigger_event_id)

    async def handle_job(self, job: ArtifactEnrichmentJob) -> None:
        artifact = await self._repository.load_artifact_registry_row(job.artifact_id)
        if artifact is None:
            return

        canonical_id = str(artifact["canonical_id"])
        if not canonical_id.startswith("x:post:"):
            return

        post_id = canonical_id.split("x:post:", 1)[1]
        run_idempotency_key = f"enrich:{job.artifact_id}:x:{job.refresh_mode}:{job.depth_budget}"
        async with self._repository.transaction():
            run_id = await self._repository.begin_enrichment_run(
                artifact_id=job.artifact_id,
                provider="x",
                refresh_mode=job.refresh_mode,
                depth_budget=job.depth_budget,
                job_idempotency_key=run_idempotency_key,
            )

        profile = self._api_client.default_request_profile()
        payload = await self._api_client.get_posts_by_ids(post_ids=[post_id], profile=profile)
        snapshot_draft = self._response_mapper.map_post_lookup_response(
            artifact_id=job.artifact_id,
            requested_post_id=post_id,
            payload=payload,
        )

        discovered_links_json, discovered_observations = self._url_discovery.extract_and_canonicalize(
            x_snapshot_projection=snapshot_draft.normalized_projection,
        )
        snapshot_draft = XPostSnapshotDraft(
            artifact_id=snapshot_draft.artifact_id,
            post_id=snapshot_draft.post_id,
            content_anchor_post_version=snapshot_draft.content_anchor_post_version,
            author_summary_json=snapshot_draft.author_summary_json,
            text_full=snapshot_draft.text_full,
            text_excerpt=snapshot_draft.text_excerpt,
            conversation_id=snapshot_draft.conversation_id,
            referenced_post_ids_json=snapshot_draft.referenced_post_ids_json,
            discovered_links_json=discovered_links_json,
            media_summary_json=snapshot_draft.media_summary_json,
            metrics_summary_json=snapshot_draft.metrics_summary_json,
            normalized_projection=snapshot_draft.normalized_projection,
            evidence_limitations=snapshot_draft.evidence_limitations,
            fetch_anomalies=snapshot_draft.fetch_anomalies,
            status=snapshot_draft.status,
        )

        current_snapshot = await self._repository.load_current_snapshot_for_artifact(job.artifact_id)
        if (
            current_snapshot is not None
            and str(current_snapshot.get("provider")) == "x"
            and str(current_snapshot.get("snapshot_type")) == "x_post"
            and str(current_snapshot.get("content_anchor")) == snapshot_draft.content_anchor_post_version
        ):
            async with self._repository.transaction():
                await self._repository.finish_enrichment_run(
                    artifact_enrichment_run_id=run_id,
                    status=snapshot_draft.status,
                    content_anchor=snapshot_draft.content_anchor_post_version,
                )
            return

        async with self._repository.transaction():
            snapshot_id = await self._repository.insert_snapshot(snapshot_draft)
            await self._repository.upsert_snapshot_x_post(snapshot_id=snapshot_id, draft=snapshot_draft)
            await self._repository.update_artifact_current_snapshot(
                artifact_id=job.artifact_id,
                snapshot_id=snapshot_id,
                status=snapshot_draft.status,
            )

            for item in discovered_observations:
                await self._repository.insert_discovered_url_observation(
                    parent_candidate_group_id=job.candidate_group_id,
                    parent_artifact_id=job.artifact_id,
                    parent_snapshot_id=snapshot_id,
                    observed_url=item.observed_url,
                    context_path=item.context_path,
                    discovery_reason="embedded_link",
                    depth_remaining=max(0, job.depth_budget - 1),
                )

            await self._repository.insert_outbox_event(
                event_type="artifact.snapshot.updated.v1",
                aggregate_type="artifact",
                aggregate_id=job.artifact_id,
                dedupe_key=f"artifact:snapshot_updated:{job.artifact_id}:{snapshot_id}",
                payload_json={
                    "artifact_id": job.artifact_id,
                    "candidate_group_id": job.candidate_group_id,
                    "provider_route": "x",
                    "snapshot_id": snapshot_id,
                    "snapshot_type": "x_post",
                    "status": snapshot_draft.status,
                    "content_anchor": snapshot_draft.content_anchor_post_version,
                },
            )

            await self._repository.finish_enrichment_run(
                artifact_enrichment_run_id=run_id,
                status=snapshot_draft.status,
                content_anchor=snapshot_draft.content_anchor_post_version,
            )
```

---

## 6-10. `src/services/x_enricher/worker.py`

```python
from __future__ import annotations

import asyncio
from dataclasses import dataclass

from .config import XEnricherConfig
from .redis_streams import RedisStreamConsumer, StreamMessage
from .service import XEnricherService


@dataclass(slots=True, frozen=True)
class WorkerBatchResult:
    processed: int = 0
    acked: int = 0
    skipped: int = 0


class XEnricherWorker:
    def __init__(
        self,
        config: XEnricherConfig,
        *,
        consumer: RedisStreamConsumer,
        service: XEnricherService,
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
            return True

        job = await self._service.rehydrate_job(trigger_event_id)
        if job is None:
            return True

        await self._service.handle_job(job)
        return True
```

---

## 6-11. `src/services/x_enricher/main.py`

```python
from __future__ import annotations

import asyncio

from redis.asyncio import Redis
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from .config import XEnricherConfig
from .redis_streams import RedisStreamConsumer
from .repositories import XEnricherRepository
from .response_mapper import XResponseMapper
from .service import XEnricherService
from .url_discovery import XUrlDiscovery
from .worker import XEnricherWorker
from .x_api_client import XApiClient


async def _run() -> int:
    config = XEnricherConfig.from_env()

    engine = create_async_engine(config.database_url, future=True)
    session_factory = async_sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)
    redis_client = Redis.from_url(config.redis_url, decode_responses=False)

    async with session_factory() as session:
        repository = XEnricherRepository(session)
        api_client = XApiClient(config)
        service = XEnricherService(
            config,
            repository=repository,
            api_client=api_client,
            response_mapper=XResponseMapper(),
            url_discovery=XUrlDiscovery(),
        )
        consumer = RedisStreamConsumer(
            redis_client,
            queue_name=config.queue_name,
            consumer_group=config.consumer_group,
            consumer_name=config.consumer_name,
            block_ms=config.block_ms,
            batch_size=config.batch_size,
        )
        worker = XEnricherWorker(config, consumer=consumer, service=service)

        try:
            await worker.run_forever()
        finally:
            await api_client.close()
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

### `tests/unit/services/x_enricher/test_response_mapper.py`

검증:
- root post + includes.users + includes.media + errors 일부 포함
- mapper가 `status = partial_ready`를 만드는지
- `content_anchor_post_version`이 `post_id:last_edit_id` 형식인지
- `referenced_post_ids_json`이 root payload 기준으로 채워지는지

### `tests/unit/services/x_enricher/test_url_discovery.py`

검증:
- root post `entities.urls`
- referenced post `entities.urls`
- canonicalizer 재사용
- `discovered_links_json`의 `classification` / `canonical_url`이 채워지는지

### `tests/unit/services/x_enricher/test_content_anchor_computation.py`

검증:
- `edit_history_tweet_ids`가 비어있지 않을 때 마지막 ID를 anchor에 쓰는지
- 비어있을 때 root `post_id`로 fallback 하는지

### `tests/unit/services/x_enricher/test_reference_depth_budget.py`

검증:
- `depth_budget = 1`
- root post와 direct referenced posts까지만 projection에 들어가는지
- second-hop recursion이 없는지

### `tests/component/services/x_enricher/test_worker_rehydrates_job_from_event_outbox.py`

검증:
- Redis Streams message에는 `trigger_event_id`만 있음
- `event_outbox.payload_json`에서 `ArtifactEnrichmentJob` 복원
- service 호출 후 ack 수행

### `tests/component/services/x_enricher/test_x_snapshot_write_and_outbox_emit.py`

검증:
- root post ready
- `artifact_snapshots` / `artifact_snapshot_x_post` write
- `artifact.snapshot.updated.v1` outbox row 생성
- `artifact_registry.current_snapshot_id` 갱신

### `tests/component/services/x_enricher/test_partial_ready_on_reference_loss.py`

검증:
- root post는 있음
- referenced post 일부 누락 + `errors` 배열 존재
- snapshot status = `partial_ready`
- `fetch_anomalies` / `evidence_limitations` 기록

---

## 8. 이번 단계가 구조를 지키는 이유

이번 문서는 아래 경계를 유지한다.

- `router-normalizer`는 candidate proposal까지만
- `x-enricher`는 외부 증거 수집까지만
- reroot는 여전히 assembler만
- judge/policy/notifier는 전혀 건드리지 않음

특히 중요한 점은 두 가지다.

1. **X HTML scraping fallback을 넣지 않았다.**  
   즉, X evidence는 공식 API 기반으로만 수집한다.

2. **discovered links를 observation으로만 남긴다.**  
   즉, X post가 GitHub repo를 가리켜도 이 단계에서 primary를 바꾸지 않는다.

이 두 규칙이 stage 5 구조를 가장 잘 지킨다.

---

## 9. 다음 단계

이 단계가 끝나면 다음 구현 순서는 아래가 맞다.

1. `web-enricher`
2. `evidence-assembler`

그 이후에야:

3. `analysis-router`
4. `judge-openai`
5. `analysis-validator`
6. `policy-engine`
7. `notifier-telegram`

즉, 지금은 여전히 **stage 5 evidence layer** 안에 있다.

---

## 최종 한 줄 결론

이번 단계의 최소-change 정답은 **`artifact.enrich.requested.v1` thin payload를 기준으로 `q.artifact.enrich.x`를 소비하고, X 공식 API `/2/tweets` one-hop expansions만 사용해 `artifact_snapshot_x_post`와 `discovered_url_observations`를 append-only로 기록한 뒤, `artifact.snapshot.updated.v1`를 내보내는 좁은 `x-enricher` worker를 고정하는 것**이다.


---

## Source file: `31_web_enricher_skeleton_and_code_draft_v0_1.md`

# 31단계: `web-enricher` 스켈레톤 + 실제 코드 초안 v0.1

## 0. 문서 목적

이 문서는 프로젝트 소스의 README/정본 단계 문서, `05_stage5_external_enrichers.md`, `11_stage11_execution_contracts_v0_1.md`, `12_migration_spec_0001_0004_v0_1.md`, 그리고 `29_gh_enricher_skeleton_and_code_draft_v0_1.md`, `30_x_enricher_skeleton_and_code_draft_v0_1.md`까지의 구현 흐름을 바탕으로,  
**`web-enricher`의 첫 구현 묶음**을 실제 코드 초안 수준으로 내리는 문서다.

이번 단계의 목적은 여섯 가지다.

1. `q.artifact.enrich.web` Redis Streams를 소비하는 **일반 웹 전용 enrichment 경계**를 코드로 고정
2. `artifact.enrich.requested.v1` thin payload를 기준으로, `event_outbox`에서 다시 **`ArtifactEnrichmentJob`** 을 복원하는 rehydration 경계를 고정
3. 제한적 GET 기반 fetch, redirect cap, content-type allowlist, body size cap, metadata/excerpt 추출을 **최소-change 구현**으로 고정
4. `artifact_enrichment_runs`, `artifact_snapshots`, `artifact_snapshot_web_article`, `discovered_url_observations`, `event_outbox`에 대한 **web-enricher 전용 DB 경계**를 코드로 고정
5. `artifact.snapshot.updated.v1` outbox emit까지 닫아, 다음 단계의 `evidence-assembler`가 같은 패턴으로 이어질 수 있게 고정
6. 이 모든 것을 넣어도 `web-enricher`가 여전히 **비-LLM evidence 수집기**로만 남도록 고정

핵심 전제:

- `web-enricher`는 **판단기**가 아니다.
- `web-enricher`는 **candidate mutation 계층**이 아니다.
- `web-enricher`는 **reroot를 확정하지 않는다.**
- `web-enricher`는 **보수적 metadata + excerpt + outbound links 수집기**다.
- `web-enricher`는 `router-normalizer`의 canonicalization 규칙을 **재정의하지 않는다.**

---

## 1. 왜 지금 이 단계가 정확한 다음 단계인가

`README_minimal_update_v0_3.md`는 현재 구현 상태를 아래처럼 고정했다.

- `router-normalizer` consumer/integration hardening 완료
- `gh-enricher` v0.1 초안 완료
- `x-enricher` v0.1 초안 완료
- 다음 구현 순서:
  - **`web-enricher`**
  - `evidence-assembler`

즉, 지금 시점에서 collector, outbox-relay, router-normalizer, gh-enricher, x-enricher를 다시 열어보는 것은 순서상 후퇴다.  
이제 stage 5의 세 번째 source enricher 본체인 **web evidence boundary**로 들어가는 것이 맞다.

또한 5단계 정본은 enrichers를 다음처럼 분리했다.

- `gh-enricher`
- `x-enricher`
- `web-enricher`
- `evidence-assembler`

그리고 11단계 실행 계약은 `web-enricher`가 직접 쓰는 durable 테이블과 허용 secret을 이미 고정했다.

- 직접 쓰는 durable 테이블:
  - `artifact_enrichment_runs`
  - `artifact_snapshots`
  - `artifact_snapshot_web_article`
  - `discovered_url_observations`
  - `event_outbox`
- 허용 secret:
  - **없음**

즉, 지금은 stage 5를 다시 설계하는 단계가 아니라,  
**이미 잠긴 web evidence boundary를 첫 runnable package로 내리는 단계**다.

---

## 2. 이번 단계에서 발견되는 작은 충돌과 최소-change 해석

이번 단계에는 작은 충돌이 세 개 있다.

### 충돌 지점 A — 5단계 정본은 `reroot_candidate_suggestion` 언급이 있지만, 실행 계약은 fetcher mutation을 금지한다

5단계 정본 문서는 web-enricher가 article 본문에서 GitHub/X 링크를 발견하면 `reroot_candidate_suggestion`을 emit할 수 있다는 서술을 포함한다.  
하지만 11단계 실행 계약은 다음을 더 강하게 잠갔다.

- enricher는 **자기 방식으로 artifact를 정의하면 안 된다**
- reroot는 **evidence-assembler 단일 지점**에서만 반영한다

즉,

- **정본 설계의 풍부한 표현**: reroot suggestion까지 고려
- **실행 계약의 더 좁은 강제**: fetcher는 observation까지만

### 최소-change 해석 A

이번 v0.1에서는 아래 해석이 가장 보수적이다.

1. web-enricher는 outbound links를 추출한다.
2. 공유 canonicalizer를 재사용해 classification / canonical_url까지 구조화한다.
3. 하지만 durable write는 **`discovered_url_observations`만** 수행한다.
4. 별도 `reroot_candidate_suggestion` 이벤트나 candidate mutation은 **지금 넣지 않는다**.
5. evidence-assembler가 snapshot + discovered observations를 보고 reroot를 판단한다.

이렇게 해야 stage 5 정본의 intent를 훼손하지 않으면서도,  
11단계 실행 계약의 더 좁은 책임 경계를 유지할 수 있다.

---

### 충돌 지점 B — `web-enricher`는 구현 순서상 지금 필요하지만, bounded assumptions에서는 기본 flag-off로 둔다

11단계 bounded assumptions는 다음을 잠갔다.

- **`web-enricher`는 metadata/excerpt만 수집하고 기본 flag-off**

즉,

- **구현 순서상**: 지금 web-enricher 코드 패키지를 작성해야 한다
- **운영 순서상**: 기본 rollout에서는 아직 켜지 않을 수 있다

### 최소-change 해석 B

이번 v0.1에서는 아래처럼 해석하는 것이 맞다.

1. 코드 패키지는 지금 만든다.
2. fetch 경계, DB 경계, outbox emit까지 구현한다.
3. 하지만 rollout 기본값은 계속 `ENABLE_WEB_ENRICH=false`로 둔다.
4. 실제 운영 활성은 `evidence-assembler`와 stage 5 전체 검증 이후로 미룬다.

즉, **코드 구현과 rollout default는 같은 것이 아니다.**

---

### 충돌 지점 C — final URL / canonical URL candidate가 stage 4 artifact canonical_url과 다를 수 있다

일반 웹은 redirect, canonical `<link>`, og:url 때문에 다음이 자주 발생한다.

- `artifact_registry.canonical_url`  
  !=
- fetch 후 `final_url`
  !=
- HTML 내부 `canonical_url_candidate`

하지만 11단계 실행 계약과 5단계 정본은 둘 다 **enricher가 artifact identity를 재정의하지 말라**고 잠갔다.

### 최소-change 해석 C

이번 v0.1에서는 아래가 맞다.

1. `artifact_registry`는 건드리지 않는다.
2. `final_url`과 `canonical_url_candidate`는 **snapshot child row**에 저장한다.
3. parent `artifact_snapshots.normalized_projection`에도 parser/fetch context를 보강한다.
4. 필요 시 evidence-assembler가 이후 강한 GitHub/X anchor를 보고 reroot를 판단한다.
5. 지금 단계에서 web-enricher가 registry canonical row를 rewrite하지 않는다.

이렇게 해야 stage 4 canonicalization과 stage 5 fetch를 섞지 않는다.

---

## 3. 범위와 비범위

### 3-1. 포함 범위

- Redis Streams consumer group bootstrap
- `event_outbox` 기반 `ArtifactEnrichmentJob` 복원
- `artifact_registry` 재조회
- 제한적 GET fetch
- redirect cap / body cap / content-type allowlist
- HTML/text metadata + excerpt 추출
- outbound links 추출 및 canonicalization 재사용
- `artifact_enrichment_runs` / `artifact_snapshots` / `artifact_snapshot_web_article` / `discovered_url_observations` / `event_outbox` write
- `artifact.snapshot.updated.v1` outbox emit
- 최소 단위 tests

### 3-2. 제외 범위

- headless browser fallback
- login / cookie jar / session reuse
- paywall bypass
- OCR / image understanding
- JS execution
- full article archival
- candidate reroot 확정
- bundle assembly
- OpenAI judge / policy / notifier
- multi-consumer reclaim hardening

즉, 이번 문서는 **실제 소비 가능한 web evidence worker**를 닫되, 그 범위를 stage 5 경계 안으로 제한한다.

---

## 4. 대상 파일 트리

```text
src/services/web_enricher/
  __init__.py
  config.py
  models.py
  web_fetch_client.py
  article_parser.py
  url_discovery.py
  repositories.py
  redis_streams.py
  service.py
  worker.py
  main.py

tests/
  unit/
    services/
      web_enricher/
        test_article_parser.py
        test_url_discovery.py
        test_content_anchor_computation.py
        test_content_type_guard.py
  component/
    services/
      web_enricher/
        test_worker_rehydrates_job_from_event_outbox.py
        test_web_snapshot_write_and_outbox_emit.py
        test_partial_ready_on_metadata_sparse_page.py
        test_redirect_final_url_preserved_in_snapshot.py
```

원칙:

- 일반 웹 관련 구현은 `src/services/web_enricher/` 아래로만 모은다.
- canonicalization 규칙은 **공유 canonicalizer**를 재사용한다.
- web-enricher 내부에 새 artifact canonicalizer를 만들지 않는다.
- outbound link discovery는 observation만 남기고, candidate mutation은 뒤 단계로 넘긴다.

---

## 5. 이번 단계에서 고정할 구현 규칙

## 5-1. Redis payload는 계속 얇게 유지한다

`outbox-relay`가 Redis Streams에 싣는 메시지는 이미 최소 필드로 잠겨 있다.

- `job_id`
- `stage_name`
- `root_object_type`
- `root_object_id`
- `idempotency_key`
- `pipeline_run_id`
- `not_before`
- `trigger_event_id`

따라서 `web-enricher` consumer는 Redis 본문에서 business payload를 기대하면 안 된다.  
**반드시 `trigger_event_id`로 `event_outbox`를 다시 조회해 `ArtifactEnrichmentJob`을 복원**해야 한다.

이 규칙이 중요한 이유는,
Redis를 durable source처럼 취급하지 않기 위해서다.

---

## 5-2. 입력 계약은 `artifact.enrich.requested.v1`만 받는다

이번 단계의 `web-enricher`는 입력 이벤트를 좁게 제한한다.

허용 입력:

- `artifact.enrich.requested.v1`
  - `provider_route = "web"`

무시 또는 reject:

- `provider_route != "web"`
- `artifact.snapshot.updated.v1`
- `candidate.bundle.refresh.v1`
- 기타 maintenance/replay 이벤트

그리고 기본적으로 아래만 지원한다.

- `artifact_type == web_article`

`unknown_link`, `short_url_unresolved`는 stage 4 deterministic contract의 실패/보류 상태이므로,  
이번 v0.1 web-enricher가 다시 rescue path를 만들지 않는다.

---

## 5-3. auth mode는 anonymous public만 둔다

`gh-enricher`는 GitHub App, `x-enricher`는 Bearer Token을 쓴다.  
반면 web-enricher는 허용 secret이 없다.

이번 단계의 기본 원칙은 아래처럼 잠근다.

### 기본
- `auth_mode = anonymous_public`

### 의도적으로 두지 않는 것
- shared browser cookie jar
- login session
- user-specific token
- proxy rotation
- browser automation fallback

즉, 일반 웹은 **공개적으로 읽히는 surface만** 얇게 수집한다.

---

## 5-4. fetch 경로는 제한적 GET + redirect cap + body cap으로 고정한다

이번 단계의 web evidence 수집은 crawler가 아니라 **thin document fetch**다.

기본 경로:

1. `artifact_registry.canonical_url`에서 시작 URL 확보
2. GET 요청
3. redirect가 오면 `Location`을 따라가되 hop cap 안에서만 진행
4. 최종 response의 `content-type` 검사
5. allowlist에 맞는 경우에만 body 일부를 읽음
6. title/description/canonical/meta/excerpt/outbound links를 구조화

권장 정책:

- method: `GET`
- timeout: 낮게
- redirect hop cap: 고정
- body bytes cap: 고정
- content-type allowlist:
  - `text/html`
  - `application/xhtml+xml`
  - `text/plain`
  - `text/markdown`
- no JS
- no cookies
- no iframe/subresource fetch

중요:
- `HEAD` 우선 전략을 굳이 넣지 않는다.
- 일반 웹은 preview와 본문이 다를 수 있어서, 제한된 GET이 더 보수적이다.

---

## 5-5. parser는 metadata-first + excerpt-second로 고정한다

일반 웹은 구조가 제각각이므로, parser를 지나치게 공격적으로 두면 곧 brittle해진다.  
이번 v0.1의 parser는 아래 순서를 따른다.

### HTML일 때
1. `<title>`
2. meta:
   - `description`
   - `og:title`
   - `og:description`
   - `og:site_name`
   - `author`
   - `article:published_time`
   - `parsely-pub-date`
3. `<link rel="canonical">`
4. visible text chunks:
   - `article`
   - `main`
   - `p`
   - `h1/h2/h3`
   - `li`
5. outbound `<a href>` 추출

### plain text / markdown일 때
1. 첫 줄 title 후보
2. body excerpt
3. URL regex 추출

중요:
- readability/full boilerplate removal은 기본 경로에 넣지 않는다.
- parser 실패 시 headless fallback을 넣지 않는다.
- sparse metadata는 **`partial_ready` 또는 `low_evidence`** 로 honest하게 남긴다.

---

## 5-6. content anchor는 `final_url + content_hash` 기반으로 고정한다

web 쪽 freshness 기준은 다음처럼 둔다.

권장 strong anchor 입력:

- `final_url`
- `content_hash`

권장 content anchor:

```text
web:{sha256(final_url + "|" + content_hash)}
```

보조 정보로 parent `normalized_projection`에 남긴다.

- `final_url`
- `response_headers_subset`
- `content_type`
- `content_length_observed`

중요:
- `artifact_id`는 대상 identity다.
- `content_anchor`는 그 대상에서 관측한 **특정 body version** 이다.
- 둘을 섞지 않는다.

---

## 5-7. discovered links는 observation만 남긴다

article 본문과 본문 excerpt에는 GitHub / X / 일반 article 링크가 숨어 있을 수 있다.

하지만 `web-enricher`가 여기서 새 candidate를 직접 만들면 구조가 찢어진다.  
따라서 이번 단계의 규칙은 단순하다.

1. outbound links를 추출한다.
2. 공유 canonicalizer를 재사용한다.
3. `discovered_url_observations`에 **observation만 남긴다.**
4. supporting artifact 연결이나 reroot 확정은 여기서 하지 않는다.

즉,
`web-enricher`는 “이 article에서 이런 링크를 봤다”까지만 말한다.

---

## 5-8. 출력 경계는 세 단계다

### 1) `artifact_enrichment_runs`
- job 시작
- status transition
- content anchor 기록
- idempotency key 기록

### 2) `artifact_snapshots` + `artifact_snapshot_web_article`
- parent snapshot row
- web child projection row
- discovered URL observation append

### 3) `artifact.snapshot.updated.v1`
- 다음 단계 assembler / maintenance가 재조회할 수 있는 얇은 outbox event

즉,
이번 단계의 끝은 **snapshot append + thin event emit**이다.

---

## 5-9. partial success를 기본 전제로 둔다

일반 웹은 매우 지저분하다.

예를 들어:

- HTML은 왔지만 canonical/title/meta가 빈약함
- 본문은 왔지만 excerpt 추출이 약함
- text/plain만 반환됨
- 최종 URL은 살아 있지만 content-type이 비허용
- 429/403/5xx가 섞임

따라서 이번 단계의 기본 상태 모델은 아래처럼 둔다.

- `ready`
- `partial_ready`
- `failed_transient`
- `failed_permanent`
- `rate_limited`
- `access_denied`
- `unsupported`
- `low_evidence`

규칙 예시:

- HTML OK + title/excerpt/outbound links 확보
  - `ready`
- 최종 URL + excerpt는 있지만 metadata sparse
  - `partial_ready`
- content-type 비허용
  - `unsupported`
- 429
  - `rate_limited`
- 403/401
  - `access_denied`
- text가 거의 없고 links도 없음
  - `low_evidence`

중요:
- `partial_ready`와 `low_evidence`는 예외가 아니라 **정상 상태**다.

---

## 5-10. idempotency key는 입력 상태를 기준으로 만든다

11단계는 web enrich idempotency 키를 아래처럼 잠갔다.

```text
enrich:web:{artifact_id}:{refresh_mode}:{content_anchor_hint}
```

이번 v0.1에서는 아래처럼 해석한다.

- job idempotency key:
  - `enrich:web:{artifact_id}:{refresh_mode}:{depth_budget}`
- snapshot dedupe key:
  - `(artifact_id, provider, content_anchor, snapshot_type)`

즉,

- 같은 job이 다시 들어와도 run row는 중복 억제 가능
- 같은 content anchor면 snapshot append는 건너뛸 수 있다

---

## 5-11. current snapshot reuse는 fetch 이후에만 허용한다

이번 단계의 최소-change 최적화는 아래만 둔다.

1. 실제 fetch는 수행한다.
2. 새 `content_anchor`를 계산한다.
3. current snapshot이
   - provider == `web`
   - snapshot_type == `web_article`
   - content_anchor == 새 anchor
   면
4. 새 snapshot append 없이 run만 종료 가능하다.

중요:
- fetch 이전 short-circuit는 두지 않는다.
- web은 redirect/final_url/content-hash를 직접 확인해야 하기 때문이다.

---

## 5-12. q.artifact.enrich.web worker는 ack-first가 아니라 state-first다

consumer는 다음 순서를 지킨다.

1. Redis stream read
2. `trigger_event_id`로 `event_outbox` 재조회
3. `ArtifactEnrichmentJob` 복원
4. DB transaction 안에서
   - enrichment run row 시작
   - snapshot / discovered links / outbox write
5. commit 성공
6. stream ack

즉,
**ack는 state write 뒤**에 온다.

---

## 6. 코드 초안

## 6-1. `src/services/web_enricher/__init__.py`

```python
from .config import WebEnricherConfig
from .service import WebEnricherService

__all__ = [
    "WebEnricherConfig",
    "WebEnricherService",
]
```

---

## 6-2. `src/services/web_enricher/config.py`

```python
from __future__ import annotations

import os
from dataclasses import dataclass


class WebEnricherConfigurationError(ValueError):
    pass


@dataclass(slots=True, frozen=True)
class WebEnricherConfig:
    app_env: str
    database_url: str
    redis_url: str
    queue_name: str
    consumer_group: str
    consumer_name: str
    batch_size: int
    block_ms: int

    request_timeout_ms: int
    max_redirects: int
    max_bytes: int
    excerpt_chars: int
    max_outbound_links: int
    user_agent: str
    content_type_allowlist: tuple[str, ...]

    provider_name: str
    snapshot_type: str
    log_level: str

    @classmethod
    def from_env(cls) -> "WebEnricherConfig":
        database_url = os.getenv("DATABASE_URL", "").strip()
        redis_url = os.getenv("REDIS_URL", "").strip()
        if not database_url:
            raise WebEnricherConfigurationError("DATABASE_URL is required")
        if not redis_url:
            raise WebEnricherConfigurationError("REDIS_URL is required")

        allowlist_raw = os.getenv(
            "WEB_FETCH_CONTENT_TYPE_ALLOWLIST",
            "text/html,application/xhtml+xml,text/plain,text/markdown",
        ).strip()

        cfg = cls(
            app_env=os.getenv("APP_ENV", "dev").strip().lower(),
            database_url=database_url,
            redis_url=redis_url,
            queue_name=os.getenv("WEB_ENRICHER_QUEUE_NAME", "q.artifact.enrich.web").strip(),
            consumer_group=os.getenv("WEB_ENRICHER_CONSUMER_GROUP", "web-enricher").strip(),
            consumer_name=os.getenv("WEB_ENRICHER_CONSUMER_NAME", "web-enricher-1").strip(),
            batch_size=int(os.getenv("WEB_ENRICHER_BATCH_SIZE", "20")),
            block_ms=int(os.getenv("WEB_ENRICHER_BLOCK_MS", "5000")),
            request_timeout_ms=int(os.getenv("WEB_FETCH_TIMEOUT_MS", "6000")),
            max_redirects=int(os.getenv("WEB_FETCH_MAX_REDIRECTS", "4")),
            max_bytes=int(os.getenv("WEB_FETCH_MAX_BYTES", "262144")),
            excerpt_chars=int(os.getenv("WEB_FETCH_EXCERPT_CHARS", "1600")),
            max_outbound_links=int(os.getenv("WEB_FETCH_MAX_OUTBOUND_LINKS", "50")),
            user_agent=os.getenv("WEB_FETCH_USER_AGENT", "catchbot-web-enricher/0.1").strip(),
            content_type_allowlist=tuple(
                part.strip().lower() for part in allowlist_raw.split(",") if part.strip()
            ),
            provider_name="web",
            snapshot_type="web_article",
            log_level=os.getenv("LOG_LEVEL", "INFO").strip().upper(),
        )
        cfg.validate()
        return cfg

    def validate(self) -> None:
        if self.batch_size <= 0 or self.batch_size > 100:
            raise WebEnricherConfigurationError("WEB_ENRICHER_BATCH_SIZE must be between 1 and 100")
        if self.block_ms <= 0:
            raise WebEnricherConfigurationError("WEB_ENRICHER_BLOCK_MS must be > 0")
        if self.request_timeout_ms <= 0:
            raise WebEnricherConfigurationError("WEB_FETCH_TIMEOUT_MS must be > 0")
        if self.max_redirects <= 0 or self.max_redirects > 10:
            raise WebEnricherConfigurationError("WEB_FETCH_MAX_REDIRECTS must be between 1 and 10")
        if self.max_bytes <= 0:
            raise WebEnricherConfigurationError("WEB_FETCH_MAX_BYTES must be > 0")
        if self.excerpt_chars <= 0:
            raise WebEnricherConfigurationError("WEB_FETCH_EXCERPT_CHARS must be > 0")
        if self.max_outbound_links <= 0:
            raise WebEnricherConfigurationError("WEB_FETCH_MAX_OUTBOUND_LINKS must be > 0")
        if not self.queue_name:
            raise WebEnricherConfigurationError("WEB_ENRICHER_QUEUE_NAME must not be empty")
        if not self.consumer_group:
            raise WebEnricherConfigurationError("WEB_ENRICHER_CONSUMER_GROUP must not be empty")
        if not self.consumer_name:
            raise WebEnricherConfigurationError("WEB_ENRICHER_CONSUMER_NAME must not be empty")
        if not self.user_agent:
            raise WebEnricherConfigurationError("WEB_FETCH_USER_AGENT must not be empty")
        if not self.content_type_allowlist:
            raise WebEnricherConfigurationError("WEB_FETCH_CONTENT_TYPE_ALLOWLIST must not be empty")
        if self.provider_name != "web":
            raise WebEnricherConfigurationError("provider_name must be web")
        if self.snapshot_type != "web_article":
            raise WebEnricherConfigurationError("snapshot_type must be web_article")
```

---

## 6-3. `src/services/web_enricher/models.py`

```python
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any


@dataclass(slots=True, frozen=True)
class ArtifactEnrichmentJob:
    job_id: str
    candidate_group_id: str
    artifact_id: str
    artifact_type: str
    provider_route: str
    refresh_mode: str
    depth_budget: int
    requested_at: datetime


@dataclass(slots=True, frozen=True)
class FetchedDocument:
    requested_url: str
    final_url: str
    status_code: int
    content_type: str | None
    body_text: str | None
    response_headers_subset: dict[str, str]
    content_hash: str | None
    fetch_anomalies: list[str] = field(default_factory=list)


@dataclass(slots=True, frozen=True)
class WebArticleSnapshotDraft:
    artifact_id: str
    final_url: str
    canonical_url_candidate: str | None
    site_name: str | None
    title: str | None
    description: str | None
    author: str | None
    published_at: datetime | None
    content_hash: str | None
    main_text_excerpt: str | None
    outbound_links_json: list[dict[str, Any]]
    normalized_projection: dict[str, Any] | None
    evidence_limitations: list[str] = field(default_factory=list)
    fetch_anomalies: list[str] = field(default_factory=list)
    status: str = "ready"
    auth_mode: str = "anonymous_public"

    @property
    def content_anchor(self) -> str | None:
        if not self.content_hash:
            return None
        return self.normalized_projection.get("content_anchor") if self.normalized_projection else None
```

---

## 6-4. `src/services/web_enricher/web_fetch_client.py`

```python
from __future__ import annotations

import hashlib
from typing import Any
from urllib.parse import urljoin

import httpx

from .config import WebEnricherConfig
from .models import FetchedDocument


class WebFetchError(Exception):
    pass


class WebRateLimitedError(WebFetchError):
    pass


class WebAccessDeniedError(WebFetchError):
    pass


class WebFetchPermanentError(WebFetchError):
    pass


class WebFetchTransientError(WebFetchError):
    pass


class UnsupportedContentTypeError(WebFetchError):
    pass


class WebFetchClient:
    def __init__(self, config: WebEnricherConfig) -> None:
        self._config = config
        self._client = httpx.AsyncClient(
            timeout=self._config.request_timeout_ms / 1000.0,
            follow_redirects=False,
            headers={
                "User-Agent": self._config.user_agent,
                "Accept": "text/html,application/xhtml+xml,text/plain,text/markdown;q=0.9,*/*;q=0.1",
            },
        )

    async def close(self) -> None:
        await self._client.aclose()

    async def fetch(self, url: str) -> FetchedDocument:
        current_url = url
        anomalies: list[str] = []
        last_response: httpx.Response | None = None

        for _ in range(self._config.max_redirects + 1):
            response = await self._client.get(current_url)
            last_response = response

            if response.status_code in {301, 302, 303, 307, 308}:
                location = response.headers.get("location")
                if not location:
                    raise WebFetchPermanentError("redirect_without_location")
                current_url = urljoin(str(response.request.url), location)
                continue

            break

        if last_response is None:
            raise WebFetchTransientError("no_response")

        if last_response.status_code == 429:
            raise WebRateLimitedError("rate_limited")
        if last_response.status_code in {401, 403}:
            raise WebAccessDeniedError("access_denied")
        if last_response.status_code == 404:
            raise WebFetchPermanentError("not_found")
        if last_response.status_code >= 500:
            raise WebFetchTransientError(f"server_error_{last_response.status_code}")
        if last_response.status_code >= 400:
            raise WebFetchPermanentError(f"http_{last_response.status_code}")

        content_type = (last_response.headers.get("content-type") or "").split(";")[0].strip().lower() or None
        if content_type is not None and content_type not in self._config.content_type_allowlist:
            raise UnsupportedContentTypeError(content_type)

        raw_bytes = last_response.content[: self._config.max_bytes]
        if len(last_response.content) > self._config.max_bytes:
            anomalies.append("body_truncated_at_max_bytes")

        body_text = raw_bytes.decode(last_response.encoding or "utf-8", errors="replace")
        content_hash = hashlib.sha256(raw_bytes).hexdigest() if raw_bytes else None

        return FetchedDocument(
            requested_url=url,
            final_url=str(last_response.url),
            status_code=last_response.status_code,
            content_type=content_type,
            body_text=body_text,
            response_headers_subset={
                "content-type": last_response.headers.get("content-type", ""),
                "etag": last_response.headers.get("etag", ""),
                "last-modified": last_response.headers.get("last-modified", ""),
            },
            content_hash=content_hash,
            fetch_anomalies=anomalies,
        )
```

---

## 6-5. `src/services/web_enricher/article_parser.py`

```python
from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime
from html import unescape
from html.parser import HTMLParser
from typing import Any
from urllib.parse import urljoin


_TAG_STRIP_RE = re.compile(r"<[^>]+>")
_WS_RE = re.compile(r"\s+")


@dataclass(slots=True, frozen=True)
class ParsedArticle:
    canonical_url_candidate: str | None
    site_name: str | None
    title: str | None
    description: str | None
    author: str | None
    published_at: datetime | None
    main_text_excerpt: str | None
    outbound_links: list[str]
    normalized_projection: dict[str, Any]


class _MetadataParser(HTMLParser):
    def __init__(self, *, base_url: str, excerpt_chars: int, max_outbound_links: int) -> None:
        super().__init__(convert_charrefs=True)
        self._base_url = base_url
        self._excerpt_chars = excerpt_chars
        self._max_outbound_links = max_outbound_links

        self.title_chunks: list[str] = []
        self.current_tag_stack: list[str] = []
        self.text_chunks: list[str] = []
        self.outbound_links: list[str] = []

        self.meta: dict[str, str] = {}
        self.canonical_url_candidate: str | None = None
        self._skip_depth = 0

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        tag = tag.lower()
        attrs_map = {k.lower(): (v or "") for k, v in attrs}
        self.current_tag_stack.append(tag)

        if tag in {"script", "style", "noscript"}:
            self._skip_depth += 1
            return

        if tag == "meta":
            key = (
                attrs_map.get("property")
                or attrs_map.get("name")
                or attrs_map.get("itemprop")
            ).strip().lower()
            value = attrs_map.get("content", "").strip()
            if key and value and key not in self.meta:
                self.meta[key] = value

        if tag == "link":
            rel = attrs_map.get("rel", "").strip().lower()
            href = attrs_map.get("href", "").strip()
            if "canonical" in rel and href and self.canonical_url_candidate is None:
                self.canonical_url_candidate = urljoin(self._base_url, href)

        if tag == "a":
            href = attrs_map.get("href", "").strip()
            if href and len(self.outbound_links) < self._max_outbound_links:
                abs_href = urljoin(self._base_url, href)
                if abs_href.startswith(("http://", "https://")):
                    self.outbound_links.append(abs_href)

    def handle_endtag(self, tag: str) -> None:
        tag = tag.lower()
        if tag in {"script", "style", "noscript"} and self._skip_depth > 0:
            self._skip_depth -= 1
        if self.current_tag_stack:
            self.current_tag_stack.pop()

    def handle_data(self, data: str) -> None:
        if self._skip_depth > 0:
            return
        text = _WS_RE.sub(" ", data).strip()
        if not text:
            return

        current = self.current_tag_stack[-1] if self.current_tag_stack else ""
        if current == "title":
            self.title_chunks.append(text)
            return

        if current in {"article", "main", "p", "h1", "h2", "h3", "li", "blockquote"}:
            self.text_chunks.append(text)

    def build(self) -> ParsedArticle:
        title = self._first_non_empty(
            " ".join(self.title_chunks).strip(),
            self.meta.get("og:title"),
            self.meta.get("twitter:title"),
        )
        description = self._first_non_empty(
            self.meta.get("description"),
            self.meta.get("og:description"),
            self.meta.get("twitter:description"),
        )
        site_name = self._first_non_empty(
            self.meta.get("og:site_name"),
            self.meta.get("application-name"),
        )
        author = self._first_non_empty(
            self.meta.get("author"),
            self.meta.get("article:author"),
            self.meta.get("parsely-author"),
        )
        published_at = self._parse_datetime(
            self._first_non_empty(
                self.meta.get("article:published_time"),
                self.meta.get("parsely-pub-date"),
                self.meta.get("pubdate"),
                self.meta.get("date"),
            )
        )

        excerpt = " ".join(self.text_chunks).strip()
        excerpt = excerpt[: self._excerpt_chars] if excerpt else None

        return ParsedArticle(
            canonical_url_candidate=self.canonical_url_candidate,
            site_name=site_name,
            title=title,
            description=description,
            author=author,
            published_at=published_at,
            main_text_excerpt=excerpt,
            outbound_links=self.outbound_links,
            normalized_projection={
                "meta": self.meta,
                "title_chunks": self.title_chunks,
            },
        )

    @staticmethod
    def _first_non_empty(*values: str | None) -> str | None:
        for value in values:
            if value and value.strip():
                return value.strip()
        return None

    @staticmethod
    def _parse_datetime(value: str | None) -> datetime | None:
        if not value:
            return None
        try:
            return datetime.fromisoformat(value.replace("Z", "+00:00"))
        except Exception:
            return None


class ArticleParser:
    def __init__(self, *, excerpt_chars: int, max_outbound_links: int) -> None:
        self._excerpt_chars = excerpt_chars
        self._max_outbound_links = max_outbound_links

    def parse(
        self,
        *,
        final_url: str,
        content_type: str | None,
        body_text: str | None,
    ) -> ParsedArticle:
        body_text = body_text or ""
        if content_type in {"text/plain", "text/markdown"}:
            return self._parse_plain(final_url=final_url, body_text=body_text)

        parser = _MetadataParser(
            base_url=final_url,
            excerpt_chars=self._excerpt_chars,
            max_outbound_links=self._max_outbound_links,
        )
        parser.feed(body_text)
        return parser.build()

    def _parse_plain(self, *, final_url: str, body_text: str) -> ParsedArticle:
        clean = _WS_RE.sub(" ", body_text).strip()
        title = None
        excerpt = None
        if clean:
            parts = body_text.splitlines()
            for line in parts:
                line = line.strip()
                if line:
                    title = line[:120]
                    break
            excerpt = clean[: self._excerpt_chars]

        outbound_links = [
            match for match in re.findall(r"https?://[^\s<>()\[\]{}\"']+", body_text)
        ][: self._max_outbound_links]

        return ParsedArticle(
            canonical_url_candidate=None,
            site_name=None,
            title=title,
            description=None,
            author=None,
            published_at=None,
            main_text_excerpt=excerpt,
            outbound_links=outbound_links,
            normalized_projection={
                "plain_text_mode": True,
            },
        )
```

---

## 6-6. `src/services/web_enricher/url_discovery.py`

```python
from __future__ import annotations

from src.services.router_normalizer.canonicalizer import Canonicalizer
from src.services.router_normalizer.models import ObservedUrl


class WebUrlDiscovery:
    def __init__(self) -> None:
        self._canonicalizer = Canonicalizer()

    def extract_and_canonicalize(
        self,
        *,
        outbound_links: list[str],
    ) -> tuple[list[dict[str, str | None]], list[ObservedUrl]]:
        observed: list[ObservedUrl] = [
            ObservedUrl(
                observed_url=url,
                source_kind="web_outbound_link",
                context_path=f"web_article.outbound_links[{idx}]",
            )
            for idx, url in enumerate(outbound_links)
        ]

        _, normalized = self._canonicalizer.canonicalize_many(observed)
        discovered_links_json = [
            {
                "observed_url": item.observed_url,
                "normalized_url": item.normalized_url,
                "resolved_url": item.resolved_url,
                "canonical_url": item.canonical_url,
                "classification": item.classification,
                "context_path": item.context_path,
                "source_kind": item.source_kind,
            }
            for item in normalized
        ]
        return discovered_links_json, normalized
```

---

## 6-7. `src/services/web_enricher/repositories.py`

```python
from __future__ import annotations

import json
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Any, AsyncIterator, Mapping

import sqlalchemy as sa
from sqlalchemy.ext.asyncio import AsyncSession

from .models import ArtifactEnrichmentJob, WebArticleSnapshotDraft


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


class WebEnricherRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    @asynccontextmanager
    async def transaction(self) -> AsyncIterator[AsyncSession]:
        if self._session.in_transaction():
            yield self._session
            return
        async with self._session.begin():
            yield self._session

    async def load_job_by_trigger_event_id(self, trigger_event_id: str) -> ArtifactEnrichmentJob | None:
        result = await self._session.execute(
            sa.text(
                """
                SELECT event_id, payload_json
                FROM event_outbox
                WHERE event_id = CAST(:event_id AS uuid)
                  AND event_type = 'artifact.enrich.requested.v1'
                """
            ),
            {"event_id": trigger_event_id},
        )
        row = result.mappings().first()
        if row is None:
            return None
        payload = row["payload_json"] or {}
        if payload.get("provider_route") != "web":
            return None

        return ArtifactEnrichmentJob(
            job_id=str(row["event_id"]),
            candidate_group_id=str(payload["candidate_group_id"]),
            artifact_id=str(payload["artifact_id"]),
            artifact_type=str(payload["artifact_type"]),
            provider_route=str(payload["provider_route"]),
            refresh_mode=str(payload.get("refresh_mode", "standard")),
            depth_budget=int(payload.get("depth_budget", 1)),
            requested_at=datetime.fromisoformat(str(payload["requested_at"])),
        )

    async def load_artifact_registry_row(self, artifact_id: str) -> Mapping[str, Any] | None:
        result = await self._session.execute(
            sa.text(
                """
                SELECT *
                FROM artifact_registry
                WHERE artifact_id = CAST(:artifact_id AS uuid)
                """
            ),
            {"artifact_id": artifact_id},
        )
        return result.mappings().first()

    async def load_current_snapshot_for_artifact(self, artifact_id: str) -> Mapping[str, Any] | None:
        result = await self._session.execute(
            sa.text(
                """
                SELECT s.*
                FROM artifact_registry a
                JOIN artifact_snapshots s
                  ON s.snapshot_id = a.current_snapshot_id
                WHERE a.artifact_id = CAST(:artifact_id AS uuid)
                """
            ),
            {"artifact_id": artifact_id},
        )
        return result.mappings().first()

    async def begin_enrichment_run(
        self,
        *,
        artifact_id: str,
        provider: str,
        refresh_mode: str,
        depth_budget: int,
        job_idempotency_key: str,
    ) -> str:
        result = await self._session.execute(
            sa.text(
                """
                INSERT INTO artifact_enrichment_runs (
                    artifact_id,
                    provider,
                    refresh_mode,
                    depth_budget,
                    status,
                    job_idempotency_key,
                    requested_at,
                    started_at
                )
                VALUES (
                    CAST(:artifact_id AS uuid),
                    :provider,
                    :refresh_mode,
                    :depth_budget,
                    'fetching'::snapshot_status_enum,
                    :job_idempotency_key,
                    now(),
                    now()
                )
                ON CONFLICT (job_idempotency_key)
                DO UPDATE SET started_at = EXCLUDED.started_at
                RETURNING artifact_enrichment_run_id
                """
            ),
            {
                "artifact_id": artifact_id,
                "provider": provider,
                "refresh_mode": refresh_mode,
                "depth_budget": depth_budget,
                "job_idempotency_key": job_idempotency_key,
            },
        )
        return str(result.scalar_one())

    async def finish_enrichment_run(
        self,
        *,
        artifact_enrichment_run_id: str,
        status: str,
        content_anchor: str | None,
    ) -> None:
        await self._session.execute(
            sa.text(
                """
                UPDATE artifact_enrichment_runs
                SET
                    status = CAST(:status AS snapshot_status_enum),
                    content_anchor = :content_anchor,
                    finished_at = now()
                WHERE artifact_enrichment_run_id = CAST(:run_id AS uuid)
                """
            ),
            {
                "run_id": artifact_enrichment_run_id,
                "status": status,
                "content_anchor": content_anchor,
            },
        )

    async def insert_snapshot(self, draft: WebArticleSnapshotDraft) -> str:
        result = await self._session.execute(
            sa.text(
                """
                INSERT INTO artifact_snapshots (
                    artifact_id,
                    provider,
                    snapshot_type,
                    status,
                    fetched_at,
                    content_anchor,
                    auth_mode,
                    normalized_projection,
                    raw_payload_ref,
                    evidence_limitations,
                    fetch_anomalies
                )
                VALUES (
                    CAST(:artifact_id AS uuid),
                    'web',
                    'web_article',
                    CAST(:status AS snapshot_status_enum),
                    now(),
                    :content_anchor,
                    :auth_mode,
                    CAST(:normalized_projection AS jsonb),
                    NULL,
                    CAST(:evidence_limitations AS jsonb),
                    CAST(:fetch_anomalies AS jsonb)
                )
                ON CONFLICT (artifact_id, provider, content_anchor, snapshot_type)
                DO UPDATE SET
                    status = EXCLUDED.status
                RETURNING snapshot_id
                """
            ),
            {
                "artifact_id": draft.artifact_id,
                "status": draft.status,
                "content_anchor": draft.content_anchor,
                "auth_mode": draft.auth_mode,
                "normalized_projection": _jsonb_dumps(draft.normalized_projection),
                "evidence_limitations": _jsonb_dumps(draft.evidence_limitations),
                "fetch_anomalies": _jsonb_dumps(draft.fetch_anomalies),
            },
        )
        return str(result.scalar_one())

    async def upsert_snapshot_web_article(
        self,
        *,
        snapshot_id: str,
        draft: WebArticleSnapshotDraft,
    ) -> None:
        await self._session.execute(
            sa.text(
                """
                INSERT INTO artifact_snapshot_web_article (
                    snapshot_id,
                    final_url,
                    canonical_url_candidate,
                    site_name,
                    title,
                    description,
                    author,
                    published_at,
                    content_hash,
                    main_text_excerpt,
                    outbound_links_json
                )
                VALUES (
                    CAST(:snapshot_id AS uuid),
                    :final_url,
                    :canonical_url_candidate,
                    :site_name,
                    :title,
                    :description,
                    :author,
                    :published_at,
                    :content_hash,
                    :main_text_excerpt,
                    CAST(:outbound_links_json AS jsonb)
                )
                ON CONFLICT (snapshot_id)
                DO UPDATE SET
                    final_url = EXCLUDED.final_url,
                    canonical_url_candidate = EXCLUDED.canonical_url_candidate,
                    site_name = EXCLUDED.site_name,
                    title = EXCLUDED.title,
                    description = EXCLUDED.description,
                    author = EXCLUDED.author,
                    published_at = EXCLUDED.published_at,
                    content_hash = EXCLUDED.content_hash,
                    main_text_excerpt = EXCLUDED.main_text_excerpt,
                    outbound_links_json = EXCLUDED.outbound_links_json
                """
            ),
            {
                "snapshot_id": snapshot_id,
                "final_url": draft.final_url,
                "canonical_url_candidate": draft.canonical_url_candidate,
                "site_name": draft.site_name,
                "title": draft.title,
                "description": draft.description,
                "author": draft.author,
                "published_at": draft.published_at,
                "content_hash": draft.content_hash,
                "main_text_excerpt": draft.main_text_excerpt,
                "outbound_links_json": _jsonb_dumps(draft.outbound_links_json),
            },
        )

    async def update_artifact_current_snapshot(
        self,
        *,
        artifact_id: str,
        snapshot_id: str,
        status: str,
    ) -> None:
        await self._session.execute(
            sa.text(
                """
                UPDATE artifact_registry
                SET
                    current_snapshot_id = CAST(:snapshot_id AS uuid),
                    current_status = CAST(:status AS snapshot_status_enum),
                    updated_at = now()
                WHERE artifact_id = CAST(:artifact_id AS uuid)
                """
            ),
            {
                "artifact_id": artifact_id,
                "snapshot_id": snapshot_id,
                "status": status,
            },
        )

    async def insert_discovered_url_observation(
        self,
        *,
        parent_candidate_group_id: str,
        parent_artifact_id: str,
        parent_snapshot_id: str,
        observed_url: str,
        context_path: str | None,
        discovery_reason: str,
        depth_remaining: int,
    ) -> None:
        await self._session.execute(
            sa.text(
                """
                INSERT INTO discovered_url_observations (
                    parent_candidate_group_id,
                    parent_artifact_id,
                    parent_snapshot_id,
                    observed_url,
                    context_path,
                    discovery_reason,
                    depth_remaining,
                    created_at
                )
                VALUES (
                    CAST(:parent_candidate_group_id AS uuid),
                    CAST(:parent_artifact_id AS uuid),
                    CAST(:parent_snapshot_id AS uuid),
                    :observed_url,
                    :context_path,
                    :discovery_reason,
                    :depth_remaining,
                    now()
                )
                """
            ),
            {
                "parent_candidate_group_id": parent_candidate_group_id,
                "parent_artifact_id": parent_artifact_id,
                "parent_snapshot_id": parent_snapshot_id,
                "observed_url": observed_url,
                "context_path": context_path,
                "discovery_reason": discovery_reason,
                "depth_remaining": depth_remaining,
            },
        )

    async def insert_outbox_event(
        self,
        *,
        event_type: str,
        aggregate_type: str,
        aggregate_id: str,
        dedupe_key: str,
        payload_json: dict[str, Any],
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
                    :event_type,
                    :aggregate_type,
                    CAST(:aggregate_id AS uuid),
                    :dedupe_key,
                    CAST(:payload_json AS jsonb),
                    'pending'::outbox_status_enum,
                    now()
                )
                ON CONFLICT (dedupe_key) DO NOTHING
                """
            ),
            {
                "event_type": event_type,
                "aggregate_type": aggregate_type,
                "aggregate_id": aggregate_id,
                "dedupe_key": dedupe_key,
                "payload_json": _jsonb_dumps(payload_json),
            },
        )
```

---

## 6-8. `src/services/web_enricher/redis_streams.py`

```python
from __future__ import annotations

from dataclasses import dataclass

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

## 6-9. `src/services/web_enricher/service.py`

```python
from __future__ import annotations

import hashlib

from .article_parser import ArticleParser
from .config import WebEnricherConfig
from .models import ArtifactEnrichmentJob, WebArticleSnapshotDraft
from .repositories import WebEnricherRepository
from .url_discovery import WebUrlDiscovery
from .web_fetch_client import (
    UnsupportedContentTypeError,
    WebAccessDeniedError,
    WebFetchClient,
    WebFetchPermanentError,
    WebFetchTransientError,
    WebRateLimitedError,
)


class WebEnricherService:
    def __init__(
        self,
        config: WebEnricherConfig,
        *,
        repository: WebEnricherRepository,
        fetch_client: WebFetchClient,
        article_parser: ArticleParser,
        url_discovery: WebUrlDiscovery,
    ) -> None:
        self._config = config
        self._repository = repository
        self._fetch_client = fetch_client
        self._article_parser = article_parser
        self._url_discovery = url_discovery

    async def rehydrate_job(self, trigger_event_id: str) -> ArtifactEnrichmentJob | None:
        return await self._repository.load_job_by_trigger_event_id(trigger_event_id)

    async def handle_job(self, job: ArtifactEnrichmentJob) -> None:
        artifact = await self._repository.load_artifact_registry_row(job.artifact_id)
        if artifact is None:
            return
        if str(artifact.get("artifact_type")) != "web_article":
            return

        start_url = str(artifact.get("canonical_url") or "").strip()
        if not start_url.startswith(("http://", "https://")):
            return

        run_idempotency_key = f"enrich:web:{job.artifact_id}:{job.refresh_mode}:{job.depth_budget}"
        async with self._repository.transaction():
            run_id = await self._repository.begin_enrichment_run(
                artifact_id=job.artifact_id,
                provider="web",
                refresh_mode=job.refresh_mode,
                depth_budget=job.depth_budget,
                job_idempotency_key=run_idempotency_key,
            )

        try:
            fetched = await self._fetch_client.fetch(start_url)
        except WebRateLimitedError:
            async with self._repository.transaction():
                await self._repository.finish_enrichment_run(
                    artifact_enrichment_run_id=run_id,
                    status="rate_limited",
                    content_anchor=None,
                )
            return
        except WebAccessDeniedError:
            async with self._repository.transaction():
                await self._repository.finish_enrichment_run(
                    artifact_enrichment_run_id=run_id,
                    status="access_denied",
                    content_anchor=None,
                )
            return
        except UnsupportedContentTypeError as exc:
            draft = WebArticleSnapshotDraft(
                artifact_id=job.artifact_id,
                final_url=start_url,
                canonical_url_candidate=None,
                site_name=None,
                title=None,
                description=None,
                author=None,
                published_at=None,
                content_hash=None,
                main_text_excerpt=None,
                outbound_links_json=[],
                normalized_projection={
                    "start_url": start_url,
                    "unsupported_content_type": str(exc),
                },
                evidence_limitations=["web_content_type_not_supported"],
                fetch_anomalies=["unsupported_content_type"],
                status="unsupported",
            )
            async with self._repository.transaction():
                snapshot_id = await self._repository.insert_snapshot(draft)
                await self._repository.upsert_snapshot_web_article(snapshot_id=snapshot_id, draft=draft)
                await self._repository.update_artifact_current_snapshot(
                    artifact_id=job.artifact_id,
                    snapshot_id=snapshot_id,
                    status=draft.status,
                )
                await self._repository.insert_outbox_event(
                    event_type="artifact.snapshot.updated.v1",
                    aggregate_type="artifact",
                    aggregate_id=job.artifact_id,
                    dedupe_key=f"artifact:snapshot_updated:{job.artifact_id}:{snapshot_id}",
                    payload_json={
                        "artifact_id": job.artifact_id,
                        "candidate_group_id": job.candidate_group_id,
                        "provider_route": "web",
                        "snapshot_id": snapshot_id,
                        "snapshot_type": "web_article",
                        "status": draft.status,
                        "content_anchor": draft.content_anchor,
                    },
                )
                await self._repository.finish_enrichment_run(
                    artifact_enrichment_run_id=run_id,
                    status=draft.status,
                    content_anchor=draft.content_anchor,
                )
            return
        except WebFetchPermanentError:
            async with self._repository.transaction():
                await self._repository.finish_enrichment_run(
                    artifact_enrichment_run_id=run_id,
                    status="failed_permanent",
                    content_anchor=None,
                )
            return
        except WebFetchTransientError:
            async with self._repository.transaction():
                await self._repository.finish_enrichment_run(
                    artifact_enrichment_run_id=run_id,
                    status="failed_transient",
                    content_anchor=None,
                )
            return

        parsed = self._article_parser.parse(
            final_url=fetched.final_url,
            content_type=fetched.content_type,
            body_text=fetched.body_text,
        )
        discovered_links_json, discovered_observations = self._url_discovery.extract_and_canonicalize(
            outbound_links=parsed.outbound_links,
        )

        content_anchor = None
        if fetched.content_hash:
            content_anchor = "web:" + hashlib.sha256(
                f"{fetched.final_url}|{fetched.content_hash}".encode("utf-8")
            ).hexdigest()

        evidence_limitations = []
        fetch_anomalies = list(fetched.fetch_anomalies)

        status = "ready"
        if not parsed.title and not parsed.main_text_excerpt:
            status = "low_evidence"
            evidence_limitations.append("web_title_and_excerpt_missing")
        elif not parsed.title or not parsed.description:
            status = "partial_ready"
            evidence_limitations.append("web_metadata_sparse")
        if fetched.content_type in {"text/plain", "text/markdown"}:
            if status == "ready":
                status = "partial_ready"
            evidence_limitations.append("web_plain_text_mode")
        if not discovered_links_json:
            evidence_limitations.append("web_outbound_links_missing")

        draft = WebArticleSnapshotDraft(
            artifact_id=job.artifact_id,
            final_url=fetched.final_url,
            canonical_url_candidate=parsed.canonical_url_candidate,
            site_name=parsed.site_name,
            title=parsed.title,
            description=parsed.description,
            author=parsed.author,
            published_at=parsed.published_at,
            content_hash=fetched.content_hash,
            main_text_excerpt=parsed.main_text_excerpt[: self._config.excerpt_chars] if parsed.main_text_excerpt else None,
            outbound_links_json=discovered_links_json,
            normalized_projection={
                "start_url": start_url,
                "final_url": fetched.final_url,
                "response_headers_subset": fetched.response_headers_subset,
                "content_type": fetched.content_type,
                "content_anchor": content_anchor,
                "parser_projection": parsed.normalized_projection,
            },
            evidence_limitations=evidence_limitations,
            fetch_anomalies=fetch_anomalies,
            status=status,
        )

        current_snapshot = await self._repository.load_current_snapshot_for_artifact(job.artifact_id)
        if (
            current_snapshot is not None
            and str(current_snapshot.get("provider")) == "web"
            and str(current_snapshot.get("snapshot_type")) == "web_article"
            and str(current_snapshot.get("content_anchor")) == draft.content_anchor
        ):
            async with self._repository.transaction():
                await self._repository.finish_enrichment_run(
                    artifact_enrichment_run_id=run_id,
                    status=draft.status,
                    content_anchor=draft.content_anchor,
                )
            return

        async with self._repository.transaction():
            snapshot_id = await self._repository.insert_snapshot(draft)
            await self._repository.upsert_snapshot_web_article(snapshot_id=snapshot_id, draft=draft)
            await self._repository.update_artifact_current_snapshot(
                artifact_id=job.artifact_id,
                snapshot_id=snapshot_id,
                status=draft.status,
            )

            for item in discovered_observations:
                await self._repository.insert_discovered_url_observation(
                    parent_candidate_group_id=job.candidate_group_id,
                    parent_artifact_id=job.artifact_id,
                    parent_snapshot_id=snapshot_id,
                    observed_url=item.observed_url,
                    context_path=item.context_path,
                    discovery_reason="embedded_link",
                    depth_remaining=max(0, job.depth_budget - 1),
                )

            await self._repository.insert_outbox_event(
                event_type="artifact.snapshot.updated.v1",
                aggregate_type="artifact",
                aggregate_id=job.artifact_id,
                dedupe_key=f"artifact:snapshot_updated:{job.artifact_id}:{snapshot_id}",
                payload_json={
                    "artifact_id": job.artifact_id,
                    "candidate_group_id": job.candidate_group_id,
                    "provider_route": "web",
                    "snapshot_id": snapshot_id,
                    "snapshot_type": "web_article",
                    "status": draft.status,
                    "content_anchor": draft.content_anchor,
                },
            )
            await self._repository.finish_enrichment_run(
                artifact_enrichment_run_id=run_id,
                status=draft.status,
                content_anchor=draft.content_anchor,
            )
```

---

## 6-10. `src/services/web_enricher/worker.py`

```python
from __future__ import annotations

import asyncio
from dataclasses import dataclass

from .config import WebEnricherConfig
from .redis_streams import RedisStreamConsumer, StreamMessage
from .service import WebEnricherService


@dataclass(slots=True, frozen=True)
class WorkerBatchResult:
    processed: int = 0
    acked: int = 0
    skipped: int = 0


class WebEnricherWorker:
    def __init__(
        self,
        config: WebEnricherConfig,
        *,
        consumer: RedisStreamConsumer,
        service: WebEnricherService,
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
            return True

        job = await self._service.rehydrate_job(trigger_event_id)
        if job is None:
            return True

        await self._service.handle_job(job)
        return True
```

---

## 6-11. `src/services/web_enricher/main.py`

```python
from __future__ import annotations

import asyncio

from redis.asyncio import Redis
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from .article_parser import ArticleParser
from .config import WebEnricherConfig
from .redis_streams import RedisStreamConsumer
from .repositories import WebEnricherRepository
from .service import WebEnricherService
from .url_discovery import WebUrlDiscovery
from .web_fetch_client import WebFetchClient
from .worker import WebEnricherWorker


async def _run() -> int:
    config = WebEnricherConfig.from_env()

    engine = create_async_engine(config.database_url, future=True)
    session_factory = async_sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)
    redis_client = Redis.from_url(config.redis_url, decode_responses=False)

    async with session_factory() as session:
        repository = WebEnricherRepository(session)
        fetch_client = WebFetchClient(config)
        service = WebEnricherService(
            config,
            repository=repository,
            fetch_client=fetch_client,
            article_parser=ArticleParser(
                excerpt_chars=config.excerpt_chars,
                max_outbound_links=config.max_outbound_links,
            ),
            url_discovery=WebUrlDiscovery(),
        )
        consumer = RedisStreamConsumer(
            redis_client,
            queue_name=config.queue_name,
            consumer_group=config.consumer_group,
            consumer_name=config.consumer_name,
            block_ms=config.block_ms,
            batch_size=config.batch_size,
        )
        worker = WebEnricherWorker(config, consumer=consumer, service=service)

        try:
            await worker.run_forever()
        finally:
            await fetch_client.close()
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

### `tests/unit/services/web_enricher/test_article_parser.py`

검증:
- HTML에 `<title>`, `meta[name=description]`, `og:site_name`, `link rel=canonical`, `article:published_time`, 여러 `<a href>`가 있을 때
- parser가 `title/description/site_name/canonical_url_candidate/published_at/outbound_links`를 채우는지
- excerpt가 visible text 기반으로 잘리는지

### `tests/unit/services/web_enricher/test_url_discovery.py`

검증:
- outbound links에 GitHub/X/article URL이 섞여 있을 때
- shared canonicalizer가 재사용되는지
- `classification` / `canonical_url` / `context_path`가 채워지는지

### `tests/unit/services/web_enricher/test_content_anchor_computation.py`

검증:
- 같은 `final_url + content_hash`면 같은 anchor
- `final_url` 또는 `content_hash`가 바뀌면 anchor가 바뀌는지

### `tests/unit/services/web_enricher/test_content_type_guard.py`

검증:
- `text/html`, `text/plain`, `text/markdown`은 허용
- `application/pdf`, `image/png`, `application/json`은 `UnsupportedContentTypeError`
- body cap 초과 시 anomaly가 남는지

### `tests/component/services/web_enricher/test_worker_rehydrates_job_from_event_outbox.py`

검증:
- Redis Streams message에는 `trigger_event_id`만 있음
- `event_outbox.payload_json`에서 `ArtifactEnrichmentJob` 복원
- service 호출 후 ack 수행

### `tests/component/services/web_enricher/test_web_snapshot_write_and_outbox_emit.py`

검증:
- fetch + parse 성공
- `artifact_snapshots` / `artifact_snapshot_web_article` write
- `artifact_registry.current_snapshot_id/current_status` 갱신
- `artifact.snapshot.updated.v1` outbox row 생성

### `tests/component/services/web_enricher/test_partial_ready_on_metadata_sparse_page.py`

검증:
- HTML은 왔지만 title/description 일부 없음
- excerpt는 있음
- snapshot status = `partial_ready`
- `evidence_limitations`에 `web_metadata_sparse`가 들어가는지

### `tests/component/services/web_enricher/test_redirect_final_url_preserved_in_snapshot.py`

검증:
- start URL과 final URL이 다름
- child row의 `final_url`이 redirect 이후 값인지
- `canonical_url_candidate`가 별도로 보존되는지
- artifact registry canonical row는 바뀌지 않는지

---

## 8. 이번 단계가 구조를 지키는 이유

이번 문서는 아래 경계를 유지한다.

- `router-normalizer`는 candidate proposal까지만
- `gh-enricher`, `x-enricher`, `web-enricher`는 외부 증거 수집까지만
- reroot는 여전히 assembler만
- judge/policy/notifier는 전혀 건드리지 않음

특히 중요한 점은 세 가지다.

1. **headless browser fallback을 넣지 않았다.**  
   즉, web evidence는 제한적 GET + metadata/excerpt만 수집한다.

2. **outbound links를 observation으로만 남긴다.**  
   즉, article이 GitHub repo를 가리켜도 이 단계에서 primary를 바꾸지 않는다.

3. **artifact registry canonical row를 rewrite하지 않는다.**  
   즉, `final_url`과 `canonical_url_candidate`는 snapshot evidence로만 보존한다.

이 세 규칙이 stage 5 구조를 가장 잘 지킨다.

---

## 9. 다음 단계

이 단계가 끝나면 다음 구현 순서는 아래가 맞다.

1. `evidence-assembler`

그 이후에야:

2. `analysis-router`
3. `judge-openai`
4. `analysis-validator`
5. `policy-engine`
6. `notifier-telegram`

즉, 지금은 여전히 **stage 5 evidence layer** 안에 있다.

---

## 최종 한 줄 결론

이번 단계의 최소-change 정답은 **`artifact.enrich.requested.v1` thin payload를 기준으로 `q.artifact.enrich.web`를 소비하고, 제한적 GET + redirect cap + content-type/body cap 아래에서 metadata/excerpt/outbound links만 수집해 `artifact_snapshot_web_article`와 `discovered_url_observations`를 append-only로 기록한 뒤, `artifact.snapshot.updated.v1`를 내보내는 좁은 `web-enricher` worker를 고정하는 것**이다.


---

## Source file: `32_evidence_assembler_skeleton_and_code_draft_v0_1.md`

# 32단계: `evidence-assembler` 스켈레톤 + 실제 코드 초안 v0.1

## 0. 문서 목적

이 문서는 프로젝트 소스의 README/정본 단계 문서, `05_stage5_external_enrichers.md`, `11_stage11_execution_contracts_v0_1.md`, `12_migration_spec_0001_0004_v0_1.md`, 그리고 `29_gh_enricher_skeleton_and_code_draft_v0_1.md`, `30_x_enricher_skeleton_and_code_draft_v0_1.md`, `31_web_enricher_skeleton_and_code_draft_v0_1.md`까지의 구현 흐름을 바탕으로,
**`evidence-assembler`의 첫 구현 묶음**을 실제 코드 초안 수준으로 내리는 문서다.

이번 단계의 목적은 여섯 가지다.

1. `q.candidate.bundle` Redis Streams를 소비하는 **bundle assembly 전용 경계**를 코드로 고정
2. `artifact.snapshot.updated.v1` / `candidate.bundle.refresh.v1` thin payload를 기준으로, `event_outbox`에서 다시 **bundle refresh request**를 복원하는 rehydration 경계를 고정
3. `candidate_group_proposals`, `candidate_group_members`, `artifact_registry`, source-specific snapshots, `discovered_url_observations`를 모아 **candidate-centered evidence bundle**을 append-only로 생성하는 경계를 고정
4. **text-only idea snapshot 생성**, **reroot 판단**, **evidence limitations 통합**, **token budget profile 산출**, **ready-for-analysis 판정**을 assembler 단일 지점으로 고정
5. `candidate_reroot_events`, `candidate_evidence_bundles`, `candidate_evidence_members`, `artifact_snapshot_text_idea`, `event_outbox`에 대한 **evidence-assembler 전용 DB 경계**를 코드로 고정
6. `analysis.requested.v1` outbox emit까지 닫아, 다음 단계의 `analysis-router`가 얇은 ID payload를 기준으로 자연스럽게 이어질 수 있게 고정

핵심 전제:

- `evidence-assembler`는 **판단기**가 아니다.
- `evidence-assembler`는 **외부 fetcher**가 아니다.
- `evidence-assembler`는 **LLM을 호출하지 않는다.**
- `evidence-assembler`만 **current primary 변경(reroot)** 을 반영할 수 있다.
- `evidence-assembler`는 **append-only bundle + mutable current pointer** 구조를 유지한다.

---

## 1. 왜 지금 이 단계가 정확한 다음 단계인가

현재 인덱스 기준 구현 상태는 아래로 고정돼 있다.

- `router-normalizer` deterministic core + consumer/integration hardening 완료
- `gh-enricher` v0.1 완료
- `x-enricher` v0.1 완료
- `web-enricher` v0.1 완료
- 다음 구현 순서: **`evidence-assembler`**

즉, 지금 시점에서 collector / outbox-relay / normalizer / source enrichers를 다시 여는 것은 순서상 후퇴다.
이제 stage 5 evidence layer의 마지막 조각인 **candidate-centered assembly boundary** 를 닫는 것이 맞다.

또한 5단계 정본은 source enrichers 셋과 assembler를 분리하면서,
**primary 교체는 assembler만 할 수 있어야 한다**고 잠갔다.
그리고 11단계 실행 계약은 `evidence-assembler`의 직접 쓰기 테이블을 아래처럼 이미 고정했다.

- `artifact_snapshot_text_idea`
- `candidate_reroot_events`
- `candidate_evidence_bundles`
- `candidate_evidence_members`
- `event_outbox`

즉, 지금은 stage 5를 다시 설계하는 단계가 아니라,
**이미 잠긴 bundle/reroot/text_idea/ready-for-analysis 경계를 첫 runnable package로 내리는 단계**다.

---

## 2. 이번 단계에서 발견되는 작은 충돌과 최소-change 해석

### 충돌 지점 A — discovered links는 충분히 중요하지만, 현재 service ownership에는 새 artifact/member 생성 권한이 없다

5단계 정본은 discovered links 처리에서 아래 의도를 남겼다.

1. 공유 canonicalizer 사용
2. 동일 canonical artifact면 observation만 추가
3. 새 artifact면 supporting artifact로 우선 연결
4. reroot suggestion은 별도 이벤트로 분리

하지만 현재 실행 계약/서비스 책임 매트릭스에서 `evidence-assembler`는 아래 쓰기 권한만 가진다.

- `artifact_snapshot_text_idea`
- `candidate_reroot_events`
- `candidate_evidence_bundles`
- `candidate_evidence_members`
- `event_outbox`

즉,
- `artifact_registry` 새 upsert
- `candidate_group_members`에 새 discovered artifact 추가

같은 책임은 잠겨 있지 않다.

### 최소-change 해석 A

이번 v0.1에서는 아래 해석이 가장 보수적이다.

1. assembler는 **현재 candidate membership 안에 이미 존재하는 artifact들**을 우선 대상으로 쓴다.
2. `discovered_url_observations`는
   - reroot 정황 보강
   - discovered links summary
   - evidence limitations / supporting narrative
   에는 반영한다.
3. 하지만 **새 artifact 생성**이나 **candidate membership 확장**은 하지 않는다.
4. 따라서 v0.1 reroot는 **이미 후보에 들어와 있는 artifact들 사이에서만** 일어난다.

이 해석의 장점:

- 기존 서비스 ownership을 깨지 않음
- append-only lineage 구조를 그대로 유지함
- 현재 migration/서비스 책임표와 충돌하지 않음

즉, v0.1 assembler는
**“새 supporting artifact를 발명하는 서비스”가 아니라, “이미 확보된 candidate와 snapshot을 조립하는 서비스”** 로 고정한다.

---

### 충돌 지점 B — `text_idea`는 external enricher가 아닌데 `0003_enrichment_bundles` 스키마에 들어 있다

이건 구조상 약간 어색하다.

- `artifact_snapshot_text_idea`는 source message 기반 local projection이다.
- 외부 API를 호출하지 않는다.
- 그런데 snapshot 계층상 `0003_enrichment_bundles` 안에 있다.

### 최소-change 해석 B

이번 v0.1에서는 아래 해석이 가장 안전하다.

1. `text_idea`는 external source가 아니라 **candidate assembly 보조 snapshot** 이다.
2. 생성 시점은 assembler다.
3. 다만 durability/lineage 일관성을 위해 `artifact_snapshots` 부모 row + `artifact_snapshot_text_idea` child row를 사용한다.
4. provider는 `local_text_idea`, snapshot_type은 `text_idea`로 둔다.

즉,
**외부 fetch는 아니지만 snapshot model에는 올라간다**는 해석이다.

---

### 충돌 지점 C — bundle은 append-only인데 current pointer는 mutable이다

5단계/11단계/12단계는 동시에 아래를 잠갔다.

- bundle row는 append-only
- `candidate_group_proposals.current_bundle_id`는 mutable current pointer
- reroot history는 `candidate_reroot_events` append-only
- current primary는 `candidate_group_proposals.current_primary_artifact_id` mutable

### 최소-change 해석 C

이번 v0.1에서는 아래로 고정한다.

1. reroot 여부와 무관하게 bundle은 항상 **새 row append**
2. current primary가 바뀌면
   - `candidate_reroot_events` append
   - `candidate_group_proposals.current_primary_artifact_id` update
3. 새 bundle이 만들어지면
   - `candidate_group_proposals.current_bundle_id` update
4. overwrite는 없다

즉,
**history는 append-only, current pointer만 mutable** 이다.

---

## 3. 범위와 비범위

### 3-1. 포함 범위

- Redis Streams consumer group bootstrap
- `event_outbox` 기반 bundle refresh request 재구성
- `candidate_group_proposals` / `candidate_group_members` / `artifact_registry` / current snapshots / discovered observations 재조회
- `text_idea` snapshot materialization
- deterministic reroot rule evaluation
- bundle composition
- evidence limitations aggregation
- token budget profile calculation
- `ready_for_analysis` 판정
- `analysis.requested.v1` outbox emit
- 최소 unit/component tests

### 3-2. 제외 범위

- 새 artifact creation / artifact_registry mutation
- candidate membership expansion from discovered URLs
- external HTTP fetch
- LLM 호출
- judge / policy / notifier 호출
- queue reclaim / DLQ hardening
- eval/governance UI

즉, 이번 문서는
**실제 소비 가능한 bundle assembly worker** 를 닫되,
그 범위를 stage 5 `EvidenceBundle` 경계 안으로 제한한다.

---

## 4. 대상 파일 트리

```text
src/services/evidence_assembler/
  __init__.py
  config.py
  models.py
  text_idea_builder.py
  reroot_rules.py
  readiness.py
  token_budget.py
  repositories.py
  redis_streams.py
  service.py
  worker.py
  main.py

tests/
  unit/
    services/
      evidence_assembler/
        test_reroot_rules.py
        test_text_idea_builder.py
        test_readiness.py
        test_token_budget.py
  component/
    services/
      evidence_assembler/
        test_worker_rehydrates_trigger_event.py
        test_bundle_write_and_analysis_request_emit.py
        test_reroot_event_and_current_primary_update.py
        test_text_idea_snapshot_materialization.py
```

원칙:

- evidence assembly 관련 구현은 `src/services/evidence_assembler/` 아래로만 모은다.
- canonicalization 규칙은 **공유 계층 결과를 재사용**하며 여기서 새 canonicalizer를 만들지 않는다.
- primary 변경은 여기서만 일어나지만, **artifact identity를 새로 정의하지는 않는다.**

---

## 5. 이번 단계에서 고정할 구현 규칙

### 5-1. Redis payload는 계속 얇게 유지한다

`outbox-relay`가 Redis Streams에 싣는 메시지는 이미 최소 필드로 잠겨 있다.

- `job_id`
- `stage_name`
- `root_object_type`
- `root_object_id`
- `idempotency_key`
- `pipeline_run_id`
- `not_before`
- `trigger_event_id`

따라서 `evidence-assembler` consumer는 Redis 본문에서 business payload를 기대하면 안 된다.
**반드시 `trigger_event_id`로 `event_outbox`를 다시 조회해 bundle refresh request를 복원**해야 한다.

---

### 5-2. 입력 계약은 두 종류만 받는다

허용 입력:

- `artifact.snapshot.updated.v1`
- `candidate.bundle.refresh.v1`

기본 경로는 다음이다.

- source enricher가 snapshot append 후 `artifact.snapshot.updated.v1` emit
- assembler가 candidate context를 다시 읽고 bundle 재조립
- replay/manual/maintenance는 필요 시 `candidate.bundle.refresh.v1` emit 가능

즉, assembler는 **snapshot-driven refresh** 를 기본으로 한다.

---

### 5-3. bundle 조립 입력 원천은 Postgres current + history row다

조립 시 assembler가 읽는 기준은 아래다.

- `candidate_group_proposals`
- `candidate_group_members`
- `artifact_registry`
- `artifact_snapshots` current snapshot
- source-specific snapshot child tables
- `discovered_url_observations`
- 필요 시 `source_messages` / `source_message_versions`

중요:
- Redis는 queue일 뿐이다.
- event_outbox payload도 rehydration key일 뿐이다.
- 실제 bundle truth는 **PostgreSQL current + append-only history** 다.

---

### 5-4. `text_idea` snapshot 생성 조건은 좁게 둔다

assembler는 아무 때나 `text_idea` snapshot을 만들면 안 된다.

권장 조건:

1. candidate group current primary가 `text_idea`
   또는
2. external snapshot이 아직 하나도 usable하지 않지만,
   source message text surface가 있고 candidate는 유지 중일 때

생성 규칙:

- 부모 `artifact_snapshots` row append
  - `provider = local_text_idea`
  - `snapshot_type = text_idea`
  - `status = ready` 또는 `low_evidence`
- child `artifact_snapshot_text_idea` row append
- `hash_surface`, `display_surface`, `dev_context_signals_json` 저장

즉,
`text_idea`는 fallback이 아니라 **정상적인 evidence surface** 다.

---

### 5-5. reroot 규칙 v0.1은 “이미 candidate에 있는 artifact들 사이에서만” 허용한다

#### A. `github_subpath` / `github_repo_page` → `github_repo`

허용 조건:

- current primary ∈ {`github_subpath`, `github_repo_page`}
- candidate membership 안에 `github_repo` artifact가 이미 존재
- repo snapshot status ∈ {`ready`, `partial_ready`}

결론:
- repo로 reroot
- subpath/page는 supporting 유지

#### B. `x_post` → `github_repo`

허용 조건:

- current primary = `x_post`
- candidate membership 안에 `github_repo` 또는 repo-anchor member가 이미 존재
- GitHub snapshot status ∈ {`ready`, `partial_ready`}
- GitHub evidence strength가 X보다 구조적으로 강함
  - 예: README + manifest/tree + tests/examples 신호 존재

#### C. `web_article` → `github_repo`

허용 조건:

- current primary = `web_article`
- candidate membership 안에 `github_repo` 또는 repo-anchor member가 이미 존재
- GitHub snapshot status ∈ {`ready`, `partial_ready`}
- article snapshot보다 GitHub snapshot이 더 직접적인 분석 anchor

#### D. `text_idea`

유지 조건:

- 외부 usable snapshot이 없거나
- external snapshot이 모두 `failed_*`, `unsupported`, `low_evidence` 위주거나
- discovered observations는 있지만 candidate membership 안에 stronger artifact가 아직 없을 때

즉, reroot는 “GitHub면 무조건 승격”이 아니라
**already-known stronger anchor가 준비됐을 때만** 일어난다.

---

### 5-6. discovered observations는 bundle narrative에 반영하되 membership은 늘리지 않는다

이번 v0.1에서 `discovered_url_observations`는 아래 용도로만 쓴다.

- `discovered_links_summary_json`
- reroot 근거 보강
- evidence limitations 서술

하지 않는 것:

- 새 artifact 생성
- 새 candidate member 추가
- new supporting snapshot fetch 요청 생성

즉,
**observation은 observation으로만 남기고, candidate graph를 확장하지 않는다.**

---

### 5-7. evidence limitations는 source별 limitation을 deterministic하게 합친다

집계 원천:

- primary snapshot `evidence_limitations`
- supporting snapshot `evidence_limitations`
- status 기반 synthetic limitation
  - `partial_ready`
  - `low_evidence`
  - `rate_limited`
  - `access_denied`
  - `unsupported`
- text_idea synthetic limitation
  - `external_artifact_not_confirmed`

합성 규칙:

- dedupe 후 stable order 유지
- primary limitation 우선
- supporting limitation 뒤에 부착
- discovered observation은 limitation이 아니라 summary로 우선 보냄

---

### 5-8. token budget profile은 assembler가 deterministic하게 만든다

권장 profile:

- `small`
- `medium`
- `large`

권장 산정 규칙:

- GitHub primary
  - README + manifest + tree summary 중심이면 `small`
  - tests/CI/examples 일부 포함이면 `medium`
  - release summary + docs excerpt까지 포함이면 `large`
- X primary
  - root post만이면 `small`
  - referenced post/author/media 요약 포함이면 `medium`
- Web primary
  - title/description/excerpt + outbound links summary면 `small`
  - long excerpt + rich outbound links면 `medium`
- multi-source supporting 조합이 많으면 `large`

중요:
- assembler는 실제 prompt를 만들지 않는다.
- 단지 **judge가 사용할 bundle size profile** 을 미리 고정한다.

---

### 5-9. `ready_for_analysis` 규칙은 보수적으로 둔다

기본 ready 조건:

1. current primary artifact 결정됨
2. current primary snapshot 존재
3. primary snapshot status ∈ {`ready`, `partial_ready`, `low_evidence`}
4. evidence limitations 집계 완료
5. token budget profile 산출 완료

즉,
`low_evidence` 여도 bundle 자체는 만들 수 있다.
다만 그 약함을 숨기지 않는다.

반대로 아래는 not-ready:

- primary snapshot이 전혀 없음
- candidate group membership/current primary가 깨짐
- bundle input을 구성할 최소 row가 부족함

이 경우 `analysis.requested.v1`는 emit하지 않는다.

---

### 5-10. bundle은 append-only + idempotent input hash로 관리한다

권장 bundle idempotency key:

```text
bundle:{candidate_group_id}:{bundle_profile_version}:{bundle_input_hash}
```

`bundle_input_hash` 구성 예시:

- candidate_group_id
- current_primary_artifact_id
- supporting artifact ids stable list
- snapshot ids stable list
- reroot_count
- text_idea snapshot id (있을 때)
- discovered observations digest

규칙:

- 같은 input hash면 새 bundle row append 생략 가능
- 하지만 `analysis.requested.v1` 재emit 여부는 trigger 종류에 따라 분리 가능
- overwrite는 금지

---

### 5-11. current pointer update 순서는 state-first로 고정한다

권장 순서:

1. 필요 시 `candidate_reroot_events` append
2. 필요 시 `candidate_group_proposals.current_primary_artifact_id` update
3. 새 `candidate_evidence_bundles` row append
4. 새 `candidate_evidence_members` append
5. `candidate_group_proposals.current_bundle_id` update
6. `ready_for_analysis == true`이면 `analysis.requested.v1` outbox insert
7. commit 후 stream ack

즉,
**history row → current pointer → outbox** 순서다.

---

### 5-12. output event는 `analysis.requested.v1` 하나로 닫는다

emit 조건:

- 새 bundle row가 append됐고
- `ready_for_analysis = true`

payload 최소 필드:

- `candidate_group_id`
- `bundle_id`
- `judge_profile`
- `escalation_allowed`

`judge_profile` v0.1 결정 규칙:

- primary ∈ {`github_repo`, `github_subpath`, `github_repo_page`, `github_gist`} → `github_primary`
- primary = `x_post` → `x_primary`
- primary ∈ {`web_article`, `text_idea`} → `idea_primary`

중요:
- 실제 model 선택은 다음 단계 `analysis-router` 책임이다.
- assembler는 **profile hint** 만 준다.

---

## 6. 코드 초안

## 6-1. `src/services/evidence_assembler/__init__.py`

```python
from .config import EvidenceAssemblerConfig
from .service import EvidenceAssemblerService
from .worker import EvidenceAssemblerWorker

__all__ = [
    "EvidenceAssemblerConfig",
    "EvidenceAssemblerService",
    "EvidenceAssemblerWorker",
]
```

---

## 6-2. `src/services/evidence_assembler/config.py`

```python
from __future__ import annotations

import os
from dataclasses import dataclass


class EvidenceAssemblerConfigurationError(ValueError):
    pass


@dataclass(slots=True, frozen=True)
class EvidenceAssemblerConfig:
    app_env: str
    database_url: str
    redis_url: str
    queue_name: str
    consumer_group: str
    consumer_name: str
    batch_size: int
    block_ms: int
    bundle_profile_version: str
    enable_text_idea: bool
    enable_reroot: bool
    log_level: str

    @classmethod
    def from_env(cls) -> "EvidenceAssemblerConfig":
        database_url = os.getenv("DATABASE_URL", "").strip()
        redis_url = os.getenv("REDIS_URL", "").strip()
        if not database_url:
            raise EvidenceAssemblerConfigurationError("DATABASE_URL is required")
        if not redis_url:
            raise EvidenceAssemblerConfigurationError("REDIS_URL is required")

        cfg = cls(
            app_env=os.getenv("APP_ENV", "dev").strip().lower(),
            database_url=database_url,
            redis_url=redis_url,
            queue_name=os.getenv("EVIDENCE_ASSEMBLER_QUEUE_NAME", "q.candidate.bundle").strip(),
            consumer_group=os.getenv("EVIDENCE_ASSEMBLER_CONSUMER_GROUP", "evidence-assembler").strip(),
            consumer_name=os.getenv("EVIDENCE_ASSEMBLER_CONSUMER_NAME", "evidence-assembler-1").strip(),
            batch_size=int(os.getenv("EVIDENCE_ASSEMBLER_BATCH_SIZE", "20")),
            block_ms=int(os.getenv("EVIDENCE_ASSEMBLER_BLOCK_MS", "5000")),
            bundle_profile_version=os.getenv("EVIDENCE_ASSEMBLER_BUNDLE_PROFILE_VERSION", "bundle_profile_v1").strip(),
            enable_text_idea=os.getenv("ENABLE_TEXT_IDEA", "true").strip().lower() not in {"0", "false", "no"},
            enable_reroot=os.getenv("EVIDENCE_ASSEMBLER_ENABLE_REROOT", "true").strip().lower() not in {"0", "false", "no"},
            log_level=os.getenv("LOG_LEVEL", "INFO").strip().upper(),
        )
        cfg.validate()
        return cfg

    def validate(self) -> None:
        if self.batch_size <= 0 or self.batch_size > 100:
            raise EvidenceAssemblerConfigurationError("EVIDENCE_ASSEMBLER_BATCH_SIZE must be between 1 and 100")
        if self.block_ms <= 0:
            raise EvidenceAssemblerConfigurationError("EVIDENCE_ASSEMBLER_BLOCK_MS must be > 0")
        if not self.queue_name:
            raise EvidenceAssemblerConfigurationError("EVIDENCE_ASSEMBLER_QUEUE_NAME must not be empty")
        if not self.consumer_group:
            raise EvidenceAssemblerConfigurationError("EVIDENCE_ASSEMBLER_CONSUMER_GROUP must not be empty")
        if not self.consumer_name:
            raise EvidenceAssemblerConfigurationError("EVIDENCE_ASSEMBLER_CONSUMER_NAME must not be empty")
        if not self.bundle_profile_version:
            raise EvidenceAssemblerConfigurationError("EVIDENCE_ASSEMBLER_BUNDLE_PROFILE_VERSION must not be empty")
```

---

## 6-3. `src/services/evidence_assembler/models.py`

```python
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any


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

## 6-4. `src/services/evidence_assembler/text_idea_builder.py`

```python
from __future__ import annotations

import hashlib
import re
import unicodedata
from dataclasses import asdict
from typing import Any

from .models import TextIdeaSnapshotDraft


_DEV_KEYWORDS = (
    "github",
    "repo",
    "tool",
    "workflow",
    "agent",
    "prompt",
    "vibe coding",
    "automation",
    "dev",
    "code",
)


class TextIdeaBuilder:
    def build(
        self,
        *,
        artifact_id: str,
        source_message_id: str,
        source_version_no: int,
        text_surface: str | None,
    ) -> TextIdeaSnapshotDraft | None:
        display_surface = self._normalize_display(text_surface)
        if not display_surface:
            return None

        hash_surface = self._normalize_hash(display_surface)
        signals = {
            "keyword_hits": [kw for kw in _DEV_KEYWORDS if kw in display_surface.lower()],
            "has_code_fence": "```" in display_surface,
            "has_url": "http://" in display_surface or "https://" in display_surface,
            "length_chars": len(display_surface),
        }
        limitations: list[str] = []
        if not signals["keyword_hits"]:
            limitations.append("weak_dev_context")
        if signals["length_chars"] < 40:
            limitations.append("short_text_surface")

        return TextIdeaSnapshotDraft(
            artifact_id=artifact_id,
            source_message_id=source_message_id,
            source_version_no=source_version_no,
            hash_surface=hash_surface,
            display_surface=display_surface,
            dev_context_signals_json=signals,
            status="low_evidence" if limitations else "ready",
            evidence_limitations=limitations,
        )

    @staticmethod
    def input_hash(draft: TextIdeaSnapshotDraft) -> str:
        payload = asdict(draft)
        return hashlib.sha256(str(payload).encode("utf-8")).hexdigest()

    @staticmethod
    def _normalize_display(value: str | None) -> str | None:
        if not value:
            return None
        v = unicodedata.normalize("NFKC", value)
        v = v.replace("\r\n", "\n").replace("\r", "\n").strip()
        return v or None

    @staticmethod
    def _normalize_hash(value: str) -> str:
        v = unicodedata.normalize("NFKC", value)
        v = re.sub(r"\s+", " ", v).strip()
        return hashlib.sha256(v.encode("utf-8")).hexdigest()
```

---

## 6-5. `src/services/evidence_assembler/reroot_rules.py`

```python
from __future__ import annotations

from .models import CandidateMemberRecord, RerootDecision, SnapshotRecord


class RerootRules:
    READY_STATES = {"ready", "partial_ready"}

    def decide(
        self,
        *,
        current_primary_artifact_id: str,
        artifact_types: dict[str, str],
        current_snapshots: dict[str, SnapshotRecord],
    ) -> RerootDecision:
        current_type = artifact_types.get(current_primary_artifact_id)
        if current_type is None:
            return RerootDecision(False, current_primary_artifact_id, current_primary_artifact_id, None)

        repo_candidates = [
            artifact_id
            for artifact_id, artifact_type in artifact_types.items()
            if artifact_type == "github_repo" and artifact_id in current_snapshots and current_snapshots[artifact_id].status in self.READY_STATES
        ]
        if not repo_candidates:
            return RerootDecision(False, current_primary_artifact_id, current_primary_artifact_id, None)

        chosen_repo = sorted(repo_candidates)[0]

        if current_type in {"github_subpath", "github_repo_page"} and chosen_repo != current_primary_artifact_id:
            return RerootDecision(True, current_primary_artifact_id, chosen_repo, "reroot_repo_anchor")

        if current_type in {"x_post", "web_article"} and chosen_repo != current_primary_artifact_id:
            return RerootDecision(True, current_primary_artifact_id, chosen_repo, f"reroot_{current_type}_to_repo")

        return RerootDecision(False, current_primary_artifact_id, current_primary_artifact_id, None)
```

---

## 6-6. `src/services/evidence_assembler/readiness.py`

```python
from __future__ import annotations

from .models import SnapshotRecord


class ReadinessEvaluator:
    READY_STATES = {"ready", "partial_ready", "low_evidence"}

    def is_ready_for_analysis(
        self,
        *,
        primary_snapshot: SnapshotRecord | None,
        evidence_limitations: list[str],
        token_budget_profile: str | None,
    ) -> bool:
        if primary_snapshot is None:
            return False
        if primary_snapshot.status not in self.READY_STATES:
            return False
        if not token_budget_profile:
            return False
        return True
```

---

## 6-7. `src/services/evidence_assembler/token_budget.py`

```python
from __future__ import annotations

from .models import SnapshotRecord


class TokenBudgetProfiler:
    def choose(
        self,
        *,
        primary_snapshot: SnapshotRecord,
        supporting_snapshot_count: int,
        discovered_links_count: int,
    ) -> str:
        snapshot_type = primary_snapshot.snapshot_type

        if snapshot_type == "github_repo":
            if supporting_snapshot_count >= 3 or discovered_links_count >= 6:
                return "large"
            if supporting_snapshot_count >= 1:
                return "medium"
            return "small"

        if snapshot_type == "x_post":
            if supporting_snapshot_count >= 2:
                return "medium"
            return "small"

        if snapshot_type in {"web_article", "text_idea"}:
            if supporting_snapshot_count >= 2 or discovered_links_count >= 4:
                return "medium"
            return "small"

        return "small"
```

---

## 6-8. `src/services/evidence_assembler/repositories.py`

```python
from __future__ import annotations

from datetime import datetime
from typing import Any, Iterable

import sqlalchemy as sa
from sqlalchemy.ext.asyncio import AsyncSession

from .models import (
    BundleTriggerEnvelope,
    BundleMemberDraft,
    CandidateGroupRecord,
    CandidateMemberRecord,
    DiscoveredLinkSummary,
    EvidenceBundleDraft,
    SnapshotRecord,
    TextIdeaSnapshotDraft,
)


class EvidenceAssemblerRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def load_trigger_envelope(self, trigger_event_id: str) -> BundleTriggerEnvelope | None:
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
        return BundleTriggerEnvelope(
            event_id=str(row["event_id"]),
            event_type=str(row["event_type"]),
            candidate_group_id=str(payload.get("candidate_group_id") or payload.get("root_object_id") or ""),
            trigger_object_type=payload.get("trigger_object_type"),
            trigger_object_id=payload.get("trigger_object_id"),
            snapshot_id=payload.get("snapshot_id"),
            occurred_at=None,
        )

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
        return CandidateGroupRecord(**{k: (str(v) if k.endswith("_id") and v is not None else v) for k, v in row.items()})

    async def load_candidate_members(self, candidate_group_id: str) -> list[CandidateMemberRecord]:
        result = await self._session.execute(
            sa.text(
                """
                SELECT cgm.artifact_id, ar.artifact_type, cgm.member_role, cgm.member_order
                FROM candidate_group_members cgm
                JOIN artifact_registry ar ON ar.artifact_id = cgm.artifact_id
                WHERE cgm.candidate_group_id = CAST(:candidate_group_id AS uuid)
                ORDER BY cgm.member_role, cgm.member_order NULLS LAST, cgm.created_at
                """
            ),
            {"candidate_group_id": candidate_group_id},
        )
        rows = result.mappings().all()
        return [CandidateMemberRecord(artifact_id=str(r["artifact_id"]), artifact_type=str(r["artifact_type"]), member_role=str(r["member_role"]), member_order=r["member_order"]) for r in rows]

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

    async def load_discovered_links(self, candidate_group_id: str) -> list[DiscoveredLinkSummary]:
        result = await self._session.execute(
            sa.text(
                """
                SELECT observed_url, context_path, discovery_reason, parent_artifact_id, parent_snapshot_id
                FROM discovered_url_observations
                WHERE parent_candidate_group_id = CAST(:candidate_group_id AS uuid)
                ORDER BY created_at
                """
            ),
            {"candidate_group_id": candidate_group_id},
        )
        return [
            DiscoveredLinkSummary(
                observed_url=str(r["observed_url"]),
                context_path=r["context_path"],
                discovery_reason=str(r["discovery_reason"]),
                parent_artifact_id=str(r["parent_artifact_id"]),
                parent_snapshot_id=str(r["parent_snapshot_id"]) if r["parent_snapshot_id"] is not None else None,
            )
            for r in result.mappings().all()
        ]

    async def load_source_text_surface(self, source_message_id: str, source_version_no: int) -> str | None:
        version = await self._session.execute(
            sa.text(
                """
                SELECT text_surface
                FROM source_message_versions
                WHERE source_message_id = CAST(:source_message_id AS uuid)
                  AND version_no = :version_no
                """
            ),
            {"source_message_id": source_message_id, "version_no": source_version_no},
        )
        row = version.mappings().first()
        if row and row["text_surface"]:
            return str(row["text_surface"])

        current = await self._session.execute(
            sa.text(
                """
                SELECT text_surface FROM source_messages
                WHERE source_message_id = CAST(:source_message_id AS uuid)
                """
            ),
            {"source_message_id": source_message_id},
        )
        crow = current.mappings().first()
        return str(crow["text_surface"]) if crow and crow["text_surface"] else None

    async def append_text_idea_snapshot(self, draft: TextIdeaSnapshotDraft) -> tuple[str, str]:
        parent = await self._session.execute(
            sa.text(
                """
                INSERT INTO artifact_snapshots (
                    artifact_id, provider, snapshot_type, status, fetched_at,
                    content_anchor, auth_mode, normalized_projection,
                    raw_payload_ref, evidence_limitations, fetch_anomalies
                ) VALUES (
                    CAST(:artifact_id AS uuid), 'local_text_idea', 'text_idea', :status, now(),
                    :content_anchor, 'local', CAST(:normalized_projection AS jsonb),
                    NULL, CAST(:evidence_limitations AS jsonb), CAST(:fetch_anomalies AS jsonb)
                ) RETURNING snapshot_id
                """
            ),
            {
                "artifact_id": draft.artifact_id,
                "status": draft.status,
                "content_anchor": draft.hash_surface,
                "normalized_projection": sa.text("NULL") if False else None,
                "evidence_limitations": draft.evidence_limitations,
                "fetch_anomalies": [],
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
                    CAST(:snapshot_id AS uuid), CAST(:source_message_id AS uuid), :source_version_no,
                    :hash_surface, :display_surface, CAST(:dev_context_signals_json AS jsonb)
                )
                """
            ),
            {
                "snapshot_id": snapshot_id,
                "source_message_id": draft.source_message_id,
                "source_version_no": draft.source_version_no,
                "hash_surface": draft.hash_surface,
                "display_surface": draft.display_surface,
                "dev_context_signals_json": draft.dev_context_signals_json,
            },
        )
        return snapshot_id, draft.status

    async def append_reroot_event(self, *, candidate_group_id: str, from_artifact_id: str, to_artifact_id: str, reason_code: str) -> None:
        await self._session.execute(
            sa.text(
                """
                INSERT INTO candidate_reroot_events (
                    candidate_group_id, from_artifact_id, to_artifact_id, reason_code, trigger_snapshot_id, created_at
                ) VALUES (
                    CAST(:candidate_group_id AS uuid), CAST(:from_artifact_id AS uuid), CAST(:to_artifact_id AS uuid),
                    :reason_code, NULL, now()
                )
                """
            ),
            {
                "candidate_group_id": candidate_group_id,
                "from_artifact_id": from_artifact_id,
                "to_artifact_id": to_artifact_id,
                "reason_code": reason_code,
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

    async def append_bundle(self, draft: EvidenceBundleDraft) -> str:
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
                    1,
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
                ) RETURNING bundle_id
                """
            ),
            {
                "candidate_group_id": draft.candidate_group_id,
                "initial_primary_artifact_id": draft.initial_primary_artifact_id,
                "current_primary_artifact_id": draft.current_primary_artifact_id,
                "bundle_profile_version": draft.bundle_profile_version,
                "bundle_input_hash": draft.bundle_input_hash,
                "reroot_count": draft.reroot_count,
                "primary_summary": draft.primary_summary,
                "supporting_summaries_json": draft.supporting_summaries_json,
                "discovered_links_summary_json": draft.discovered_links_summary_json,
                "evidence_limitations": draft.evidence_limitations,
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
                        bundle_id, artifact_id, snapshot_id, member_role, member_order
                    ) VALUES (
                        CAST(:bundle_id AS uuid), CAST(:artifact_id AS uuid), CAST(:snapshot_id AS uuid),
                        :member_role, :member_order
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

    async def insert_analysis_requested_outbox(self, *, candidate_group_id: str, bundle_id: str, judge_profile: str) -> None:
        await self._session.execute(
            sa.text(
                """
                INSERT INTO event_outbox (
                    event_type, aggregate_type, aggregate_id, dedupe_key,
                    payload_json, status, created_at
                ) VALUES (
                    'analysis.requested.v1', 'candidate_group', CAST(:candidate_group_id AS uuid),
                    :dedupe_key,
                    CAST(:payload_json AS jsonb), 'pending', now()
                )
                ON CONFLICT (dedupe_key) DO NOTHING
                """
            ),
            {
                "candidate_group_id": candidate_group_id,
                "dedupe_key": f"analysis-request:{candidate_group_id}:{bundle_id}",
                "payload_json": {
                    "candidate_group_id": candidate_group_id,
                    "bundle_id": bundle_id,
                    "judge_profile": judge_profile,
                    "escalation_allowed": True,
                },
            },
        )
```

---

## 6-9. `src/services/evidence_assembler/redis_streams.py`

```python
from __future__ import annotations

from dataclasses import dataclass

from redis.asyncio import Redis


@dataclass(slots=True, frozen=True)
class StreamMessage:
    stream: str
    message_id: str
    fields: dict[str, str]


class RedisStreamConsumer:
    def __init__(self, client: Redis, *, queue_name: str, consumer_group: str, consumer_name: str, block_ms: int, batch_size: int) -> None:
        self._client = client
        self._queue_name = queue_name
        self._consumer_group = consumer_group
        self._consumer_name = consumer_name
        self._block_ms = block_ms
        self._batch_size = batch_size

    async def ensure_group(self) -> None:
        try:
            await self._client.xgroup_create(name=self._queue_name, groupname=self._consumer_group, id="0", mkstream=True)
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
        out: list[StreamMessage] = []
        for stream_name, entries in payload or []:
            s = stream_name.decode() if isinstance(stream_name, bytes) else str(stream_name)
            for message_id, fields in entries:
                mid = message_id.decode() if isinstance(message_id, bytes) else str(message_id)
                decoded = {
                    (k.decode() if isinstance(k, bytes) else str(k)): (v.decode() if isinstance(v, bytes) else str(v))
                    for k, v in fields.items()
                }
                out.append(StreamMessage(stream=s, message_id=mid, fields=decoded))
        return out

    async def ack(self, message_id: str) -> None:
        await self._client.xack(self._queue_name, self._consumer_group, message_id)
```

---

## 6-10. `src/services/evidence_assembler/service.py`

```python
from __future__ import annotations

import hashlib
import json
import logging

from .config import EvidenceAssemblerConfig
from .models import BundleMemberDraft, EvidenceBundleDraft
from .readiness import ReadinessEvaluator
from .reroot_rules import RerootRules
from .text_idea_builder import TextIdeaBuilder
from .token_budget import TokenBudgetProfiler


class EvidenceAssemblerService:
    def __init__(
        self,
        config: EvidenceAssemblerConfig,
        *,
        repository,
        text_idea_builder: TextIdeaBuilder | None = None,
        reroot_rules: RerootRules | None = None,
        readiness: ReadinessEvaluator | None = None,
        token_budget: TokenBudgetProfiler | None = None,
        logger: logging.Logger | None = None,
    ) -> None:
        self._config = config
        self._repository = repository
        self._text_idea_builder = text_idea_builder or TextIdeaBuilder()
        self._reroot_rules = reroot_rules or RerootRules()
        self._readiness = readiness or ReadinessEvaluator()
        self._token_budget = token_budget or TokenBudgetProfiler()
        self._logger = logger or logging.getLogger(__name__)

    async def rehydrate_trigger(self, trigger_event_id: str):
        return await self._repository.load_trigger_envelope(trigger_event_id)

    async def handle_trigger(self, envelope) -> str | None:
        candidate = await self._repository.load_candidate_group(envelope.candidate_group_id)
        if candidate is None:
            return None

        members = await self._repository.load_candidate_members(candidate.candidate_group_id)
        artifact_ids = [m.artifact_id for m in members]
        snapshots = await self._repository.load_current_snapshots(artifact_ids)
        discovered = await self._repository.load_discovered_links(candidate.candidate_group_id)

        artifact_types = {m.artifact_id: m.artifact_type for m in members}

        # optional text_idea materialization
        if self._config.enable_text_idea and candidate.current_primary_artifact_id in artifact_types and artifact_types[candidate.current_primary_artifact_id] == "text_idea":
            if candidate.current_primary_artifact_id not in snapshots:
                text_surface = await self._repository.load_source_text_surface(candidate.source_message_id, candidate.source_version_no)
                draft = self._text_idea_builder.build(
                    artifact_id=candidate.current_primary_artifact_id,
                    source_message_id=candidate.source_message_id,
                    source_version_no=candidate.source_version_no,
                    text_surface=text_surface,
                )
                if draft is not None:
                    snapshot_id, _status = await self._repository.append_text_idea_snapshot(draft)
                    snapshots = await self._repository.load_current_snapshots(artifact_ids)

        reroot = self._reroot_rules.decide(
            current_primary_artifact_id=candidate.current_primary_artifact_id,
            artifact_types=artifact_types,
            current_snapshots=snapshots,
        ) if self._config.enable_reroot else None

        current_primary_artifact_id = candidate.current_primary_artifact_id
        reroot_count = 0
        if reroot and reroot.changed:
            await self._repository.append_reroot_event(
                candidate_group_id=candidate.candidate_group_id,
                from_artifact_id=reroot.from_artifact_id,
                to_artifact_id=reroot.to_artifact_id,
                reason_code=reroot.reason_code or "reroot",
            )
            await self._repository.update_current_primary(
                candidate_group_id=candidate.candidate_group_id,
                artifact_id=reroot.to_artifact_id,
            )
            current_primary_artifact_id = reroot.to_artifact_id
            reroot_count = 1

        primary_snapshot = snapshots.get(current_primary_artifact_id)
        if primary_snapshot is None:
            return None

        supporting_members = [m for m in members if m.artifact_id != current_primary_artifact_id and m.artifact_id in snapshots]
        supporting_members = sorted(supporting_members, key=lambda m: (m.member_role, m.member_order or 999999, m.artifact_id))

        limitations: list[str] = []
        limitations.extend(primary_snapshot.evidence_limitations or [])
        for member in supporting_members:
            limitations.extend(snapshots[member.artifact_id].evidence_limitations or [])
        limitations = list(dict.fromkeys([x for x in limitations if x]))

        token_profile = self._token_budget.choose(
            primary_snapshot=primary_snapshot,
            supporting_snapshot_count=len(supporting_members),
            discovered_links_count=len(discovered),
        )
        ready = self._readiness.is_ready_for_analysis(
            primary_snapshot=primary_snapshot,
            evidence_limitations=limitations,
            token_budget_profile=token_profile,
        )

        discovered_summary = [
            {
                "observed_url": d.observed_url,
                "context_path": d.context_path,
                "discovery_reason": d.discovery_reason,
                "parent_artifact_id": d.parent_artifact_id,
                "parent_snapshot_id": d.parent_snapshot_id,
            }
            for d in discovered
        ]

        bundle_members = [
            BundleMemberDraft(
                artifact_id=current_primary_artifact_id,
                snapshot_id=primary_snapshot.snapshot_id,
                member_role="primary",
                member_order=0,
            )
        ]
        bundle_members.extend(
            [
                BundleMemberDraft(
                    artifact_id=m.artifact_id,
                    snapshot_id=snapshots[m.artifact_id].snapshot_id,
                    member_role="supporting",
                    member_order=i + 1,
                )
                for i, m in enumerate(supporting_members)
            ]
        )

        bundle_input_hash = hashlib.sha256(
            json.dumps(
                {
                    "candidate_group_id": candidate.candidate_group_id,
                    "current_primary_artifact_id": current_primary_artifact_id,
                    "snapshot_ids": [bm.snapshot_id for bm in bundle_members],
                    "reroot_count": reroot_count,
                    "discovered": discovered_summary,
                    "bundle_profile_version": self._config.bundle_profile_version,
                },
                ensure_ascii=False,
                sort_keys=True,
            ).encode("utf-8")
        ).hexdigest()

        judge_profile = self._choose_judge_profile(primary_snapshot.snapshot_type)

        bundle = EvidenceBundleDraft(
            candidate_group_id=candidate.candidate_group_id,
            initial_primary_artifact_id=candidate.initial_primary_artifact_id,
            current_primary_artifact_id=current_primary_artifact_id,
            bundle_profile_version=self._config.bundle_profile_version,
            bundle_input_hash=bundle_input_hash,
            reroot_count=reroot_count,
            primary_summary={
                "artifact_id": current_primary_artifact_id,
                "snapshot_id": primary_snapshot.snapshot_id,
                "snapshot_type": primary_snapshot.snapshot_type,
                "status": primary_snapshot.status,
                "content_anchor": primary_snapshot.content_anchor,
                "normalized_projection": primary_snapshot.normalized_projection,
            },
            supporting_summaries_json=[
                {
                    "artifact_id": m.artifact_id,
                    "snapshot_id": snapshots[m.artifact_id].snapshot_id,
                    "snapshot_type": snapshots[m.artifact_id].snapshot_type,
                    "status": snapshots[m.artifact_id].status,
                }
                for m in supporting_members
            ],
            discovered_links_summary_json=discovered_summary,
            evidence_limitations=limitations,
            ready_for_analysis=ready,
            token_budget_profile=token_profile,
            members=bundle_members,
            judge_profile=judge_profile,
        )

        bundle_id = await self._repository.append_bundle(bundle)
        await self._repository.update_current_bundle(candidate_group_id=candidate.candidate_group_id, bundle_id=bundle_id)
        if bundle.ready_for_analysis and bundle.judge_profile is not None:
            await self._repository.insert_analysis_requested_outbox(
                candidate_group_id=candidate.candidate_group_id,
                bundle_id=bundle_id,
                judge_profile=bundle.judge_profile,
            )
        return bundle_id

    @staticmethod
    def _choose_judge_profile(snapshot_type: str) -> str:
        if snapshot_type in {"github_repo", "github_subpath", "github_repo_page", "github_gist"}:
            return "github_primary"
        if snapshot_type == "x_post":
            return "x_primary"
        return "idea_primary"
```

---

## 6-11. `src/services/evidence_assembler/worker.py`

```python
from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass

from .config import EvidenceAssemblerConfig
from .redis_streams import RedisStreamConsumer, StreamMessage
from .service import EvidenceAssemblerService


@dataclass(slots=True, frozen=True)
class WorkerBatchResult:
    processed: int = 0
    acked: int = 0
    skipped: int = 0


class EvidenceAssemblerWorker:
    def __init__(
        self,
        config: EvidenceAssemblerConfig,
        *,
        consumer: RedisStreamConsumer,
        service: EvidenceAssemblerService,
        logger: logging.Logger | None = None,
    ) -> None:
        self._config = config
        self._consumer = consumer
        self._service = service
        self._logger = logger or logging.getLogger(__name__)
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
        skipped = 0
        for message in messages:
            processed += 1
            ack_now = await self._process(message)
            if ack_now:
                await self._consumer.ack(message.message_id)
                acked += 1
            else:
                skipped += 1
        return WorkerBatchResult(processed=processed, acked=acked, skipped=skipped)

    async def _process(self, message: StreamMessage) -> bool:
        trigger_event_id = message.fields.get("trigger_event_id")
        if not trigger_event_id:
            self._logger.error("evidence_assembler_missing_trigger_event_id", extra={"stream_message_id": message.message_id})
            return True

        envelope = await self._service.rehydrate_trigger(trigger_event_id)
        if envelope is None or not envelope.candidate_group_id:
            self._logger.warning("evidence_assembler_missing_trigger_row", extra={"trigger_event_id": trigger_event_id})
            return True

        await self._service.handle_trigger(envelope)
        return True
```

---

## 6-12. `src/services/evidence_assembler/main.py`

```python
from __future__ import annotations

import asyncio
import logging

from redis.asyncio import Redis
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from .config import EvidenceAssemblerConfig
from .redis_streams import RedisStreamConsumer
from .repositories import EvidenceAssemblerRepository
from .service import EvidenceAssemblerService
from .worker import EvidenceAssemblerWorker


async def _amain() -> None:
    config = EvidenceAssemblerConfig.from_env()
    logging.basicConfig(level=getattr(logging, config.log_level, logging.INFO))

    engine = create_async_engine(config.database_url, future=True)
    session_factory = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    redis = Redis.from_url(config.redis_url, decode_responses=False)

    async with session_factory() as session:
        repository = EvidenceAssemblerRepository(session)
        service = EvidenceAssemblerService(config, repository=repository)
        consumer = RedisStreamConsumer(
            redis,
            queue_name=config.queue_name,
            consumer_group=config.consumer_group,
            consumer_name=config.consumer_name,
            block_ms=config.block_ms,
            batch_size=config.batch_size,
        )
        worker = EvidenceAssemblerWorker(config, consumer=consumer, service=service)
        await worker.run_forever()


def main() -> None:
    asyncio.run(_amain())


if __name__ == "__main__":
    main()
```

---

## 7. 이번 단계가 구조를 지키는 이유

이 문서는 아래 경계를 유지한다.

- `router-normalizer`는 candidate proposal까지만
- `gh-enricher`, `x-enricher`, `web-enricher`는 snapshot + discovered observations까지만
- `evidence-assembler`만 reroot와 bundle을 수행
- `analysis-router` 이후 judge/policy/notifier는 아직 건드리지 않음

특히 중요한 점은 세 가지다.

1. **새 artifact를 만들지 않았다.**
   즉, assembler가 artifact identity 계층을 다시 침범하지 않는다.

2. **history는 append-only로 남겼다.**
   즉, reroot는 `candidate_reroot_events`, bundle은 `candidate_evidence_bundles`에 남고 current pointer만 갱신한다.

3. **analysis handoff도 thin payload로만 보냈다.**
   즉, `analysis.requested.v1`는 다음 단계가 PostgreSQL을 다시 읽도록 만드는 rehydration key일 뿐이다.

이 세 규칙이 stage 5와 stage 6 사이의 경계를 가장 잘 지킨다.

---

## 8. 다음 단계 예상

이 단계가 끝나면 다음 구현 순서는 아래가 가장 자연스럽다.

1. `33_evidence_assembler_integration_hardening_v0_1.md`
   - current bundle reuse
   - duplicate trigger idempotency
   - reroot edge-case hardening
   - discovered observation handling 정밀화

2. `34_analysis_router_skeleton_and_code_draft_v0_1.md`
   - `analysis.requested.v1` consumer
   - judge profile/model 선택
   - escalation 여부 결정

즉, evidence-assembler는 **이번 32단계 한 번으로 구조 초안은 닫히지만, operational hardening까지 보수적으로 보려면 한 단계 더 갈 가능성이 높다.**

---

## 최종 한 줄 결론

이번 단계의 최소-change 정답은 **`artifact.snapshot.updated.v1` / `candidate.bundle.refresh.v1` thin payload를 기준으로 `q.candidate.bundle`를 소비하고, current candidate membership + current snapshots + discovered observations + optional text_idea snapshot을 조합해 append-only `candidate_evidence_bundles`와 `candidate_evidence_members`를 기록하고, 필요 시 reroot lineage를 남긴 뒤, `ready_for_analysis`일 때만 `analysis.requested.v1`를 emit하는 좁은 `evidence-assembler` worker를 고정하는 것**이다.
