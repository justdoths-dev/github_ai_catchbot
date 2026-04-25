# 05 migration code drafts stage13 stage16 bundle v0 1
이 문서는 프로젝트 소스 파일 수 제한(40개)을 피하기 위한 **통합 번들**이다.
구조 변경 문서가 아니며, 아래 원본 문서를 **순서와 내용을 보존한 채** 묶는다.

## 포함 원본 파일
- `13_response_0001_ingest_core_draft.md`
- `14_response_0002_normalization_candidates_draft.md`
- `15_response_0003_enrichment_bundles_draft.md`
- `16_response_0004_judge_delivery_observability_draft.md`

## 통합 원칙
- 아키텍처 불변식은 유지한다.
- 원본 파일명은 아래 섹션 제목으로 보존한다.
- 충돌 시 해석 우선순위는 기존과 동일하게 정본 단계 문서 → 실행 계약 → migration 정본 → 구현 초안이다.


---

## Source file: `13_response_0001_ingest_core_draft.md`

소스 문서 재확인했고, 지금 턴은 **12단계 문서에서 지정한 그대로 `0001_ingest_core`를 실제 Alembic migration 초안으로 내리는 단계**다. 이 순서는 이미 고정되어 있고, collector는 어디까지나 **원문/버전/outbox 저장까지만** 책임진다. 즉, 지금은 로직이 아니라 **durable ingest 경계**를 코드로 고정하는 턴이다. fileciteturn15file3 fileciteturn15file2 fileciteturn15file15

# `0001_ingest_core` Alembic 초안

파일 경로 권장:

```text
migrations/versions/0001_ingest_core.py
```

코드는 바로 붙여 넣을 수 있는 형태로 준다.

```python
"""0001_ingest_core

Revision ID: 0001_ingest_core
Revises: None
Create Date: 2026-04-13 00:00:00

"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


# Revision identifiers, used by Alembic.
revision = "0001_ingest_core"
down_revision = None
branch_labels = None
depends_on = None


def _pg_enum(name: str, *values: str) -> postgresql.ENUM:
    return postgresql.ENUM(*values, name=name, create_type=False)


artifact_type_enum = _pg_enum(
    "artifact_type_enum",
    "github_repo",
    "github_subpath",
    "github_gist",
    "github_repo_page",
    "x_post",
    "web_article",
    "text_idea",
    "unknown_link",
    "short_url_unresolved",
)

verdict_enum = _pg_enum(
    "verdict_enum",
    "inspect_now",
    "later",
    "skip",
)

delivery_decision_enum = _pg_enum(
    "delivery_decision_enum",
    "send_now",
    "send_digest",
    "suppress",
)

urgency_profile_enum = _pg_enum(
    "urgency_profile_enum",
    "high",
    "normal_silent",
    "digest",
    "suppressed",
)

outbox_status_enum = _pg_enum(
    "outbox_status_enum",
    "pending",
    "published",
    "failed",
)

snapshot_status_enum = _pg_enum(
    "snapshot_status_enum",
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
)

notification_status_enum = _pg_enum(
    "notification_status_enum",
    "planned",
    "rendered",
    "queued",
    "sent",
    "edited",
    "suppressed",
    "failed_retryable",
    "failed_terminal",
)

job_attempt_status_enum = _pg_enum(
    "job_attempt_status_enum",
    "pending",
    "running",
    "succeeded",
    "failed_retryable",
    "failed_terminal",
    "abandoned",
)

replay_type_enum = _pg_enum(
    "replay_type_enum",
    "source",
    "enrich",
    "judge",
    "delivery",
    "full_pipeline",
)

ALL_ENUMS = [
    artifact_type_enum,
    verdict_enum,
    delivery_decision_enum,
    urgency_profile_enum,
    outbox_status_enum,
    snapshot_status_enum,
    notification_status_enum,
    job_attempt_status_enum,
    replay_type_enum,
]


def _create_enums() -> None:
    bind = op.get_bind()
    for enum in ALL_ENUMS:
        enum.create(bind, checkfirst=True)


def _drop_enums() -> None:
    bind = op.get_bind()
    for enum in reversed(ALL_ENUMS):
        enum.drop(bind, checkfirst=True)


def upgrade() -> None:
    # pgcrypto is required for gen_random_uuid()
    op.execute("CREATE EXTENSION IF NOT EXISTS pgcrypto")

    _create_enums()

    # ---------------------------------------------------------------------
    # telegram_channel_registry
    # ---------------------------------------------------------------------
    op.create_table(
        "telegram_channel_registry",
        sa.Column(
            "registry_id",
            postgresql.UUID(as_uuid=True),
            primary_key=True,
            nullable=False,
            server_default=sa.text("gen_random_uuid()"),
        ),
        sa.Column("source_kind", sa.Text(), nullable=False),
        sa.Column("source_value", sa.Text(), nullable=False),
        sa.Column(
            "desired_state",
            sa.Text(),
            nullable=False,
            server_default=sa.text("'active'"),
        ),
        sa.Column(
            "access_state",
            sa.Text(),
            nullable=False,
            server_default=sa.text("'unresolved'"),
        ),
        sa.Column("chat_id", sa.BigInteger(), nullable=True),
        sa.Column("username_snapshot", sa.Text(), nullable=True),
        sa.Column("title_snapshot", sa.Text(), nullable=True),
        sa.Column("chat_type", sa.Text(), nullable=True),
        sa.Column("last_resolved_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_join_attempt_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_history_sync_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_seen_message_id", sa.BigInteger(), nullable=True),
        sa.Column("last_seen_message_date", sa.DateTime(timezone=True), nullable=True),
        sa.Column(
            "priority_weight",
            sa.Integer(),
            nullable=False,
            server_default=sa.text("100"),
        ),
        sa.Column("notes", sa.Text(), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
    )
    op.create_index(
        "idx_channel_registry_chat_id",
        "telegram_channel_registry",
        ["chat_id"],
        unique=False,
    )
    op.create_index(
        "idx_channel_registry_state",
        "telegram_channel_registry",
        ["desired_state", "access_state"],
        unique=False,
    )
    op.create_index(
        "uq_channel_registry_source_active",
        "telegram_channel_registry",
        ["source_kind", "source_value"],
        unique=True,
        postgresql_where=sa.text("desired_state <> 'removed'"),
    )

    # ---------------------------------------------------------------------
    # telegram_raw_updates
    # ---------------------------------------------------------------------
    op.create_table(
        "telegram_raw_updates",
        sa.Column("update_seq", sa.BigInteger(), primary_key=True, autoincrement=True),
        sa.Column(
            "received_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.Column("update_type", sa.Text(), nullable=False),
        sa.Column("chat_id", sa.BigInteger(), nullable=True),
        sa.Column("message_id", sa.BigInteger(), nullable=True),
        sa.Column(
            "payload_json",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=False,
        ),
        sa.Column(
            "apply_status",
            sa.Text(),
            nullable=False,
            server_default=sa.text("'pending'"),
        ),
        sa.Column("applied_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("error_text", sa.Text(), nullable=True),
    )
    op.create_index(
        "idx_raw_updates_apply_status_received_at",
        "telegram_raw_updates",
        ["apply_status", "received_at"],
        unique=False,
    )
    op.create_index(
        "idx_raw_updates_chat_message",
        "telegram_raw_updates",
        ["chat_id", "message_id"],
        unique=False,
    )
    op.create_index(
        "idx_raw_updates_received_at",
        "telegram_raw_updates",
        ["received_at"],
        unique=False,
    )

    # ---------------------------------------------------------------------
    # source_messages
    # ---------------------------------------------------------------------
    op.create_table(
        "source_messages",
        sa.Column(
            "source_message_id",
            postgresql.UUID(as_uuid=True),
            primary_key=True,
            nullable=False,
            server_default=sa.text("gen_random_uuid()"),
        ),
        sa.Column(
            "platform",
            sa.Text(),
            nullable=False,
            server_default=sa.text("'telegram'"),
        ),
        sa.Column("chat_id", sa.BigInteger(), nullable=False),
        sa.Column("message_id", sa.BigInteger(), nullable=False),
        sa.Column("logical_post_key", sa.Text(), nullable=False),
        sa.Column(
            "is_channel_post",
            sa.Boolean(),
            nullable=False,
            server_default=sa.text("false"),
        ),
        sa.Column("posted_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("edited_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("deleted_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column(
            "delete_kind",
            sa.Text(),
            nullable=False,
            server_default=sa.text("'none'"),
        ),
        sa.Column(
            "current_version_no",
            sa.Integer(),
            nullable=False,
            server_default=sa.text("1"),
        ),
        sa.Column("message_link", sa.Text(), nullable=True),
        sa.Column("author_signature", sa.Text(), nullable=True),
        sa.Column(
            "forward_info_json",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=True,
        ),
        sa.Column("content_type", sa.Text(), nullable=True),
        sa.Column("text_body", sa.Text(), nullable=True),
        sa.Column("caption_text", sa.Text(), nullable=True),
        sa.Column("text_surface", sa.Text(), nullable=True),
        sa.Column(
            "entities_json",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=True,
        ),
        sa.Column(
            "url_surface_json",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=True,
        ),
        sa.Column(
            "raw_message_json",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=False,
        ),
        sa.Column(
            "first_seen_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.Column(
            "last_seen_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.CheckConstraint(
            "platform = 'telegram'",
            name="ck_source_messages_platform_telegram",
        ),
        sa.UniqueConstraint(
            "platform",
            "chat_id",
            "message_id",
            name="uq_source_messages_platform_chat_message",
        ),
    )
    op.create_index(
        "idx_source_messages_logical_post_key",
        "source_messages",
        ["logical_post_key"],
        unique=False,
    )
    op.create_index(
        "idx_source_messages_deleted_at",
        "source_messages",
        ["deleted_at"],
        unique=False,
    )
    op.create_index(
        "idx_source_messages_last_seen_at",
        "source_messages",
        ["last_seen_at"],
        unique=False,
    )
    op.create_index(
        "idx_source_messages_chat_posted",
        "source_messages",
        ["chat_id", "posted_at"],
        unique=False,
    )

    # ---------------------------------------------------------------------
    # source_message_versions
    # ---------------------------------------------------------------------
    op.create_table(
        "source_message_versions",
        sa.Column(
            "source_message_version_id",
            postgresql.UUID(as_uuid=True),
            primary_key=True,
            nullable=False,
            server_default=sa.text("gen_random_uuid()"),
        ),
        sa.Column(
            "source_message_id",
            postgresql.UUID(as_uuid=True),
            nullable=False,
        ),
        sa.Column("version_no", sa.Integer(), nullable=False),
        sa.Column("version_reason", sa.Text(), nullable=False),
        sa.Column(
            "observed_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.Column("telegram_edit_date", sa.DateTime(timezone=True), nullable=True),
        sa.Column("text_surface", sa.Text(), nullable=True),
        sa.Column(
            "entities_json",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=True,
        ),
        sa.Column(
            "raw_message_json",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=False,
        ),
        sa.Column("content_hash", sa.Text(), nullable=False),
        sa.ForeignKeyConstraint(
            ["source_message_id"],
            ["source_messages.source_message_id"],
            name="fk_source_message_versions_source_message_id",
            ondelete="RESTRICT",
        ),
        sa.UniqueConstraint(
            "source_message_id",
            "version_no",
            name="uq_source_message_versions_source_version",
        ),
    )
    op.create_index(
        "idx_source_message_versions_source_observed",
        "source_message_versions",
        ["source_message_id", "observed_at"],
        unique=False,
    )
    op.create_index(
        "idx_source_message_versions_content_hash",
        "source_message_versions",
        ["source_message_id", "content_hash"],
        unique=False,
    )

    # ---------------------------------------------------------------------
    # event_outbox
    # ---------------------------------------------------------------------
    op.create_table(
        "event_outbox",
        sa.Column(
            "event_id",
            postgresql.UUID(as_uuid=True),
            primary_key=True,
            nullable=False,
            server_default=sa.text("gen_random_uuid()"),
        ),
        sa.Column("event_type", sa.Text(), nullable=False),
        sa.Column("aggregate_type", sa.Text(), nullable=False),
        sa.Column(
            "aggregate_id",
            postgresql.UUID(as_uuid=True),
            nullable=False,
        ),
        sa.Column("dedupe_key", sa.Text(), nullable=False),
        sa.Column(
            "payload_json",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=False,
        ),
        sa.Column(
            "status",
            outbox_status_enum,
            nullable=False,
            server_default=sa.text("'pending'::outbox_status_enum"),
        ),
        sa.Column("published_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column(
            "fail_count",
            sa.Integer(),
            nullable=False,
            server_default=sa.text("0"),
        ),
        sa.Column("last_error", sa.Text(), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.UniqueConstraint("dedupe_key", name="uq_event_outbox_dedupe_key"),
    )
    op.create_index(
        "idx_event_outbox_status_created",
        "event_outbox",
        ["status", "created_at"],
        unique=False,
    )
    op.create_index(
        "idx_event_outbox_aggregate",
        "event_outbox",
        ["aggregate_type", "aggregate_id"],
        unique=False,
    )


def downgrade() -> None:
    # ---------------------------------------------------------------------
    # event_outbox
    # ---------------------------------------------------------------------
    op.drop_index("idx_event_outbox_aggregate", table_name="event_outbox")
    op.drop_index("idx_event_outbox_status_created", table_name="event_outbox")
    op.drop_table("event_outbox")

    # ---------------------------------------------------------------------
    # source_message_versions
    # ---------------------------------------------------------------------
    op.drop_index(
        "idx_source_message_versions_content_hash",
        table_name="source_message_versions",
    )
    op.drop_index(
        "idx_source_message_versions_source_observed",
        table_name="source_message_versions",
    )
    op.drop_table("source_message_versions")

    # ---------------------------------------------------------------------
    # source_messages
    # ---------------------------------------------------------------------
    op.drop_index("idx_source_messages_chat_posted", table_name="source_messages")
    op.drop_index("idx_source_messages_last_seen_at", table_name="source_messages")
    op.drop_index("idx_source_messages_deleted_at", table_name="source_messages")
    op.drop_index("idx_source_messages_logical_post_key", table_name="source_messages")
    op.drop_table("source_messages")

    # ---------------------------------------------------------------------
    # telegram_raw_updates
    # ---------------------------------------------------------------------
    op.drop_index("idx_raw_updates_received_at", table_name="telegram_raw_updates")
    op.drop_index("idx_raw_updates_chat_message", table_name="telegram_raw_updates")
    op.drop_index(
        "idx_raw_updates_apply_status_received_at",
        table_name="telegram_raw_updates",
    )
    op.drop_table("telegram_raw_updates")

    # ---------------------------------------------------------------------
    # telegram_channel_registry
    # ---------------------------------------------------------------------
    op.drop_index(
        "uq_channel_registry_source_active",
        table_name="telegram_channel_registry",
    )
    op.drop_index("idx_channel_registry_state", table_name="telegram_channel_registry")
    op.drop_index("idx_channel_registry_chat_id", table_name="telegram_channel_registry")
    op.drop_table("telegram_channel_registry")

    # Keep pgcrypto installed; dropping extension during downgrade is risky
    # because later/local objects may still depend on it.
    _drop_enums()
```

## 구현 메모

이 초안은 네가 방금 정리한 11/12단계 문서와 3단계 collector 문서를 그대로 코드로 내린 것이다. 즉, `0001`은 **채널 registry + raw update journal + current source row + immutable version history + transactional outbox**까지만 만든다. 그게 collector의 좁은 책임 경계와 맞다. fileciteturn15file3 fileciteturn15file2 fileciteturn15file15

여기서 일부러 넣지 않은 것도 있다. full text search, trigram, 대량 GIN, partitioning은 아직 넣지 않았다. 현재 우선순위는 검색 성능이 아니라 **correctness, replayability, rollback 단위 유지**이기 때문이다. `updated_at`도 DB trigger 대신 application-side update를 전제로 뒀다. 이것도 네 문서에서 이미 그렇게 잠갔다. fileciteturn15file3

또 하나 중요한 점은 `source_message_versions`를 `chat_id/message_id` 중복 저장 대신 `source_message_id` FK 중심으로 정규화했다는 것이다. 이건 12단계 migration 상세안의 현재 정본을 따른 구현 선택이다. 3단계 개념 문서에서는 원문 식별을 강조했지만, 실제 migration 상세안은 FK 기반 canonical join 방향으로 정리돼 있다. 최소 변경 원칙상 지금은 그 상세안을 따르는 게 맞다. fileciteturn14file0

## 바로 검증할 것

다음 순서로 확인하면 된다.

```bash
alembic upgrade 0001_ingest_core
alembic downgrade base
alembic upgrade 0001_ingest_core
```

추가로 테이블/인덱스 확인:

```sql
SELECT tablename
FROM pg_tables
WHERE schemaname = 'public'
  AND tablename IN (
    'telegram_channel_registry',
    'telegram_raw_updates',
    'source_messages',
    'source_message_versions',
    'event_outbox'
  )
ORDER BY tablename;
```

```sql
SELECT indexname, tablename
FROM pg_indexes
WHERE schemaname = 'public'
  AND tablename IN (
    'telegram_channel_registry',
    'telegram_raw_updates',
    'source_messages',
    'source_message_versions',
    'event_outbox'
  )
ORDER BY tablename, indexname;
```

## 이 초안에서 일부러 보수적으로 둔 부분

- `apply_status`, `desired_state`, `access_state`, `delete_kind`는 아직 text로 유지했다.  
  이유는 문서상 native enum으로 잠긴 폐쇄 집합은 아니고, 지금 턴의 핵심은 collector durable boundary를 먼저 세우는 것이기 때문이다. fileciteturn14file0
- `pgcrypto`는 upgrade에서만 보장하고 downgrade에서는 지우지 않았다.  
  로컬/후속 migration 의존성 때문에 extension까지 되감는 건 위험하다.
- partial unique index는 `desired_state <> 'removed'` 조건으로만 걸었다.  
  운영 중 soft-removed registry 재등록 여지를 남기기 위해서다. fileciteturn14file0

## 다음 턴에 바로 할 일

이제 맞는 순서는 하나다.  
**`0002_normalization_candidates` Alembic 초안**으로 바로 들어가면 된다. 그 단계에서 `normalization_runs`, `artifact_registry`, `artifact_observations`, `candidate_group_proposals`, `candidate_group_members`를 만들고, 4단계 문서대로 artifact dedupe와 proposal dedupe의 구조를 DB에 고정해야 한다. fileciteturn15file7 fileciteturn15file5

다음 답변에서는 내가 바로 **`0002_normalization_candidates` Alembic 초안 전체 코드**를 이어서 주겠다.


---

## Source file: `14_response_0002_normalization_candidates_draft.md`

다시 정리해서 준다.  
프로젝트 소스 칸의 README와 단계 문서들을 기준으로 다시 맞췄고, 현재 순서는 **`0001_ingest_core` 다음에 `0002_normalization_candidates`** 로 가는 것이 맞다. 이 migration은 4단계 문서가 요구한 `normalization_runs`, `artifact_registry`, `artifact_observations`, `candidate_group_proposals`, `candidate_group_members`를 DB에 고정하는 역할이어야 하며, 이 단계는 어디까지나 **deterministic normalization / proposal 경계**여야 한다. 또한 11/12단계 문서 해석상 `candidate_group_proposals`는 이름은 proposal이지만 v1에서는 **durable candidate aggregate**로 사용한다. fileciteturn18file9 fileciteturn19file1 fileciteturn18file11 fileciteturn19file0 fileciteturn18file13

# `0002_normalization_candidates` Alembic 초안

권장 파일 경로:

```text
migrations/versions/0002_normalization_candidates.py
```

```python
"""0002_normalization_candidates

Revision ID: 0002_normalization_candidates
Revises: 0001_ingest_core
Create Date: 2026-04-13 00:10:00

"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


# Revision identifiers, used by Alembic.
revision = "0002_normalization_candidates"
down_revision = "0001_ingest_core"
branch_labels = None
depends_on = None


def _pg_enum(name: str, *values: str) -> postgresql.ENUM:
    return postgresql.ENUM(*values, name=name, create_type=False)


artifact_type_enum = _pg_enum(
    "artifact_type_enum",
    "github_repo",
    "github_subpath",
    "github_gist",
    "github_repo_page",
    "x_post",
    "web_article",
    "text_idea",
    "unknown_link",
    "short_url_unresolved",
)

snapshot_status_enum = _pg_enum(
    "snapshot_status_enum",
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
)


def upgrade() -> None:
    # ---------------------------------------------------------------------
    # normalization_runs
    # ---------------------------------------------------------------------
    op.create_table(
        "normalization_runs",
        sa.Column(
            "normalization_run_id",
            postgresql.UUID(as_uuid=True),
            primary_key=True,
            nullable=False,
            server_default=sa.text("gen_random_uuid()"),
        ),
        sa.Column(
            "source_message_id",
            postgresql.UUID(as_uuid=True),
            nullable=False,
        ),
        sa.Column("source_version_no", sa.Integer(), nullable=False),
        sa.Column("normalizer_version", sa.Text(), nullable=False),
        sa.Column(
            "signal_detected",
            sa.Boolean(),
            nullable=False,
            server_default=sa.text("false"),
        ),
        sa.Column(
            "candidate_eligible",
            sa.Boolean(),
            nullable=False,
            server_default=sa.text("false"),
        ),
        sa.Column("trigger_strength", sa.Text(), nullable=True),
        sa.Column("result_hash", sa.Text(), nullable=True),
        sa.Column(
            "completed_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.ForeignKeyConstraint(
            ["source_message_id"],
            ["source_messages.source_message_id"],
            name="fk_normalization_runs_source_message_id",
            ondelete="RESTRICT",
        ),
        sa.UniqueConstraint(
            "source_message_id",
            "source_version_no",
            "normalizer_version",
            name="uq_normalization_runs_source_version_normalizer",
        ),
    )
    op.create_index(
        "idx_normalization_runs_source_version",
        "normalization_runs",
        ["source_message_id", "source_version_no"],
        unique=False,
    )

    # ---------------------------------------------------------------------
    # normalization_suppression_traces
    # ---------------------------------------------------------------------
    op.create_table(
        "normalization_suppression_traces",
        sa.Column(
            "suppression_trace_id",
            postgresql.UUID(as_uuid=True),
            primary_key=True,
            nullable=False,
            server_default=sa.text("gen_random_uuid()"),
        ),
        sa.Column(
            "normalization_run_id",
            postgresql.UUID(as_uuid=True),
            nullable=False,
        ),
        sa.Column("reason_code", sa.Text(), nullable=False),
        sa.Column("trigger_strength", sa.Text(), nullable=True),
        sa.Column(
            "notes_json",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=True,
        ),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.ForeignKeyConstraint(
            ["normalization_run_id"],
            ["normalization_runs.normalization_run_id"],
            name="fk_suppression_traces_normalization_run_id",
            ondelete="CASCADE",
        ),
    )
    op.create_index(
        "idx_suppression_traces_run",
        "normalization_suppression_traces",
        ["normalization_run_id"],
        unique=False,
    )
    op.create_index(
        "idx_suppression_traces_reason",
        "normalization_suppression_traces",
        ["reason_code"],
        unique=False,
    )

    # ---------------------------------------------------------------------
    # artifact_registry
    # ---------------------------------------------------------------------
    op.create_table(
        "artifact_registry",
        sa.Column(
            "artifact_id",
            postgresql.UUID(as_uuid=True),
            primary_key=True,
            nullable=False,
            server_default=sa.text("gen_random_uuid()"),
        ),
        sa.Column("artifact_type", artifact_type_enum, nullable=False),
        sa.Column("canonical_id", sa.Text(), nullable=False),
        sa.Column("canonical_url", sa.Text(), nullable=True),
        sa.Column("normalized_host", sa.Text(), nullable=True),
        sa.Column(
            "artifact_key_json",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=True,
        ),
        sa.Column(
            "current_snapshot_id",
            postgresql.UUID(as_uuid=True),
            nullable=True,
        ),
        sa.Column("current_status", snapshot_status_enum, nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.UniqueConstraint("canonical_id", name="uq_artifact_registry_canonical_id"),
    )
    op.create_index(
        "idx_artifact_registry_type_host",
        "artifact_registry",
        ["artifact_type", "normalized_host"],
        unique=False,
    )
    op.create_index(
        "idx_artifact_registry_updated_at",
        "artifact_registry",
        ["updated_at"],
        unique=False,
    )

    # ---------------------------------------------------------------------
    # artifact_observations
    # ---------------------------------------------------------------------
    op.create_table(
        "artifact_observations",
        sa.Column(
            "artifact_observation_id",
            postgresql.UUID(as_uuid=True),
            primary_key=True,
            nullable=False,
            server_default=sa.text("gen_random_uuid()"),
        ),
        sa.Column(
            "artifact_id",
            postgresql.UUID(as_uuid=True),
            nullable=False,
        ),
        sa.Column(
            "source_message_id",
            postgresql.UUID(as_uuid=True),
            nullable=False,
        ),
        sa.Column("source_version_no", sa.Integer(), nullable=False),
        sa.Column("observed_url", sa.Text(), nullable=True),
        sa.Column("source_kind", sa.Text(), nullable=False),
        sa.Column("normalized_url", sa.Text(), nullable=True),
        sa.Column("resolved_url", sa.Text(), nullable=True),
        sa.Column("canonical_url", sa.Text(), nullable=True),
        sa.Column("classification", sa.Text(), nullable=True),
        sa.Column("context_path", sa.Text(), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.ForeignKeyConstraint(
            ["artifact_id"],
            ["artifact_registry.artifact_id"],
            name="fk_artifact_observations_artifact_id",
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["source_message_id"],
            ["source_messages.source_message_id"],
            name="fk_artifact_observations_source_message_id",
            ondelete="RESTRICT",
        ),
    )
    op.create_index(
        "idx_artifact_observations_artifact_created",
        "artifact_observations",
        ["artifact_id", "created_at"],
        unique=False,
    )
    op.create_index(
        "idx_artifact_observations_source_version",
        "artifact_observations",
        ["source_message_id", "source_version_no"],
        unique=False,
    )

    # ---------------------------------------------------------------------
    # candidate_group_proposals
    # ---------------------------------------------------------------------
    op.create_table(
        "candidate_group_proposals",
        sa.Column(
            "candidate_group_id",
            postgresql.UUID(as_uuid=True),
            primary_key=True,
            nullable=False,
            server_default=sa.text("gen_random_uuid()"),
        ),
        sa.Column(
            "source_message_id",
            postgresql.UUID(as_uuid=True),
            nullable=False,
        ),
        sa.Column("source_version_no", sa.Integer(), nullable=False),
        sa.Column(
            "initial_primary_artifact_id",
            postgresql.UUID(as_uuid=True),
            nullable=False,
        ),
        sa.Column(
            "current_primary_artifact_id",
            postgresql.UUID(as_uuid=True),
            nullable=False,
        ),
        sa.Column(
            "proposal_status",
            sa.Text(),
            nullable=False,
            server_default=sa.text("'proposed'"),
        ),
        sa.Column("normalizer_version", sa.Text(), nullable=False),
        sa.Column("dedupe_subject_key", sa.Text(), nullable=False),
        sa.Column(
            "current_bundle_id",
            postgresql.UUID(as_uuid=True),
            nullable=True,
        ),
        sa.Column(
            "current_analysis_id",
            postgresql.UUID(as_uuid=True),
            nullable=True,
        ),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.ForeignKeyConstraint(
            ["source_message_id"],
            ["source_messages.source_message_id"],
            name="fk_candidate_groups_source_message_id",
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["initial_primary_artifact_id"],
            ["artifact_registry.artifact_id"],
            name="fk_candidate_groups_initial_primary_artifact_id",
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["current_primary_artifact_id"],
            ["artifact_registry.artifact_id"],
            name="fk_candidate_groups_current_primary_artifact_id",
            ondelete="RESTRICT",
        ),
        sa.UniqueConstraint(
            "source_message_id",
            "source_version_no",
            "dedupe_subject_key",
            name="uq_candidate_groups_source_version_dedupe_subject",
        ),
    )
    op.create_index(
        "idx_candidate_groups_current_primary",
        "candidate_group_proposals",
        ["current_primary_artifact_id"],
        unique=False,
    )
    op.create_index(
        "idx_candidate_groups_status_created",
        "candidate_group_proposals",
        ["proposal_status", "created_at"],
        unique=False,
    )
    op.create_index(
        "idx_candidate_groups_source",
        "candidate_group_proposals",
        ["source_message_id", "source_version_no"],
        unique=False,
    )

    # ---------------------------------------------------------------------
    # candidate_group_members
    # ---------------------------------------------------------------------
    op.create_table(
        "candidate_group_members",
        sa.Column(
            "candidate_group_member_id",
            postgresql.UUID(as_uuid=True),
            primary_key=True,
            nullable=False,
            server_default=sa.text("gen_random_uuid()"),
        ),
        sa.Column(
            "candidate_group_id",
            postgresql.UUID(as_uuid=True),
            nullable=False,
        ),
        sa.Column(
            "artifact_id",
            postgresql.UUID(as_uuid=True),
            nullable=False,
        ),
        sa.Column("member_role", sa.Text(), nullable=False),
        sa.Column("member_order", sa.Integer(), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.ForeignKeyConstraint(
            ["candidate_group_id"],
            ["candidate_group_proposals.candidate_group_id"],
            name="fk_candidate_group_members_candidate_group_id",
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["artifact_id"],
            ["artifact_registry.artifact_id"],
            name="fk_candidate_group_members_artifact_id",
            ondelete="RESTRICT",
        ),
        sa.UniqueConstraint(
            "candidate_group_id",
            "artifact_id",
            "member_role",
            name="uq_candidate_group_members_group_artifact_role",
        ),
    )
    op.create_index(
        "idx_candidate_members_group",
        "candidate_group_members",
        ["candidate_group_id"],
        unique=False,
    )
    op.create_index(
        "idx_candidate_members_artifact",
        "candidate_group_members",
        ["artifact_id"],
        unique=False,
    )


def downgrade() -> None:
    # ---------------------------------------------------------------------
    # candidate_group_members
    # ---------------------------------------------------------------------
    op.drop_index(
        "idx_candidate_members_artifact",
        table_name="candidate_group_members",
    )
    op.drop_index(
        "idx_candidate_members_group",
        table_name="candidate_group_members",
    )
    op.drop_table("candidate_group_members")

    # ---------------------------------------------------------------------
    # candidate_group_proposals
    # ---------------------------------------------------------------------
    op.drop_index(
        "idx_candidate_groups_source",
        table_name="candidate_group_proposals",
    )
    op.drop_index(
        "idx_candidate_groups_status_created",
        table_name="candidate_group_proposals",
    )
    op.drop_index(
        "idx_candidate_groups_current_primary",
        table_name="candidate_group_proposals",
    )
    op.drop_table("candidate_group_proposals")

    # ---------------------------------------------------------------------
    # artifact_observations
    # ---------------------------------------------------------------------
    op.drop_index(
        "idx_artifact_observations_source_version",
        table_name="artifact_observations",
    )
    op.drop_index(
        "idx_artifact_observations_artifact_created",
        table_name="artifact_observations",
    )
    op.drop_table("artifact_observations")

    # ---------------------------------------------------------------------
    # artifact_registry
    # ---------------------------------------------------------------------
    op.drop_index(
        "idx_artifact_registry_updated_at",
        table_name="artifact_registry",
    )
    op.drop_index(
        "idx_artifact_registry_type_host",
        table_name="artifact_registry",
    )
    op.drop_table("artifact_registry")

    # ---------------------------------------------------------------------
    # normalization_suppression_traces
    # ---------------------------------------------------------------------
    op.drop_index(
        "idx_suppression_traces_reason",
        table_name="normalization_suppression_traces",
    )
    op.drop_index(
        "idx_suppression_traces_run",
        table_name="normalization_suppression_traces",
    )
    op.drop_table("normalization_suppression_traces")

    # ---------------------------------------------------------------------
    # normalization_runs
    # ---------------------------------------------------------------------
    op.drop_index(
        "idx_normalization_runs_source_version",
        table_name="normalization_runs",
    )
    op.drop_table("normalization_runs")
```

# 왜 이렇게 썼는지

이 코드는 12단계 migration 상세안의 `0002` 컬럼/제약/인덱스 구조를 그대로 반영했다. 특히 `normalization_runs -> artifact_registry -> artifact_observations -> candidate_group_proposals -> candidate_group_members` 순서를 유지했고, `current_snapshot_id`, `current_bundle_id`, `current_analysis_id`는 **nullable pointer만 만들고 FK는 뒤 migration으로 미뤘다**. 그게 cross-migration FK patch 원칙과 맞다. fileciteturn19file0

또 4단계 문서가 요구한 핵심도 그대로 반영했다.  
artifact dedupe는 `artifact_registry.canonical_id` unique로, proposal dedupe는 `(source_message_id, source_version_no, dedupe_subject_key)` unique로 고정했다. 그리고 반드시 남겨야 한다고 못 박힌 5개 테이블을 전부 포함했다. fileciteturn19file1

`candidate_group_proposals`를 durable aggregate처럼 다룬 것도 임의 변경이 아니다. 11단계 실행 계약과 12단계 migration 상세안이 둘 다 그 해석을 최소 변경안으로 고정했기 때문이다. fileciteturn18file13 fileciteturn19file2

# 바로 확인할 것

```bash
alembic upgrade 0002_normalization_candidates
alembic downgrade 0001_ingest_core
alembic upgrade 0002_normalization_candidates
```

간단 확인 SQL:

```sql
SELECT tablename
FROM pg_tables
WHERE schemaname = 'public'
  AND tablename IN (
    'normalization_runs',
    'normalization_suppression_traces',
    'artifact_registry',
    'artifact_observations',
    'candidate_group_proposals',
    'candidate_group_members'
  )
ORDER BY tablename;
```

```sql
SELECT indexname, tablename
FROM pg_indexes
WHERE schemaname = 'public'
  AND tablename IN (
    'normalization_runs',
    'normalization_suppression_traces',
    'artifact_registry',
    'artifact_observations',
    'candidate_group_proposals',
    'candidate_group_members'
  )
ORDER BY tablename, indexname;
```

# 다음 단계

이제 다음은 **`0003_enrichment_bundles` Alembic 초안**이다.  
그 단계에서 `artifact_snapshots`, `candidate_reroot_events`, `candidate_evidence_bundles`, `candidate_evidence_members`, 그리고 `artifact_registry.current_snapshot_id` FK patch를 넣어야 5단계 문서가 요구한 reroot/content-anchor/bundle 구조를 DB에 고정할 수 있다. fileciteturn18file18


---

## Source file: `15_response_0003_enrichment_bundles_draft.md`

# `0003_enrichment_bundles` Alembic 초안

README와 프로젝트 소스의 단계 문서들을 기준으로 맞춘 결과, 이번 단계는 **`0003_enrichment_bundles`** 가 맞다.  
이 단계의 책임은 5단계 문서와 12단계 migration 상세안이 잠근 대로 **비-LLM evidence 수집/조립 경계**다.

즉, 이 migration은 아래를 코드로 내린다.

- `artifact_enrichment_runs`
- `artifact_snapshots`
- source-specific snapshot tables
- `discovered_url_observations`
- `candidate_reroot_events`
- `candidate_evidence_bundles`
- `candidate_evidence_members`
- `artifact_registry.current_snapshot_id` FK patch

---

## 권장 파일 경로

```text
migrations/versions/0003_enrichment_bundles.py
```

## Alembic 초안 코드

```python
"""0003_enrichment_bundles

Revision ID: 0003_enrichment_bundles
Revises: 0002_normalization_candidates
Create Date: 2026-04-13 00:20:00

"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


# Revision identifiers, used by Alembic.
revision = "0003_enrichment_bundles"
down_revision = "0002_normalization_candidates"
branch_labels = None
depends_on = None


def _pg_enum(name: str, *values: str) -> postgresql.ENUM:
    return postgresql.ENUM(*values, name=name, create_type=False)


snapshot_status_enum = _pg_enum(
    "snapshot_status_enum",
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
)


def upgrade() -> None:
    # ---------------------------------------------------------------------
    # artifact_enrichment_runs
    # ---------------------------------------------------------------------
    op.create_table(
        "artifact_enrichment_runs",
        sa.Column(
            "artifact_enrichment_run_id",
            postgresql.UUID(as_uuid=True),
            primary_key=True,
            nullable=False,
            server_default=sa.text("gen_random_uuid()"),
        ),
        sa.Column(
            "artifact_id",
            postgresql.UUID(as_uuid=True),
            nullable=False,
        ),
        sa.Column("provider", sa.Text(), nullable=False),
        sa.Column("refresh_mode", sa.Text(), nullable=False),
        sa.Column(
            "depth_budget",
            sa.Integer(),
            nullable=False,
            server_default=sa.text("1"),
        ),
        sa.Column(
            "status",
            snapshot_status_enum,
            nullable=False,
            server_default=sa.text("'pending'::snapshot_status_enum"),
        ),
        sa.Column("content_anchor", sa.Text(), nullable=True),
        sa.Column("job_idempotency_key", sa.Text(), nullable=False),
        sa.Column(
            "requested_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("finished_at", sa.DateTime(timezone=True), nullable=True),
        sa.ForeignKeyConstraint(
            ["artifact_id"],
            ["artifact_registry.artifact_id"],
            name="fk_enrich_runs_artifact_id",
            ondelete="RESTRICT",
        ),
        sa.UniqueConstraint(
            "job_idempotency_key",
            name="uq_enrich_runs_job_idempotency_key",
        ),
    )
    op.create_index(
        "idx_enrich_runs_artifact_provider",
        "artifact_enrichment_runs",
        ["artifact_id", "provider"],
        unique=False,
    )
    op.create_index(
        "idx_enrich_runs_status_requested",
        "artifact_enrichment_runs",
        ["status", "requested_at"],
        unique=False,
    )

    # ---------------------------------------------------------------------
    # artifact_snapshots
    # ---------------------------------------------------------------------
    op.create_table(
        "artifact_snapshots",
        sa.Column(
            "snapshot_id",
            postgresql.UUID(as_uuid=True),
            primary_key=True,
            nullable=False,
            server_default=sa.text("gen_random_uuid()"),
        ),
        sa.Column(
            "artifact_id",
            postgresql.UUID(as_uuid=True),
            nullable=False,
        ),
        sa.Column("provider", sa.Text(), nullable=False),
        sa.Column("snapshot_type", sa.Text(), nullable=False),
        sa.Column("status", snapshot_status_enum, nullable=False),
        sa.Column(
            "fetched_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.Column("content_anchor", sa.Text(), nullable=False),
        sa.Column("auth_mode", sa.Text(), nullable=True),
        sa.Column(
            "normalized_projection",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=True,
        ),
        sa.Column("raw_payload_ref", sa.Text(), nullable=True),
        sa.Column(
            "evidence_limitations",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=True,
        ),
        sa.Column(
            "fetch_anomalies",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=True,
        ),
        sa.ForeignKeyConstraint(
            ["artifact_id"],
            ["artifact_registry.artifact_id"],
            name="fk_artifact_snapshots_artifact_id",
            ondelete="RESTRICT",
        ),
        sa.UniqueConstraint(
            "artifact_id",
            "provider",
            "content_anchor",
            "snapshot_type",
            name="uq_artifact_snapshots_artifact_provider_anchor_type",
        ),
    )
    op.create_index(
        "idx_artifact_snapshots_artifact_fetched",
        "artifact_snapshots",
        ["artifact_id", "fetched_at"],
        unique=False,
    )
    op.create_index(
        "idx_artifact_snapshots_status",
        "artifact_snapshots",
        ["status"],
        unique=False,
    )

    # ---------------------------------------------------------------------
    # artifact_snapshot_github_repo
    # ---------------------------------------------------------------------
    op.create_table(
        "artifact_snapshot_github_repo",
        sa.Column(
            "snapshot_id",
            postgresql.UUID(as_uuid=True),
            primary_key=True,
            nullable=False,
        ),
        sa.Column("repo_full_name", sa.Text(), nullable=False),
        sa.Column("default_branch", sa.Text(), nullable=True),
        sa.Column("resolved_ref", sa.Text(), nullable=True),
        sa.Column("content_anchor_commit_sha", sa.Text(), nullable=True),
        sa.Column(
            "repo_flags_json",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=True,
        ),
        sa.Column("license_spdx", sa.Text(), nullable=True),
        sa.Column(
            "topics_json",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=True,
        ),
        sa.Column("readme_excerpt", sa.Text(), nullable=True),
        sa.Column(
            "detected_build_systems_json",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=True,
        ),
        sa.Column(
            "detected_languages_json",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=True,
        ),
        sa.Column(
            "key_paths_json",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=True,
        ),
        sa.Column(
            "test_paths_json",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=True,
        ),
        sa.Column(
            "ci_paths_json",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=True,
        ),
        sa.Column(
            "examples_paths_json",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=True,
        ),
        sa.Column(
            "docs_paths_json",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=True,
        ),
        sa.Column(
            "release_summary_json",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=True,
        ),
        sa.ForeignKeyConstraint(
            ["snapshot_id"],
            ["artifact_snapshots.snapshot_id"],
            name="fk_snapshot_gh_repo_snapshot_id",
            ondelete="CASCADE",
        ),
    )
    op.create_index(
        "idx_snapshot_gh_repo_full_name",
        "artifact_snapshot_github_repo",
        ["repo_full_name"],
        unique=False,
    )

    # ---------------------------------------------------------------------
    # artifact_snapshot_github_file_samples
    # ---------------------------------------------------------------------
    op.create_table(
        "artifact_snapshot_github_file_samples",
        sa.Column(
            "file_sample_id",
            postgresql.UUID(as_uuid=True),
            primary_key=True,
            nullable=False,
            server_default=sa.text("gen_random_uuid()"),
        ),
        sa.Column(
            "snapshot_id",
            postgresql.UUID(as_uuid=True),
            nullable=False,
        ),
        sa.Column("path", sa.Text(), nullable=False),
        sa.Column("role", sa.Text(), nullable=False),
        sa.Column("size_bytes", sa.Integer(), nullable=True),
        sa.Column("content_hash", sa.Text(), nullable=True),
        sa.Column("excerpt", sa.Text(), nullable=True),
        sa.Column("raw_blob_ref", sa.Text(), nullable=True),
        sa.ForeignKeyConstraint(
            ["snapshot_id"],
            ["artifact_snapshots.snapshot_id"],
            name="fk_gh_file_samples_snapshot_id",
            ondelete="CASCADE",
        ),
        sa.UniqueConstraint(
            "snapshot_id",
            "path",
            "role",
            name="uq_gh_file_samples_snapshot_path_role",
        ),
    )
    op.create_index(
        "idx_gh_file_samples_snapshot",
        "artifact_snapshot_github_file_samples",
        ["snapshot_id"],
        unique=False,
    )

    # ---------------------------------------------------------------------
    # artifact_snapshot_x_post
    # ---------------------------------------------------------------------
    op.create_table(
        "artifact_snapshot_x_post",
        sa.Column(
            "snapshot_id",
            postgresql.UUID(as_uuid=True),
            primary_key=True,
            nullable=False,
        ),
        sa.Column("post_id", sa.Text(), nullable=False),
        sa.Column("content_anchor_post_version", sa.Text(), nullable=False),
        sa.Column(
            "author_summary_json",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=True,
        ),
        sa.Column("text_full", sa.Text(), nullable=True),
        sa.Column("text_excerpt", sa.Text(), nullable=True),
        sa.Column("conversation_id", sa.Text(), nullable=True),
        sa.Column(
            "referenced_post_ids_json",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=True,
        ),
        sa.Column(
            "discovered_links_json",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=True,
        ),
        sa.Column(
            "media_summary_json",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=True,
        ),
        sa.Column(
            "metrics_summary_json",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=True,
        ),
        sa.ForeignKeyConstraint(
            ["snapshot_id"],
            ["artifact_snapshots.snapshot_id"],
            name="fk_snapshot_x_post_snapshot_id",
            ondelete="CASCADE",
        ),
    )
    op.create_index(
        "idx_snapshot_x_post_id",
        "artifact_snapshot_x_post",
        ["post_id"],
        unique=False,
    )

    # ---------------------------------------------------------------------
    # artifact_snapshot_web_article
    # ---------------------------------------------------------------------
    op.create_table(
        "artifact_snapshot_web_article",
        sa.Column(
            "snapshot_id",
            postgresql.UUID(as_uuid=True),
            primary_key=True,
            nullable=False,
        ),
        sa.Column("final_url", sa.Text(), nullable=False),
        sa.Column("canonical_url_candidate", sa.Text(), nullable=True),
        sa.Column("site_name", sa.Text(), nullable=True),
        sa.Column("title", sa.Text(), nullable=True),
        sa.Column("description", sa.Text(), nullable=True),
        sa.Column("author", sa.Text(), nullable=True),
        sa.Column("published_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("content_hash", sa.Text(), nullable=True),
        sa.Column("main_text_excerpt", sa.Text(), nullable=True),
        sa.Column(
            "outbound_links_json",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=True,
        ),
        sa.ForeignKeyConstraint(
            ["snapshot_id"],
            ["artifact_snapshots.snapshot_id"],
            name="fk_snapshot_web_article_snapshot_id",
            ondelete="CASCADE",
        ),
    )

    # ---------------------------------------------------------------------
    # artifact_snapshot_text_idea
    # ---------------------------------------------------------------------
    op.create_table(
        "artifact_snapshot_text_idea",
        sa.Column(
            "snapshot_id",
            postgresql.UUID(as_uuid=True),
            primary_key=True,
            nullable=False,
        ),
        sa.Column(
            "source_message_id",
            postgresql.UUID(as_uuid=True),
            nullable=False,
        ),
        sa.Column("source_version_no", sa.Integer(), nullable=False),
        sa.Column("hash_surface", sa.Text(), nullable=False),
        sa.Column("display_surface", sa.Text(), nullable=True),
        sa.Column(
            "dev_context_signals_json",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=True,
        ),
        sa.ForeignKeyConstraint(
            ["snapshot_id"],
            ["artifact_snapshots.snapshot_id"],
            name="fk_snapshot_text_idea_snapshot_id",
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["source_message_id"],
            ["source_messages.source_message_id"],
            name="fk_text_idea_source_message_id",
            ondelete="RESTRICT",
        ),
    )
    op.create_index(
        "idx_snapshot_text_idea_source",
        "artifact_snapshot_text_idea",
        ["source_message_id", "source_version_no"],
        unique=False,
    )

    # ---------------------------------------------------------------------
    # discovered_url_observations
    # ---------------------------------------------------------------------
    op.create_table(
        "discovered_url_observations",
        sa.Column(
            "discovered_url_observation_id",
            postgresql.UUID(as_uuid=True),
            primary_key=True,
            nullable=False,
            server_default=sa.text("gen_random_uuid()"),
        ),
        sa.Column(
            "parent_candidate_group_id",
            postgresql.UUID(as_uuid=True),
            nullable=False,
        ),
        sa.Column(
            "parent_artifact_id",
            postgresql.UUID(as_uuid=True),
            nullable=False,
        ),
        sa.Column(
            "parent_snapshot_id",
            postgresql.UUID(as_uuid=True),
            nullable=False,
        ),
        sa.Column("observed_url", sa.Text(), nullable=False),
        sa.Column("context_path", sa.Text(), nullable=True),
        sa.Column("discovery_reason", sa.Text(), nullable=False),
        sa.Column(
            "depth_remaining",
            sa.Integer(),
            nullable=False,
            server_default=sa.text("0"),
        ),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.ForeignKeyConstraint(
            ["parent_candidate_group_id"],
            ["candidate_group_proposals.candidate_group_id"],
            name="fk_discovered_urls_candidate_group_id",
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["parent_artifact_id"],
            ["artifact_registry.artifact_id"],
            name="fk_discovered_urls_parent_artifact_id",
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["parent_snapshot_id"],
            ["artifact_snapshots.snapshot_id"],
            name="fk_discovered_urls_parent_snapshot_id",
            ondelete="RESTRICT",
        ),
    )
    op.create_index(
        "idx_discovered_urls_parent_candidate",
        "discovered_url_observations",
        ["parent_candidate_group_id", "created_at"],
        unique=False,
    )

    # ---------------------------------------------------------------------
    # candidate_reroot_events
    # ---------------------------------------------------------------------
    op.create_table(
        "candidate_reroot_events",
        sa.Column(
            "candidate_reroot_event_id",
            postgresql.UUID(as_uuid=True),
            primary_key=True,
            nullable=False,
            server_default=sa.text("gen_random_uuid()"),
        ),
        sa.Column(
            "candidate_group_id",
            postgresql.UUID(as_uuid=True),
            nullable=False,
        ),
        sa.Column(
            "from_artifact_id",
            postgresql.UUID(as_uuid=True),
            nullable=False,
        ),
        sa.Column(
            "to_artifact_id",
            postgresql.UUID(as_uuid=True),
            nullable=False,
        ),
        sa.Column("reason_code", sa.Text(), nullable=False),
        sa.Column(
            "trigger_snapshot_id",
            postgresql.UUID(as_uuid=True),
            nullable=True,
        ),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.ForeignKeyConstraint(
            ["candidate_group_id"],
            ["candidate_group_proposals.candidate_group_id"],
            name="fk_reroot_events_candidate_group_id",
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["from_artifact_id"],
            ["artifact_registry.artifact_id"],
            name="fk_reroot_events_from_artifact_id",
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["to_artifact_id"],
            ["artifact_registry.artifact_id"],
            name="fk_reroot_events_to_artifact_id",
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["trigger_snapshot_id"],
            ["artifact_snapshots.snapshot_id"],
            name="fk_reroot_events_trigger_snapshot_id",
            ondelete="RESTRICT",
        ),
    )
    op.create_index(
        "idx_reroot_events_candidate_created",
        "candidate_reroot_events",
        ["candidate_group_id", "created_at"],
        unique=False,
    )

    # ---------------------------------------------------------------------
    # candidate_evidence_bundles
    # ---------------------------------------------------------------------
    op.create_table(
        "candidate_evidence_bundles",
        sa.Column(
            "bundle_id",
            postgresql.UUID(as_uuid=True),
            primary_key=True,
            nullable=False,
            server_default=sa.text("gen_random_uuid()"),
        ),
        sa.Column(
            "candidate_group_id",
            postgresql.UUID(as_uuid=True),
            nullable=False,
        ),
        sa.Column(
            "initial_primary_artifact_id",
            postgresql.UUID(as_uuid=True),
            nullable=False,
        ),
        sa.Column(
            "current_primary_artifact_id",
            postgresql.UUID(as_uuid=True),
            nullable=False,
        ),
        sa.Column(
            "bundle_version",
            sa.Integer(),
            nullable=False,
            server_default=sa.text("1"),
        ),
        sa.Column("bundle_profile_version", sa.Text(), nullable=False),
        sa.Column("bundle_input_hash", sa.Text(), nullable=False),
        sa.Column(
            "reroot_count",
            sa.Integer(),
            nullable=False,
            server_default=sa.text("0"),
        ),
        sa.Column(
            "primary_summary",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=True,
        ),
        sa.Column(
            "supporting_summaries_json",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=True,
        ),
        sa.Column(
            "discovered_links_summary_json",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=True,
        ),
        sa.Column(
            "evidence_limitations",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=True,
        ),
        sa.Column(
            "ready_for_analysis",
            sa.Boolean(),
            nullable=False,
            server_default=sa.text("false"),
        ),
        sa.Column("token_budget_profile", sa.Text(), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.ForeignKeyConstraint(
            ["candidate_group_id"],
            ["candidate_group_proposals.candidate_group_id"],
            name="fk_bundles_candidate_group_id",
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["initial_primary_artifact_id"],
            ["artifact_registry.artifact_id"],
            name="fk_bundles_initial_primary_artifact_id",
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["current_primary_artifact_id"],
            ["artifact_registry.artifact_id"],
            name="fk_bundles_current_primary_artifact_id",
            ondelete="RESTRICT",
        ),
        sa.UniqueConstraint(
            "candidate_group_id",
            "bundle_profile_version",
            "bundle_input_hash",
            name="uq_bundles_candidate_profile_input",
        ),
    )
    op.create_index(
        "idx_bundles_candidate_created",
        "candidate_evidence_bundles",
        ["candidate_group_id", "created_at"],
        unique=False,
    )
    op.create_index(
        "idx_bundles_ready",
        "candidate_evidence_bundles",
        ["ready_for_analysis", "created_at"],
        unique=False,
    )

    # ---------------------------------------------------------------------
    # candidate_evidence_members
    # ---------------------------------------------------------------------
    op.create_table(
        "candidate_evidence_members",
        sa.Column(
            "candidate_evidence_member_id",
            postgresql.UUID(as_uuid=True),
            primary_key=True,
            nullable=False,
            server_default=sa.text("gen_random_uuid()"),
        ),
        sa.Column(
            "bundle_id",
            postgresql.UUID(as_uuid=True),
            nullable=False,
        ),
        sa.Column(
            "artifact_id",
            postgresql.UUID(as_uuid=True),
            nullable=False,
        ),
        sa.Column(
            "snapshot_id",
            postgresql.UUID(as_uuid=True),
            nullable=False,
        ),
        sa.Column("member_role", sa.Text(), nullable=False),
        sa.Column("member_order", sa.Integer(), nullable=True),
        sa.ForeignKeyConstraint(
            ["bundle_id"],
            ["candidate_evidence_bundles.bundle_id"],
            name="fk_bundle_members_bundle_id",
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["artifact_id"],
            ["artifact_registry.artifact_id"],
            name="fk_bundle_members_artifact_id",
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["snapshot_id"],
            ["artifact_snapshots.snapshot_id"],
            name="fk_bundle_members_snapshot_id",
            ondelete="RESTRICT",
        ),
        sa.UniqueConstraint(
            "bundle_id",
            "artifact_id",
            "snapshot_id",
            "member_role",
            name="uq_bundle_members_bundle_artifact_snap_role",
        ),
    )
    op.create_index(
        "idx_bundle_members_bundle",
        "candidate_evidence_members",
        ["bundle_id"],
        unique=False,
    )

    # ---------------------------------------------------------------------
    # cross-migration FK patch
    # ---------------------------------------------------------------------
    op.create_foreign_key(
        "fk_artifact_registry_current_snapshot",
        "artifact_registry",
        "artifact_snapshots",
        ["current_snapshot_id"],
        ["snapshot_id"],
        ondelete="RESTRICT",
    )


def downgrade() -> None:
    # ---------------------------------------------------------------------
    # cross-migration FK patch
    # ---------------------------------------------------------------------
    op.drop_constraint(
        "fk_artifact_registry_current_snapshot",
        "artifact_registry",
        type_="foreignkey",
    )

    # ---------------------------------------------------------------------
    # candidate_evidence_members
    # ---------------------------------------------------------------------
    op.drop_index(
        "idx_bundle_members_bundle",
        table_name="candidate_evidence_members",
    )
    op.drop_table("candidate_evidence_members")

    # ---------------------------------------------------------------------
    # candidate_evidence_bundles
    # ---------------------------------------------------------------------
    op.drop_index(
        "idx_bundles_ready",
        table_name="candidate_evidence_bundles",
    )
    op.drop_index(
        "idx_bundles_candidate_created",
        table_name="candidate_evidence_bundles",
    )
    op.drop_table("candidate_evidence_bundles")

    # ---------------------------------------------------------------------
    # candidate_reroot_events
    # ---------------------------------------------------------------------
    op.drop_index(
        "idx_reroot_events_candidate_created",
        table_name="candidate_reroot_events",
    )
    op.drop_table("candidate_reroot_events")

    # ---------------------------------------------------------------------
    # discovered_url_observations
    # ---------------------------------------------------------------------
    op.drop_index(
        "idx_discovered_urls_parent_candidate",
        table_name="discovered_url_observations",
    )
    op.drop_table("discovered_url_observations")

    # ---------------------------------------------------------------------
    # artifact_snapshot_text_idea
    # ---------------------------------------------------------------------
    op.drop_index(
        "idx_snapshot_text_idea_source",
        table_name="artifact_snapshot_text_idea",
    )
    op.drop_table("artifact_snapshot_text_idea")

    # ---------------------------------------------------------------------
    # artifact_snapshot_web_article
    # ---------------------------------------------------------------------
    op.drop_table("artifact_snapshot_web_article")

    # ---------------------------------------------------------------------
    # artifact_snapshot_x_post
    # ---------------------------------------------------------------------
    op.drop_index(
        "idx_snapshot_x_post_id",
        table_name="artifact_snapshot_x_post",
    )
    op.drop_table("artifact_snapshot_x_post")

    # ---------------------------------------------------------------------
    # artifact_snapshot_github_file_samples
    # ---------------------------------------------------------------------
    op.drop_index(
        "idx_gh_file_samples_snapshot",
        table_name="artifact_snapshot_github_file_samples",
    )
    op.drop_table("artifact_snapshot_github_file_samples")

    # ---------------------------------------------------------------------
    # artifact_snapshot_github_repo
    # ---------------------------------------------------------------------
    op.drop_index(
        "idx_snapshot_gh_repo_full_name",
        table_name="artifact_snapshot_github_repo",
    )
    op.drop_table("artifact_snapshot_github_repo")

    # ---------------------------------------------------------------------
    # artifact_snapshots
    # ---------------------------------------------------------------------
    op.drop_index(
        "idx_artifact_snapshots_status",
        table_name="artifact_snapshots",
    )
    op.drop_index(
        "idx_artifact_snapshots_artifact_fetched",
        table_name="artifact_snapshots",
    )
    op.drop_table("artifact_snapshots")

    # ---------------------------------------------------------------------
    # artifact_enrichment_runs
    # ---------------------------------------------------------------------
    op.drop_index(
        "idx_enrich_runs_status_requested",
        table_name="artifact_enrichment_runs",
    )
    op.drop_index(
        "idx_enrich_runs_artifact_provider",
        table_name="artifact_enrichment_runs",
    )
    op.drop_table("artifact_enrichment_runs")
```

## 왜 이렇게 내렸는가

이 초안은 5단계 문서가 잠근 아래 원칙을 그대로 따른다.

- `gh-enricher / x-enricher / web-enricher / evidence-assembler` 분리
- snapshot append-only
- discovered URL은 observation만 기록
- reroot는 assembler 단일 지점에서 반영

그래서 이 migration은 판단 로직이나 LLM 관련 테이블을 넣지 않고, 외부 증거와 candidate-centered bundle 경계까지만 DB로 고정한다.

또 12단계 상세안이 요구한 대로 `artifact_registry.current_snapshot_id`는 `0002`에서 nullable pointer만 두고, `0003`에서야 `artifact_snapshots(snapshot_id)` FK를 붙였다. 반대로 `candidate_group_proposals.current_bundle_id`와 `current_analysis_id`는 아직 대상 테이블이 뒤 단계에 걸려 있으므로 여기서 FK를 걸지 않는다. 그건 `0004` 책임이다.

## 바로 검증할 것

```bash
alembic upgrade 0003_enrichment_bundles
alembic downgrade 0002_normalization_candidates
alembic upgrade 0003_enrichment_bundles
```

테이블 확인:

```sql
SELECT tablename
FROM pg_tables
WHERE schemaname = 'public'
  AND tablename IN (
    'artifact_enrichment_runs',
    'artifact_snapshots',
    'artifact_snapshot_github_repo',
    'artifact_snapshot_github_file_samples',
    'artifact_snapshot_x_post',
    'artifact_snapshot_web_article',
    'artifact_snapshot_text_idea',
    'discovered_url_observations',
    'candidate_reroot_events',
    'candidate_evidence_bundles',
    'candidate_evidence_members'
  )
ORDER BY tablename;
```

FK patch 확인:

```sql
SELECT
  conname,
  conrelid::regclass AS table_name,
  confrelid::regclass AS referenced_table
FROM pg_constraint
WHERE conname = 'fk_artifact_registry_current_snapshot';
```

## 다음 단계

다음 순서는 그대로 **`0004_judge_delivery_observability` Alembic 초안**이다.

그 단계에서만 아래를 만든다.

- `judge_runs`
- `judge_outputs`
- `analyses`
- `notification_plans`
- `notification_renders`
- `notification_delivery_records`
- `pipeline_runs`
- `job_attempts`
- `state_transitions`
- `dead_letter_entries`
- `replay_requests`

그리고 그 단계에서 `candidate_group_proposals.current_bundle_id` / `current_analysis_id` FK patch를 넣어야 한다.


---

## Source file: `16_response_0004_judge_delivery_observability_draft.md`

# `0004_judge_delivery_observability` Alembic 초안

README와 프로젝트 소스의 단계 문서들을 다시 확인했고, 이번 단계는 **`0004_judge_delivery_observability`** 가 맞다.  
이 단계는 12단계 migration 상세안과 6~8단계 문서가 잠근 경계를 실제 DB로 내리는 단계다.

즉, 여기서 해야 하는 일은 다음이다.

- judge 실행 lineage 고정
- `judge_output_v1`와 최종 `analysis_v1` 저장 경계 고정
- notification intent / render / delivery result 분리
- replay / retry / DLQ / state transition / pipeline run 저장
- `candidate_group_proposals.current_bundle_id`
- `candidate_group_proposals.current_analysis_id`

위 두 current pointer의 **cross-migration FK patch** 추가

핵심은 이것이다.

- **LLM은 `judge_output_v1`까지만**
- **최종 verdict / delivery는 deterministic policy-engine이 확정**
- **notifier는 presentation / delivery만**
- **replay/recovery 기준은 PostgreSQL**

---

## 권장 파일 경로

```text
migrations/versions/0004_judge_delivery_observability.py
```

## Alembic 초안 코드

```python
"""0004_judge_delivery_observability

Revision ID: 0004_judge_delivery_observability
Revises: 0003_enrichment_bundles
Create Date: 2026-04-13 00:30:00

"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


# Revision identifiers, used by Alembic.
revision = "0004_judge_delivery_observability"
down_revision = "0003_enrichment_bundles"
branch_labels = None
depends_on = None


def _pg_enum(name: str, *values: str) -> postgresql.ENUM:
    return postgresql.ENUM(*values, name=name, create_type=False)


verdict_enum = _pg_enum(
    "verdict_enum",
    "inspect_now",
    "later",
    "skip",
)

delivery_decision_enum = _pg_enum(
    "delivery_decision_enum",
    "send_now",
    "send_digest",
    "suppress",
)

urgency_profile_enum = _pg_enum(
    "urgency_profile_enum",
    "high",
    "normal_silent",
    "digest",
    "suppressed",
)

notification_status_enum = _pg_enum(
    "notification_status_enum",
    "planned",
    "rendered",
    "queued",
    "sent",
    "edited",
    "suppressed",
    "failed_retryable",
    "failed_terminal",
)

job_attempt_status_enum = _pg_enum(
    "job_attempt_status_enum",
    "pending",
    "running",
    "succeeded",
    "failed_retryable",
    "failed_terminal",
    "abandoned",
)

replay_type_enum = _pg_enum(
    "replay_type_enum",
    "source",
    "enrich",
    "judge",
    "delivery",
    "full_pipeline",
)


def upgrade() -> None:
    # ---------------------------------------------------------------------
    # judge_runs
    # ---------------------------------------------------------------------
    op.create_table(
        "judge_runs",
        sa.Column(
            "judge_run_id",
            postgresql.UUID(as_uuid=True),
            primary_key=True,
            nullable=False,
            server_default=sa.text("gen_random_uuid()"),
        ),
        sa.Column(
            "bundle_id",
            postgresql.UUID(as_uuid=True),
            nullable=False,
        ),
        sa.Column("judge_profile", sa.Text(), nullable=False),
        sa.Column("model", sa.Text(), nullable=False),
        sa.Column("reasoning_effort", sa.Text(), nullable=False),
        sa.Column("prompt_version", sa.Text(), nullable=False),
        sa.Column("schema_version", sa.Text(), nullable=False),
        sa.Column("policy_version", sa.Text(), nullable=False),
        sa.Column("prompt_cache_key", sa.Text(), nullable=True),
        sa.Column(
            "status",
            sa.Text(),
            nullable=False,
            server_default=sa.text("'pending'"),
        ),
        sa.Column(
            "schema_retry_count",
            sa.Integer(),
            nullable=False,
            server_default=sa.text("0"),
        ),
        sa.Column(
            "escalated_from_judge_run_id",
            postgresql.UUID(as_uuid=True),
            nullable=True,
        ),
        sa.Column("input_tokens", sa.Integer(), nullable=True),
        sa.Column("cached_input_tokens", sa.Integer(), nullable=True),
        sa.Column("output_tokens", sa.Integer(), nullable=True),
        sa.Column("reasoning_tokens", sa.Integer(), nullable=True),
        sa.Column("latency_ms", sa.Integer(), nullable=True),
        sa.Column("finish_reason", sa.Text(), nullable=True),
        sa.Column(
            "refusal_detected",
            sa.Boolean(),
            nullable=False,
            server_default=sa.text("false"),
        ),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("finished_at", sa.DateTime(timezone=True), nullable=True),
        sa.ForeignKeyConstraint(
            ["bundle_id"],
            ["candidate_evidence_bundles.bundle_id"],
            name="fk_judge_runs_bundle_id",
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["escalated_from_judge_run_id"],
            ["judge_runs.judge_run_id"],
            name="fk_judge_runs_escalated_from",
            ondelete="RESTRICT",
        ),
        sa.UniqueConstraint(
            "bundle_id",
            "prompt_version",
            "model",
            "reasoning_effort",
            name="uq_judge_runs_bundle_prompt_model_effort",
        ),
    )
    op.create_index(
        "idx_judge_runs_bundle",
        "judge_runs",
        ["bundle_id"],
        unique=False,
    )
    op.create_index(
        "idx_judge_runs_status_started",
        "judge_runs",
        ["status", "started_at"],
        unique=False,
    )

    # ---------------------------------------------------------------------
    # judge_outputs
    # ---------------------------------------------------------------------
    op.create_table(
        "judge_outputs",
        sa.Column(
            "judge_output_id",
            postgresql.UUID(as_uuid=True),
            primary_key=True,
            nullable=False,
            server_default=sa.text("gen_random_uuid()"),
        ),
        sa.Column(
            "judge_run_id",
            postgresql.UUID(as_uuid=True),
            nullable=False,
        ),
        sa.Column(
            "candidate_group_id",
            postgresql.UUID(as_uuid=True),
            nullable=False,
        ),
        sa.Column("judge_schema_version", sa.Text(), nullable=False),
        sa.Column(
            "payload_json",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=False,
        ),
        sa.Column("model_proposed_verdict", verdict_enum, nullable=True),
        sa.Column("model_confidence_band", sa.Text(), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.ForeignKeyConstraint(
            ["judge_run_id"],
            ["judge_runs.judge_run_id"],
            name="fk_judge_outputs_judge_run_id",
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["candidate_group_id"],
            ["candidate_group_proposals.candidate_group_id"],
            name="fk_judge_outputs_candidate_group_id",
            ondelete="RESTRICT",
        ),
    )
    op.create_index(
        "idx_judge_outputs_candidate_created",
        "judge_outputs",
        ["candidate_group_id", "created_at"],
        unique=False,
    )

    # ---------------------------------------------------------------------
    # analyses
    # ---------------------------------------------------------------------
    op.create_table(
        "analyses",
        sa.Column(
            "analysis_id",
            postgresql.UUID(as_uuid=True),
            primary_key=True,
            nullable=False,
            server_default=sa.text("gen_random_uuid()"),
        ),
        sa.Column(
            "candidate_group_id",
            postgresql.UUID(as_uuid=True),
            nullable=False,
        ),
        sa.Column(
            "judge_output_id",
            postgresql.UUID(as_uuid=True),
            nullable=False,
        ),
        sa.Column("schema_version", sa.Text(), nullable=False),
        sa.Column("policy_version", sa.Text(), nullable=False),
        sa.Column("prompt_version", sa.Text(), nullable=False),
        sa.Column("delivery_policy_version", sa.Text(), nullable=False),
        sa.Column("verdict", verdict_enum, nullable=False),
        sa.Column("delivery_decision", delivery_decision_enum, nullable=False),
        sa.Column(
            "scores_json",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=False,
        ),
        sa.Column(
            "reason_codes_json",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=False,
        ),
        sa.Column("evidence_limitations_ko", sa.Text(), nullable=True),
        sa.Column("recommended_action_ko", sa.Text(), nullable=True),
        sa.Column("freshness_note_ko", sa.Text(), nullable=True),
        sa.Column("model_proposed_verdict", verdict_enum, nullable=True),
        sa.Column(
            "policy_reconciled_flag",
            sa.Boolean(),
            nullable=False,
            server_default=sa.text("true"),
        ),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.ForeignKeyConstraint(
            ["candidate_group_id"],
            ["candidate_group_proposals.candidate_group_id"],
            name="fk_analyses_candidate_group_id",
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["judge_output_id"],
            ["judge_outputs.judge_output_id"],
            name="fk_analyses_judge_output_id",
            ondelete="RESTRICT",
        ),
        sa.UniqueConstraint(
            "judge_output_id",
            "policy_version",
            "delivery_policy_version",
            name="uq_analyses_judge_output_policy_delivery_policy",
        ),
    )
    op.create_index(
        "idx_analyses_candidate_created",
        "analyses",
        ["candidate_group_id", "created_at"],
        unique=False,
    )
    op.create_index(
        "idx_analyses_verdict_created",
        "analyses",
        ["verdict", "created_at"],
        unique=False,
    )

    # ---------------------------------------------------------------------
    # notification_plans
    # ---------------------------------------------------------------------
    op.create_table(
        "notification_plans",
        sa.Column(
            "notification_plan_id",
            postgresql.UUID(as_uuid=True),
            primary_key=True,
            nullable=False,
            server_default=sa.text("gen_random_uuid()"),
        ),
        sa.Column(
            "analysis_id",
            postgresql.UUID(as_uuid=True),
            nullable=False,
        ),
        sa.Column(
            "candidate_group_id",
            postgresql.UUID(as_uuid=True),
            nullable=False,
        ),
        sa.Column(
            "delivery_decision",
            delivery_decision_enum,
            nullable=False,
        ),
        sa.Column(
            "urgency_profile",
            urgency_profile_enum,
            nullable=False,
        ),
        sa.Column("target_chat_id", sa.BigInteger(), nullable=False),
        sa.Column("target_thread_id", sa.BigInteger(), nullable=True),
        sa.Column("render_profile", sa.Text(), nullable=True),
        sa.Column("dedupe_subject_key", sa.Text(), nullable=False),
        sa.Column("material_change_hash", sa.Text(), nullable=False),
        sa.Column("send_after", sa.DateTime(timezone=True), nullable=True),
        sa.Column("suppress_reason_code", sa.Text(), nullable=True),
        sa.Column(
            "status",
            notification_status_enum,
            nullable=False,
            server_default=sa.text("'planned'::notification_status_enum"),
        ),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.ForeignKeyConstraint(
            ["analysis_id"],
            ["analyses.analysis_id"],
            name="fk_notification_plans_analysis_id",
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["candidate_group_id"],
            ["candidate_group_proposals.candidate_group_id"],
            name="fk_notification_plans_candidate_group_id",
            ondelete="RESTRICT",
        ),
        sa.UniqueConstraint(
            "analysis_id",
            "target_chat_id",
            "material_change_hash",
            name="uq_notification_plans_analysis_target_material",
        ),
    )
    op.create_index(
        "idx_notification_plans_status_send_after",
        "notification_plans",
        ["status", "send_after"],
        unique=False,
    )
    op.create_index(
        "idx_notification_plans_dedupe_subject",
        "notification_plans",
        ["dedupe_subject_key"],
        unique=False,
    )

    # ---------------------------------------------------------------------
    # notification_renders
    # ---------------------------------------------------------------------
    op.create_table(
        "notification_renders",
        sa.Column(
            "notification_render_id",
            postgresql.UUID(as_uuid=True),
            primary_key=True,
            nullable=False,
            server_default=sa.text("gen_random_uuid()"),
        ),
        sa.Column(
            "notification_plan_id",
            postgresql.UUID(as_uuid=True),
            nullable=False,
        ),
        sa.Column("message_text", sa.Text(), nullable=False),
        sa.Column(
            "entities_json",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=True,
        ),
        sa.Column(
            "link_preview_options_json",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=True,
        ),
        sa.Column(
            "reply_markup_json",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=True,
        ),
        sa.Column(
            "disable_notification",
            sa.Boolean(),
            nullable=False,
            server_default=sa.text("false"),
        ),
        sa.Column(
            "protect_content",
            sa.Boolean(),
            nullable=False,
            server_default=sa.text("false"),
        ),
        sa.Column(
            "parse_strategy",
            sa.Text(),
            nullable=False,
            server_default=sa.text("'entities'"),
        ),
        sa.Column("render_hash", sa.Text(), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.ForeignKeyConstraint(
            ["notification_plan_id"],
            ["notification_plans.notification_plan_id"],
            name="fk_notification_renders_plan_id",
            ondelete="RESTRICT",
        ),
        sa.UniqueConstraint(
            "notification_plan_id",
            "render_hash",
            name="uq_notification_renders_plan_render_hash",
        ),
    )

    # ---------------------------------------------------------------------
    # notification_delivery_records
    # ---------------------------------------------------------------------
    op.create_table(
        "notification_delivery_records",
        sa.Column(
            "notification_delivery_record_id",
            postgresql.UUID(as_uuid=True),
            primary_key=True,
            nullable=False,
            server_default=sa.text("gen_random_uuid()"),
        ),
        sa.Column(
            "notification_plan_id",
            postgresql.UUID(as_uuid=True),
            nullable=False,
        ),
        sa.Column("telegram_chat_id", sa.BigInteger(), nullable=True),
        sa.Column("telegram_message_id", sa.BigInteger(), nullable=True),
        sa.Column(
            "delivery_status",
            notification_status_enum,
            nullable=False,
        ),
        sa.Column("sent_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("edited_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column(
            "attempt_count",
            sa.Integer(),
            nullable=False,
            server_default=sa.text("1"),
        ),
        sa.Column("transport_error_code", sa.Text(), nullable=True),
        sa.Column("transport_error_class", sa.Text(), nullable=True),
        sa.Column(
            "telegram_response_json",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=True,
        ),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.ForeignKeyConstraint(
            ["notification_plan_id"],
            ["notification_plans.notification_plan_id"],
            name="fk_notification_delivery_plan_id",
            ondelete="RESTRICT",
        ),
    )
    op.create_index(
        "idx_notification_delivery_plan_created",
        "notification_delivery_records",
        ["notification_plan_id", "created_at"],
        unique=False,
    )
    op.create_index(
        "idx_notification_delivery_status_created",
        "notification_delivery_records",
        ["delivery_status", "created_at"],
        unique=False,
    )

    # ---------------------------------------------------------------------
    # pipeline_runs
    # ---------------------------------------------------------------------
    op.create_table(
        "pipeline_runs",
        sa.Column(
            "pipeline_run_id",
            postgresql.UUID(as_uuid=True),
            primary_key=True,
            nullable=False,
            server_default=sa.text("gen_random_uuid()"),
        ),
        sa.Column("trigger_source", sa.Text(), nullable=False),
        sa.Column("run_kind", sa.Text(), nullable=False),
        sa.Column("root_object_type", sa.Text(), nullable=False),
        sa.Column(
            "root_object_id",
            postgresql.UUID(as_uuid=True),
            nullable=False,
        ),
        sa.Column(
            "started_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.Column("finished_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("terminal_status", sa.Text(), nullable=True),
    )
    op.create_index(
        "idx_pipeline_runs_root",
        "pipeline_runs",
        ["root_object_type", "root_object_id"],
        unique=False,
    )
    op.create_index(
        "idx_pipeline_runs_started",
        "pipeline_runs",
        ["started_at"],
        unique=False,
    )

    # ---------------------------------------------------------------------
    # job_attempts
    # ---------------------------------------------------------------------
    op.create_table(
        "job_attempts",
        sa.Column(
            "job_attempt_id",
            postgresql.UUID(as_uuid=True),
            primary_key=True,
            nullable=False,
            server_default=sa.text("gen_random_uuid()"),
        ),
        sa.Column("stage_name", sa.Text(), nullable=False),
        sa.Column("queue_name", sa.Text(), nullable=False),
        sa.Column("root_object_type", sa.Text(), nullable=False),
        sa.Column(
            "root_object_id",
            postgresql.UUID(as_uuid=True),
            nullable=False,
        ),
        sa.Column(
            "attempt_no",
            sa.Integer(),
            nullable=False,
            server_default=sa.text("1"),
        ),
        sa.Column("lease_owner", sa.Text(), nullable=True),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("finished_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column(
            "attempt_status",
            job_attempt_status_enum,
            nullable=False,
            server_default=sa.text("'pending'::job_attempt_status_enum"),
        ),
        sa.Column("error_code", sa.Text(), nullable=True),
        sa.Column("retry_after_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
    )
    op.create_index(
        "idx_job_attempts_queue_status_retry_after",
        "job_attempts",
        ["queue_name", "attempt_status", "retry_after_at"],
        unique=False,
    )
    op.create_index(
        "idx_job_attempts_root",
        "job_attempts",
        ["root_object_type", "root_object_id"],
        unique=False,
    )

    # ---------------------------------------------------------------------
    # state_transitions
    # ---------------------------------------------------------------------
    op.create_table(
        "state_transitions",
        sa.Column(
            "state_transition_id",
            postgresql.UUID(as_uuid=True),
            primary_key=True,
            nullable=False,
            server_default=sa.text("gen_random_uuid()"),
        ),
        sa.Column("object_type", sa.Text(), nullable=False),
        sa.Column(
            "object_id",
            postgresql.UUID(as_uuid=True),
            nullable=False,
        ),
        sa.Column("from_state", sa.Text(), nullable=True),
        sa.Column("to_state", sa.Text(), nullable=False),
        sa.Column("reason_code", sa.Text(), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
    )
    op.create_index(
        "idx_state_transitions_object_created",
        "state_transitions",
        ["object_type", "object_id", "created_at"],
        unique=False,
    )

    # ---------------------------------------------------------------------
    # dead_letter_entries
    # ---------------------------------------------------------------------
    op.create_table(
        "dead_letter_entries",
        sa.Column(
            "dead_letter_entry_id",
            postgresql.UUID(as_uuid=True),
            primary_key=True,
            nullable=False,
            server_default=sa.text("gen_random_uuid()"),
        ),
        sa.Column("stage_name", sa.Text(), nullable=False),
        sa.Column("queue_name", sa.Text(), nullable=False),
        sa.Column("root_object_type", sa.Text(), nullable=False),
        sa.Column(
            "root_object_id",
            postgresql.UUID(as_uuid=True),
            nullable=False,
        ),
        sa.Column("last_error_code", sa.Text(), nullable=True),
        sa.Column("last_error_snippet", sa.Text(), nullable=True),
        sa.Column(
            "retry_count",
            sa.Integer(),
            nullable=False,
            server_default=sa.text("0"),
        ),
        sa.Column(
            "first_failed_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.Column(
            "last_failed_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.Column("next_manual_action", sa.Text(), nullable=True),
        sa.Column("replay_hint", sa.Text(), nullable=True),
    )
    op.create_index(
        "idx_dlq_stage_last_failed",
        "dead_letter_entries",
        ["stage_name", "last_failed_at"],
        unique=False,
    )

    # ---------------------------------------------------------------------
    # replay_requests
    # ---------------------------------------------------------------------
    op.create_table(
        "replay_requests",
        sa.Column(
            "replay_request_id",
            postgresql.UUID(as_uuid=True),
            primary_key=True,
            nullable=False,
            server_default=sa.text("gen_random_uuid()"),
        ),
        sa.Column("replay_type", replay_type_enum, nullable=False),
        sa.Column("root_object_type", sa.Text(), nullable=False),
        sa.Column(
            "root_object_id",
            postgresql.UUID(as_uuid=True),
            nullable=False,
        ),
        sa.Column("requested_by", sa.Text(), nullable=True),
        sa.Column(
            "requested_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.Column(
            "status",
            sa.Text(),
            nullable=False,
            server_default=sa.text("'pending'"),
        ),
    )
    op.create_index(
        "idx_replay_requests_status_requested",
        "replay_requests",
        ["status", "requested_at"],
        unique=False,
    )

    # ---------------------------------------------------------------------
    # cross-migration FK patch
    # ---------------------------------------------------------------------
    op.create_foreign_key(
        "fk_candidate_groups_current_bundle_id",
        "candidate_group_proposals",
        "candidate_evidence_bundles",
        ["current_bundle_id"],
        ["bundle_id"],
        ondelete="RESTRICT",
    )

    op.create_foreign_key(
        "fk_candidate_groups_current_analysis_id",
        "candidate_group_proposals",
        "analyses",
        ["current_analysis_id"],
        ["analysis_id"],
        ondelete="RESTRICT",
    )


def downgrade() -> None:
    # ---------------------------------------------------------------------
    # cross-migration FK patch
    # ---------------------------------------------------------------------
    op.drop_constraint(
        "fk_candidate_groups_current_analysis_id",
        "candidate_group_proposals",
        type_="foreignkey",
    )
    op.drop_constraint(
        "fk_candidate_groups_current_bundle_id",
        "candidate_group_proposals",
        type_="foreignkey",
    )

    # ---------------------------------------------------------------------
    # replay_requests
    # ---------------------------------------------------------------------
    op.drop_index(
        "idx_replay_requests_status_requested",
        table_name="replay_requests",
    )
    op.drop_table("replay_requests")

    # ---------------------------------------------------------------------
    # dead_letter_entries
    # ---------------------------------------------------------------------
    op.drop_index(
        "idx_dlq_stage_last_failed",
        table_name="dead_letter_entries",
    )
    op.drop_table("dead_letter_entries")

    # ---------------------------------------------------------------------
    # state_transitions
    # ---------------------------------------------------------------------
    op.drop_index(
        "idx_state_transitions_object_created",
        table_name="state_transitions",
    )
    op.drop_table("state_transitions")

    # ---------------------------------------------------------------------
    # job_attempts
    # ---------------------------------------------------------------------
    op.drop_index(
        "idx_job_attempts_root",
        table_name="job_attempts",
    )
    op.drop_index(
        "idx_job_attempts_queue_status_retry_after",
        table_name="job_attempts",
    )
    op.drop_table("job_attempts")

    # ---------------------------------------------------------------------
    # pipeline_runs
    # ---------------------------------------------------------------------
    op.drop_index(
        "idx_pipeline_runs_started",
        table_name="pipeline_runs",
    )
    op.drop_index(
        "idx_pipeline_runs_root",
        table_name="pipeline_runs",
    )
    op.drop_table("pipeline_runs")

    # ---------------------------------------------------------------------
    # notification_delivery_records
    # ---------------------------------------------------------------------
    op.drop_index(
        "idx_notification_delivery_status_created",
        table_name="notification_delivery_records",
    )
    op.drop_index(
        "idx_notification_delivery_plan_created",
        table_name="notification_delivery_records",
    )
    op.drop_table("notification_delivery_records")

    # ---------------------------------------------------------------------
    # notification_renders
    # ---------------------------------------------------------------------
    op.drop_table("notification_renders")

    # ---------------------------------------------------------------------
    # notification_plans
    # ---------------------------------------------------------------------
    op.drop_index(
        "idx_notification_plans_dedupe_subject",
        table_name="notification_plans",
    )
    op.drop_index(
        "idx_notification_plans_status_send_after",
        table_name="notification_plans",
    )
    op.drop_table("notification_plans")

    # ---------------------------------------------------------------------
    # analyses
    # ---------------------------------------------------------------------
    op.drop_index(
        "idx_analyses_verdict_created",
        table_name="analyses",
    )
    op.drop_index(
        "idx_analyses_candidate_created",
        table_name="analyses",
    )
    op.drop_table("analyses")

    # ---------------------------------------------------------------------
    # judge_outputs
    # ---------------------------------------------------------------------
    op.drop_index(
        "idx_judge_outputs_candidate_created",
        table_name="judge_outputs",
    )
    op.drop_table("judge_outputs")

    # ---------------------------------------------------------------------
    # judge_runs
    # ---------------------------------------------------------------------
    op.drop_index(
        "idx_judge_runs_status_started",
        table_name="judge_runs",
    )
    op.drop_index(
        "idx_judge_runs_bundle",
        table_name="judge_runs",
    )
    op.drop_table("judge_runs")
```

## 왜 이렇게 내렸는가

이 초안은 아래 문서 제약을 그대로 따른다.

### 1. 6단계 judge 경계 유지

6단계 문서가 잠근 구조상:

- `judge-openai`는 `judge_output_v1`만 생성
- 최종 verdict / delivery는 deterministic policy-engine이 확정

그래서 `judge_outputs`와 `analyses`를 분리했고, `analyses`에만 최종 `verdict`, `delivery_decision`, `policy_reconciled_flag`를 둔다.

### 2. 7단계 notification 계층 분리

7단계 문서가 잠근 구조상:

- `notification_plan`
- `notification_render`
- `notification_delivery_record`

이 셋은 반드시 분리되어야 한다.

그래서 이 migration도:

- 전달 의도
- 렌더 결과
- 실제 Telegram transport 결과

를 각각 다른 테이블로 유지한다.

### 3. 8단계 replay / recovery / observability 유지

8단계 문서와 12단계 상세안이 잠근 구조상:

- PostgreSQL이 durable source of truth
- Redis는 rebuild 대상
- replay는 overwrite가 아니라 새 run / 새 analysis 생성
- DLQ / retry / pipeline run / state transition이 DB에 남아야 함

그래서 `pipeline_runs`, `job_attempts`, `state_transitions`, `dead_letter_entries`, `replay_requests`를 여기서 같이 만든다.

### 4. cross-migration FK patch

12단계 상세안이 명시한 대로:

- `candidate_group_proposals.current_bundle_id`
- `candidate_group_proposals.current_analysis_id`

이 두 nullable pointer는 `0002`에서는 컬럼만 만들고, 대상 테이블이 생긴 뒤인 `0004`에서 FK를 붙인다.

## 바로 검증할 것

```bash
alembic upgrade 0004_judge_delivery_observability
alembic downgrade 0003_enrichment_bundles
alembic upgrade 0004_judge_delivery_observability
```

테이블 확인:

```sql
SELECT tablename
FROM pg_tables
WHERE schemaname = 'public'
  AND tablename IN (
    'judge_runs',
    'judge_outputs',
    'analyses',
    'notification_plans',
    'notification_renders',
    'notification_delivery_records',
    'pipeline_runs',
    'job_attempts',
    'state_transitions',
    'dead_letter_entries',
    'replay_requests'
  )
ORDER BY tablename;
```

FK patch 확인:

```sql
SELECT
  conname,
  conrelid::regclass AS table_name,
  confrelid::regclass AS referenced_table
FROM pg_constraint
WHERE conname IN (
  'fk_candidate_groups_current_bundle_id',
  'fk_candidate_groups_current_analysis_id'
)
ORDER BY conname;
```

인덱스 확인:

```sql
SELECT indexname, tablename
FROM pg_indexes
WHERE schemaname = 'public'
  AND tablename IN (
    'judge_runs',
    'judge_outputs',
    'analyses',
    'notification_plans',
    'notification_renders',
    'notification_delivery_records',
    'pipeline_runs',
    'job_attempts',
    'state_transitions',
    'dead_letter_entries',
    'replay_requests'
  )
ORDER BY tablename, indexname;
```

## 다음 단계

이제 migration 기준으로는 `0001`~`0004` 초안이 다 내려온 상태다.

따라서 다음 구현 순서는 11/12단계 문서가 잠근 대로 아래가 맞다.

1. `0001`~`0004` 실제 Alembic 적용/왕복 검증
2. enum / FK / unique / index 동작 확인
3. 그 다음 **collector 구현 시작**
4. 순서상 제일 먼저 들어갈 서비스는 `collector-telegram` 또는 `outbox-relay`

즉, 다음 턴부터는 migration 문서 단계가 아니라 **실제 서비스 구현 단계**로 넘어가는 것이 맞다.
