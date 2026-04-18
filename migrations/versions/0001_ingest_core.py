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
