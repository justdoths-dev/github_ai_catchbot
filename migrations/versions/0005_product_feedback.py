"""0005_product_feedback

Revision ID: 0005_product_feedback
Revises: 0004_judge_delivery_obs
Create Date: 2026-07-13 00:00:00

"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


# Revision identifiers, used by Alembic.
revision = "0005_product_feedback"
down_revision = "0004_judge_delivery_obs"
branch_labels = None
depends_on = None


verdict_enum = postgresql.ENUM(
    "inspect_now",
    "later",
    "skip",
    name="verdict_enum",
    create_type=False,
)

delivery_decision_enum = postgresql.ENUM(
    "send_now",
    "send_digest",
    "suppress",
    name="delivery_decision_enum",
    create_type=False,
)

FEEDBACK_CATEGORIES = (
    "useful",
    "useful_now",
    "useful_later",
    "false_positive",
    "false_negative",
    "hype",
    "duplicate",
    "stale",
    "wrong_priority",
    "wrong_primary",
    "insufficient_evidence",
    "bad_summary",
    "bad_channel_fit",
)


def upgrade() -> None:
    op.create_table(
        "notification_material_claims",
        sa.Column(
            "notification_material_claim_id",
            postgresql.UUID(as_uuid=True),
            primary_key=True,
            nullable=False,
            server_default=sa.text("gen_random_uuid()"),
        ),
        sa.Column("dedupe_subject_key", sa.Text(), nullable=False),
        sa.Column("target_chat_id", sa.BigInteger(), nullable=False),
        sa.Column("material_change_hash", sa.Text(), nullable=False),
        sa.Column("notification_plan_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.ForeignKeyConstraint(
            ["notification_plan_id"],
            ["notification_plans.notification_plan_id"],
            name="fk_notification_material_claims_plan_id",
            ondelete="RESTRICT",
            deferrable=True,
            initially="DEFERRED",
        ),
        sa.UniqueConstraint(
            "dedupe_subject_key",
            "target_chat_id",
            "material_change_hash",
            name="uq_notification_material_claims_subject_target_material",
        ),
        sa.UniqueConstraint(
            "notification_plan_id",
            name="uq_notification_material_claims_plan_id",
        ),
    )
    op.create_table(
        "notification_feedback",
        sa.Column(
            "feedback_id",
            postgresql.UUID(as_uuid=True),
            primary_key=True,
            nullable=False,
            server_default=sa.text("gen_random_uuid()"),
        ),
        sa.Column("operator_action_key", sa.Text(), nullable=False),
        sa.Column("feedback_category", sa.Text(), nullable=False),
        sa.Column("analysis_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("candidate_group_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("notification_plan_id", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column(
            "notification_delivery_record_id",
            postgresql.UUID(as_uuid=True),
            nullable=True,
        ),
        sa.Column("channel_registry_id", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("final_verdict", verdict_enum, nullable=False),
        sa.Column("delivery_decision", delivery_decision_enum, nullable=False),
        sa.Column("primary_artifact_type", sa.Text(), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.CheckConstraint(
            "feedback_category IN (" + ",".join(f"'{value}'" for value in FEEDBACK_CATEGORIES) + ")",
            name="ck_notification_feedback_category",
        ),
        sa.CheckConstraint(
            "operator_action_key ~ '^[A-Za-z0-9._:-]{1,128}$'",
            name="ck_notification_feedback_action_key",
        ),
        sa.ForeignKeyConstraint(
            ["analysis_id"],
            ["analyses.analysis_id"],
            name="fk_notification_feedback_analysis_id",
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["candidate_group_id"],
            ["candidate_group_proposals.candidate_group_id"],
            name="fk_notification_feedback_candidate_group_id",
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["notification_plan_id"],
            ["notification_plans.notification_plan_id"],
            name="fk_notification_feedback_plan_id",
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["notification_delivery_record_id"],
            ["notification_delivery_records.notification_delivery_record_id"],
            name="fk_notification_feedback_delivery_record_id",
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["channel_registry_id"],
            ["telegram_channel_registry.registry_id"],
            name="fk_notification_feedback_channel_registry_id",
            ondelete="RESTRICT",
        ),
        sa.UniqueConstraint(
            "operator_action_key",
            name="uq_notification_feedback_operator_action_key",
        ),
    )
    op.create_index(
        "idx_notification_feedback_channel_created",
        "notification_feedback",
        ["channel_registry_id", "created_at"],
        unique=False,
    )
    op.create_index(
        "idx_notification_feedback_category_created",
        "notification_feedback",
        ["feedback_category", "created_at"],
        unique=False,
    )
    op.create_index(
        "idx_notification_feedback_candidate_created",
        "notification_feedback",
        ["candidate_group_id", "created_at"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index(
        "idx_notification_feedback_candidate_created",
        table_name="notification_feedback",
    )
    op.drop_index(
        "idx_notification_feedback_category_created",
        table_name="notification_feedback",
    )
    op.drop_index(
        "idx_notification_feedback_channel_created",
        table_name="notification_feedback",
    )
    op.drop_table("notification_feedback")
    op.drop_table("notification_material_claims")
