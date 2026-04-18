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
