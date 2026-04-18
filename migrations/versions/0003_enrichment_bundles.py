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
