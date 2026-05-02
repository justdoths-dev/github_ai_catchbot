"""0004_judge_delivery_obs

Revision ID: 0004_judge_delivery_obs
Revises: 0003_enrichment_bundles
Create Date: 2026-04-13 00:30:00

"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


# Revision identifiers, used by Alembic.
revision = "0004_judge_delivery_obs"
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
