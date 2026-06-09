from __future__ import annotations

import argparse
import json
import os
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping, Protocol, Sequence
from uuid import UUID

from tools import local_db_notifier_fixture_replay_runner as notifier_runner


SCHEMA_VERSION = "local_db_full_e2e_dry_run_notification_replay_v1"
ZERO_UUID = UUID("00000000-0000-0000-0000-000000000000")

policy_runner = notifier_runner.policy_runner
validator_runner = policy_runner.validator_runner
fake_judge_runner = validator_runner.fake_judge_runner
analysis_router_runner = fake_judge_runner.analysis_router_runner
evidence_bundle_runner = analysis_router_runner.evidence_bundle_runner
source_candidate_runner = evidence_bundle_runner.source_candidate_runner
github_snapshot_runner = evidence_bundle_runner.github_snapshot_runner

STAGE_TRUE_KEYS = (
    "database_url_guard_passed",
    "source_message_created",
    "artifact_created",
    "candidate_group_created",
    "artifact_snapshot_created",
    "evidence_bundle_created",
    "analysis_requested_event_created",
    "judge_run_created",
    "judge_call_requested_event_created",
    "judge_output_created",
    "judge_output_ready_event_created",
    "analysis_validated_state_transition_created",
    "analysis_policy_apply_event_created",
    "analysis_created",
    "notification_plan_intent_event_created",
    "notification_plan_created",
    "notification_render_created",
    "notification_delivery_record_created",
    "notification_delivery_state_transition_created",
    "notification_delivery_result_event_created",
)
SIDE_EFFECT_FALSE_KEYS = (
    "telegram_called",
    "send_message_called",
    "edit_message_called",
    "openai_called",
    "live_github_called",
    "live_telegram_called",
    "workers_started",
    "redis_mutation",
    "production_db_write",
    "alembic_or_ddl_ran",
)
PREDECESSOR_FALSE_KEYS = (
    "telegram_called",
    "send_message_called",
    "edit_message_called",
    "openai_called",
    "live_github_called",
    "live_telegram_called",
    "workers_started",
    "redis_mutation",
    "production_db_write",
)
SAFE_EXCEPTION_MESSAGES = {
    "durable_chain_ids_missing",
    "durable_chain_verification_failed",
}


@dataclass(frozen=True, slots=True)
class RunnerResult:
    exit_code: int
    report: dict[str, Any]


@dataclass(frozen=True, slots=True)
class FullChainVerificationResult:
    source_message_created: bool
    artifact_created: bool
    candidate_group_created: bool
    artifact_snapshot_created: bool
    evidence_bundle_created: bool
    analysis_requested_event_created: bool
    judge_run_created: bool
    judge_call_requested_event_created: bool
    judge_output_created: bool
    judge_output_ready_event_created: bool
    analysis_validated_state_transition_created: bool
    analysis_policy_apply_event_created: bool
    analysis_created: bool
    notification_plan_intent_event_created: bool
    notification_plan_created: bool
    notification_render_created: bool
    notification_delivery_record_created: bool
    notification_delivery_state_transition_created: bool
    notification_delivery_result_event_created: bool
    checks_failed: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class ChainIds:
    artifact_id: UUID
    candidate_group_id: UUID
    snapshot_id: UUID
    bundle_id: UUID
    judge_run_id: UUID
    judge_output_id: UUID
    analysis_id: UUID
    notification_plan_id: UUID
    notification_delivery_record_id: UUID


class NotifierReplayRunner(Protocol):
    def run(
        self,
        *,
        database_url: str,
        source_fixture_path: Path,
        github_snapshot_fixture_path: Path,
        replay_namespace: str,
        env: Mapping[str, str],
        repo_root: Path,
    ) -> notifier_runner.RunnerResult: ...


class FullChainVerifier(Protocol):
    def verify(
        self,
        *,
        database_url: str,
        source_fixture_path: Path,
        github_snapshot_fixture_path: Path,
        replay_namespace: str,
        repo_root: Path,
    ) -> FullChainVerificationResult: ...


class DefaultNotifierReplayRunner:
    def run(
        self,
        *,
        database_url: str,
        source_fixture_path: Path,
        github_snapshot_fixture_path: Path,
        replay_namespace: str,
        env: Mapping[str, str],
        repo_root: Path,
    ) -> notifier_runner.RunnerResult:
        args = argparse.Namespace(
            database_url=database_url,
            source_fixture=str(source_fixture_path),
            github_snapshot_fixture=str(github_snapshot_fixture_path),
            replay_namespace=replay_namespace,
            confirm_local_test_db=True,
        )
        predecessor_env = dict(env)
        predecessor_env["APP_ENV"] = "test"
        return notifier_runner.run(args, env=predecessor_env, repo_root=repo_root)


class SqlAlchemyFullChainVerifier:
    def verify(
        self,
        *,
        database_url: str,
        source_fixture_path: Path,
        github_snapshot_fixture_path: Path,
        replay_namespace: str,
        repo_root: Path,
    ) -> FullChainVerificationResult:
        _bootstrap_repo_imports()
        import sqlalchemy as sa

        source_fixture = source_candidate_runner.load_source_fixture(source_fixture_path, repo_root=repo_root)
        github_fixture = github_snapshot_runner.load_github_snapshot_fixture(
            github_snapshot_fixture_path,
            repo_root=repo_root,
        )
        engine = sa.create_engine(database_url, future=True)
        try:
            with engine.connect() as connection:
                ids = _fetch_chain_ids(
                    connection,
                    source_fixture=source_fixture,
                    github_fixture=github_fixture,
                    replay_namespace=replay_namespace,
                )
                counts = _fetch_durable_counts(
                    connection,
                    ids=ids,
                    source_fixture=source_fixture,
                    github_fixture=github_fixture,
                    replay_namespace=replay_namespace,
                )
        finally:
            engine.dispose()
        return _verification_from_counts(counts)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Replay the closed local/test DB fixture chain from SourceMessage "
            "through a dry-run notification.delivery.result.v1 and print stable "
            "sanitized JSON only."
        )
    )
    parser.add_argument("--database-url", required=True)
    parser.add_argument("--source-fixture", required=True)
    parser.add_argument("--github-snapshot-fixture", required=True)
    parser.add_argument("--replay-namespace", required=True)
    parser.add_argument("--confirm-local-test-db", action="store_true")
    return parser


def render_json(report: Mapping[str, Any]) -> str:
    return json.dumps(report, indent=2) + "\n"


def run(
    args: argparse.Namespace,
    *,
    env: Mapping[str, str] | None = None,
    verifier: FullChainVerifier | None = None,
    predecessor_runner: NotifierReplayRunner | None = None,
    repo_root: Path | None = None,
) -> RunnerResult:
    effective_env = env if env is not None else os.environ
    root = repo_root or _repo_root()
    report = _base_report()
    checks_failed: list[str] = []

    if not args.confirm_local_test_db:
        checks_failed.append("confirm_local_test_db_required")

    app_env = effective_env.get("APP_ENV", "").strip().lower()
    if app_env in {"prod", "production", "live"}:
        checks_failed.append("app_env_production_rejected")

    namespace_ok, namespace_failures = source_candidate_runner.validate_replay_namespace(args.replay_namespace)
    checks_failed.extend(namespace_failures)

    database_ok, database_failures, _ = validate_database_url(args.database_url)
    report["database_url_guard_passed"] = database_ok
    checks_failed.extend(database_failures)

    try:
        source_candidate_runner.load_source_fixture(Path(args.source_fixture), repo_root=root)
    except Exception:  # noqa: BLE001 - operator output must stay sanitized.
        checks_failed.append("source_fixture_load_failed")

    try:
        github_snapshot_runner.load_github_snapshot_fixture(Path(args.github_snapshot_fixture), repo_root=root)
    except Exception:  # noqa: BLE001 - operator output must stay sanitized.
        checks_failed.append("github_snapshot_fixture_load_failed")

    if checks_failed:
        return _finish(report, checks_failed)

    if not namespace_ok:
        checks_failed.append("precondition_missing")
        return _finish(report, checks_failed)

    active_predecessor = predecessor_runner or DefaultNotifierReplayRunner()
    try:
        predecessor = active_predecessor.run(
            database_url=args.database_url,
            source_fixture_path=Path(args.source_fixture),
            github_snapshot_fixture_path=Path(args.github_snapshot_fixture),
            replay_namespace=args.replay_namespace,
            env=effective_env,
            repo_root=root,
        )
    except Exception:  # noqa: BLE001 - never echo DB or runtime error bodies.
        checks_failed.append("notifier_replay_failed")
        return _finish(report, checks_failed)

    if predecessor.exit_code != 0 or predecessor.report.get("status") != "pass":
        checks_failed.append("notifier_replay_failed")
        return _finish(report, checks_failed)
    for key in PREDECESSOR_FALSE_KEYS:
        if predecessor.report.get(key) is not False:
            checks_failed.append(f"{key}:unexpected")
    if checks_failed:
        return _finish(report, checks_failed)

    active_verifier = verifier or SqlAlchemyFullChainVerifier()
    try:
        verification = active_verifier.verify(
            database_url=args.database_url,
            source_fixture_path=Path(args.source_fixture),
            github_snapshot_fixture_path=Path(args.github_snapshot_fixture),
            replay_namespace=args.replay_namespace,
            repo_root=root,
        )
    except Exception as exc:  # noqa: BLE001 - never echo DB errors or URLs.
        checks_failed.append(_safe_failure_code(exc))
        return _finish(report, checks_failed)

    report.update(
        {
            "source_message_created": verification.source_message_created,
            "artifact_created": verification.artifact_created,
            "candidate_group_created": verification.candidate_group_created,
            "artifact_snapshot_created": verification.artifact_snapshot_created,
            "evidence_bundle_created": verification.evidence_bundle_created,
            "analysis_requested_event_created": verification.analysis_requested_event_created,
            "judge_run_created": verification.judge_run_created,
            "judge_call_requested_event_created": verification.judge_call_requested_event_created,
            "judge_output_created": verification.judge_output_created,
            "judge_output_ready_event_created": verification.judge_output_ready_event_created,
            "analysis_validated_state_transition_created": verification.analysis_validated_state_transition_created,
            "analysis_policy_apply_event_created": verification.analysis_policy_apply_event_created,
            "analysis_created": verification.analysis_created,
            "notification_plan_intent_event_created": verification.notification_plan_intent_event_created,
            "notification_plan_created": verification.notification_plan_created,
            "notification_render_created": verification.notification_render_created,
            "notification_delivery_record_created": verification.notification_delivery_record_created,
            "notification_delivery_state_transition_created": verification.notification_delivery_state_transition_created,
            "notification_delivery_result_event_created": verification.notification_delivery_result_event_created,
        }
    )
    checks_failed.extend(verification.checks_failed)

    for key in STAGE_TRUE_KEYS:
        if report.get(key) is not True:
            checks_failed.append(f"{key}:missing")
    for key in SIDE_EFFECT_FALSE_KEYS:
        if report.get(key) is not False:
            checks_failed.append(f"{key}:unexpected")

    return _finish(report, checks_failed)


def validate_database_url(database_url: str | None):
    return notifier_runner.validate_database_url(database_url)


def _fetch_chain_ids(
    connection: Any,
    *,
    source_fixture: Any,
    github_fixture: Any,
    replay_namespace: str,
) -> ChainIds:
    normalizer_version = source_candidate_runner.build_normalizer_version(replay_namespace)
    artifact_id = _uuid_or_zero(
        _scalar(
            connection,
            """
            SELECT artifact_id
            FROM artifact_registry
            WHERE canonical_id = :canonical_id
            """,
            {"canonical_id": github_fixture.artifact_canonical_id},
        )
    )
    candidate_group_id = _uuid_or_zero(
        _scalar(
            connection,
            """
            SELECT candidate_group_id
            FROM candidate_group_proposals
            WHERE source_message_id = CAST(:source_message_id AS uuid)
              AND source_version_no = :source_version_no
              AND normalizer_version = :normalizer_version
              AND dedupe_subject_key = :canonical_id
            """,
            {
                "source_message_id": str(source_fixture.source_message_id),
                "source_version_no": source_fixture.source_version_no,
                "normalizer_version": normalizer_version,
                "canonical_id": github_fixture.artifact_canonical_id,
            },
        )
    )
    snapshot_id = _uuid_or_zero(
        _scalar(
            connection,
            """
            SELECT snapshot_id
            FROM artifact_snapshots
            WHERE artifact_id = CAST(:artifact_id AS uuid)
              AND provider = 'github'
              AND snapshot_type = :snapshot_type
              AND content_anchor = :content_anchor
            """,
            {
                "artifact_id": str(artifact_id),
                "snapshot_type": github_fixture.snapshot_type,
                "content_anchor": github_fixture.content_anchor,
            },
        )
    )
    bundle_id = _uuid_or_zero(
        _scalar(
            connection,
            """
            SELECT current_bundle_id
            FROM candidate_group_proposals
            WHERE candidate_group_id = CAST(:candidate_group_id AS uuid)
            """,
            {"candidate_group_id": str(candidate_group_id)},
        )
    )
    judge_run_id = _uuid_or_zero(
        _scalar(
            connection,
            """
            SELECT judge_run_id
            FROM judge_runs
            WHERE bundle_id = CAST(:bundle_id AS uuid)
              AND model = :model
              AND reasoning_effort = :reasoning_effort
              AND prompt_version = :prompt_version
            """,
            {
                "bundle_id": str(bundle_id),
                "model": analysis_router_runner.DEFAULT_MODEL,
                "reasoning_effort": analysis_router_runner.DEFAULT_REASONING_EFFORT,
                "prompt_version": fake_judge_runner.EXPECTED_PROMPT_VERSION,
            },
        )
    )
    judge_output_id = _uuid_or_zero(
        _scalar(
            connection,
            """
            SELECT judge_output_id
            FROM judge_outputs
            WHERE judge_run_id = CAST(:judge_run_id AS uuid)
              AND judge_schema_version = :judge_schema_version
            ORDER BY created_at, judge_output_id
            LIMIT 1
            """,
            {
                "judge_run_id": str(judge_run_id),
                "judge_schema_version": fake_judge_runner.JUDGE_SCHEMA_VERSION,
            },
        )
    )
    analysis_id = _uuid_or_zero(
        _scalar(
            connection,
            """
            SELECT analysis_id
            FROM analyses
            WHERE judge_output_id = CAST(:judge_output_id AS uuid)
              AND policy_version = :policy_version
              AND delivery_policy_version = :delivery_policy_version
            ORDER BY created_at, analysis_id
            LIMIT 1
            """,
            {
                "judge_output_id": str(judge_output_id),
                "policy_version": policy_runner.POLICY_VERSION,
                "delivery_policy_version": policy_runner.DELIVERY_POLICY_VERSION,
            },
        )
    )
    notification_plan_id = _uuid_or_zero(
        _scalar(
            connection,
            """
            SELECT notification_plan_id
            FROM notification_plans
            WHERE analysis_id = CAST(:analysis_id AS uuid)
              AND candidate_group_id = CAST(:candidate_group_id AS uuid)
            ORDER BY created_at, notification_plan_id
            LIMIT 1
            """,
            {
                "analysis_id": str(analysis_id),
                "candidate_group_id": str(candidate_group_id),
            },
        )
    )
    delivery_record_id = _uuid_or_zero(
        _scalar(
            connection,
            """
            SELECT notification_delivery_record_id
            FROM notification_delivery_records
            WHERE notification_plan_id = CAST(:notification_plan_id AS uuid)
              AND delivery_status = 'suppressed'::notification_status_enum
              AND telegram_message_id IS NULL
              AND attempt_count = 0
            ORDER BY created_at, notification_delivery_record_id
            LIMIT 1
            """,
            {"notification_plan_id": str(notification_plan_id)},
        )
    )
    return ChainIds(
        artifact_id=artifact_id,
        candidate_group_id=candidate_group_id,
        snapshot_id=snapshot_id,
        bundle_id=bundle_id,
        judge_run_id=judge_run_id,
        judge_output_id=judge_output_id,
        analysis_id=analysis_id,
        notification_plan_id=notification_plan_id,
        notification_delivery_record_id=delivery_record_id,
    )


def _fetch_durable_counts(
    connection: Any,
    *,
    ids: ChainIds,
    source_fixture: Any,
    github_fixture: Any,
    replay_namespace: str,
) -> dict[str, int]:
    analysis_dedupe = evidence_bundle_runner.build_analysis_requested_dedupe_key(
        replay_namespace=replay_namespace,
        candidate_group_id=ids.candidate_group_id,
        bundle_id=ids.bundle_id,
    )
    judge_dedupe = analysis_router_runner.build_judge_call_requested_dedupe_key(
        replay_namespace=replay_namespace,
        bundle_id=ids.bundle_id,
        model=analysis_router_runner.DEFAULT_MODEL,
        reasoning_effort=analysis_router_runner.DEFAULT_REASONING_EFFORT,
        prompt_version=fake_judge_runner.EXPECTED_PROMPT_VERSION,
    )
    ready_dedupe = fake_judge_runner.build_judge_output_ready_dedupe_key(
        replay_namespace=replay_namespace,
        judge_run_id=ids.judge_run_id,
        judge_output_id=ids.judge_output_id,
    )
    policy_dedupe = validator_runner.build_analysis_policy_apply_dedupe_key(
        replay_namespace=replay_namespace,
        judge_run_id=ids.judge_run_id,
        judge_output_id=ids.judge_output_id,
    )
    notify_prefix = f"local-db-policy-engine:{replay_namespace}:notification.plan.created:%"
    delivery_dedupe = notifier_runner.build_delivery_result_event_dedupe_key(
        replay_namespace=replay_namespace,
        notification_plan_id=ids.notification_plan_id,
        notification_delivery_record_id=ids.notification_delivery_record_id,
    )
    return {
        "source_message_current_rows": _count(
            connection,
            """
            SELECT count(*)
            FROM source_messages
            WHERE source_message_id = CAST(:source_message_id AS uuid)
              AND current_version_no = :source_version_no
            """,
            {
                "source_message_id": str(source_fixture.source_message_id),
                "source_version_no": source_fixture.source_version_no,
            },
        ),
        "source_message_version_rows": _count(
            connection,
            """
            SELECT count(*)
            FROM source_message_versions
            WHERE source_message_id = CAST(:source_message_id AS uuid)
              AND version_no = :source_version_no
            """,
            {
                "source_message_id": str(source_fixture.source_message_id),
                "source_version_no": source_fixture.source_version_no,
            },
        ),
        "artifact_rows": _count(
            connection,
            """
            SELECT count(*)
            FROM artifact_registry
            WHERE artifact_id = CAST(:artifact_id AS uuid)
              AND canonical_id = :canonical_id
            """,
            {"artifact_id": str(ids.artifact_id), "canonical_id": github_fixture.artifact_canonical_id},
        ),
        "candidate_group_rows": _count(
            connection,
            """
            SELECT count(*)
            FROM candidate_group_proposals
            WHERE candidate_group_id = CAST(:candidate_group_id AS uuid)
              AND source_message_id = CAST(:source_message_id AS uuid)
              AND source_version_no = :source_version_no
              AND dedupe_subject_key = :canonical_id
            """,
            {
                "candidate_group_id": str(ids.candidate_group_id),
                "source_message_id": str(source_fixture.source_message_id),
                "source_version_no": source_fixture.source_version_no,
                "canonical_id": github_fixture.artifact_canonical_id,
            },
        ),
        "candidate_group_member_rows": _count(
            connection,
            """
            SELECT count(*)
            FROM candidate_group_members
            WHERE candidate_group_id = CAST(:candidate_group_id AS uuid)
              AND artifact_id = CAST(:artifact_id AS uuid)
              AND member_role = 'primary'
            """,
            {"candidate_group_id": str(ids.candidate_group_id), "artifact_id": str(ids.artifact_id)},
        ),
        "artifact_snapshot_rows": _count(
            connection,
            """
            SELECT count(*)
            FROM artifact_snapshots
            WHERE snapshot_id = CAST(:snapshot_id AS uuid)
              AND artifact_id = CAST(:artifact_id AS uuid)
              AND provider = 'github'
              AND snapshot_type = :snapshot_type
              AND content_anchor = :content_anchor
            """,
            {
                "snapshot_id": str(ids.snapshot_id),
                "artifact_id": str(ids.artifact_id),
                "snapshot_type": github_fixture.snapshot_type,
                "content_anchor": github_fixture.content_anchor,
            },
        ),
        "artifact_snapshot_github_rows": _count(
            connection,
            """
            SELECT count(*)
            FROM artifact_snapshot_github_repo
            WHERE snapshot_id = CAST(:snapshot_id AS uuid)
              AND repo_full_name = :repo_full_name
            """,
            {"snapshot_id": str(ids.snapshot_id), "repo_full_name": github_fixture.repo_full_name},
        ),
        "evidence_bundle_rows": _count(
            connection,
            """
            SELECT count(*)
            FROM candidate_evidence_bundles
            WHERE bundle_id = CAST(:bundle_id AS uuid)
              AND candidate_group_id = CAST(:candidate_group_id AS uuid)
              AND ready_for_analysis IS TRUE
            """,
            {"bundle_id": str(ids.bundle_id), "candidate_group_id": str(ids.candidate_group_id)},
        ),
        "evidence_member_rows": _count(
            connection,
            """
            SELECT count(*)
            FROM candidate_evidence_members
            WHERE bundle_id = CAST(:bundle_id AS uuid)
              AND artifact_id = CAST(:artifact_id AS uuid)
              AND snapshot_id = CAST(:snapshot_id AS uuid)
              AND member_role = 'primary'
            """,
            {
                "bundle_id": str(ids.bundle_id),
                "artifact_id": str(ids.artifact_id),
                "snapshot_id": str(ids.snapshot_id),
            },
        ),
        "candidate_current_bundle_rows": _count(
            connection,
            """
            SELECT count(*)
            FROM candidate_group_proposals
            WHERE candidate_group_id = CAST(:candidate_group_id AS uuid)
              AND current_bundle_id = CAST(:bundle_id AS uuid)
            """,
            {"candidate_group_id": str(ids.candidate_group_id), "bundle_id": str(ids.bundle_id)},
        ),
        "analysis_requested_events": _count(
            connection,
            """
            SELECT count(*)
            FROM event_outbox
            WHERE event_type = :event_type
              AND aggregate_type = 'candidate_group'
              AND aggregate_id = CAST(:candidate_group_id AS uuid)
              AND dedupe_key = :dedupe_key
            """,
            {
                "event_type": evidence_bundle_runner.ANALYSIS_REQUESTED_EVENT_TYPE,
                "candidate_group_id": str(ids.candidate_group_id),
                "dedupe_key": analysis_dedupe,
            },
        ),
        "judge_run_rows": _count(
            connection,
            """
            SELECT count(*)
            FROM judge_runs
            WHERE judge_run_id = CAST(:judge_run_id AS uuid)
              AND bundle_id = CAST(:bundle_id AS uuid)
              AND status = 'succeeded'
            """,
            {"judge_run_id": str(ids.judge_run_id), "bundle_id": str(ids.bundle_id)},
        ),
        "judge_call_requested_events": _count(
            connection,
            """
            SELECT count(*)
            FROM event_outbox
            WHERE event_type = :event_type
              AND aggregate_id = CAST(:judge_run_id AS uuid)
              AND dedupe_key = :dedupe_key
            """,
            {
                "event_type": analysis_router_runner.JUDGE_CALL_REQUESTED_EVENT_TYPE,
                "judge_run_id": str(ids.judge_run_id),
                "dedupe_key": judge_dedupe,
            },
        ),
        "judge_output_rows": _count(
            connection,
            """
            SELECT count(*)
            FROM judge_outputs
            WHERE judge_output_id = CAST(:judge_output_id AS uuid)
              AND judge_run_id = CAST(:judge_run_id AS uuid)
              AND judge_schema_version = :judge_schema_version
            """,
            {
                "judge_output_id": str(ids.judge_output_id),
                "judge_run_id": str(ids.judge_run_id),
                "judge_schema_version": fake_judge_runner.JUDGE_SCHEMA_VERSION,
            },
        ),
        "judge_output_ready_events": _count(
            connection,
            """
            SELECT count(*)
            FROM event_outbox
            WHERE event_type = :event_type
              AND aggregate_id = CAST(:judge_run_id AS uuid)
              AND dedupe_key = :dedupe_key
            """,
            {
                "event_type": fake_judge_runner.JUDGE_OUTPUT_READY_EVENT_TYPE,
                "judge_run_id": str(ids.judge_run_id),
                "dedupe_key": ready_dedupe,
            },
        ),
        "analysis_validation_state_transitions": _count(
            connection,
            """
            SELECT count(*)
            FROM state_transitions
            WHERE object_type = 'judge_run'
              AND object_id = CAST(:judge_run_id AS uuid)
              AND from_state = 'succeeded'
              AND to_state = 'analysis_validated'
              AND reason_code = 'judge_output_validated'
            """,
            {"judge_run_id": str(ids.judge_run_id)},
        ),
        "analysis_policy_apply_events": _count(
            connection,
            """
            SELECT count(*)
            FROM event_outbox
            WHERE event_type = :event_type
              AND aggregate_id = CAST(:judge_run_id AS uuid)
              AND dedupe_key = :dedupe_key
            """,
            {
                "event_type": validator_runner.ANALYSIS_POLICY_APPLY_EVENT_TYPE,
                "judge_run_id": str(ids.judge_run_id),
                "dedupe_key": policy_dedupe,
            },
        ),
        "analysis_rows": _count(
            connection,
            """
            SELECT count(*)
            FROM analyses
            WHERE analysis_id = CAST(:analysis_id AS uuid)
              AND candidate_group_id = CAST(:candidate_group_id AS uuid)
              AND judge_output_id = CAST(:judge_output_id AS uuid)
              AND schema_version = :schema_version
              AND policy_version = :policy_version
              AND delivery_policy_version = :delivery_policy_version
            """,
            {
                "analysis_id": str(ids.analysis_id),
                "candidate_group_id": str(ids.candidate_group_id),
                "judge_output_id": str(ids.judge_output_id),
                "schema_version": policy_runner.ANALYSIS_SCHEMA_VERSION,
                "policy_version": policy_runner.POLICY_VERSION,
                "delivery_policy_version": policy_runner.DELIVERY_POLICY_VERSION,
            },
        ),
        "notification_plan_intent_events": _count(
            connection,
            """
            SELECT count(*)
            FROM event_outbox
            WHERE event_type = :event_type
              AND aggregate_type = 'analysis'
              AND aggregate_id = CAST(:analysis_id AS uuid)
              AND dedupe_key LIKE :dedupe_prefix
            """,
            {
                "event_type": policy_runner.NOTIFICATION_PLAN_CREATED_EVENT_TYPE,
                "analysis_id": str(ids.analysis_id),
                "dedupe_prefix": notify_prefix,
            },
        ),
        "notification_plan_rows": _count(
            connection,
            """
            SELECT count(*)
            FROM notification_plans
            WHERE notification_plan_id = CAST(:notification_plan_id AS uuid)
              AND analysis_id = CAST(:analysis_id AS uuid)
              AND candidate_group_id = CAST(:candidate_group_id AS uuid)
              AND status = 'suppressed'::notification_status_enum
            """,
            {
                "notification_plan_id": str(ids.notification_plan_id),
                "analysis_id": str(ids.analysis_id),
                "candidate_group_id": str(ids.candidate_group_id),
            },
        ),
        "notification_render_rows": _count(
            connection,
            """
            SELECT count(*)
            FROM notification_renders
            WHERE notification_plan_id = CAST(:notification_plan_id AS uuid)
            """,
            {"notification_plan_id": str(ids.notification_plan_id)},
        ),
        "notification_delivery_record_rows": _count(
            connection,
            """
            SELECT count(*)
            FROM notification_delivery_records
            WHERE notification_plan_id = CAST(:notification_plan_id AS uuid)
              AND delivery_status = 'suppressed'::notification_status_enum
              AND telegram_message_id IS NULL
              AND attempt_count = 0
              AND transport_error_code = :reason_code
              AND telegram_response_json @> CAST(:telegram_response_json AS jsonb)
            """,
            {
                "notification_plan_id": str(ids.notification_plan_id),
                "reason_code": notifier_runner.DELIVERY_STATE_REASON_CODE,
                "telegram_response_json": _json_dumps({"dry_run": True, "local_fixture": True}),
            },
        ),
        "notification_delivery_state_transitions": _count(
            connection,
            """
            SELECT count(*)
            FROM state_transitions
            WHERE object_type = 'notification_plan'
              AND object_id = CAST(:notification_plan_id AS uuid)
              AND from_state = :from_state
              AND to_state = :to_state
              AND reason_code = :reason_code
            """,
            {
                "notification_plan_id": str(ids.notification_plan_id),
                "from_state": notifier_runner.DELIVERY_FROM_STATE,
                "to_state": notifier_runner.DELIVERY_TO_STATE,
                "reason_code": notifier_runner.DELIVERY_STATE_REASON_CODE,
            },
        ),
        "notification_delivery_result_events": _count(
            connection,
            """
            SELECT count(*)
            FROM event_outbox
            WHERE event_type = :event_type
              AND aggregate_type = 'notification_plan'
              AND aggregate_id = CAST(:notification_plan_id AS uuid)
              AND dedupe_key = :dedupe_key
            """,
            {
                "event_type": notifier_runner.NOTIFICATION_DELIVERY_RESULT_EVENT_TYPE,
                "notification_plan_id": str(ids.notification_plan_id),
                "dedupe_key": delivery_dedupe,
            },
        ),
    }


def _verification_from_counts(counts: Mapping[str, int]) -> FullChainVerificationResult:
    stage_counts = {
        "source_message_created": (
            counts.get("source_message_current_rows", 0),
            counts.get("source_message_version_rows", 0),
        ),
        "artifact_created": (counts.get("artifact_rows", 0),),
        "candidate_group_created": (
            counts.get("candidate_group_rows", 0),
            counts.get("candidate_group_member_rows", 0),
        ),
        "artifact_snapshot_created": (
            counts.get("artifact_snapshot_rows", 0),
            counts.get("artifact_snapshot_github_rows", 0),
        ),
        "evidence_bundle_created": (
            counts.get("evidence_bundle_rows", 0),
            counts.get("evidence_member_rows", 0),
            counts.get("candidate_current_bundle_rows", 0),
        ),
        "analysis_requested_event_created": (counts.get("analysis_requested_events", 0),),
        "judge_run_created": (counts.get("judge_run_rows", 0),),
        "judge_call_requested_event_created": (counts.get("judge_call_requested_events", 0),),
        "judge_output_created": (counts.get("judge_output_rows", 0),),
        "judge_output_ready_event_created": (counts.get("judge_output_ready_events", 0),),
        "analysis_validated_state_transition_created": (
            counts.get("analysis_validation_state_transitions", 0),
        ),
        "analysis_policy_apply_event_created": (counts.get("analysis_policy_apply_events", 0),),
        "analysis_created": (counts.get("analysis_rows", 0),),
        "notification_plan_intent_event_created": (counts.get("notification_plan_intent_events", 0),),
        "notification_plan_created": (counts.get("notification_plan_rows", 0),),
        "notification_render_created": (counts.get("notification_render_rows", 0),),
        "notification_delivery_record_created": (counts.get("notification_delivery_record_rows", 0),),
        "notification_delivery_state_transition_created": (
            counts.get("notification_delivery_state_transitions", 0),
        ),
        "notification_delivery_result_event_created": (counts.get("notification_delivery_result_events", 0),),
    }
    stage_flags = {key: all(value == 1 for value in values) for key, values in stage_counts.items()}
    failures = [f"{key}:missing" for key, value in stage_flags.items() if value is not True]
    return FullChainVerificationResult(
        source_message_created=stage_flags["source_message_created"],
        artifact_created=stage_flags["artifact_created"],
        candidate_group_created=stage_flags["candidate_group_created"],
        artifact_snapshot_created=stage_flags["artifact_snapshot_created"],
        evidence_bundle_created=stage_flags["evidence_bundle_created"],
        analysis_requested_event_created=stage_flags["analysis_requested_event_created"],
        judge_run_created=stage_flags["judge_run_created"],
        judge_call_requested_event_created=stage_flags["judge_call_requested_event_created"],
        judge_output_created=stage_flags["judge_output_created"],
        judge_output_ready_event_created=stage_flags["judge_output_ready_event_created"],
        analysis_validated_state_transition_created=stage_flags["analysis_validated_state_transition_created"],
        analysis_policy_apply_event_created=stage_flags["analysis_policy_apply_event_created"],
        analysis_created=stage_flags["analysis_created"],
        notification_plan_intent_event_created=stage_flags["notification_plan_intent_event_created"],
        notification_plan_created=stage_flags["notification_plan_created"],
        notification_render_created=stage_flags["notification_render_created"],
        notification_delivery_record_created=stage_flags["notification_delivery_record_created"],
        notification_delivery_state_transition_created=stage_flags["notification_delivery_state_transition_created"],
        notification_delivery_result_event_created=stage_flags["notification_delivery_result_event_created"],
        checks_failed=tuple(failures),
    )


def _base_report() -> dict[str, Any]:
    return {
        "schema_version": SCHEMA_VERSION,
        "status": "pass",
        "database_url_guard_passed": False,
        "source_message_created": False,
        "artifact_created": False,
        "candidate_group_created": False,
        "artifact_snapshot_created": False,
        "evidence_bundle_created": False,
        "analysis_requested_event_created": False,
        "judge_run_created": False,
        "judge_call_requested_event_created": False,
        "judge_output_created": False,
        "judge_output_ready_event_created": False,
        "analysis_validated_state_transition_created": False,
        "analysis_policy_apply_event_created": False,
        "analysis_created": False,
        "notification_plan_intent_event_created": False,
        "notification_plan_created": False,
        "notification_render_created": False,
        "notification_delivery_record_created": False,
        "notification_delivery_state_transition_created": False,
        "notification_delivery_result_event_created": False,
        "telegram_called": False,
        "send_message_called": False,
        "edit_message_called": False,
        "openai_called": False,
        "live_github_called": False,
        "live_telegram_called": False,
        "workers_started": False,
        "redis_mutation": False,
        "production_db_write": False,
        "alembic_or_ddl_ran": False,
        "checks_failed": [],
    }


def _finish(report: dict[str, Any], checks_failed: Sequence[str]) -> RunnerResult:
    normalized_failures = list(dict.fromkeys(checks_failed))
    report["checks_failed"] = normalized_failures
    report["status"] = "fail" if normalized_failures else "pass"
    return RunnerResult(exit_code=0 if report["status"] == "pass" else 1, report=report)


def _scalar(connection: Any, sql: str, params: Mapping[str, Any]) -> Any:
    import sqlalchemy as sa

    return connection.execute(sa.text(sql), params).scalar_one_or_none()


def _count(connection: Any, sql: str, params: Mapping[str, Any]) -> int:
    import sqlalchemy as sa

    return int(connection.execute(sa.text(sql), params).scalar_one())


def _uuid_or_zero(value: Any) -> UUID:
    if value is None:
        return ZERO_UUID
    return UUID(str(value))


def _json_dumps(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, default=str)


def _safe_failure_code(exc: Exception) -> str:
    message = str(exc)
    if message in SAFE_EXCEPTION_MESSAGES:
        return message
    return exc.__class__.__name__


def _bootstrap_repo_imports() -> None:
    repo_root = _repo_root()
    src_root = repo_root / "src"
    for path in (repo_root, src_root):
        value = str(path)
        if value not in sys.path:
            sys.path.insert(0, value)


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    result = run(args)
    sys.stdout.write(render_json(result.report))
    return result.exit_code


if __name__ == "__main__":
    raise SystemExit(main())
