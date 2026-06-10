from __future__ import annotations

import argparse
import json
import os
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping, Protocol, Sequence
from uuid import UUID

from tools import local_db_policy_engine_fixture_replay_runner as policy_runner


SCHEMA_VERSION = "local_db_judge_output_ready_policy_apply_fixture_runner_v1"
JUDGE_OUTPUT_READY_EVENT_TYPE = "judge.output.ready.v1"
ANALYSIS_POLICY_APPLY_EVENT_TYPE = "analysis.policy.apply.v1"
NOTIFICATION_PLAN_CREATED_EVENT_TYPE = "notification.plan.created.v1"
NOTIFICATION_SIDE_EFFECT_FAILURES = frozenset(
    {
        "notification_plan_created:unexpected",
        "notification_render_created:unexpected",
        "notification_delivery_created:unexpected",
    }
)

validator_runner = policy_runner.validator_runner
fake_judge_runner = validator_runner.fake_judge_runner
analysis_router_runner = fake_judge_runner.analysis_router_runner
evidence_bundle_runner = analysis_router_runner.evidence_bundle_runner
source_candidate_runner = evidence_bundle_runner.source_candidate_runner
github_snapshot_runner = evidence_bundle_runner.github_snapshot_runner

SIDE_EFFECT_FALSE_KEYS = (
    "openai_called",
    "telegram_called",
    "live_github_called",
    "workers_started",
    "redis_mutation",
    "production_db_write",
    "alembic_or_ddl_ran",
    "notification_created",
)
SAFE_EXCEPTION_MESSAGES = {
    "judge_output_ready_event_missing_or_invalid",
    "judge_output_ready_event_ambiguous",
    "judge_run_missing",
    "judge_output_missing",
    "bundle_context_missing",
    "candidate_context_missing",
    "durable_relation_invalid",
    "structured_output_schema_invalid",
    "semantic_validation_failed",
    "model_refusal",
    "analysis_policy_apply_event_missing_or_invalid",
}


@dataclass(frozen=True, slots=True)
class RunnerResult:
    exit_code: int
    report: dict[str, Any]


@dataclass(frozen=True, slots=True)
class ReadyEventResolutionResult:
    judge_output_ready_event_id: UUID | None
    judge_output_ready_event_found: bool
    checks_failed: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class ReplayExecutionResult:
    judge_output_ready_event_found: bool
    judge_run_loaded: bool
    judge_output_loaded: bool
    evidence_bundle_loaded: bool
    candidate_group_loaded: bool
    analysis_validator_passed: bool
    refusal_stopped_at_validator: bool
    analysis_policy_apply_event_created: bool
    policy_engine_applied: bool
    analysis_created: bool
    notification_plan_created_event_created: bool
    notification_created: bool
    analysis_policy_apply_event_count: int = 0
    analysis_row_count: int = 0
    notification_plan_created_event_count: int = 0
    checks_failed: tuple[str, ...] = ()


class JudgeOutputReadyResolver(Protocol):
    def resolve(
        self,
        *,
        database_url: str,
        replay_namespace: str,
        source_fixture_path: Path | None,
        github_snapshot_fixture_path: Path | None,
        env: Mapping[str, str],
        repo_root: Path,
    ) -> ReadyEventResolutionResult: ...


class JudgeOutputReadyPolicyApplyExecutor(Protocol):
    def execute(
        self,
        *,
        database_url: str,
        replay_namespace: str,
        judge_output_ready_event_id: UUID,
    ) -> ReplayExecutionResult: ...


class DefaultJudgeOutputReadyResolver:
    def resolve(
        self,
        *,
        database_url: str,
        replay_namespace: str,
        source_fixture_path: Path | None,
        github_snapshot_fixture_path: Path | None,
        env: Mapping[str, str],
        repo_root: Path,
    ) -> ReadyEventResolutionResult:
        _bootstrap_repo_imports()

        first_lookup = _find_ready_event_ids_by_namespace(
            database_url=database_url,
            replay_namespace=replay_namespace,
        )
        if len(first_lookup) > 1:
            return ReadyEventResolutionResult(
                judge_output_ready_event_id=None,
                judge_output_ready_event_found=False,
                checks_failed=("judge_output_ready_event_ambiguous",),
            )
        if len(first_lookup) == 1:
            return ReadyEventResolutionResult(
                judge_output_ready_event_id=first_lookup[0],
                judge_output_ready_event_found=True,
            )

        if source_fixture_path is None or github_snapshot_fixture_path is None:
            return ReadyEventResolutionResult(
                judge_output_ready_event_id=None,
                judge_output_ready_event_found=False,
                checks_failed=("judge_output_ready_event_missing_or_invalid",),
            )

        args = argparse.Namespace(
            database_url=database_url,
            source_fixture=str(source_fixture_path),
            github_snapshot_fixture=str(github_snapshot_fixture_path),
            replay_namespace=replay_namespace,
            confirm_local_test_db=True,
        )
        predecessor_env = dict(env)
        predecessor_env["APP_ENV"] = "test"
        predecessor = fake_judge_runner.run(args, env=predecessor_env, repo_root=repo_root)
        if not _fake_judge_predecessor_acceptable(predecessor):
            return ReadyEventResolutionResult(
                judge_output_ready_event_id=None,
                judge_output_ready_event_found=False,
                checks_failed=("judge_output_ready_fixture_replay_failed",),
            )

        second_lookup = _find_ready_event_ids_by_namespace(
            database_url=database_url,
            replay_namespace=replay_namespace,
        )
        if len(second_lookup) > 1:
            return ReadyEventResolutionResult(
                judge_output_ready_event_id=None,
                judge_output_ready_event_found=False,
                checks_failed=("judge_output_ready_event_ambiguous",),
            )
        if len(second_lookup) != 1:
            return ReadyEventResolutionResult(
                judge_output_ready_event_id=None,
                judge_output_ready_event_found=False,
                checks_failed=("judge_output_ready_event_missing_or_invalid",),
            )
        return ReadyEventResolutionResult(
            judge_output_ready_event_id=second_lookup[0],
            judge_output_ready_event_found=True,
        )


class SqlAlchemyJudgeOutputReadyPolicyApplyExecutor:
    def execute(
        self,
        *,
        database_url: str,
        replay_namespace: str,
        judge_output_ready_event_id: UUID,
    ) -> ReplayExecutionResult:
        _bootstrap_repo_imports()
        import sqlalchemy as sa

        engine = sa.create_engine(database_url, future=True)
        try:
            with engine.begin() as connection:
                return _execute_judge_output_ready_policy_apply(
                    connection,
                    replay_namespace=replay_namespace,
                    judge_output_ready_event_id=judge_output_ready_event_id,
                )
        finally:
            engine.dispose()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Consume one local/test DB judge.output.ready.v1 event, run the "
            "analysis-validator handoff, apply deterministic policy, and stop "
            "at the notification.plan.created.v1 plan-intent boundary."
        )
    )
    parser.add_argument("--database-url", required=True)
    parser.add_argument("--judge-output-ready-event-id")
    parser.add_argument("--source-fixture")
    parser.add_argument("--github-snapshot-fixture")
    parser.add_argument("--replay-namespace")
    parser.add_argument("--confirm-local-test-db", action="store_true")
    return parser


def render_json(report: Mapping[str, Any]) -> str:
    return json.dumps(report, indent=2) + "\n"


def run(
    args: argparse.Namespace,
    *,
    env: Mapping[str, str] | None = None,
    resolver: JudgeOutputReadyResolver | None = None,
    executor: JudgeOutputReadyPolicyApplyExecutor | None = None,
    repo_root: Path | None = None,
) -> RunnerResult:
    effective_env = env if env is not None else os.environ
    root = repo_root or _repo_root()
    report = _base_report()
    checks_failed: list[str] = []

    if not args.confirm_local_test_db:
        checks_failed.append("confirm_local_test_db_required")

    if effective_env.get("APP_ENV", "").strip().lower() != "test":
        checks_failed.append("app_env_test_required")

    database_ok, database_failures, _ = validate_database_url(args.database_url)
    report["database_url_guard_passed"] = database_ok
    checks_failed.extend(database_failures)

    explicit_ready_event_id = _uuid_or_none(getattr(args, "judge_output_ready_event_id", None))
    if getattr(args, "judge_output_ready_event_id", None) and explicit_ready_event_id is None:
        checks_failed.append("judge_output_ready_event_id_invalid")

    source_fixture = _path_or_none(getattr(args, "source_fixture", None))
    github_fixture = _path_or_none(getattr(args, "github_snapshot_fixture", None))
    fixture_pair_supplied = source_fixture is not None or github_fixture is not None
    if fixture_pair_supplied and (source_fixture is None or github_fixture is None):
        checks_failed.append("fixture_path_pair_required")

    supplied_namespace = _string_or_none(getattr(args, "replay_namespace", None))
    if explicit_ready_event_id is None and supplied_namespace is None:
        checks_failed.append("ready_event_selector_required")

    if supplied_namespace is not None:
        namespace_ok, namespace_failures = source_candidate_runner.validate_replay_namespace(supplied_namespace)
        checks_failed.extend(namespace_failures)
        if not namespace_ok:
            supplied_namespace = None

    if explicit_ready_event_id is None and fixture_pair_supplied and source_fixture and github_fixture:
        try:
            source_candidate_runner.load_source_fixture(source_fixture, repo_root=root)
        except Exception:  # noqa: BLE001 - operator output must stay sanitized.
            checks_failed.append("source_fixture_load_failed")
        try:
            github_snapshot_runner.load_github_snapshot_fixture(github_fixture, repo_root=root)
        except Exception:  # noqa: BLE001 - operator output must stay sanitized.
            checks_failed.append("github_snapshot_fixture_load_failed")

    if checks_failed:
        return _finish(report, checks_failed)

    replay_namespace = (
        _namespace_for_explicit_ready_event(explicit_ready_event_id)
        if explicit_ready_event_id is not None
        else supplied_namespace
    )
    if replay_namespace is None:
        return _finish(report, ["ready_event_selector_required"])

    ready_event_id = explicit_ready_event_id
    if ready_event_id is None:
        active_resolver = resolver or DefaultJudgeOutputReadyResolver()
        try:
            resolution = active_resolver.resolve(
                database_url=args.database_url,
                replay_namespace=replay_namespace,
                source_fixture_path=source_fixture,
                github_snapshot_fixture_path=github_fixture,
                env=effective_env,
                repo_root=root,
            )
        except Exception:  # noqa: BLE001 - never echo DB errors or URLs.
            return _finish(report, ["judge_output_ready_event_resolution_failed"])
        report["judge_output_ready_event_found"] = resolution.judge_output_ready_event_found
        checks_failed.extend(resolution.checks_failed)
        ready_event_id = resolution.judge_output_ready_event_id
        if ready_event_id is None:
            checks_failed.append("judge_output_ready_event_missing_or_invalid")
        if checks_failed:
            return _finish(report, checks_failed)

    active_executor = executor or SqlAlchemyJudgeOutputReadyPolicyApplyExecutor()
    try:
        execution = active_executor.execute(
            database_url=args.database_url,
            replay_namespace=replay_namespace,
            judge_output_ready_event_id=ready_event_id,
        )
    except Exception as exc:  # noqa: BLE001 - never echo DB errors or URLs.
        return _finish(report, [_safe_failure_code(exc)])

    report.update(
        {
            "judge_output_ready_event_found": execution.judge_output_ready_event_found,
            "judge_run_loaded": execution.judge_run_loaded,
            "judge_output_loaded": execution.judge_output_loaded,
            "evidence_bundle_loaded": execution.evidence_bundle_loaded,
            "candidate_group_loaded": execution.candidate_group_loaded,
            "analysis_validator_passed": execution.analysis_validator_passed,
            "refusal_stopped_at_validator": execution.refusal_stopped_at_validator,
            "analysis_policy_apply_event_created": execution.analysis_policy_apply_event_created,
            "policy_engine_applied": execution.policy_engine_applied,
            "analysis_created": execution.analysis_created,
            "notification_plan_created_event_created": execution.notification_plan_created_event_created,
            "notification_created": execution.notification_created,
        }
    )
    checks_failed.extend(execution.checks_failed)
    checks_failed.extend(_execution_consistency_failures(execution))

    for key in SIDE_EFFECT_FALSE_KEYS:
        if report.get(key) is not False:
            checks_failed.append(f"{key}:unexpected")

    return _finish(report, checks_failed)


def validate_database_url(database_url: str | None):
    return policy_runner.validate_database_url(database_url)


def _execute_judge_output_ready_policy_apply(
    connection: Any,
    *,
    replay_namespace: str,
    judge_output_ready_event_id: UUID,
) -> ReplayExecutionResult:
    checks_failed: list[str] = []

    event = _load_judge_output_ready_event_by_id(connection, judge_output_ready_event_id)
    if event is None:
        return _execution_result(
            judge_output_ready_event_found=False,
            checks_failed=("judge_output_ready_event_missing_or_invalid",),
        )

    judge_run = validator_runner._load_judge_run(connection, event.judge_run_id)
    if judge_run is None:
        validator_runner._insert_or_reuse_state_transition(
            connection,
            object_id=event.judge_run_id,
            from_state=None,
            to_state="analysis_failed_missing_run",
            reason_code="validator_missing_judge_run",
        )
        return _execution_result(
            judge_output_ready_event_found=True,
            checks_failed=("judge_run_missing",),
        )

    judge_output = validator_runner._load_judge_output(connection, event.judge_output_id)
    if judge_output is None:
        validator_runner._insert_or_reuse_state_transition(
            connection,
            object_id=judge_run.judge_run_id,
            from_state=judge_run.status,
            to_state="analysis_failed_missing_output",
            reason_code="validator_missing_judge_output",
        )
        return _execution_result(
            judge_output_ready_event_found=True,
            judge_run_loaded=True,
            checks_failed=("judge_output_missing",),
        )

    bundle = validator_runner._load_bundle_context(connection, judge_run.bundle_id)
    if bundle is None:
        validator_runner._insert_or_reuse_state_transition(
            connection,
            object_id=judge_run.judge_run_id,
            from_state=judge_run.status,
            to_state="analysis_failed_identity_mismatch",
            reason_code="validator_bundle_missing",
        )
        return _execution_result(
            judge_output_ready_event_found=True,
            judge_run_loaded=True,
            judge_output_loaded=True,
            checks_failed=("bundle_context_missing",),
        )

    refusal_detected = validator_runner._is_refusal(event, judge_run, judge_output)
    relation_failures = _durable_relation_failures(
        event=event,
        judge_run=judge_run,
        judge_output=judge_output,
        bundle=bundle,
        allow_refusal=refusal_detected,
    )
    if relation_failures:
        validator_runner._insert_or_reuse_state_transition(
            connection,
            object_id=judge_run.judge_run_id,
            from_state=judge_run.status,
            to_state="analysis_failed_identity_mismatch",
            reason_code="validator_durable_relation_invalid",
        )
        return _execution_result(
            judge_output_ready_event_found=True,
            judge_run_loaded=True,
            judge_output_loaded=True,
            evidence_bundle_loaded=True,
            candidate_group_loaded=True,
            checks_failed=tuple(relation_failures),
        )

    if refusal_detected:
        validator_runner._insert_or_reuse_state_transition(
            connection,
            object_id=judge_run.judge_run_id,
            from_state=judge_run.status,
            to_state=validator_runner.REFUSED_TO_STATE,
            reason_code=validator_runner.REFUSED_REASON_CODE,
        )
        notification_created = _notification_durable_exists(
            connection,
            candidate_group_id=judge_output.candidate_group_id,
        )
        return _execution_result(
            judge_output_ready_event_found=True,
            judge_run_loaded=True,
            judge_output_loaded=True,
            evidence_bundle_loaded=True,
            candidate_group_loaded=True,
            refusal_stopped_at_validator=True,
            notification_created=notification_created,
        )

    output_before = validator_runner._canonical_json(judge_output.payload_json)
    schema_valid, schema_failures = validator_runner.validate_judge_output_v1_payload(judge_output.payload_json)
    if not schema_valid:
        validator_runner._insert_or_reuse_state_transition(
            connection,
            object_id=judge_run.judge_run_id,
            from_state=judge_run.status,
            to_state="analysis_failed_schema",
            reason_code="validator_schema_invalid",
        )
        return _execution_result(
            judge_output_ready_event_found=True,
            judge_run_loaded=True,
            judge_output_loaded=True,
            evidence_bundle_loaded=True,
            candidate_group_loaded=True,
            checks_failed=tuple(schema_failures or ("structured_output_schema_invalid",)),
        )

    semantic_valid, semantic_failures = validator_runner.validate_semantic_consistency(
        judge_output.payload_json,
        judge_output,
    )
    if not semantic_valid:
        validator_runner._insert_or_reuse_state_transition(
            connection,
            object_id=judge_run.judge_run_id,
            from_state=judge_run.status,
            to_state="analysis_failed_semantic",
            reason_code="validator_semantic_invalid",
        )
        return _execution_result(
            judge_output_ready_event_found=True,
            judge_run_loaded=True,
            judge_output_loaded=True,
            evidence_bundle_loaded=True,
            candidate_group_loaded=True,
            checks_failed=tuple(semantic_failures or ("semantic_validation_failed",)),
        )

    validator_runner._insert_or_reuse_state_transition(
        connection,
        object_id=judge_run.judge_run_id,
        from_state=judge_run.status,
        to_state=validator_runner.VALIDATED_TO_STATE,
        reason_code=validator_runner.VALIDATED_REASON_CODE,
    )
    validator_runner._insert_or_reuse_analysis_policy_apply_event(
        connection,
        replay_namespace=replay_namespace,
        judge_run_id=judge_run.judge_run_id,
        judge_output_id=judge_output.judge_output_id,
        candidate_group_id=judge_output.candidate_group_id,
        bundle_id=bundle.bundle_id,
    )

    policy_event_count = _analysis_policy_apply_event_count(
        connection,
        replay_namespace=replay_namespace,
        judge_run_id=judge_run.judge_run_id,
        judge_output_id=judge_output.judge_output_id,
    )
    output_after = validator_runner._load_judge_output(connection, judge_output.judge_output_id)
    if output_after is None or validator_runner._canonical_json(output_after.payload_json) != output_before:
        checks_failed.append("judge_output_mutated")

    if policy_event_count != 1:
        checks_failed.append("analysis_policy_apply_event_count_not_one")
        return _execution_result(
            judge_output_ready_event_found=True,
            judge_run_loaded=True,
            judge_output_loaded=True,
            evidence_bundle_loaded=True,
            candidate_group_loaded=True,
            analysis_validator_passed=True,
            analysis_policy_apply_event_created=policy_event_count == 1,
            analysis_policy_apply_event_count=policy_event_count,
            checks_failed=tuple(checks_failed),
        )

    notification_side_effects_before = _load_notification_side_effect_flags(
        connection,
        candidate_group_id=judge_output.candidate_group_id,
    )
    policy_result = policy_runner._execute_policy_engine_replay(connection, replay_namespace=replay_namespace)
    notification_side_effects_after = _load_notification_side_effect_flags(
        connection,
        candidate_group_id=judge_output.candidate_group_id,
    )
    notification_created = _notification_side_effect_created(
        before=notification_side_effects_before,
        after=notification_side_effects_after,
    )
    checks_failed.extend(
        _policy_result_failures(
            policy_result,
            notification_side_effects_unchanged=notification_side_effects_before == notification_side_effects_after,
        )
    )
    analysis_id = _load_analysis_id(connection, judge_output_id=judge_output.judge_output_id)
    analysis_count = _analysis_row_count(connection, judge_output_id=judge_output.judge_output_id)
    notification_event_count = _notification_plan_created_event_count(connection, analysis_id=analysis_id)

    return _execution_result(
        judge_output_ready_event_found=True,
        judge_run_loaded=True,
        judge_output_loaded=True,
        evidence_bundle_loaded=True,
        candidate_group_loaded=True,
        analysis_validator_passed=not checks_failed,
        analysis_policy_apply_event_created=policy_event_count == 1,
        policy_engine_applied=policy_result.policy_state_transition_recorded,
        analysis_created=policy_result.analysis_created,
        notification_plan_created_event_created=policy_result.notification_plan_intent_event_created,
        notification_created=notification_created,
        analysis_policy_apply_event_count=policy_event_count,
        analysis_row_count=analysis_count,
        notification_plan_created_event_count=notification_event_count,
        checks_failed=tuple(dict.fromkeys(checks_failed)),
    )


def _load_judge_output_ready_event_by_id(
    connection: Any,
    event_id: UUID,
) -> validator_runner.JudgeOutputReadyEvent | None:
    import sqlalchemy as sa

    row = connection.execute(
        sa.text(
            """
            SELECT event_id, payload_json
            FROM event_outbox
            WHERE event_id = CAST(:event_id AS uuid)
              AND event_type = :event_type
            """
        ),
        {"event_id": str(event_id), "event_type": JUDGE_OUTPUT_READY_EVENT_TYPE},
    ).mappings().first()
    if row is None:
        return None

    payload = _json_loads(row["payload_json"]) or {}
    judge_run_id = _uuid_or_none(payload.get("judge_run_id"))
    judge_output_id = _uuid_or_none(payload.get("judge_output_id"))
    finish_reason = _string_or_none(payload.get("finish_reason"))
    refusal_detected = payload.get("refusal_detected")
    if (
        judge_run_id is None
        or judge_output_id is None
        or finish_reason is None
        or not isinstance(refusal_detected, bool)
    ):
        return None
    return validator_runner.JudgeOutputReadyEvent(
        event_id=UUID(str(row["event_id"])),
        judge_run_id=judge_run_id,
        judge_output_id=judge_output_id,
        finish_reason=finish_reason,
        refusal_detected=refusal_detected,
    )


def _durable_relation_failures(
    *,
    event: validator_runner.JudgeOutputReadyEvent,
    judge_run: validator_runner.JudgeRunRecord,
    judge_output: validator_runner.JudgeOutputRecord,
    bundle: validator_runner.BundleContext,
    allow_refusal: bool,
) -> list[str]:
    failures: list[str] = []
    if judge_run.judge_run_id != event.judge_run_id:
        failures.append("judge_run_id_mismatch")
    if judge_run.status != "succeeded":
        failures.append("judge_run_status_not_succeeded")
    if judge_run.refusal_detected and not allow_refusal:
        failures.append("judge_run_refusal_detected")
    if judge_output.judge_output_id != event.judge_output_id:
        failures.append("judge_output_id_mismatch")
    if judge_output.judge_run_id != event.judge_run_id:
        failures.append("judge_output_run_mismatch")
    if judge_output.candidate_group_id != bundle.candidate_group_id:
        failures.append("judge_output_bundle_candidate_mismatch")
    if judge_run.bundle_id != bundle.bundle_id:
        failures.append("judge_run_bundle_mismatch")
    if bundle.current_bundle_id != bundle.bundle_id:
        failures.append("candidate_current_bundle_mismatch")
    if not bundle.ready_for_analysis:
        failures.append("evidence_bundle_not_ready")
    return failures


def _find_ready_event_ids_by_namespace(*, database_url: str, replay_namespace: str) -> tuple[UUID, ...]:
    _bootstrap_repo_imports()
    import sqlalchemy as sa

    engine = sa.create_engine(database_url, future=True)
    try:
        with engine.connect() as connection:
            rows = connection.execute(
                sa.text(
                    """
                    SELECT event_id
                    FROM event_outbox
                    WHERE event_type = :event_type
                      AND strpos(dedupe_key, :namespace_marker) > 0
                    ORDER BY created_at DESC, event_id DESC
                    LIMIT 2
                    """
                ),
                {
                    "event_type": JUDGE_OUTPUT_READY_EVENT_TYPE,
                    "namespace_marker": f":{replay_namespace}:judge.output.ready:",
                },
            ).scalars().all()
    finally:
        engine.dispose()
    return tuple(UUID(str(row)) for row in rows)


def _analysis_policy_apply_event_count(
    connection: Any,
    *,
    replay_namespace: str,
    judge_run_id: UUID,
    judge_output_id: UUID,
) -> int:
    import sqlalchemy as sa

    return int(
        connection.execute(
            sa.text(
                """
                SELECT count(*)
                FROM event_outbox
                WHERE event_type = :event_type
                  AND dedupe_key = :dedupe_key
                """
            ),
            {
                "event_type": ANALYSIS_POLICY_APPLY_EVENT_TYPE,
                "dedupe_key": validator_runner.build_analysis_policy_apply_dedupe_key(
                    replay_namespace=replay_namespace,
                    judge_run_id=judge_run_id,
                    judge_output_id=judge_output_id,
                ),
            },
        ).scalar_one()
    )


def _analysis_row_count(connection: Any, *, judge_output_id: UUID) -> int:
    import sqlalchemy as sa

    return int(
        connection.execute(
            sa.text(
                """
                SELECT count(*)
                FROM analyses
                WHERE judge_output_id = CAST(:judge_output_id AS uuid)
                  AND policy_version = :policy_version
                  AND delivery_policy_version = :delivery_policy_version
                """
            ),
            {
                "judge_output_id": str(judge_output_id),
                "policy_version": policy_runner.POLICY_VERSION,
                "delivery_policy_version": policy_runner.DELIVERY_POLICY_VERSION,
            },
        ).scalar_one()
    )


def _load_analysis_id(connection: Any, *, judge_output_id: UUID) -> UUID | None:
    import sqlalchemy as sa

    value = connection.execute(
        sa.text(
            """
            SELECT analysis_id
            FROM analyses
            WHERE judge_output_id = CAST(:judge_output_id AS uuid)
              AND policy_version = :policy_version
              AND delivery_policy_version = :delivery_policy_version
            ORDER BY created_at DESC, analysis_id DESC
            LIMIT 1
            """
        ),
        {
            "judge_output_id": str(judge_output_id),
            "policy_version": policy_runner.POLICY_VERSION,
            "delivery_policy_version": policy_runner.DELIVERY_POLICY_VERSION,
        },
    ).scalar_one_or_none()
    return UUID(str(value)) if value is not None else None


def _notification_plan_created_event_count(connection: Any, *, analysis_id: UUID | None) -> int:
    if analysis_id is None:
        return 0

    import sqlalchemy as sa

    return int(
        connection.execute(
            sa.text(
                """
                SELECT count(*)
                FROM event_outbox
                WHERE event_type = :event_type
                  AND aggregate_type = 'analysis'
                  AND aggregate_id = CAST(:analysis_id AS uuid)
                """
            ),
            {
                "event_type": NOTIFICATION_PLAN_CREATED_EVENT_TYPE,
                "analysis_id": str(analysis_id),
            },
        ).scalar_one()
    )


def _notification_durable_exists(connection: Any, *, candidate_group_id: UUID) -> bool:
    forbidden = policy_runner._load_forbidden_side_effect_flags(
        connection,
        candidate_group_id=candidate_group_id,
    )
    return any(forbidden.values())


def _fake_judge_predecessor_acceptable(predecessor: fake_judge_runner.RunnerResult) -> bool:
    if predecessor.exit_code != 0 or predecessor.report.get("status") != "pass":
        return False
    required_true = (
        "judge_call_requested_event_found",
        "judge_run_pending_confirmed",
        "bundle_context_loaded",
        "fake_judge_output_created_or_reused",
        "judge_run_succeeded",
        "judge_output_ready_event_created",
        "structured_output_schema_valid",
        "usage_telemetry_recorded",
    )
    required_false = (
        "openai_called",
        "live_github_called",
        "live_telegram_called",
        "workers_started",
        "redis_mutation",
        "production_db_write",
        "analysis_created",
        "notification_created",
    )
    return all(predecessor.report.get(key) is True for key in required_true) and all(
        predecessor.report.get(key) is False for key in required_false
    )


def _execution_consistency_failures(execution: ReplayExecutionResult) -> list[str]:
    failures: list[str] = []
    common_required = (
        "judge_output_ready_event_found",
        "judge_run_loaded",
        "judge_output_loaded",
        "evidence_bundle_loaded",
        "candidate_group_loaded",
    )
    for key in common_required:
        if getattr(execution, key) is not True:
            failures.append(f"{key}:missing")

    if execution.refusal_stopped_at_validator:
        if execution.analysis_policy_apply_event_created:
            failures.append("analysis_policy_apply_event_created:unexpected")
        if execution.policy_engine_applied:
            failures.append("policy_engine_applied:unexpected")
        if execution.analysis_created:
            failures.append("analysis_created:unexpected")
        if execution.notification_plan_created_event_created:
            failures.append("notification_plan_created_event_created:unexpected")
        if execution.analysis_policy_apply_event_count != 0:
            failures.append("analysis_policy_apply_event_count_not_zero")
        if execution.analysis_row_count != 0:
            failures.append("analysis_row_count_not_zero")
        if execution.notification_plan_created_event_count != 0:
            failures.append("notification_plan_created_event_count_not_zero")
        return failures

    required_for_policy = (
        "analysis_validator_passed",
        "analysis_policy_apply_event_created",
        "policy_engine_applied",
        "analysis_created",
    )
    for key in required_for_policy:
        if getattr(execution, key) is not True:
            failures.append(f"{key}:missing")

    if execution.analysis_policy_apply_event_count != 1:
        failures.append("analysis_policy_apply_event_count_not_one")
    if execution.analysis_row_count != 1:
        failures.append("analysis_row_count_not_one")
    if execution.notification_plan_created_event_created and execution.notification_plan_created_event_count != 1:
        failures.append("notification_plan_created_event_count_not_one")
    if not execution.notification_plan_created_event_created and execution.notification_plan_created_event_count != 0:
        failures.append("notification_plan_created_event_count_unexpected")
    return failures


def _load_notification_side_effect_flags(connection: Any, *, candidate_group_id: UUID) -> dict[str, bool]:
    return policy_runner._load_forbidden_side_effect_flags(connection, candidate_group_id=candidate_group_id)


def _notification_side_effect_created(*, before: Mapping[str, bool], after: Mapping[str, bool]) -> bool:
    return any(bool(after.get(key)) and not bool(before.get(key)) for key in after)


def _policy_result_failures(
    policy_result: policy_runner.ReplayExecutionResult,
    *,
    notification_side_effects_unchanged: bool,
) -> tuple[str, ...]:
    if not notification_side_effects_unchanged:
        return policy_result.checks_failed
    return tuple(
        failure for failure in policy_result.checks_failed if failure not in NOTIFICATION_SIDE_EFFECT_FAILURES
    )


def _execution_result(
    *,
    judge_output_ready_event_found: bool = False,
    judge_run_loaded: bool = False,
    judge_output_loaded: bool = False,
    evidence_bundle_loaded: bool = False,
    candidate_group_loaded: bool = False,
    analysis_validator_passed: bool = False,
    refusal_stopped_at_validator: bool = False,
    analysis_policy_apply_event_created: bool = False,
    policy_engine_applied: bool = False,
    analysis_created: bool = False,
    notification_plan_created_event_created: bool = False,
    notification_created: bool = False,
    analysis_policy_apply_event_count: int = 0,
    analysis_row_count: int = 0,
    notification_plan_created_event_count: int = 0,
    checks_failed: Sequence[str] = (),
) -> ReplayExecutionResult:
    return ReplayExecutionResult(
        judge_output_ready_event_found=judge_output_ready_event_found,
        judge_run_loaded=judge_run_loaded,
        judge_output_loaded=judge_output_loaded,
        evidence_bundle_loaded=evidence_bundle_loaded,
        candidate_group_loaded=candidate_group_loaded,
        analysis_validator_passed=analysis_validator_passed,
        refusal_stopped_at_validator=refusal_stopped_at_validator,
        analysis_policy_apply_event_created=analysis_policy_apply_event_created,
        policy_engine_applied=policy_engine_applied,
        analysis_created=analysis_created,
        notification_plan_created_event_created=notification_plan_created_event_created,
        notification_created=notification_created,
        analysis_policy_apply_event_count=analysis_policy_apply_event_count,
        analysis_row_count=analysis_row_count,
        notification_plan_created_event_count=notification_plan_created_event_count,
        checks_failed=tuple(dict.fromkeys(checks_failed)),
    )


def _base_report() -> dict[str, Any]:
    return {
        "schema_version": SCHEMA_VERSION,
        "status": "pass",
        "database_url_guard_passed": False,
        "judge_output_ready_event_found": False,
        "judge_run_loaded": False,
        "judge_output_loaded": False,
        "evidence_bundle_loaded": False,
        "candidate_group_loaded": False,
        "analysis_validator_passed": False,
        "refusal_stopped_at_validator": False,
        "analysis_policy_apply_event_created": False,
        "policy_engine_applied": False,
        "analysis_created": False,
        "notification_plan_created_event_created": False,
        "openai_called": False,
        "telegram_called": False,
        "live_github_called": False,
        "workers_started": False,
        "redis_mutation": False,
        "production_db_write": False,
        "alembic_or_ddl_ran": False,
        "notification_created": False,
        "checks_failed": [],
    }


def _finish(report: dict[str, Any], checks_failed: Sequence[str]) -> RunnerResult:
    normalized_failures = list(dict.fromkeys(checks_failed))
    report["checks_failed"] = normalized_failures
    report["status"] = "fail" if normalized_failures else "pass"
    return RunnerResult(exit_code=0 if report["status"] == "pass" else 1, report=report)


def _namespace_for_explicit_ready_event(event_id: UUID) -> str:
    return f"event-{event_id}"


def _path_or_none(value: Any) -> Path | None:
    text = _string_or_none(value)
    return Path(text) if text is not None else None


def _uuid_or_none(value: Any) -> UUID | None:
    if value is None:
        return None
    try:
        return UUID(str(value))
    except (TypeError, ValueError, AttributeError):
        return None


def _string_or_none(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text if text else None


def _json_loads(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, str):
        return json.loads(value)
    return value


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
