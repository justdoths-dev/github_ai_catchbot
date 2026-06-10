from __future__ import annotations

import argparse
import json
import os
import sys
from dataclasses import dataclass
from numbers import Real
from pathlib import Path
from typing import Any, Mapping, Protocol, Sequence
from uuid import UUID

from tools import local_db_fake_judge_output_fixture_replay_runner as fake_judge_runner


SCHEMA_VERSION = "local_db_analysis_validator_fixture_replay_v1"
JUDGE_OUTPUT_READY_EVENT_TYPE = "judge.output.ready.v1"
ANALYSIS_POLICY_APPLY_EVENT_TYPE = "analysis.policy.apply.v1"
JUDGE_SCHEMA_VERSION = "judge_output_v1"
EXPECTED_FINISH_REASON = fake_judge_runner.FAKE_FINISH_REASON
EXPECTED_MODEL_PROPOSED_VERDICT = "later"
EXPECTED_MODEL_CONFIDENCE_BAND = "medium"
VALIDATED_TO_STATE = "analysis_validated"
VALIDATED_REASON_CODE = "judge_output_validated"
REFUSED_TO_STATE = "analysis_refused"
REFUSED_REASON_CODE = "model_refusal"
TRUE_RESULT_KEYS = (
    "database_url_guard_passed",
    "source_candidate_replay_confirmed",
    "artifact_snapshot_replay_confirmed",
    "evidence_bundle_replay_confirmed",
    "analysis_router_replay_confirmed",
    "judge_output_replay_confirmed",
    "judge_output_ready_event_found",
    "judge_run_succeeded_confirmed",
    "judge_output_loaded",
    "bundle_context_confirmed",
    "structured_output_schema_valid",
    "semantic_validation_passed",
    "refusal_not_detected",
    "validation_state_transition_recorded",
    "analysis_policy_apply_event_created",
)
FALSE_RESULT_KEYS = (
    "judge_output_mutated",
    "analysis_created",
    "notification_created",
    "production_db_write",
    "live_github_called",
    "live_telegram_called",
    "openai_called",
    "workers_started",
    "redis_mutation",
)
REQUIRED_OUTPUT_KEYS = (
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
)
REQUIRED_SCORE_KEYS = (
    "novelty",
    "practical_usefulness",
    "evidence_strength",
    "hype_penalty",
    "confidence",
    "code_quality",
    "maintenance_signal",
    "specificity",
    "reproducibility_signal",
)
REQUIRED_INTEGER_SCORE_KEYS = (
    "novelty",
    "practical_usefulness",
    "evidence_strength",
    "hype_penalty",
    "confidence",
)
OPTIONAL_NULLABLE_SCORE_KEYS = (
    "code_quality",
    "maintenance_signal",
    "specificity",
    "reproducibility_signal",
)
LEGACY_OPTIONAL_SCORE_KEYS = (
    "implementation_signal",
    "urgency",
)
STRING_FIELDS = (
    "judge_schema_version",
    "candidate_group_id",
    "headline",
    "summary_one_line_ko",
    "skeptical_take_ko",
    "why_it_might_matter_ko",
    "recommended_action_ko",
    "freshness_note_ko",
)
ARRAY_FIELDS = (
    "comparables",
    "reason_codes",
    "red_flags_ko",
    "evidence_limitations_ko",
)
ALLOWED_MODEL_VERDICTS = frozenset({"inspect_now", "later", "skip"})
ALLOWED_CONFIDENCE_BANDS = frozenset({"low", "medium", "high"})
SAFE_EXCEPTION_MESSAGES = {
    "judge_output_ready_event_missing_or_invalid",
    "judge_run_missing",
    "judge_output_missing",
    "bundle_context_missing",
    "durable_relation_invalid",
    "structured_output_schema_invalid",
    "semantic_validation_failed",
    "model_refusal",
}


@dataclass(frozen=True, slots=True)
class RunnerResult:
    exit_code: int
    report: dict[str, Any]


@dataclass(frozen=True, slots=True)
class ReplayExecutionResult:
    judge_output_ready_event_found: bool
    judge_run_succeeded_confirmed: bool
    judge_output_loaded: bool
    bundle_context_confirmed: bool
    structured_output_schema_valid: bool
    semantic_validation_passed: bool
    refusal_not_detected: bool
    validation_state_transition_recorded: bool
    analysis_policy_apply_event_created: bool
    judge_output_mutated: bool = False
    analysis_created: bool = False
    notification_created: bool = False
    checks_failed: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class JudgeOutputReadyEvent:
    event_id: UUID
    judge_run_id: UUID
    judge_output_id: UUID
    finish_reason: str
    refusal_detected: bool


@dataclass(frozen=True, slots=True)
class JudgeRunRecord:
    judge_run_id: UUID
    bundle_id: UUID
    status: str
    finish_reason: str | None
    refusal_detected: bool


@dataclass(frozen=True, slots=True)
class JudgeOutputRecord:
    judge_output_id: UUID
    judge_run_id: UUID
    candidate_group_id: UUID
    judge_schema_version: str
    payload_json: dict[str, Any]
    model_proposed_verdict: str | None
    model_confidence_band: str | None


@dataclass(frozen=True, slots=True)
class BundleContext:
    bundle_id: UUID
    candidate_group_id: UUID
    current_bundle_id: UUID | None
    ready_for_analysis: bool


class AnalysisValidatorReplayExecutor(Protocol):
    def execute(
        self,
        *,
        database_url: str,
        replay_namespace: str,
    ) -> ReplayExecutionResult: ...


class FakeJudgeOutputReplayRunner(Protocol):
    def run(
        self,
        *,
        database_url: str,
        source_fixture_path: Path,
        github_snapshot_fixture_path: Path,
        replay_namespace: str,
        env: Mapping[str, str],
        repo_root: Path,
    ) -> fake_judge_runner.RunnerResult: ...


class DefaultFakeJudgeOutputReplayRunner:
    def run(
        self,
        *,
        database_url: str,
        source_fixture_path: Path,
        github_snapshot_fixture_path: Path,
        replay_namespace: str,
        env: Mapping[str, str],
        repo_root: Path,
    ) -> fake_judge_runner.RunnerResult:
        args = argparse.Namespace(
            database_url=database_url,
            source_fixture=str(source_fixture_path),
            github_snapshot_fixture=str(github_snapshot_fixture_path),
            replay_namespace=replay_namespace,
            confirm_local_test_db=True,
        )
        predecessor_env = dict(env)
        predecessor_env["APP_ENV"] = "test"
        return fake_judge_runner.run(args, env=predecessor_env, repo_root=repo_root)


class SqlAlchemyAnalysisValidatorReplayExecutor:
    def execute(
        self,
        *,
        database_url: str,
        replay_namespace: str,
    ) -> ReplayExecutionResult:
        _bootstrap_repo_imports()
        import sqlalchemy as sa

        engine = sa.create_engine(database_url, future=True)
        try:
            with engine.begin() as connection:
                return _execute_analysis_validator_replay(connection, replay_namespace=replay_namespace)
        finally:
            engine.dispose()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Replay predecessor local/test DB fixtures through judge.output.ready, "
            "then validate the stored judge_output_v1 as a local analysis-validator "
            "fixture and emit one namespace-scoped analysis.policy.apply.v1 event."
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
    executor: AnalysisValidatorReplayExecutor | None = None,
    predecessor_runner: FakeJudgeOutputReplayRunner | None = None,
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

    namespace_ok, namespace_failures = (
        fake_judge_runner.analysis_router_runner.evidence_bundle_runner.source_candidate_runner.validate_replay_namespace(
            args.replay_namespace
        )
    )
    checks_failed.extend(namespace_failures)

    database_ok, database_failures, _ = validate_database_url(args.database_url)
    report["database_url_guard_passed"] = database_ok
    checks_failed.extend(database_failures)

    try:
        fake_judge_runner.analysis_router_runner.evidence_bundle_runner.source_candidate_runner.load_source_fixture(
            Path(args.source_fixture),
            repo_root=root,
        )
    except Exception:  # noqa: BLE001 - operator output must stay sanitized.
        checks_failed.append("source_fixture_load_failed")

    try:
        fake_judge_runner.analysis_router_runner.evidence_bundle_runner.github_snapshot_runner.load_github_snapshot_fixture(
            Path(args.github_snapshot_fixture),
            repo_root=root,
        )
    except Exception:  # noqa: BLE001 - operator output must stay sanitized.
        checks_failed.append("github_snapshot_fixture_load_failed")

    if checks_failed:
        return _finish(report, checks_failed)

    if not namespace_ok:
        checks_failed.append("precondition_missing")
        return _finish(report, checks_failed)

    active_predecessor = predecessor_runner or DefaultFakeJudgeOutputReplayRunner()
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
        checks_failed.append("fake_judge_output_replay_failed")
        return _finish(report, checks_failed)

    if not _predecessor_result_acceptable(predecessor):
        checks_failed.append("fake_judge_output_replay_failed")
        return _finish(report, checks_failed)

    report["source_candidate_replay_confirmed"] = predecessor.report.get("source_candidate_replay_confirmed") is True
    report["artifact_snapshot_replay_confirmed"] = predecessor.report.get("artifact_snapshot_replay_confirmed") is True
    report["evidence_bundle_replay_confirmed"] = predecessor.report.get("evidence_bundle_replay_confirmed") is True
    report["analysis_router_replay_confirmed"] = predecessor.report.get("analysis_router_replay_confirmed") is True
    report["judge_output_replay_confirmed"] = _predecessor_judge_output_confirmed(predecessor.report)
    for key in (
        "source_candidate_replay_confirmed",
        "artifact_snapshot_replay_confirmed",
        "evidence_bundle_replay_confirmed",
        "analysis_router_replay_confirmed",
        "judge_output_replay_confirmed",
    ):
        if report[key] is not True:
            checks_failed.append(f"{key}:missing")
    if checks_failed:
        return _finish(report, checks_failed)

    active_executor = executor or SqlAlchemyAnalysisValidatorReplayExecutor()
    try:
        execution = active_executor.execute(
            database_url=args.database_url,
            replay_namespace=args.replay_namespace,
        )
    except Exception as exc:  # noqa: BLE001 - never echo DB errors or URLs.
        checks_failed.append(_safe_failure_code(exc))
        return _finish(report, checks_failed)

    report.update(
        {
            "judge_output_ready_event_found": execution.judge_output_ready_event_found,
            "judge_run_succeeded_confirmed": execution.judge_run_succeeded_confirmed,
            "judge_output_loaded": execution.judge_output_loaded,
            "bundle_context_confirmed": execution.bundle_context_confirmed,
            "structured_output_schema_valid": execution.structured_output_schema_valid,
            "semantic_validation_passed": execution.semantic_validation_passed,
            "refusal_not_detected": execution.refusal_not_detected,
            "validation_state_transition_recorded": execution.validation_state_transition_recorded,
            "analysis_policy_apply_event_created": execution.analysis_policy_apply_event_created,
            "judge_output_mutated": execution.judge_output_mutated,
            "analysis_created": execution.analysis_created,
            "notification_created": execution.notification_created,
        }
    )
    checks_failed.extend(execution.checks_failed)

    for key in TRUE_RESULT_KEYS:
        if report.get(key) is not True:
            checks_failed.append(f"{key}:missing")
    for key in FALSE_RESULT_KEYS:
        if report.get(key) is not False:
            checks_failed.append(f"{key}:unexpected")

    return _finish(report, checks_failed)


def validate_database_url(
    database_url: str | None,
) -> tuple[bool, list[str], fake_judge_runner.analysis_router_runner.evidence_bundle_runner.source_candidate_runner.ParsedDatabaseUrl | None]:
    return fake_judge_runner.validate_database_url(database_url)


def build_analysis_policy_apply_dedupe_key(
    *,
    replay_namespace: str,
    judge_run_id: UUID,
    judge_output_id: UUID,
) -> str:
    return f"local-db-analysis-validator:{replay_namespace}:analysis.policy.apply:{judge_run_id}:{judge_output_id}"


def validate_judge_output_v1_payload(payload: Mapping[str, Any]) -> tuple[bool, tuple[str, ...]]:
    failures: list[str] = []
    if not isinstance(payload, Mapping):
        return False, ("payload_not_object",)

    missing = [key for key in REQUIRED_OUTPUT_KEYS if key not in payload]
    if missing:
        failures.extend(f"missing:{key}" for key in missing)
        return False, tuple(failures)

    if payload.get("judge_schema_version") != JUDGE_SCHEMA_VERSION:
        failures.append("judge_schema_version_invalid")

    for key in STRING_FIELDS:
        if not isinstance(payload.get(key), str):
            failures.append(f"{key}:not_string")

    for key in ARRAY_FIELDS:
        if not isinstance(payload.get(key), list):
            failures.append(f"{key}:not_list")

    verdict = payload.get("model_proposed_verdict")
    if verdict not in ALLOWED_MODEL_VERDICTS:
        failures.append("model_proposed_verdict_invalid")

    confidence = payload.get("model_confidence_band")
    if confidence not in ALLOWED_CONFIDENCE_BANDS:
        failures.append("model_confidence_band_invalid")

    scores = payload.get("scores")
    if not isinstance(scores, Mapping):
        failures.append("scores:not_object")
    else:
        for key in REQUIRED_SCORE_KEYS:
            if key not in scores:
                failures.append(f"scores.{key}:missing")
                continue
        for key in REQUIRED_INTEGER_SCORE_KEYS:
            if key not in scores:
                continue
            value = scores.get(key)
            if isinstance(value, bool) or not isinstance(value, Real):
                failures.append(f"scores.{key}:not_numeric")
            elif value < 0 or value > 100:
                failures.append(f"scores.{key}:out_of_range")
        for key in OPTIONAL_NULLABLE_SCORE_KEYS:
            value = scores.get(key)
            if value is None:
                continue
            if isinstance(value, bool) or not isinstance(value, Real):
                failures.append(f"scores.{key}:not_numeric")
            elif value < 0 or value > 100:
                failures.append(f"scores.{key}:out_of_range")
        for key in LEGACY_OPTIONAL_SCORE_KEYS:
            if key not in scores:
                continue
            value = scores[key]
            if isinstance(value, bool) or not isinstance(value, Real):
                failures.append(f"scores.{key}:not_numeric")
            elif value < 0 or value > 100:
                failures.append(f"scores.{key}:out_of_range")

    return not failures, tuple(failures)


def validate_semantic_consistency(
    payload: Mapping[str, Any],
    output: JudgeOutputRecord,
) -> tuple[bool, tuple[str, ...]]:
    failures: list[str] = []
    if payload.get("candidate_group_id") != str(output.candidate_group_id):
        failures.append("payload_candidate_group_mismatch")
    if payload.get("judge_schema_version") != output.judge_schema_version:
        failures.append("payload_schema_version_mismatch")
    if payload.get("model_proposed_verdict") != output.model_proposed_verdict:
        failures.append("payload_model_verdict_mismatch")
    if payload.get("model_confidence_band") != output.model_confidence_band:
        failures.append("payload_model_confidence_mismatch")
    for key in ("reason_codes",):
        value = payload.get(key)
        if not isinstance(value, list) or not value:
            failures.append(f"{key}:empty")
    for key in ("headline", "recommended_action_ko"):
        value = payload.get(key)
        if not isinstance(value, str) or not value.strip():
            failures.append(f"{key}:empty")
    return not failures, tuple(failures)


def _execute_analysis_validator_replay(connection: Any, *, replay_namespace: str) -> ReplayExecutionResult:
    checks_failed: list[str] = []

    event = _load_judge_output_ready_event(connection, replay_namespace=replay_namespace)
    event_found = event is not None
    if event is None:
        checks_failed.append("judge_output_ready_event_missing_or_invalid")
        return _execution_result(judge_output_ready_event_found=event_found, checks_failed=checks_failed)

    judge_run = _load_judge_run(connection, event.judge_run_id)
    judge_run_succeeded = judge_run is not None and judge_run.status == "succeeded" and not judge_run.refusal_detected
    if judge_run is None:
        checks_failed.append("judge_run_missing")
        _insert_or_reuse_state_transition(
            connection,
            object_id=event.judge_run_id,
            from_state=None,
            to_state="analysis_failed_missing_run",
            reason_code="validator_missing_judge_run",
        )
        return _execution_result(
            judge_output_ready_event_found=event_found,
            validation_state_transition_recorded=True,
            checks_failed=checks_failed,
        )

    judge_output = _load_judge_output(connection, event.judge_output_id)
    output_loaded = judge_output is not None
    if judge_output is None:
        checks_failed.append("judge_output_missing")
        _insert_or_reuse_state_transition(
            connection,
            object_id=judge_run.judge_run_id,
            from_state=judge_run.status,
            to_state="analysis_failed_missing_output",
            reason_code="validator_missing_judge_output",
        )
        return _execution_result(
            judge_output_ready_event_found=event_found,
            judge_run_succeeded_confirmed=judge_run_succeeded,
            judge_output_loaded=output_loaded,
            validation_state_transition_recorded=True,
            checks_failed=checks_failed,
        )

    bundle = _load_bundle_context(connection, judge_run.bundle_id)
    bundle_confirmed = bundle is not None and not _durable_relation_failures(event, judge_run, judge_output, bundle)
    if bundle is None:
        checks_failed.append("bundle_context_missing")
        _insert_or_reuse_state_transition(
            connection,
            object_id=judge_run.judge_run_id,
            from_state=judge_run.status,
            to_state="analysis_failed_identity_mismatch",
            reason_code="validator_bundle_missing",
        )
        return _execution_result(
            judge_output_ready_event_found=event_found,
            judge_run_succeeded_confirmed=judge_run_succeeded,
            judge_output_loaded=output_loaded,
            validation_state_transition_recorded=True,
            checks_failed=checks_failed,
        )

    relation_failures = _durable_relation_failures(event, judge_run, judge_output, bundle)
    if relation_failures:
        checks_failed.extend(relation_failures)
        _insert_or_reuse_state_transition(
            connection,
            object_id=judge_run.judge_run_id,
            from_state=judge_run.status,
            to_state="analysis_failed_identity_mismatch",
            reason_code="validator_durable_relation_invalid",
        )
        return _execution_result(
            judge_output_ready_event_found=event_found,
            judge_run_succeeded_confirmed=judge_run_succeeded,
            judge_output_loaded=output_loaded,
            bundle_context_confirmed=bundle_confirmed,
            validation_state_transition_recorded=True,
            checks_failed=checks_failed,
        )

    if _is_refusal(event, judge_run, judge_output):
        _insert_or_reuse_state_transition(
            connection,
            object_id=judge_run.judge_run_id,
            from_state="succeeded",
            to_state=REFUSED_TO_STATE,
            reason_code=REFUSED_REASON_CODE,
        )
        return _execution_result(
            judge_output_ready_event_found=event_found,
            judge_run_succeeded_confirmed=judge_run.status == "succeeded",
            judge_output_loaded=output_loaded,
            bundle_context_confirmed=bundle_confirmed,
            refusal_not_detected=False,
            validation_state_transition_recorded=True,
            checks_failed=[REFUSED_REASON_CODE],
        )

    output_before = _canonical_json(judge_output.payload_json)
    schema_valid, schema_failures = validate_judge_output_v1_payload(judge_output.payload_json)
    if not schema_valid:
        checks_failed.extend(schema_failures or ("structured_output_schema_invalid",))
        _insert_or_reuse_state_transition(
            connection,
            object_id=judge_run.judge_run_id,
            from_state=judge_run.status,
            to_state="analysis_failed_schema",
            reason_code="validator_schema_invalid",
        )
        return _execution_result(
            judge_output_ready_event_found=event_found,
            judge_run_succeeded_confirmed=judge_run_succeeded,
            judge_output_loaded=output_loaded,
            bundle_context_confirmed=bundle_confirmed,
            structured_output_schema_valid=schema_valid,
            refusal_not_detected=True,
            validation_state_transition_recorded=True,
            checks_failed=checks_failed,
        )

    fixture_failures = _fixture_expected_failures(event, judge_output)
    if fixture_failures:
        checks_failed.extend(fixture_failures)

    semantic_valid, semantic_failures = validate_semantic_consistency(judge_output.payload_json, judge_output)
    if not semantic_valid or fixture_failures:
        checks_failed.extend(semantic_failures)
        _insert_or_reuse_state_transition(
            connection,
            object_id=judge_run.judge_run_id,
            from_state=judge_run.status,
            to_state="analysis_failed_semantic",
            reason_code="validator_semantic_invalid",
        )
        return _execution_result(
            judge_output_ready_event_found=event_found,
            judge_run_succeeded_confirmed=judge_run_succeeded,
            judge_output_loaded=output_loaded,
            bundle_context_confirmed=bundle_confirmed,
            structured_output_schema_valid=schema_valid,
            semantic_validation_passed=False,
            refusal_not_detected=True,
            validation_state_transition_recorded=True,
            checks_failed=checks_failed or ["semantic_validation_failed"],
        )

    _insert_or_reuse_state_transition(
        connection,
        object_id=judge_run.judge_run_id,
        from_state="succeeded",
        to_state=VALIDATED_TO_STATE,
        reason_code=VALIDATED_REASON_CODE,
    )
    _insert_or_reuse_analysis_policy_apply_event(
        connection,
        replay_namespace=replay_namespace,
        judge_run_id=judge_run.judge_run_id,
        judge_output_id=judge_output.judge_output_id,
        candidate_group_id=judge_output.candidate_group_id,
        bundle_id=bundle.bundle_id,
    )

    verification = _verify_success(
        connection,
        replay_namespace=replay_namespace,
        judge_run_id=judge_run.judge_run_id,
        judge_output_id=judge_output.judge_output_id,
        candidate_group_id=judge_output.candidate_group_id,
        bundle_id=bundle.bundle_id,
        output_before=output_before,
    )
    checks_failed.extend(verification["checks_failed"])

    return ReplayExecutionResult(
        judge_output_ready_event_found=event_found,
        judge_run_succeeded_confirmed=judge_run_succeeded,
        judge_output_loaded=output_loaded,
        bundle_context_confirmed=bundle_confirmed,
        structured_output_schema_valid=schema_valid,
        semantic_validation_passed=semantic_valid,
        refusal_not_detected=True,
        validation_state_transition_recorded=verification["validation_state_transition_recorded"],
        analysis_policy_apply_event_created=verification["analysis_policy_apply_event_created"],
        judge_output_mutated=verification["judge_output_mutated"],
        analysis_created=verification["analysis_created"],
        notification_created=verification["notification_created"],
        checks_failed=tuple(dict.fromkeys(checks_failed)),
    )


def _load_judge_output_ready_event(
    connection: Any,
    *,
    replay_namespace: str,
) -> JudgeOutputReadyEvent | None:
    import sqlalchemy as sa

    row = connection.execute(
        sa.text(
            """
            SELECT event_id, payload_json
            FROM event_outbox
            WHERE event_type = :event_type
              AND dedupe_key LIKE :dedupe_prefix
            ORDER BY created_at DESC, event_id DESC
            LIMIT 1
            """
        ),
        {
            "event_type": JUDGE_OUTPUT_READY_EVENT_TYPE,
            "dedupe_prefix": f"local-db-fake-judge-output:{replay_namespace}:judge.output.ready:%",
        },
    ).mappings().first()
    if row is None:
        return None

    payload = _json_loads(row["payload_json"]) or {}
    judge_run_id = _uuid_or_none(payload.get("judge_run_id"))
    judge_output_id = _uuid_or_none(payload.get("judge_output_id"))
    finish_reason = _string_or_none(payload.get("finish_reason"))
    refusal = payload.get("refusal_detected")
    if judge_run_id is None or judge_output_id is None or finish_reason is None or not isinstance(refusal, bool):
        return None
    return JudgeOutputReadyEvent(
        event_id=UUID(str(row["event_id"])),
        judge_run_id=judge_run_id,
        judge_output_id=judge_output_id,
        finish_reason=finish_reason,
        refusal_detected=refusal,
    )


def _load_judge_run(connection: Any, judge_run_id: UUID) -> JudgeRunRecord | None:
    import sqlalchemy as sa

    row = connection.execute(
        sa.text(
            """
            SELECT judge_run_id, bundle_id, status, finish_reason, refusal_detected
            FROM judge_runs
            WHERE judge_run_id = CAST(:judge_run_id AS uuid)
            """
        ),
        {"judge_run_id": str(judge_run_id)},
    ).mappings().first()
    if row is None:
        return None
    return JudgeRunRecord(
        judge_run_id=UUID(str(row["judge_run_id"])),
        bundle_id=UUID(str(row["bundle_id"])),
        status=str(row["status"]),
        finish_reason=_string_or_none(row["finish_reason"]),
        refusal_detected=bool(row["refusal_detected"]),
    )


def _load_judge_output(connection: Any, judge_output_id: UUID) -> JudgeOutputRecord | None:
    import sqlalchemy as sa

    row = connection.execute(
        sa.text(
            """
            SELECT judge_output_id, judge_run_id, candidate_group_id,
                   judge_schema_version, payload_json, model_proposed_verdict,
                   model_confidence_band
            FROM judge_outputs
            WHERE judge_output_id = CAST(:judge_output_id AS uuid)
            """
        ),
        {"judge_output_id": str(judge_output_id)},
    ).mappings().first()
    if row is None:
        return None
    return JudgeOutputRecord(
        judge_output_id=UUID(str(row["judge_output_id"])),
        judge_run_id=UUID(str(row["judge_run_id"])),
        candidate_group_id=UUID(str(row["candidate_group_id"])),
        judge_schema_version=str(row["judge_schema_version"]),
        payload_json=_json_loads(row["payload_json"]) or {},
        model_proposed_verdict=_string_or_none(row["model_proposed_verdict"]),
        model_confidence_band=_string_or_none(row["model_confidence_band"]),
    )


def _load_bundle_context(connection: Any, bundle_id: UUID) -> BundleContext | None:
    import sqlalchemy as sa

    row = connection.execute(
        sa.text(
            """
            SELECT b.bundle_id, b.candidate_group_id, b.ready_for_analysis,
                   cgp.current_bundle_id
            FROM candidate_evidence_bundles b
            JOIN candidate_group_proposals cgp
              ON cgp.candidate_group_id = b.candidate_group_id
            WHERE b.bundle_id = CAST(:bundle_id AS uuid)
            """
        ),
        {"bundle_id": str(bundle_id)},
    ).mappings().first()
    if row is None:
        return None
    return BundleContext(
        bundle_id=UUID(str(row["bundle_id"])),
        candidate_group_id=UUID(str(row["candidate_group_id"])),
        current_bundle_id=_uuid_or_none(row["current_bundle_id"]),
        ready_for_analysis=bool(row["ready_for_analysis"]),
    )


def _durable_relation_failures(
    event: JudgeOutputReadyEvent,
    judge_run: JudgeRunRecord,
    judge_output: JudgeOutputRecord,
    bundle: BundleContext,
) -> list[str]:
    failures: list[str] = []
    if judge_run.judge_run_id != event.judge_run_id:
        failures.append("judge_run_id_mismatch")
    if judge_run.status != "succeeded":
        failures.append("judge_run_status_not_succeeded")
    if judge_run.refusal_detected:
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


def _is_refusal(
    event: JudgeOutputReadyEvent,
    judge_run: JudgeRunRecord,
    judge_output: JudgeOutputRecord,
) -> bool:
    return (
        event.refusal_detected
        or judge_run.refusal_detected
        or judge_output.payload_json.get("output_kind") == "refusal"
    )


def _fixture_expected_failures(
    event: JudgeOutputReadyEvent,
    output: JudgeOutputRecord,
) -> list[str]:
    failures: list[str] = []
    if event.finish_reason != EXPECTED_FINISH_REASON:
        failures.append("judge_output_ready_finish_reason_unexpected")
    if event.refusal_detected is not False:
        failures.append("judge_output_ready_refusal_unexpected")
    if output.model_proposed_verdict != EXPECTED_MODEL_PROPOSED_VERDICT:
        failures.append("judge_output_model_verdict_unexpected")
    if output.model_confidence_band != EXPECTED_MODEL_CONFIDENCE_BAND:
        failures.append("judge_output_model_confidence_unexpected")
    return failures


def _insert_or_reuse_state_transition(
    connection: Any,
    *,
    object_id: UUID,
    from_state: str | None,
    to_state: str,
    reason_code: str,
) -> None:
    import sqlalchemy as sa

    existing = connection.execute(
        sa.text(
            """
            SELECT 1
            FROM state_transitions
            WHERE object_type = 'judge_run'
              AND object_id = CAST(:object_id AS uuid)
              AND from_state IS NOT DISTINCT FROM :from_state
              AND to_state = :to_state
              AND reason_code IS NOT DISTINCT FROM :reason_code
            LIMIT 1
            """
        ),
        {
            "object_id": str(object_id),
            "from_state": from_state,
            "to_state": to_state,
            "reason_code": reason_code,
        },
    ).scalar_one_or_none()
    if existing:
        return

    connection.execute(
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
                'judge_run',
                CAST(:object_id AS uuid),
                :from_state,
                :to_state,
                :reason_code,
                now()
            )
            """
        ),
        {
            "object_id": str(object_id),
            "from_state": from_state,
            "to_state": to_state,
            "reason_code": reason_code,
        },
    )


def _insert_or_reuse_analysis_policy_apply_event(
    connection: Any,
    *,
    replay_namespace: str,
    judge_run_id: UUID,
    judge_output_id: UUID,
    candidate_group_id: UUID,
    bundle_id: UUID,
) -> None:
    import sqlalchemy as sa

    payload = {
        "judge_run_id": str(judge_run_id),
        "judge_output_id": str(judge_output_id),
        "candidate_group_id": str(candidate_group_id),
        "bundle_id": str(bundle_id),
    }
    connection.execute(
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
            ) VALUES (
                :event_type,
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
            "event_type": ANALYSIS_POLICY_APPLY_EVENT_TYPE,
            "judge_run_id": str(judge_run_id),
            "dedupe_key": build_analysis_policy_apply_dedupe_key(
                replay_namespace=replay_namespace,
                judge_run_id=judge_run_id,
                judge_output_id=judge_output_id,
            ),
            "payload_json": _json_dumps(payload),
        },
    )


def _verify_success(
    connection: Any,
    *,
    replay_namespace: str,
    judge_run_id: UUID,
    judge_output_id: UUID,
    candidate_group_id: UUID,
    bundle_id: UUID,
    output_before: str,
) -> dict[str, Any]:
    import sqlalchemy as sa

    transition_exists = bool(
        connection.execute(
            sa.text(
                """
                SELECT 1
                FROM state_transitions
                WHERE object_type = 'judge_run'
                  AND object_id = CAST(:judge_run_id AS uuid)
                  AND from_state = 'succeeded'
                  AND to_state = :to_state
                  AND reason_code = :reason_code
                LIMIT 1
                """
            ),
            {
                "judge_run_id": str(judge_run_id),
                "to_state": VALIDATED_TO_STATE,
                "reason_code": VALIDATED_REASON_CODE,
            },
        ).scalar_one_or_none()
    )
    dedupe_key = build_analysis_policy_apply_dedupe_key(
        replay_namespace=replay_namespace,
        judge_run_id=judge_run_id,
        judge_output_id=judge_output_id,
    )
    event_row = connection.execute(
        sa.text(
            """
            SELECT payload_json
            FROM event_outbox
            WHERE event_type = :event_type
              AND aggregate_type = 'judge_run'
              AND aggregate_id = CAST(:judge_run_id AS uuid)
              AND dedupe_key = :dedupe_key
            """
        ),
        {
            "event_type": ANALYSIS_POLICY_APPLY_EVENT_TYPE,
            "judge_run_id": str(judge_run_id),
            "dedupe_key": dedupe_key,
        },
    ).mappings().first()
    expected_payload = {
        "judge_run_id": str(judge_run_id),
        "judge_output_id": str(judge_output_id),
        "candidate_group_id": str(candidate_group_id),
        "bundle_id": str(bundle_id),
    }
    event_payload = _json_loads(event_row["payload_json"]) if event_row else None
    event_exists = event_payload == expected_payload
    output_after = _load_judge_output(connection, judge_output_id)
    judge_output_mutated = output_after is None or _canonical_json(output_after.payload_json) != output_before
    downstream = _load_downstream_flags(connection, candidate_group_id=candidate_group_id)
    checks = {
        "validation_state_transition_recorded": transition_exists,
        "analysis_policy_apply_event_created": event_exists,
        "judge_output_mutated": judge_output_mutated,
        "analysis_created": downstream["analysis_created"],
        "notification_created": downstream["notification_created"],
    }
    failures = [
        f"{key}:unexpected" if key in {"judge_output_mutated", "analysis_created", "notification_created"} else f"{key}:missing"
        for key, value in checks.items()
        if (key in {"judge_output_mutated", "analysis_created", "notification_created"} and value is not False)
        or (key not in {"judge_output_mutated", "analysis_created", "notification_created"} and value is not True)
    ]
    return {**checks, "checks_failed": failures}


def _load_downstream_flags(connection: Any, *, candidate_group_id: UUID) -> dict[str, bool]:
    import sqlalchemy as sa

    row = connection.execute(
        sa.text(
            """
            SELECT
              EXISTS (
                SELECT 1 FROM analyses
                WHERE candidate_group_id = CAST(:candidate_group_id AS uuid)
              ) AS analysis_created,
              EXISTS (
                SELECT 1 FROM notification_plans
                WHERE candidate_group_id = CAST(:candidate_group_id AS uuid)
              )
              OR EXISTS (
                SELECT 1
                FROM notification_renders nr
                JOIN notification_plans np
                  ON np.notification_plan_id = nr.notification_plan_id
                WHERE np.candidate_group_id = CAST(:candidate_group_id AS uuid)
              )
              OR EXISTS (
                SELECT 1
                FROM notification_delivery_records ndr
                JOIN notification_plans np
                  ON np.notification_plan_id = ndr.notification_plan_id
                WHERE np.candidate_group_id = CAST(:candidate_group_id AS uuid)
              ) AS notification_created
            """
        ),
        {"candidate_group_id": str(candidate_group_id)},
    ).mappings().one()
    return {
        "analysis_created": bool(row["analysis_created"]),
        "notification_created": bool(row["notification_created"]),
    }


def _predecessor_result_acceptable(predecessor: fake_judge_runner.RunnerResult) -> bool:
    if predecessor.report.get("status") == "pass" and predecessor.exit_code == 0:
        return True
    allowed_successor_failures = {"analysis_policy_apply_event_created:unexpected"}
    checks_failed = set(predecessor.report.get("checks_failed") or [])
    return bool(checks_failed) and checks_failed.issubset(allowed_successor_failures)


def _predecessor_judge_output_confirmed(report: Mapping[str, Any]) -> bool:
    required = (
        "judge_call_requested_event_found",
        "judge_run_pending_confirmed",
        "bundle_context_loaded",
        "fake_judge_output_created_or_reused",
        "judge_run_succeeded",
        "judge_output_ready_event_created",
        "structured_output_schema_valid",
        "usage_telemetry_recorded",
    )
    return all(report.get(key) is True for key in required)


def _execution_result(
    *,
    judge_output_ready_event_found: bool = False,
    judge_run_succeeded_confirmed: bool = False,
    judge_output_loaded: bool = False,
    bundle_context_confirmed: bool = False,
    structured_output_schema_valid: bool = False,
    semantic_validation_passed: bool = False,
    refusal_not_detected: bool = False,
    validation_state_transition_recorded: bool = False,
    analysis_policy_apply_event_created: bool = False,
    judge_output_mutated: bool = False,
    analysis_created: bool = False,
    notification_created: bool = False,
    checks_failed: list[str] | tuple[str, ...],
) -> ReplayExecutionResult:
    return ReplayExecutionResult(
        judge_output_ready_event_found=judge_output_ready_event_found,
        judge_run_succeeded_confirmed=judge_run_succeeded_confirmed,
        judge_output_loaded=judge_output_loaded,
        bundle_context_confirmed=bundle_context_confirmed,
        structured_output_schema_valid=structured_output_schema_valid,
        semantic_validation_passed=semantic_validation_passed,
        refusal_not_detected=refusal_not_detected,
        validation_state_transition_recorded=validation_state_transition_recorded,
        analysis_policy_apply_event_created=analysis_policy_apply_event_created,
        judge_output_mutated=judge_output_mutated,
        analysis_created=analysis_created,
        notification_created=notification_created,
        checks_failed=tuple(dict.fromkeys(checks_failed)),
    )


def _base_report() -> dict[str, Any]:
    return {
        "schema_version": SCHEMA_VERSION,
        "status": "pass",
        "database_url_guard_passed": False,
        "source_candidate_replay_confirmed": False,
        "artifact_snapshot_replay_confirmed": False,
        "evidence_bundle_replay_confirmed": False,
        "analysis_router_replay_confirmed": False,
        "judge_output_replay_confirmed": False,
        "judge_output_ready_event_found": False,
        "judge_run_succeeded_confirmed": False,
        "judge_output_loaded": False,
        "bundle_context_confirmed": False,
        "structured_output_schema_valid": False,
        "semantic_validation_passed": False,
        "refusal_not_detected": False,
        "validation_state_transition_recorded": False,
        "analysis_policy_apply_event_created": False,
        "judge_output_mutated": False,
        "analysis_created": False,
        "notification_created": False,
        "production_db_write": False,
        "live_github_called": False,
        "live_telegram_called": False,
        "openai_called": False,
        "workers_started": False,
        "redis_mutation": False,
        "checks_failed": [],
    }


def _finish(report: dict[str, Any], checks_failed: Sequence[str]) -> RunnerResult:
    normalized_failures = list(dict.fromkeys(checks_failed))
    report["checks_failed"] = normalized_failures
    report["status"] = "fail" if normalized_failures else "pass"
    return RunnerResult(exit_code=0 if report["status"] == "pass" else 1, report=report)


def _json_loads(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, str):
        return json.loads(value)
    return value


def _json_dumps(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True)


def _canonical_json(value: Mapping[str, Any]) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


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
    text = str(value)
    return text if text else None


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
