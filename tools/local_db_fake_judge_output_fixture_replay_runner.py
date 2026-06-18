from __future__ import annotations

import argparse
import json
import os
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping, Protocol, Sequence
from uuid import UUID

from tools import local_db_analysis_router_fixture_replay_runner as analysis_router_runner


SCHEMA_VERSION = "local_db_fake_judge_output_fixture_replay_v1"
JUDGE_CALL_REQUESTED_EVENT_TYPE = "judge.call.requested.v1"
JUDGE_OUTPUT_READY_EVENT_TYPE = "judge.output.ready.v1"
JUDGE_SCHEMA_VERSION = "judge_output_v1"
EXPECTED_MODEL = "gpt-5.4-mini"
EXPECTED_REASONING_EFFORT = "low"
EXPECTED_PROMPT_VERSION = "judge_github_primary_v1"
EXPECTED_PROMPT_CACHE_KEY = "judge:github_primary:judge_github_primary_v1:judge_output_v1:verdict_policy_v1"
EXPECTED_MODEL_PROPOSED_VERDICT = "later"
EXPECTED_MODEL_CONFIDENCE_BAND = "medium"
FAKE_INPUT_TOKENS = 321
FAKE_CACHED_INPUT_TOKENS = 0
FAKE_OUTPUT_TOKENS = 123
FAKE_REASONING_TOKENS = 0
FAKE_LATENCY_MS = 1
FAKE_FINISH_REASON = "fake_structured_output"
TRUE_RESULT_KEYS = (
    "database_url_guard_passed",
    "source_candidate_replay_confirmed",
    "artifact_snapshot_replay_confirmed",
    "evidence_bundle_replay_confirmed",
    "analysis_router_replay_confirmed",
    "judge_call_requested_event_found",
    "judge_run_pending_confirmed",
    "bundle_context_loaded",
    "fake_judge_output_created_or_reused",
    "judge_run_succeeded",
    "judge_output_ready_event_created",
    "structured_output_schema_valid",
    "usage_telemetry_recorded",
)
FALSE_RESULT_KEYS = (
    "production_db_write",
    "live_github_called",
    "live_telegram_called",
    "openai_called",
    "workers_started",
    "redis_mutation",
    "analysis_created",
    "notification_created",
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
SAFE_EXCEPTION_MESSAGES = {
    "judge_output_payload_mismatch",
    "judge_output_duplicate_existing",
    "judge_run_succeeded_output_missing",
}
COMPARISON_GAP_REASON_CODES = ("comparison_gap", "insufficient_comparables")


@dataclass(frozen=True, slots=True)
class RunnerResult:
    exit_code: int
    report: dict[str, Any]


@dataclass(frozen=True, slots=True)
class ReplayExecutionResult:
    judge_call_requested_event_found: bool
    judge_run_pending_confirmed: bool
    bundle_context_loaded: bool
    fake_judge_output_created_or_reused: bool
    judge_run_succeeded: bool
    judge_output_ready_event_created: bool
    structured_output_schema_valid: bool
    usage_telemetry_recorded: bool
    checks_failed: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class JudgeCallRequestedEvent:
    event_id: UUID
    judge_run_id: UUID
    bundle_id: UUID
    model: str
    reasoning_effort: str
    prompt_version: str
    prompt_cache_key: str


@dataclass(frozen=True, slots=True)
class JudgeRunRecord:
    judge_run_id: UUID
    bundle_id: UUID
    model: str
    reasoning_effort: str
    prompt_version: str
    schema_version: str
    policy_version: str
    prompt_cache_key: str | None
    status: str


@dataclass(frozen=True, slots=True)
class BundleContext:
    bundle_id: UUID
    candidate_group_id: UUID
    current_bundle_id: UUID | None
    primary_summary: dict[str, Any]
    evidence_limitations: list[Any]
    ready_for_analysis: bool


@dataclass(frozen=True, slots=True)
class JudgeOutputRecord:
    judge_output_id: UUID
    judge_run_id: UUID
    candidate_group_id: UUID
    judge_schema_version: str
    payload_json: dict[str, Any]
    model_proposed_verdict: str | None
    model_confidence_band: str | None


class FakeJudgeOutputReplayExecutor(Protocol):
    def execute(
        self,
        *,
        database_url: str,
        replay_namespace: str,
    ) -> ReplayExecutionResult: ...


class AnalysisRouterReplayRunner(Protocol):
    def run(
        self,
        *,
        database_url: str,
        source_fixture_path: Path,
        github_snapshot_fixture_path: Path,
        replay_namespace: str,
        env: Mapping[str, str],
        repo_root: Path,
    ) -> analysis_router_runner.RunnerResult: ...


class DefaultAnalysisRouterReplayRunner:
    def run(
        self,
        *,
        database_url: str,
        source_fixture_path: Path,
        github_snapshot_fixture_path: Path,
        replay_namespace: str,
        env: Mapping[str, str],
        repo_root: Path,
    ) -> analysis_router_runner.RunnerResult:
        args = argparse.Namespace(
            database_url=database_url,
            source_fixture=str(source_fixture_path),
            github_snapshot_fixture=str(github_snapshot_fixture_path),
            replay_namespace=replay_namespace,
            confirm_local_test_db=True,
        )
        predecessor_env = dict(env)
        predecessor_env["APP_ENV"] = "test"
        return analysis_router_runner.run(args, env=predecessor_env, repo_root=repo_root)


class SqlAlchemyFakeJudgeOutputReplayExecutor:
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
                return _execute_fake_judge_output_replay(connection, replay_namespace=replay_namespace)
        finally:
            engine.dispose()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Replay predecessor local/test DB fixtures through judge.call.requested, "
            "then deterministically write or reuse a fake structured judge_output_v1 "
            "and emit one namespace-scoped judge.output.ready.v1 event."
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
    executor: FakeJudgeOutputReplayExecutor | None = None,
    predecessor_runner: AnalysisRouterReplayRunner | None = None,
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
        analysis_router_runner.evidence_bundle_runner.source_candidate_runner.validate_replay_namespace(
            args.replay_namespace
        )
    )
    checks_failed.extend(namespace_failures)

    database_ok, database_failures, _ = validate_database_url(args.database_url)
    report["database_url_guard_passed"] = database_ok
    checks_failed.extend(database_failures)

    try:
        analysis_router_runner.evidence_bundle_runner.source_candidate_runner.load_source_fixture(
            Path(args.source_fixture),
            repo_root=root,
        )
    except Exception:  # noqa: BLE001 - operator output must stay sanitized.
        checks_failed.append("source_fixture_load_failed")

    try:
        analysis_router_runner.evidence_bundle_runner.github_snapshot_runner.load_github_snapshot_fixture(
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

    active_predecessor = predecessor_runner or DefaultAnalysisRouterReplayRunner()
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
        checks_failed.append("analysis_router_replay_failed")
        return _finish(report, checks_failed)

    if not _predecessor_result_acceptable(predecessor):
        checks_failed.append("analysis_router_replay_failed")
        return _finish(report, checks_failed)

    report["source_candidate_replay_confirmed"] = predecessor.report.get("source_candidate_replay_confirmed") is True
    report["artifact_snapshot_replay_confirmed"] = predecessor.report.get("artifact_snapshot_replay_confirmed") is True
    report["evidence_bundle_replay_confirmed"] = predecessor.report.get("evidence_bundle_replay_confirmed") is True
    report["analysis_router_replay_confirmed"] = _predecessor_analysis_router_confirmed(predecessor.report)
    for key in (
        "source_candidate_replay_confirmed",
        "artifact_snapshot_replay_confirmed",
        "evidence_bundle_replay_confirmed",
        "analysis_router_replay_confirmed",
    ):
        if report[key] is not True:
            checks_failed.append(f"{key}:missing")
    if checks_failed:
        return _finish(report, checks_failed)

    active_executor = executor or SqlAlchemyFakeJudgeOutputReplayExecutor()
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
            "judge_call_requested_event_found": execution.judge_call_requested_event_found,
            "judge_run_pending_confirmed": execution.judge_run_pending_confirmed,
            "bundle_context_loaded": execution.bundle_context_loaded,
            "fake_judge_output_created_or_reused": execution.fake_judge_output_created_or_reused,
            "judge_run_succeeded": execution.judge_run_succeeded,
            "judge_output_ready_event_created": execution.judge_output_ready_event_created,
            "structured_output_schema_valid": execution.structured_output_schema_valid,
            "usage_telemetry_recorded": execution.usage_telemetry_recorded,
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
) -> tuple[bool, list[str], analysis_router_runner.evidence_bundle_runner.source_candidate_runner.ParsedDatabaseUrl | None]:
    return analysis_router_runner.validate_database_url(database_url)


def build_judge_output_ready_dedupe_key(
    *,
    replay_namespace: str,
    judge_run_id: UUID,
    judge_output_id: UUID,
) -> str:
    return f"local-db-fake-judge-output:{replay_namespace}:judge.output.ready:{judge_run_id}:{judge_output_id}"


def build_fake_judge_output_payload(bundle: BundleContext) -> dict[str, Any]:
    primary_summary = bundle.primary_summary
    headline = _string_or_none(primary_summary.get("repo_full_name"))
    if headline is None:
        headline = _string_or_none(primary_summary.get("headline")) or str(bundle.candidate_group_id)
    limitations = [str(value) for value in bundle.evidence_limitations if str(value)]
    if not limitations:
        limitations = ["synthetic local fixture; no GitHub API call"]
    if not any(_has_comparison_gap_marker(value) for value in limitations):
        limitations.append("comparison_gap: insufficient_comparables in local fixture")
    return {
        "judge_schema_version": JUDGE_SCHEMA_VERSION,
        "candidate_group_id": str(bundle.candidate_group_id),
        "headline": headline,
        "summary_one_line_ko": (
            "GitHub 저장소 기반 개발 도구 후보로, README/테스트/CI 경로가 fixture evidence에 포함되어 있다."
        ),
        "skeptical_take_ko": (
            "현재 증거는 synthetic fixture 기반이므로 실제 GitHub 활동성이나 사용자 채택 신호로 해석하면 안 된다."
        ),
        "why_it_might_matter_ko": (
            "개발자 워크플로우 보조 도구라는 명확한 주제와 테스트/CI/문서 경로가 있어 후속 검토 대상으로 삼을 수 있다."
        ),
        "comparables": [],
        "scores": {
            "novelty": 41,
            "practical_usefulness": 58,
            "evidence_strength": 45,
            "hype_penalty": 20,
            "confidence": 45,
            "code_quality": 58,
            "maintenance_signal": 57,
            "specificity": None,
            "reproducibility_signal": None,
        },
        "reason_codes": [
            "github_repo_fixture_evidence",
            "tests_and_ci_paths_present",
            "synthetic_fixture_limitation",
            *COMPARISON_GAP_REASON_CODES,
        ],
        "red_flags_ko": [
            "실제 GitHub API 호출 결과가 아니라 local fixture 기반이다.",
        ],
        "evidence_limitations_ko": limitations,
        "recommended_action_ko": (
            "실제 GitHub read canary 전까지는 inspect_now가 아니라 local pipeline 검증용 후보로만 취급한다."
        ),
        "freshness_note_ko": "fixture content_anchor 기준으로 고정된 local snapshot이다.",
        "model_proposed_verdict": EXPECTED_MODEL_PROPOSED_VERDICT,
        "model_confidence_band": EXPECTED_MODEL_CONFIDENCE_BAND,
    }


def structured_output_schema_valid(payload: Mapping[str, Any]) -> bool:
    if set(REQUIRED_OUTPUT_KEYS) != set(payload):
        return False
    if payload.get("judge_schema_version") != JUDGE_SCHEMA_VERSION:
        return False
    if payload.get("model_proposed_verdict") != EXPECTED_MODEL_PROPOSED_VERDICT:
        return False
    if payload.get("model_confidence_band") != EXPECTED_MODEL_CONFIDENCE_BAND:
        return False
    scores = payload.get("scores")
    if not isinstance(scores, Mapping) or set(scores) != set(REQUIRED_SCORE_KEYS):
        return False
    for key in REQUIRED_INTEGER_SCORE_KEYS:
        value = scores.get(key)
        if isinstance(value, bool) or not isinstance(value, int) or value < 0 or value > 100:
            return False
    for key in OPTIONAL_NULLABLE_SCORE_KEYS:
        value = scores.get(key)
        if value is not None and (isinstance(value, bool) or not isinstance(value, int) or value < 0 or value > 100):
            return False
    for key in (
        "comparables",
        "reason_codes",
        "red_flags_ko",
        "evidence_limitations_ko",
    ):
        if not isinstance(payload.get(key), list):
            return False
    for key in (
        "candidate_group_id",
        "headline",
        "summary_one_line_ko",
        "skeptical_take_ko",
        "why_it_might_matter_ko",
        "recommended_action_ko",
        "freshness_note_ko",
    ):
        if not isinstance(payload.get(key), str) or not payload[key]:
            return False
    return True


def _has_comparison_gap_marker(value: str) -> bool:
    normalized = value.strip().lower().replace("-", "_").replace(" ", "_")
    return any(marker in normalized for marker in COMPARISON_GAP_REASON_CODES)


def _execute_fake_judge_output_replay(connection: Any, *, replay_namespace: str) -> ReplayExecutionResult:
    checks_failed: list[str] = []

    event = _load_judge_call_requested_event(connection, replay_namespace=replay_namespace)
    event_found = event is not None
    if event is None:
        checks_failed.append("judge_call_requested_event_missing_or_invalid")
        return _execution_result(judge_call_requested_event_found=event_found, checks_failed=checks_failed)

    event_failures = _validate_judge_call_event(event)
    if event_failures:
        checks_failed.extend(event_failures)
        return _execution_result(judge_call_requested_event_found=event_found, checks_failed=checks_failed)

    judge_run = _load_judge_run(connection, event.judge_run_id)
    if judge_run is None:
        checks_failed.append("judge_run_missing")
        return _execution_result(judge_call_requested_event_found=event_found, checks_failed=checks_failed)

    judge_run_pending_confirmed = judge_run.status in {"pending", "succeeded"}
    run_failures = _validate_judge_run(judge_run, event)
    if run_failures:
        checks_failed.extend(run_failures)
        return _execution_result(
            judge_call_requested_event_found=event_found,
            judge_run_pending_confirmed=judge_run_pending_confirmed,
            checks_failed=checks_failed,
        )

    bundle = _load_bundle_context(connection, event.bundle_id)
    bundle_loaded = bundle is not None
    if bundle is None:
        checks_failed.append("bundle_context_missing")
        return _execution_result(
            judge_call_requested_event_found=event_found,
            judge_run_pending_confirmed=judge_run_pending_confirmed,
            bundle_context_loaded=bundle_loaded,
            checks_failed=checks_failed,
        )

    bundle_failures = _validate_bundle_context(bundle, event)
    if bundle_failures:
        checks_failed.extend(bundle_failures)
        return _execution_result(
            judge_call_requested_event_found=event_found,
            judge_run_pending_confirmed=judge_run_pending_confirmed,
            bundle_context_loaded=bundle_loaded,
            checks_failed=checks_failed,
        )

    payload = build_fake_judge_output_payload(bundle)
    schema_valid = structured_output_schema_valid(payload)
    if not schema_valid:
        checks_failed.append("structured_output_schema_invalid")
        return _execution_result(
            judge_call_requested_event_found=event_found,
            judge_run_pending_confirmed=judge_run_pending_confirmed,
            bundle_context_loaded=bundle_loaded,
            structured_output_schema_valid=schema_valid,
            checks_failed=checks_failed,
        )

    existing_output = _load_existing_judge_output(
        connection,
        judge_run_id=judge_run.judge_run_id,
        judge_schema_version=JUDGE_SCHEMA_VERSION,
    )
    if judge_run.status == "succeeded" and existing_output is None:
        checks_failed.append("judge_run_succeeded_output_missing")
        return _execution_result(
            judge_call_requested_event_found=event_found,
            judge_run_pending_confirmed=judge_run_pending_confirmed,
            bundle_context_loaded=bundle_loaded,
            structured_output_schema_valid=schema_valid,
            checks_failed=checks_failed,
        )
    if existing_output is not None:
        output_matches = _judge_output_matches(
            existing_output,
            candidate_group_id=bundle.candidate_group_id,
            payload=payload,
        )
        if not output_matches:
            checks_failed.append("judge_output_payload_mismatch")
            return _execution_result(
                judge_call_requested_event_found=event_found,
                judge_run_pending_confirmed=judge_run_pending_confirmed,
                bundle_context_loaded=bundle_loaded,
                fake_judge_output_created_or_reused=True,
                structured_output_schema_valid=schema_valid,
                checks_failed=checks_failed,
            )
        judge_output_id = existing_output.judge_output_id
    else:
        judge_output_id = _insert_judge_output(
            connection,
            judge_run_id=judge_run.judge_run_id,
            candidate_group_id=bundle.candidate_group_id,
            payload=payload,
        )

    if judge_run.status == "pending":
        _mark_judge_run_succeeded(connection, judge_run_id=judge_run.judge_run_id)

    _insert_or_reuse_judge_output_ready_event(
        connection,
        replay_namespace=replay_namespace,
        judge_run_id=judge_run.judge_run_id,
        judge_output_id=judge_output_id,
    )
    verification = _verify_success(
        connection,
        replay_namespace=replay_namespace,
        judge_run_id=judge_run.judge_run_id,
        judge_output_id=judge_output_id,
        candidate_group_id=bundle.candidate_group_id,
        payload=payload,
    )
    checks_failed.extend(verification["checks_failed"])

    return ReplayExecutionResult(
        judge_call_requested_event_found=event_found,
        judge_run_pending_confirmed=judge_run_pending_confirmed,
        bundle_context_loaded=bundle_loaded,
        fake_judge_output_created_or_reused=verification["fake_judge_output_created_or_reused"],
        judge_run_succeeded=verification["judge_run_succeeded"],
        judge_output_ready_event_created=verification["judge_output_ready_event_created"],
        structured_output_schema_valid=schema_valid,
        usage_telemetry_recorded=verification["usage_telemetry_recorded"],
        checks_failed=tuple(dict.fromkeys(checks_failed)),
    )


def _load_judge_call_requested_event(
    connection: Any,
    *,
    replay_namespace: str,
) -> JudgeCallRequestedEvent | None:
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
            "event_type": JUDGE_CALL_REQUESTED_EVENT_TYPE,
            "dedupe_prefix": f"local-db-analysis-router:{replay_namespace}:judge.call.requested:%",
        },
    ).mappings().first()
    if row is None:
        return None

    payload = _json_loads(row["payload_json"]) or {}
    judge_run_id = _uuid_or_none(payload.get("judge_run_id"))
    bundle_id = _uuid_or_none(payload.get("bundle_id"))
    required = ("model", "reasoning_effort", "prompt_version", "prompt_cache_key")
    if judge_run_id is None or bundle_id is None or any(not payload.get(key) for key in required):
        return None
    return JudgeCallRequestedEvent(
        event_id=UUID(str(row["event_id"])),
        judge_run_id=judge_run_id,
        bundle_id=bundle_id,
        model=str(payload["model"]),
        reasoning_effort=str(payload["reasoning_effort"]),
        prompt_version=str(payload["prompt_version"]),
        prompt_cache_key=str(payload["prompt_cache_key"]),
    )


def _validate_judge_call_event(event: JudgeCallRequestedEvent) -> list[str]:
    failures: list[str] = []
    if event.model != EXPECTED_MODEL:
        failures.append("judge_call_model_unexpected")
    if event.reasoning_effort != EXPECTED_REASONING_EFFORT:
        failures.append("judge_call_reasoning_effort_unexpected")
    if event.prompt_version != EXPECTED_PROMPT_VERSION:
        failures.append("judge_call_prompt_version_unexpected")
    if event.prompt_cache_key != EXPECTED_PROMPT_CACHE_KEY:
        failures.append("judge_call_prompt_cache_key_unexpected")
    return failures


def _load_judge_run(connection: Any, judge_run_id: UUID) -> JudgeRunRecord | None:
    import sqlalchemy as sa

    row = connection.execute(
        sa.text(
            """
            SELECT judge_run_id, bundle_id, model, reasoning_effort, prompt_version,
                   schema_version, policy_version, prompt_cache_key, status
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
        model=str(row["model"]),
        reasoning_effort=str(row["reasoning_effort"]),
        prompt_version=str(row["prompt_version"]),
        schema_version=str(row["schema_version"]),
        policy_version=str(row["policy_version"]),
        prompt_cache_key=str(row["prompt_cache_key"]) if row["prompt_cache_key"] else None,
        status=str(row["status"]),
    )


def _validate_judge_run(judge_run: JudgeRunRecord, event: JudgeCallRequestedEvent) -> list[str]:
    failures: list[str] = []
    if judge_run.judge_run_id != event.judge_run_id:
        failures.append("judge_run_id_mismatch")
    if judge_run.bundle_id != event.bundle_id:
        failures.append("judge_run_bundle_mismatch")
    if judge_run.status not in {"pending", "succeeded"}:
        failures.append("judge_run_status_not_replayable")
    if judge_run.model != event.model:
        failures.append("judge_run_model_mismatch")
    if judge_run.reasoning_effort != event.reasoning_effort:
        failures.append("judge_run_reasoning_effort_mismatch")
    if judge_run.prompt_version != event.prompt_version:
        failures.append("judge_run_prompt_version_mismatch")
    if judge_run.schema_version != JUDGE_SCHEMA_VERSION:
        failures.append("judge_run_schema_version_mismatch")
    if judge_run.prompt_cache_key != event.prompt_cache_key:
        failures.append("judge_run_prompt_cache_key_mismatch")
    return failures


def _load_bundle_context(connection: Any, bundle_id: UUID) -> BundleContext | None:
    import sqlalchemy as sa

    row = connection.execute(
        sa.text(
            """
            SELECT b.bundle_id, b.candidate_group_id, b.primary_summary,
                   b.evidence_limitations, b.ready_for_analysis, cgp.current_bundle_id
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
        primary_summary=_json_loads(row["primary_summary"]) or {},
        evidence_limitations=_json_loads(row["evidence_limitations"]) or [],
        ready_for_analysis=bool(row["ready_for_analysis"]),
    )


def _validate_bundle_context(bundle: BundleContext, event: JudgeCallRequestedEvent) -> list[str]:
    failures: list[str] = []
    if bundle.bundle_id != event.bundle_id:
        failures.append("bundle_id_mismatch")
    if not bundle.ready_for_analysis:
        failures.append("evidence_bundle_not_ready")
    if bundle.current_bundle_id != event.bundle_id:
        failures.append("candidate_current_bundle_mismatch")
    if not bundle.primary_summary:
        failures.append("bundle_primary_summary_missing")
    return failures


def _load_existing_judge_output(
    connection: Any,
    *,
    judge_run_id: UUID,
    judge_schema_version: str,
) -> JudgeOutputRecord | None:
    import sqlalchemy as sa

    rows = connection.execute(
        sa.text(
            """
            SELECT judge_output_id, judge_run_id, candidate_group_id,
                   judge_schema_version, payload_json, model_proposed_verdict,
                   model_confidence_band
            FROM judge_outputs
            WHERE judge_run_id = CAST(:judge_run_id AS uuid)
              AND judge_schema_version = :judge_schema_version
            ORDER BY created_at ASC, judge_output_id ASC
            """
        ),
        {"judge_run_id": str(judge_run_id), "judge_schema_version": judge_schema_version},
    ).mappings().all()
    if len(rows) > 1:
        raise RuntimeError("judge_output_duplicate_existing")
    if not rows:
        return None
    row = rows[0]
    return JudgeOutputRecord(
        judge_output_id=UUID(str(row["judge_output_id"])),
        judge_run_id=UUID(str(row["judge_run_id"])),
        candidate_group_id=UUID(str(row["candidate_group_id"])),
        judge_schema_version=str(row["judge_schema_version"]),
        payload_json=_json_loads(row["payload_json"]) or {},
        model_proposed_verdict=_string_or_none(row["model_proposed_verdict"]),
        model_confidence_band=_string_or_none(row["model_confidence_band"]),
    )


def _judge_output_matches(
    output: JudgeOutputRecord,
    *,
    candidate_group_id: UUID,
    payload: Mapping[str, Any],
) -> bool:
    return (
        output.candidate_group_id == candidate_group_id
        and output.judge_schema_version == JUDGE_SCHEMA_VERSION
        and output.model_proposed_verdict == EXPECTED_MODEL_PROPOSED_VERDICT
        and output.model_confidence_band == EXPECTED_MODEL_CONFIDENCE_BAND
        and _canonical_json(output.payload_json) == _canonical_json(payload)
    )


def _insert_judge_output(
    connection: Any,
    *,
    judge_run_id: UUID,
    candidate_group_id: UUID,
    payload: Mapping[str, Any],
) -> UUID:
    import sqlalchemy as sa

    result = connection.execute(
        sa.text(
            """
            INSERT INTO judge_outputs (
                judge_run_id,
                candidate_group_id,
                judge_schema_version,
                payload_json,
                model_proposed_verdict,
                model_confidence_band,
                created_at
            ) VALUES (
                CAST(:judge_run_id AS uuid),
                CAST(:candidate_group_id AS uuid),
                :judge_schema_version,
                CAST(:payload_json AS jsonb),
                :model_proposed_verdict,
                :model_confidence_band,
                now()
            )
            RETURNING judge_output_id
            """
        ),
        {
            "judge_run_id": str(judge_run_id),
            "candidate_group_id": str(candidate_group_id),
            "judge_schema_version": JUDGE_SCHEMA_VERSION,
            "payload_json": _json_dumps(payload),
            "model_proposed_verdict": EXPECTED_MODEL_PROPOSED_VERDICT,
            "model_confidence_band": EXPECTED_MODEL_CONFIDENCE_BAND,
        },
    )
    return UUID(str(result.scalar_one()))


def _mark_judge_run_succeeded(connection: Any, *, judge_run_id: UUID) -> None:
    import sqlalchemy as sa

    connection.execute(
        sa.text(
            """
            UPDATE judge_runs
            SET status = 'succeeded',
                schema_retry_count = COALESCE(schema_retry_count, 0),
                input_tokens = :input_tokens,
                cached_input_tokens = :cached_input_tokens,
                output_tokens = :output_tokens,
                reasoning_tokens = :reasoning_tokens,
                latency_ms = :latency_ms,
                finish_reason = :finish_reason,
                refusal_detected = false,
                finished_at = now()
            WHERE judge_run_id = CAST(:judge_run_id AS uuid)
              AND status = 'pending'
            """
        ),
        {
            "judge_run_id": str(judge_run_id),
            "input_tokens": FAKE_INPUT_TOKENS,
            "cached_input_tokens": FAKE_CACHED_INPUT_TOKENS,
            "output_tokens": FAKE_OUTPUT_TOKENS,
            "reasoning_tokens": FAKE_REASONING_TOKENS,
            "latency_ms": FAKE_LATENCY_MS,
            "finish_reason": FAKE_FINISH_REASON,
        },
    )


def _insert_or_reuse_judge_output_ready_event(
    connection: Any,
    *,
    replay_namespace: str,
    judge_run_id: UUID,
    judge_output_id: UUID,
) -> None:
    import sqlalchemy as sa

    payload = {
        "judge_run_id": str(judge_run_id),
        "judge_output_id": str(judge_output_id),
        "finish_reason": FAKE_FINISH_REASON,
        "refusal_detected": False,
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
            "event_type": JUDGE_OUTPUT_READY_EVENT_TYPE,
            "judge_run_id": str(judge_run_id),
            "dedupe_key": build_judge_output_ready_dedupe_key(
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
    payload: Mapping[str, Any],
) -> dict[str, Any]:
    import sqlalchemy as sa

    row = connection.execute(
        sa.text(
            """
            SELECT
              EXISTS (
                SELECT 1
                FROM judge_runs
                WHERE judge_run_id = CAST(:judge_run_id AS uuid)
                  AND status = 'succeeded'
                  AND input_tokens IS NOT NULL
                  AND cached_input_tokens IS NOT NULL
                  AND output_tokens IS NOT NULL
                  AND reasoning_tokens IS NOT NULL
                  AND latency_ms IS NOT NULL
                  AND finish_reason = :finish_reason
                  AND refusal_detected IS FALSE
              ) AS judge_run_succeeded,
              (
                SELECT count(*)
                FROM judge_outputs
                WHERE judge_run_id = CAST(:judge_run_id AS uuid)
                  AND judge_schema_version = :judge_schema_version
              ) AS judge_output_count,
              EXISTS (
                SELECT 1
                FROM event_outbox
                WHERE event_type = :event_type
                  AND aggregate_type = 'judge_run'
                  AND aggregate_id = CAST(:judge_run_id AS uuid)
                  AND dedupe_key = :dedupe_key
              ) AS ready_event_exists
            """
        ),
        {
            "judge_run_id": str(judge_run_id),
            "judge_schema_version": JUDGE_SCHEMA_VERSION,
            "finish_reason": FAKE_FINISH_REASON,
            "event_type": JUDGE_OUTPUT_READY_EVENT_TYPE,
            "dedupe_key": build_judge_output_ready_dedupe_key(
                replay_namespace=replay_namespace,
                judge_run_id=judge_run_id,
                judge_output_id=judge_output_id,
            ),
        },
    ).mappings().one()
    output = _load_existing_judge_output(
        connection,
        judge_run_id=judge_run_id,
        judge_schema_version=JUDGE_SCHEMA_VERSION,
    )
    output_matches = output is not None and output.judge_output_id == judge_output_id and _judge_output_matches(
        output,
        candidate_group_id=candidate_group_id,
        payload=payload,
    )
    checks = {
        "fake_judge_output_created_or_reused": int(row["judge_output_count"]) == 1 and output_matches,
        "judge_run_succeeded": bool(row["judge_run_succeeded"]),
        "judge_output_ready_event_created": bool(row["ready_event_exists"]),
        "usage_telemetry_recorded": bool(row["judge_run_succeeded"]),
    }
    failures = [f"{key}:missing" for key, value in checks.items() if value is not True]
    return {**checks, "checks_failed": failures}


def _predecessor_result_acceptable(predecessor: analysis_router_runner.RunnerResult) -> bool:
    if predecessor.report.get("status") == "pass" and predecessor.exit_code == 0:
        return True
    allowed_successor_failures = {"judge_run_created_or_reused:missing"}
    checks_failed = set(predecessor.report.get("checks_failed") or [])
    return bool(checks_failed) and checks_failed.issubset(allowed_successor_failures)


def _predecessor_analysis_router_confirmed(report: Mapping[str, Any]) -> bool:
    required = (
        "analysis_requested_event_found",
        "candidate_current_bundle_confirmed",
        "evidence_bundle_ready_confirmed",
        "judge_profile_allowed",
        "routing_policy_applied",
        "judge_call_requested_event_created",
        "default_model_selected",
        "prompt_cache_key_created",
    )
    return all(report.get(key) is True for key in required)


def _execution_result(
    *,
    judge_call_requested_event_found: bool = False,
    judge_run_pending_confirmed: bool = False,
    bundle_context_loaded: bool = False,
    fake_judge_output_created_or_reused: bool = False,
    judge_run_succeeded: bool = False,
    judge_output_ready_event_created: bool = False,
    structured_output_schema_valid: bool = False,
    usage_telemetry_recorded: bool = False,
    checks_failed: list[str] | tuple[str, ...],
) -> ReplayExecutionResult:
    return ReplayExecutionResult(
        judge_call_requested_event_found=judge_call_requested_event_found,
        judge_run_pending_confirmed=judge_run_pending_confirmed,
        bundle_context_loaded=bundle_context_loaded,
        fake_judge_output_created_or_reused=fake_judge_output_created_or_reused,
        judge_run_succeeded=judge_run_succeeded,
        judge_output_ready_event_created=judge_output_ready_event_created,
        structured_output_schema_valid=structured_output_schema_valid,
        usage_telemetry_recorded=usage_telemetry_recorded,
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
        "judge_call_requested_event_found": False,
        "judge_run_pending_confirmed": False,
        "bundle_context_loaded": False,
        "fake_judge_output_created_or_reused": False,
        "judge_run_succeeded": False,
        "judge_output_ready_event_created": False,
        "structured_output_schema_valid": False,
        "usage_telemetry_recorded": False,
        "production_db_write": False,
        "live_github_called": False,
        "live_telegram_called": False,
        "openai_called": False,
        "workers_started": False,
        "redis_mutation": False,
        "analysis_created": False,
        "notification_created": False,
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
