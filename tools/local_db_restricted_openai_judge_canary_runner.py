from __future__ import annotations

import argparse
import asyncio
import inspect
import json
import os
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping, Protocol, Sequence
from uuid import UUID

from tools import local_db_analysis_router_fixture_replay_runner as analysis_router_runner


SCHEMA_VERSION = "local_db_restricted_openai_judge_canary_v1"
JUDGE_CALL_REQUESTED_EVENT_TYPE = "judge.call.requested.v1"
JUDGE_OUTPUT_READY_EVENT_TYPE = "judge.output.ready.v1"
JUDGE_SCHEMA_VERSION = "judge_output_v1"
FAKE_FINISH_REASON = "stop"
EXPECTED_MODEL_PROPOSED_VERDICT = "later"
EXPECTED_MODEL_CONFIDENCE_BAND = "medium"
TRUE_RESULT_KEYS = (
    "database_url_guard_passed",
    "openai_client_injected",
    "analysis_router_replay_confirmed",
    "judge_call_requested_event_found",
    "judge_run_loaded",
    "evidence_bundle_loaded",
    "judge_request_built",
    "judge_request_uses_bundle_only",
    "openai_responses_request_shape_valid",
    "openai_structured_output_received",
    "judge_output_created",
    "judge_run_updated",
    "judge_output_ready_event_created",
)
FALSE_RESULT_KEYS = (
    "openai_live_call_authorized",
    "live_openai_called",
    "telegram_called",
    "live_github_called",
    "workers_started",
    "redis_mutation",
    "production_db_write",
    "alembic_or_ddl_ran",
    "analysis_created",
    "notification_created",
)
REQUEST_CONTEXT_KEYS = (
    "bundle_id",
    "candidate_group_id",
    "current_primary_artifact_id",
    "primary_summary",
    "supporting_summaries_json",
    "discovered_links_summary_json",
    "evidence_limitations",
    "token_budget_profile",
    "reroot_count",
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
NULLABLE_SCORE_KEYS = frozenset(
    {
        "code_quality",
        "maintenance_signal",
        "specificity",
        "reproducibility_signal",
    }
)
FORBIDDEN_REQUEST_TOKENS = (
    "source_messages",
    "source_message_versions",
    "artifact_snapshots",
    "artifact_snapshot_",
    "notification_",
    "database_url",
    "db_url",
    "api_key",
    "secret",
    "access_token",
    "bearer",
    "web_search",
    "file_search",
)
SAFE_EXCEPTION_MESSAGES = {
    "judge_output_duplicate_existing",
    "judge_output_payload_mismatch",
    "judge_run_succeeded_output_missing",
    "openai_client_create_unavailable",
}


@dataclass(frozen=True, slots=True)
class RunnerResult:
    exit_code: int
    report: dict[str, Any]


@dataclass(frozen=True, slots=True)
class ReplayExecutionResult:
    judge_call_requested_event_found: bool
    judge_run_loaded: bool
    evidence_bundle_loaded: bool
    judge_request_built: bool
    judge_request_uses_bundle_only: bool
    openai_responses_request_shape_valid: bool
    openai_structured_output_received: bool
    judge_output_created: bool
    judge_run_updated: bool
    judge_output_ready_event_created: bool
    checks_failed: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class JudgeCallRequestedEvent:
    event_id: UUID
    judge_run_id: UUID
    bundle_id: UUID
    model: str
    reasoning_effort: str
    prompt_version: str
    prompt_cache_key: str | None


@dataclass(frozen=True, slots=True)
class JudgeRunRecord:
    judge_run_id: UUID
    bundle_id: UUID
    judge_profile: str
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
    current_primary_artifact_id: UUID
    current_bundle_id: UUID | None
    primary_summary: dict[str, Any]
    supporting_summaries_json: list[Any]
    discovered_links_summary_json: list[Any]
    evidence_limitations: list[Any]
    token_budget_profile: str | None
    reroot_count: int
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


@dataclass(frozen=True, slots=True)
class ParsedOpenAIUsage:
    input_tokens: int | None
    cached_input_tokens: int | None
    output_tokens: int | None
    reasoning_tokens: int | None
    latency_ms: int


@dataclass(frozen=True, slots=True)
class ParsedOpenAIResponse:
    payload_json: dict[str, Any] | None
    refusal_text: str | None
    finish_reason: str
    usage: ParsedOpenAIUsage

    @property
    def refusal_detected(self) -> bool:
        return bool(self.refusal_text)


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


class RestrictedOpenAIJudgeCanaryExecutor(Protocol):
    def execute(
        self,
        *,
        database_url: str,
        replay_namespace: str,
        openai_client: Any,
    ) -> ReplayExecutionResult: ...


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


class SqlAlchemyRestrictedOpenAIJudgeCanaryExecutor:
    def execute(
        self,
        *,
        database_url: str,
        replay_namespace: str,
        openai_client: Any,
    ) -> ReplayExecutionResult:
        _bootstrap_repo_imports()
        import sqlalchemy as sa

        engine = sa.create_engine(database_url, future=True)
        try:
            with engine.begin() as connection:
                return _execute_restricted_openai_judge_canary(
                    connection,
                    replay_namespace=replay_namespace,
                    openai_client=openai_client,
                )
        finally:
            engine.dispose()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Replay local/test DB fixtures through analysis-router, then prove the "
            "judge-openai boundary with an injected fake Responses client only."
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
    openai_client: Any | None = None,
    executor: RestrictedOpenAIJudgeCanaryExecutor | None = None,
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

    if openai_client is None:
        checks_failed.append("openai_client_injected_required")
        return _finish(report, checks_failed)
    report["openai_client_injected"] = True

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

    report["analysis_router_replay_confirmed"] = _predecessor_analysis_router_confirmed(
        predecessor.report
    )
    if report["analysis_router_replay_confirmed"] is not True:
        checks_failed.append("analysis_router_replay_confirmed:missing")
        return _finish(report, checks_failed)

    active_executor = executor or SqlAlchemyRestrictedOpenAIJudgeCanaryExecutor()
    try:
        execution = active_executor.execute(
            database_url=args.database_url,
            replay_namespace=args.replay_namespace,
            openai_client=openai_client,
        )
    except Exception as exc:  # noqa: BLE001 - never echo DB errors or URLs.
        checks_failed.append(_safe_failure_code(exc))
        return _finish(report, checks_failed)

    report.update(
        {
            "judge_call_requested_event_found": execution.judge_call_requested_event_found,
            "judge_run_loaded": execution.judge_run_loaded,
            "evidence_bundle_loaded": execution.evidence_bundle_loaded,
            "judge_request_built": execution.judge_request_built,
            "judge_request_uses_bundle_only": execution.judge_request_uses_bundle_only,
            "openai_responses_request_shape_valid": execution.openai_responses_request_shape_valid,
            "openai_structured_output_received": execution.openai_structured_output_received,
            "judge_output_created": execution.judge_output_created,
            "judge_run_updated": execution.judge_run_updated,
            "judge_output_ready_event_created": execution.judge_output_ready_event_created,
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
    return f"local-db-restricted-openai-judge:{replay_namespace}:judge.output.ready:{judge_run_id}:{judge_output_id}"


def build_openai_responses_request(
    *,
    judge_run: JudgeRunRecord,
    bundle: BundleContext,
) -> dict[str, Any]:
    developer_prompt = "\n".join(
        [
            "You are the stage-6 OpenAI judge for github_ai_catchbot.",
            "Return only strict judge_output_v1 JSON matching the supplied schema.",
            "Use only the provided CandidateEvidenceBundle context.",
            "Do not browse, search, fetch, call tools, or assume facts outside the bundle.",
            "Do not decide final verdict or delivery_decision; downstream deterministic services do that later.",
            "",
            f"judge_profile={judge_run.judge_profile}",
            f"prompt_version={judge_run.prompt_version}",
            f"schema_version={judge_run.schema_version}",
            f"policy_version={judge_run.policy_version}",
            f"prompt_cache_key={judge_run.prompt_cache_key or ''}",
        ]
    )
    user_context = json.dumps(
        {
            "bundle_id": str(bundle.bundle_id),
            "candidate_group_id": str(bundle.candidate_group_id),
            "current_primary_artifact_id": str(bundle.current_primary_artifact_id),
            "primary_summary": bundle.primary_summary,
            "supporting_summaries_json": bundle.supporting_summaries_json,
            "discovered_links_summary_json": bundle.discovered_links_summary_json,
            "evidence_limitations": bundle.evidence_limitations,
            "token_budget_profile": bundle.token_budget_profile,
            "reroot_count": bundle.reroot_count,
        },
        ensure_ascii=False,
        sort_keys=True,
        indent=2,
    )
    request: dict[str, Any] = {
        "model": judge_run.model,
        "reasoning": {"effort": judge_run.reasoning_effort},
        "input": [
            {
                "role": "developer",
                "content": [{"type": "input_text", "text": developer_prompt}],
            },
            {
                "role": "user",
                "content": [{"type": "input_text", "text": user_context}],
            },
        ],
        "text": {
            "format": {
                "type": "json_schema",
                "name": "judge_output_v1",
                "strict": True,
                "schema": judge_output_schema(),
            }
        },
        "tools": [],
    }
    if judge_run.prompt_cache_key:
        request["prompt_cache_key"] = judge_run.prompt_cache_key
    return request


def judge_output_schema() -> dict[str, Any]:
    score_0_to_100 = {"type": "integer", "minimum": 0, "maximum": 100}
    nullable_score_0_to_100 = {"type": ["integer", "null"], "minimum": 0, "maximum": 100}
    return {
        "type": "object",
        "additionalProperties": False,
        "required": list(REQUIRED_OUTPUT_KEYS),
        "properties": {
            "judge_schema_version": {"type": "string"},
            "candidate_group_id": {"type": "string"},
            "headline": {"type": "string"},
            "summary_one_line_ko": {"type": "string"},
            "skeptical_take_ko": {"type": "string"},
            "why_it_might_matter_ko": {"type": "string"},
            "comparables": {"type": "array", "items": {"type": "string"}},
            "scores": {
                "type": "object",
                "additionalProperties": False,
                "required": list(REQUIRED_SCORE_KEYS),
                "properties": {
                    "novelty": score_0_to_100,
                    "practical_usefulness": score_0_to_100,
                    "evidence_strength": score_0_to_100,
                    "hype_penalty": score_0_to_100,
                    "confidence": score_0_to_100,
                    "code_quality": nullable_score_0_to_100,
                    "maintenance_signal": nullable_score_0_to_100,
                    "specificity": nullable_score_0_to_100,
                    "reproducibility_signal": nullable_score_0_to_100,
                },
            },
            "reason_codes": {"type": "array", "items": {"type": "string"}},
            "red_flags_ko": {"type": "array", "items": {"type": "string"}},
            "evidence_limitations_ko": {"type": "array", "items": {"type": "string"}},
            "recommended_action_ko": {"type": "string"},
            "freshness_note_ko": {"type": "string"},
            "model_proposed_verdict": {
                "type": ["string", "null"],
                "enum": ["inspect_now", "later", "skip", None],
            },
            "model_confidence_band": {
                "type": ["string", "null"],
                "enum": ["low", "medium", "high", None],
            },
        },
    }


def openai_responses_request_shape_valid(request: Mapping[str, Any]) -> bool:
    allowed_top_level = {
        "model",
        "reasoning",
        "input",
        "text",
        "tools",
        "max_output_tokens",
        "prompt_cache_key",
    }
    if set(request) - allowed_top_level:
        return False
    if not isinstance(request.get("model"), str) or not request["model"]:
        return False
    reasoning = request.get("reasoning")
    if not isinstance(reasoning, Mapping) or set(reasoning) != {"effort"}:
        return False
    if not isinstance(reasoning.get("effort"), str) or not reasoning["effort"]:
        return False
    if request.get("tools") != []:
        return False
    input_items = request.get("input")
    if not isinstance(input_items, list) or len(input_items) != 2:
        return False
    if not _input_message_valid(input_items[0], expected_role="developer"):
        return False
    if not _input_message_valid(input_items[1], expected_role="user"):
        return False
    text_format = _mapping(_mapping(request.get("text")).get("format"))
    if text_format.get("type") != "json_schema":
        return False
    if text_format.get("name") != "judge_output_v1":
        return False
    if text_format.get("strict") is not True:
        return False
    schema = text_format.get("schema")
    if not isinstance(schema, Mapping):
        return False
    if schema.get("type") != "object" or schema.get("additionalProperties") is not False:
        return False
    if text_format.get("schema", {}).get("required") != list(REQUIRED_OUTPUT_KEYS):
        return False
    prompt_cache_key = request.get("prompt_cache_key")
    if prompt_cache_key is not None and not isinstance(prompt_cache_key, str):
        return False
    return not _contains_forbidden_request_tokens(request)


def judge_request_uses_bundle_only(request: Mapping[str, Any]) -> bool:
    if _contains_forbidden_request_tokens(request):
        return False
    try:
        user_context = _extract_user_context(request)
        parsed = json.loads(user_context)
    except (TypeError, ValueError, json.JSONDecodeError):
        return False
    return isinstance(parsed, dict) and set(parsed) == set(REQUEST_CONTEXT_KEYS)


def build_fake_judge_output_payload(bundle: BundleContext) -> dict[str, Any]:
    primary_summary = bundle.primary_summary
    headline = _string_or_none(primary_summary.get("repo_full_name"))
    if headline is None:
        headline = _string_or_none(primary_summary.get("headline")) or str(bundle.candidate_group_id)
    limitations = [str(value) for value in bundle.evidence_limitations if str(value)]
    if not limitations:
        limitations = ["synthetic local fixture; no live OpenAI call"]
    return {
        "judge_schema_version": JUDGE_SCHEMA_VERSION,
        "candidate_group_id": str(bundle.candidate_group_id),
        "headline": headline,
        "summary_one_line_ko": "local/test DB evidence bundle에서 구성한 제한된 judge canary 응답이다.",
        "skeptical_take_ko": "이 응답은 injected fake client 결과이므로 실제 모델 품질이나 최신 외부 상태를 증명하지 않는다.",
        "why_it_might_matter_ko": "judge-openai 경계가 bundle-only context와 strict structured output을 연결할 수 있음을 검증한다.",
        "comparables": [],
        "scores": {
            "novelty": 35,
            "practical_usefulness": 45,
            "evidence_strength": 50,
            "hype_penalty": 35,
            "confidence": 55,
            "code_quality": 50,
            "maintenance_signal": 45,
            "specificity": 50,
            "reproducibility_signal": 45,
        },
        "reason_codes": [
            "local_test_db_bundle_only",
            "strict_structured_output",
            "fake_openai_client",
        ],
        "red_flags_ko": [
            "live OpenAI 호출이 아니므로 모델 성능을 판단할 수 없다.",
        ],
        "evidence_limitations_ko": limitations,
        "recommended_action_ko": "정책 실행 전 단계의 local canary proof로만 취급하고 downstream verdict는 만들지 않는다.",
        "freshness_note_ko": "replay namespace로 고정된 local/test DB fixture 결과이다.",
        "model_proposed_verdict": EXPECTED_MODEL_PROPOSED_VERDICT,
        "model_confidence_band": EXPECTED_MODEL_CONFIDENCE_BAND,
    }


def structured_output_payload_valid(payload: Mapping[str, Any], *, candidate_group_id: UUID | None = None) -> bool:
    if set(payload) != set(REQUIRED_OUTPUT_KEYS):
        return False
    if payload.get("judge_schema_version") != JUDGE_SCHEMA_VERSION:
        return False
    if candidate_group_id is not None and payload.get("candidate_group_id") != str(candidate_group_id):
        return False
    if payload.get("model_proposed_verdict") not in {"inspect_now", "later", "skip", None}:
        return False
    if payload.get("model_confidence_band") not in {"low", "medium", "high", None}:
        return False
    scores = payload.get("scores")
    if not isinstance(scores, dict) or set(scores) != set(REQUIRED_SCORE_KEYS):
        return False
    for key in REQUIRED_SCORE_KEYS:
        value = scores.get(key)
        if key in NULLABLE_SCORE_KEYS and value is None:
            continue
        if not isinstance(value, int) or isinstance(value, bool) or not 0 <= value <= 100:
            return False
    for key in ("comparables", "reason_codes", "red_flags_ko", "evidence_limitations_ko"):
        values = payload.get(key)
        if not isinstance(values, list) or not all(isinstance(value, str) for value in values):
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


def parse_openai_response(response: Any, *, started_monotonic: float) -> ParsedOpenAIResponse:
    payload_json = _extract_structured_payload(response)
    refusal_text = None if payload_json is not None else _extract_refusal_text(response)
    usage = _extract_usage(response, started_monotonic=started_monotonic)
    finish_reason = _normalize_finish_reason(_extract_finish_reason(response))
    return ParsedOpenAIResponse(
        payload_json=payload_json,
        refusal_text=refusal_text,
        finish_reason=finish_reason,
        usage=usage,
    )


def _execute_restricted_openai_judge_canary(
    connection: Any,
    *,
    replay_namespace: str,
    openai_client: Any,
) -> ReplayExecutionResult:
    checks_failed: list[str] = []

    event = _load_judge_call_requested_event(connection, replay_namespace=replay_namespace)
    event_found = event is not None
    if event is None:
        checks_failed.append("judge_call_requested_event_missing_or_invalid")
        return _execution_result(
            judge_call_requested_event_found=event_found,
            checks_failed=checks_failed,
        )

    judge_run = _load_judge_run(connection, event.judge_run_id)
    judge_run_loaded = judge_run is not None
    if judge_run is None:
        checks_failed.append("judge_run_missing")
        return _execution_result(
            judge_call_requested_event_found=event_found,
            judge_run_loaded=judge_run_loaded,
            checks_failed=checks_failed,
        )

    run_failures = _validate_judge_run(judge_run, event)
    if run_failures:
        checks_failed.extend(run_failures)
        return _execution_result(
            judge_call_requested_event_found=event_found,
            judge_run_loaded=judge_run_loaded,
            checks_failed=checks_failed,
        )

    bundle = _load_bundle_context(connection, event.bundle_id)
    bundle_loaded = bundle is not None
    if bundle is None:
        checks_failed.append("evidence_bundle_missing")
        return _execution_result(
            judge_call_requested_event_found=event_found,
            judge_run_loaded=judge_run_loaded,
            evidence_bundle_loaded=bundle_loaded,
            checks_failed=checks_failed,
        )

    bundle_failures = _validate_bundle_context(bundle, event)
    if bundle_failures:
        checks_failed.extend(bundle_failures)
        return _execution_result(
            judge_call_requested_event_found=event_found,
            judge_run_loaded=judge_run_loaded,
            evidence_bundle_loaded=bundle_loaded,
            checks_failed=checks_failed,
        )

    request = build_openai_responses_request(judge_run=judge_run, bundle=bundle)
    request_built = True
    uses_bundle_only = judge_request_uses_bundle_only(request)
    shape_valid = openai_responses_request_shape_valid(request)
    if not uses_bundle_only:
        checks_failed.append("judge_request_not_bundle_only")
    if not shape_valid:
        checks_failed.append("openai_responses_request_shape_invalid")
    if checks_failed:
        return _execution_result(
            judge_call_requested_event_found=event_found,
            judge_run_loaded=judge_run_loaded,
            evidence_bundle_loaded=bundle_loaded,
            judge_request_built=request_built,
            judge_request_uses_bundle_only=uses_bundle_only,
            openai_responses_request_shape_valid=shape_valid,
            checks_failed=checks_failed,
        )

    existing_output = _load_namespace_judge_output(
        connection,
        replay_namespace=replay_namespace,
        judge_run_id=judge_run.judge_run_id,
    )
    if existing_output is not None:
        if not _judge_output_matches(existing_output, candidate_group_id=bundle.candidate_group_id):
            checks_failed.append("judge_output_payload_mismatch")
            return _execution_result(
                judge_call_requested_event_found=event_found,
                judge_run_loaded=judge_run_loaded,
                evidence_bundle_loaded=bundle_loaded,
                judge_request_built=request_built,
                judge_request_uses_bundle_only=uses_bundle_only,
                openai_responses_request_shape_valid=shape_valid,
                checks_failed=checks_failed,
            )
        _insert_or_reuse_judge_output_ready_event(
            connection,
            replay_namespace=replay_namespace,
            judge_run_id=judge_run.judge_run_id,
            judge_output_id=existing_output.judge_output_id,
            finish_reason=FAKE_FINISH_REASON,
            refusal_detected=False,
        )
        verification = _verify_success(
            connection,
            replay_namespace=replay_namespace,
            judge_run_id=judge_run.judge_run_id,
            judge_output_id=existing_output.judge_output_id,
            candidate_group_id=bundle.candidate_group_id,
        )
        checks_failed.extend(verification["checks_failed"])
        return ReplayExecutionResult(
            judge_call_requested_event_found=event_found,
            judge_run_loaded=judge_run_loaded,
            evidence_bundle_loaded=bundle_loaded,
            judge_request_built=request_built,
            judge_request_uses_bundle_only=uses_bundle_only,
            openai_responses_request_shape_valid=shape_valid,
            openai_structured_output_received=verification["judge_output_created"],
            judge_output_created=verification["judge_output_created"],
            judge_run_updated=verification["judge_run_updated"],
            judge_output_ready_event_created=verification["judge_output_ready_event_created"],
            checks_failed=tuple(dict.fromkeys(checks_failed)),
        )

    _mark_judge_run_running(connection, judge_run_id=judge_run.judge_run_id)
    started = time.monotonic()
    raw_response = _call_injected_openai_client(openai_client, request)
    parsed = parse_openai_response(raw_response, started_monotonic=started)

    if parsed.refusal_detected:
        _mark_judge_run_failed(
            connection,
            judge_run_id=judge_run.judge_run_id,
            usage=parsed.usage,
            finish_reason="openai_refusal_unsupported",
            refusal_detected=True,
        )
        checks_failed.append("openai_refusal_unsupported")
        return _execution_result(
            judge_call_requested_event_found=event_found,
            judge_run_loaded=judge_run_loaded,
            evidence_bundle_loaded=bundle_loaded,
            judge_request_built=request_built,
            judge_request_uses_bundle_only=uses_bundle_only,
            openai_responses_request_shape_valid=shape_valid,
            checks_failed=checks_failed,
        )
    if parsed.payload_json is None:
        _mark_judge_run_failed(
            connection,
            judge_run_id=judge_run.judge_run_id,
            usage=parsed.usage,
            finish_reason="structured_output_missing",
            refusal_detected=False,
        )
        checks_failed.append("openai_structured_output_missing")
        return _execution_result(
            judge_call_requested_event_found=event_found,
            judge_run_loaded=judge_run_loaded,
            evidence_bundle_loaded=bundle_loaded,
            judge_request_built=request_built,
            judge_request_uses_bundle_only=uses_bundle_only,
            openai_responses_request_shape_valid=shape_valid,
            checks_failed=checks_failed,
        )

    payload = parsed.payload_json
    structured_received = payload is not None and structured_output_payload_valid(
        payload,
        candidate_group_id=bundle.candidate_group_id,
    )
    if not structured_received:
        _mark_judge_run_failed(
            connection,
            judge_run_id=judge_run.judge_run_id,
            usage=parsed.usage,
            finish_reason="structured_output_invalid",
            refusal_detected=False,
        )
        checks_failed.append("openai_structured_output_invalid")
        return _execution_result(
            judge_call_requested_event_found=event_found,
            judge_run_loaded=judge_run_loaded,
            evidence_bundle_loaded=bundle_loaded,
            judge_request_built=request_built,
            judge_request_uses_bundle_only=uses_bundle_only,
            openai_responses_request_shape_valid=shape_valid,
            openai_structured_output_received=structured_received,
            checks_failed=checks_failed,
        )

    judge_output_id = _insert_judge_output(
        connection,
        judge_run_id=judge_run.judge_run_id,
        candidate_group_id=bundle.candidate_group_id,
        judge_schema_version=judge_run.schema_version,
        payload=payload,
    )

    _mark_judge_run_succeeded(
        connection,
        judge_run_id=judge_run.judge_run_id,
        usage=parsed.usage,
        finish_reason=parsed.finish_reason,
    )
    _insert_or_reuse_judge_output_ready_event(
        connection,
        replay_namespace=replay_namespace,
        judge_run_id=judge_run.judge_run_id,
        judge_output_id=judge_output_id,
        finish_reason=parsed.finish_reason,
        refusal_detected=False,
    )
    verification = _verify_success(
        connection,
        replay_namespace=replay_namespace,
        judge_run_id=judge_run.judge_run_id,
        judge_output_id=judge_output_id,
        candidate_group_id=bundle.candidate_group_id,
    )
    checks_failed.extend(verification["checks_failed"])

    return ReplayExecutionResult(
        judge_call_requested_event_found=event_found,
        judge_run_loaded=judge_run_loaded,
        evidence_bundle_loaded=bundle_loaded,
        judge_request_built=request_built,
        judge_request_uses_bundle_only=uses_bundle_only,
        openai_responses_request_shape_valid=shape_valid,
        openai_structured_output_received=structured_received,
        judge_output_created=verification["judge_output_created"],
        judge_run_updated=verification["judge_run_updated"],
        judge_output_ready_event_created=verification["judge_output_ready_event_created"],
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
    required = ("model", "reasoning_effort", "prompt_version")
    if judge_run_id is None or bundle_id is None or any(not payload.get(key) for key in required):
        return None
    return JudgeCallRequestedEvent(
        event_id=UUID(str(row["event_id"])),
        judge_run_id=judge_run_id,
        bundle_id=bundle_id,
        model=str(payload["model"]),
        reasoning_effort=str(payload["reasoning_effort"]),
        prompt_version=str(payload["prompt_version"]),
        prompt_cache_key=_string_or_none(payload.get("prompt_cache_key")),
    )


def _load_judge_run(connection: Any, judge_run_id: UUID) -> JudgeRunRecord | None:
    import sqlalchemy as sa

    row = connection.execute(
        sa.text(
            """
            SELECT judge_run_id, bundle_id, judge_profile, model, reasoning_effort,
                   prompt_version, schema_version, policy_version, prompt_cache_key, status
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
        judge_profile=str(row["judge_profile"]),
        model=str(row["model"]),
        reasoning_effort=str(row["reasoning_effort"]),
        prompt_version=str(row["prompt_version"]),
        schema_version=str(row["schema_version"]),
        policy_version=str(row["policy_version"]),
        prompt_cache_key=_string_or_none(row["prompt_cache_key"]),
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
    if event.prompt_cache_key and judge_run.prompt_cache_key != event.prompt_cache_key:
        failures.append("judge_run_prompt_cache_key_mismatch")
    return failures


def _load_bundle_context(connection: Any, bundle_id: UUID) -> BundleContext | None:
    import sqlalchemy as sa

    row = connection.execute(
        sa.text(
            """
            SELECT b.bundle_id, b.candidate_group_id, b.current_primary_artifact_id,
                   b.primary_summary, b.supporting_summaries_json,
                   b.discovered_links_summary_json, b.evidence_limitations,
                   b.token_budget_profile, b.reroot_count, b.ready_for_analysis,
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
        current_primary_artifact_id=UUID(str(row["current_primary_artifact_id"])),
        current_bundle_id=_uuid_or_none(row["current_bundle_id"]),
        primary_summary=_json_loads(row["primary_summary"]) or {},
        supporting_summaries_json=_json_loads(row["supporting_summaries_json"]) or [],
        discovered_links_summary_json=_json_loads(row["discovered_links_summary_json"]) or [],
        evidence_limitations=_json_loads(row["evidence_limitations"]) or [],
        token_budget_profile=_string_or_none(row["token_budget_profile"]),
        reroot_count=int(row["reroot_count"]),
        ready_for_analysis=bool(row["ready_for_analysis"]),
    )


def _validate_bundle_context(bundle: BundleContext, event: JudgeCallRequestedEvent) -> list[str]:
    failures: list[str] = []
    if bundle.bundle_id != event.bundle_id:
        failures.append("bundle_id_mismatch")
    if bundle.current_bundle_id != event.bundle_id:
        failures.append("candidate_current_bundle_mismatch")
    if not bundle.ready_for_analysis:
        failures.append("evidence_bundle_not_ready")
    if not bundle.primary_summary:
        failures.append("bundle_primary_summary_missing")
    if not bundle.token_budget_profile:
        failures.append("bundle_token_budget_profile_missing")
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


def _load_namespace_judge_output(
    connection: Any,
    *,
    replay_namespace: str,
    judge_run_id: UUID,
) -> JudgeOutputRecord | None:
    import sqlalchemy as sa

    row = connection.execute(
        sa.text(
            """
            SELECT payload_json
            FROM event_outbox
            WHERE event_type = :event_type
              AND aggregate_type = 'judge_run'
              AND aggregate_id = CAST(:judge_run_id AS uuid)
              AND dedupe_key LIKE :dedupe_prefix
            ORDER BY created_at DESC, event_id DESC
            LIMIT 1
            """
        ),
        {
            "event_type": JUDGE_OUTPUT_READY_EVENT_TYPE,
            "judge_run_id": str(judge_run_id),
            "dedupe_prefix": f"local-db-restricted-openai-judge:{replay_namespace}:judge.output.ready:%",
        },
    ).mappings().first()
    if row is None:
        return None
    payload = _json_loads(row["payload_json"]) or {}
    judge_output_id = _uuid_or_none(payload.get("judge_output_id"))
    if judge_output_id is None:
        return None
    return _load_judge_output_by_id(connection, judge_output_id=judge_output_id)


def _load_judge_output_by_id(
    connection: Any,
    *,
    judge_output_id: UUID,
) -> JudgeOutputRecord | None:
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


def _judge_output_matches(
    output: JudgeOutputRecord,
    *,
    candidate_group_id: UUID,
) -> bool:
    return (
        output.candidate_group_id == candidate_group_id
        and output.judge_schema_version == JUDGE_SCHEMA_VERSION
        and output.model_proposed_verdict == output.payload_json.get("model_proposed_verdict")
        and output.model_confidence_band == output.payload_json.get("model_confidence_band")
        and structured_output_payload_valid(output.payload_json, candidate_group_id=candidate_group_id)
    )


def _insert_judge_output(
    connection: Any,
    *,
    judge_run_id: UUID,
    candidate_group_id: UUID,
    judge_schema_version: str,
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
            "judge_schema_version": judge_schema_version,
            "payload_json": _json_dumps(payload),
            "model_proposed_verdict": _string_or_none(payload.get("model_proposed_verdict")),
            "model_confidence_band": _string_or_none(payload.get("model_confidence_band")),
        },
    )
    return UUID(str(result.scalar_one()))


def _mark_judge_run_running(connection: Any, *, judge_run_id: UUID) -> None:
    import sqlalchemy as sa

    connection.execute(
        sa.text(
            """
            UPDATE judge_runs
            SET status = 'running',
                started_at = COALESCE(started_at, now())
            WHERE judge_run_id = CAST(:judge_run_id AS uuid)
              AND status = 'pending'
            """
        ),
        {"judge_run_id": str(judge_run_id)},
    )


def _mark_judge_run_succeeded(
    connection: Any,
    *,
    judge_run_id: UUID,
    usage: ParsedOpenAIUsage,
    finish_reason: str,
) -> None:
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
              AND status IN ('pending', 'running', 'succeeded')
            """
        ),
        {
            "judge_run_id": str(judge_run_id),
            "input_tokens": usage.input_tokens,
            "cached_input_tokens": usage.cached_input_tokens,
            "output_tokens": usage.output_tokens,
            "reasoning_tokens": usage.reasoning_tokens,
            "latency_ms": usage.latency_ms,
            "finish_reason": finish_reason,
        },
    )


def _mark_judge_run_failed(
    connection: Any,
    *,
    judge_run_id: UUID,
    usage: ParsedOpenAIUsage,
    finish_reason: str,
    refusal_detected: bool,
) -> None:
    import sqlalchemy as sa

    connection.execute(
        sa.text(
            """
            UPDATE judge_runs
            SET status = 'failed_terminal',
                input_tokens = :input_tokens,
                cached_input_tokens = :cached_input_tokens,
                output_tokens = :output_tokens,
                reasoning_tokens = :reasoning_tokens,
                latency_ms = :latency_ms,
                finish_reason = :finish_reason,
                refusal_detected = :refusal_detected,
                finished_at = now()
            WHERE judge_run_id = CAST(:judge_run_id AS uuid)
              AND status IN ('pending', 'running')
            """
        ),
        {
            "judge_run_id": str(judge_run_id),
            "input_tokens": usage.input_tokens,
            "cached_input_tokens": usage.cached_input_tokens,
            "output_tokens": usage.output_tokens,
            "reasoning_tokens": usage.reasoning_tokens,
            "latency_ms": usage.latency_ms,
            "finish_reason": finish_reason,
            "refusal_detected": refusal_detected,
        },
    )


def _insert_or_reuse_judge_output_ready_event(
    connection: Any,
    *,
    replay_namespace: str,
    judge_run_id: UUID,
    judge_output_id: UUID,
    finish_reason: str,
    refusal_detected: bool,
) -> None:
    import sqlalchemy as sa

    payload = {
        "judge_run_id": str(judge_run_id),
        "judge_output_id": str(judge_output_id),
        "finish_reason": finish_reason,
        "refusal_detected": refusal_detected,
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
              ) AS judge_run_updated,
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
    output = _load_judge_output_by_id(connection, judge_output_id=judge_output_id)
    output_matches = (
        output is not None
        and output.judge_output_id == judge_output_id
        and output.judge_run_id == judge_run_id
        and _judge_output_matches(output, candidate_group_id=candidate_group_id)
    )
    checks = {
        "judge_output_created": int(row["judge_output_count"]) >= 1 and output_matches,
        "judge_run_updated": bool(row["judge_run_updated"]),
        "judge_output_ready_event_created": bool(row["ready_event_exists"]),
    }
    failures = [f"{key}:missing" for key, value in checks.items() if value is not True]
    return {**checks, "checks_failed": failures}


def _call_injected_openai_client(openai_client: Any, request: Mapping[str, Any]) -> Any:
    responses = getattr(openai_client, "responses", None)
    create = getattr(responses, "create", None)
    if not callable(create):
        create = getattr(openai_client, "create", None)
    if not callable(create):
        raise RuntimeError("openai_client_create_unavailable")
    response = create(**dict(request))
    if inspect.isawaitable(response):
        return asyncio.run(response)
    return response


def _extract_structured_payload(response: Any) -> dict[str, Any] | None:
    output_text = _read_value(response, "output_text")
    if isinstance(output_text, str) and output_text.strip():
        parsed = _json_object_or_none(output_text)
        if parsed is not None:
            return parsed
    for block in _content_blocks(response):
        if _read_value(block, "type") != "output_text":
            continue
        text = _read_value(block, "text")
        if isinstance(text, str) and text.strip():
            parsed = _json_object_or_none(text)
            if parsed is not None:
                return parsed
    return None


def _extract_refusal_text(response: Any) -> str | None:
    texts: list[str] = []
    for block in _content_blocks(response):
        if _read_value(block, "type") != "refusal":
            continue
        text = _read_value(block, "refusal") or _read_value(block, "text")
        if isinstance(text, str) and text.strip():
            texts.append(text.strip())
    return "\n".join(texts) if texts else None


def _content_blocks(response: Any) -> list[Any]:
    output = _read_value(response, "output")
    if not isinstance(output, list):
        return []
    blocks: list[Any] = []
    for item in output:
        if _read_value(item, "type") != "message":
            continue
        content = _read_value(item, "content")
        if isinstance(content, list):
            blocks.extend(content)
    return blocks


def _extract_usage(response: Any, *, started_monotonic: float) -> ParsedOpenAIUsage:
    usage = _read_value(response, "usage")
    input_details = _read_value(usage, "input_tokens_details") if usage is not None else None
    output_details = _read_value(usage, "output_tokens_details") if usage is not None else None
    return ParsedOpenAIUsage(
        input_tokens=_int_or_none(_read_value(usage, "input_tokens")),
        cached_input_tokens=_int_or_none(_read_value(input_details, "cached_tokens")),
        output_tokens=_int_or_none(_read_value(usage, "output_tokens")),
        reasoning_tokens=_int_or_none(_read_value(output_details, "reasoning_tokens")),
        latency_ms=max(0, int((time.monotonic() - started_monotonic) * 1000)),
    )


def _extract_finish_reason(response: Any) -> str | None:
    incomplete_details = _read_value(response, "incomplete_details")
    incomplete_reason = _string_or_none(_read_value(incomplete_details, "reason"))
    if incomplete_reason:
        return incomplete_reason
    finish_reason = _string_or_none(_read_value(response, "finish_reason"))
    if finish_reason:
        return finish_reason
    return _string_or_none(_read_value(response, "status"))


def _normalize_finish_reason(value: str | None) -> str:
    if value in {None, "", "completed", "complete", "success", "succeeded"}:
        return FAKE_FINISH_REASON
    return str(value)


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
    judge_run_loaded: bool = False,
    evidence_bundle_loaded: bool = False,
    judge_request_built: bool = False,
    judge_request_uses_bundle_only: bool = False,
    openai_responses_request_shape_valid: bool = False,
    openai_structured_output_received: bool = False,
    judge_output_created: bool = False,
    judge_run_updated: bool = False,
    judge_output_ready_event_created: bool = False,
    checks_failed: list[str] | tuple[str, ...],
) -> ReplayExecutionResult:
    return ReplayExecutionResult(
        judge_call_requested_event_found=judge_call_requested_event_found,
        judge_run_loaded=judge_run_loaded,
        evidence_bundle_loaded=evidence_bundle_loaded,
        judge_request_built=judge_request_built,
        judge_request_uses_bundle_only=judge_request_uses_bundle_only,
        openai_responses_request_shape_valid=openai_responses_request_shape_valid,
        openai_structured_output_received=openai_structured_output_received,
        judge_output_created=judge_output_created,
        judge_run_updated=judge_run_updated,
        judge_output_ready_event_created=judge_output_ready_event_created,
        checks_failed=tuple(dict.fromkeys(checks_failed)),
    )


def _base_report() -> dict[str, Any]:
    return {
        "schema_version": SCHEMA_VERSION,
        "status": "pass",
        "database_url_guard_passed": False,
        "openai_live_call_authorized": False,
        "openai_client_injected": False,
        "analysis_router_replay_confirmed": False,
        "judge_call_requested_event_found": False,
        "judge_run_loaded": False,
        "evidence_bundle_loaded": False,
        "judge_request_built": False,
        "judge_request_uses_bundle_only": False,
        "openai_responses_request_shape_valid": False,
        "openai_structured_output_received": False,
        "judge_output_created": False,
        "judge_run_updated": False,
        "judge_output_ready_event_created": False,
        "live_openai_called": False,
        "telegram_called": False,
        "live_github_called": False,
        "workers_started": False,
        "redis_mutation": False,
        "production_db_write": False,
        "alembic_or_ddl_ran": False,
        "analysis_created": False,
        "notification_created": False,
        "checks_failed": [],
    }


def _finish(report: dict[str, Any], checks_failed: Sequence[str]) -> RunnerResult:
    normalized_failures = list(dict.fromkeys(checks_failed))
    report["checks_failed"] = normalized_failures
    report["status"] = "fail" if normalized_failures else "pass"
    return RunnerResult(exit_code=0 if report["status"] == "pass" else 1, report=report)


def _input_message_valid(message: Any, *, expected_role: str) -> bool:
    if not isinstance(message, Mapping) or message.get("role") != expected_role:
        return False
    content = message.get("content")
    if not isinstance(content, list) or len(content) != 1:
        return False
    block = content[0]
    if not isinstance(block, Mapping):
        return False
    return block.get("type") == "input_text" and isinstance(block.get("text"), str) and bool(block["text"])


def _extract_user_context(request: Mapping[str, Any]) -> str:
    input_items = request.get("input")
    if not isinstance(input_items, list) or len(input_items) < 2:
        raise ValueError("input.missing")
    user_message = input_items[1]
    if not isinstance(user_message, Mapping):
        raise ValueError("input.user.invalid")
    content = user_message.get("content")
    if not isinstance(content, list) or not content:
        raise ValueError("input.user.content")
    block = content[0]
    if not isinstance(block, Mapping) or not isinstance(block.get("text"), str):
        raise ValueError("input.user.text")
    return str(block["text"])


def _contains_forbidden_request_tokens(request: Mapping[str, Any]) -> bool:
    text = _canonical_json(request).lower()
    return any(token in text for token in FORBIDDEN_REQUEST_TOKENS)


def _mapping(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _json_loads(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, str):
        return json.loads(value)
    return value


def _json_dumps(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True)


def _canonical_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _json_object_or_none(value: str) -> dict[str, Any] | None:
    try:
        parsed = json.loads(value)
    except json.JSONDecodeError:
        return None
    return parsed if isinstance(parsed, dict) else None


def _read_value(value: Any, key: str) -> Any:
    if value is None:
        return None
    if isinstance(value, Mapping):
        return value.get(key)
    return getattr(value, key, None)


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


def _int_or_none(value: Any) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


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
