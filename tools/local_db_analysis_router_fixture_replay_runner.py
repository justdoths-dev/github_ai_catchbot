from __future__ import annotations

import argparse
import json
import os
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping, Protocol, Sequence
from uuid import UUID

from tools import local_db_evidence_bundle_fixture_replay_runner as evidence_bundle_runner


SCHEMA_VERSION = "local_db_analysis_router_fixture_replay_v1"
ANALYSIS_REQUESTED_EVENT_TYPE = "analysis.requested.v1"
JUDGE_CALL_REQUESTED_EVENT_TYPE = "judge.call.requested.v1"
DEFAULT_MODEL = "gpt-5.4-mini"
ESCALATION_MODEL = "gpt-5.4"
DEFAULT_REASONING_EFFORT = "low"
ESCALATION_REASONING_EFFORT = "medium"
SCHEMA_OUTPUT_VERSION = "judge_output_v1"
POLICY_VERSION = "verdict_policy_v1"
PROMPT_VERSIONS = {
    "github_primary": "judge_github_primary_v1",
    "x_primary": "judge_x_primary_v1",
    "text_idea_primary": "judge_text_idea_primary_v1",
}
ALLOWED_JUDGE_PROFILES = frozenset(PROMPT_VERSIONS)
TRUE_RESULT_KEYS = (
    "database_url_guard_passed",
    "source_candidate_replay_confirmed",
    "artifact_snapshot_replay_confirmed",
    "evidence_bundle_replay_confirmed",
    "analysis_requested_event_found",
    "candidate_current_bundle_confirmed",
    "evidence_bundle_ready_confirmed",
    "judge_profile_allowed",
    "routing_policy_applied",
    "judge_run_created_or_reused",
    "judge_call_requested_event_created",
    "default_model_selected",
    "prompt_cache_key_created",
)
FALSE_RESULT_KEYS = (
    "production_db_write",
    "live_github_called",
    "live_telegram_called",
    "openai_called",
    "workers_started",
    "redis_mutation",
    "judge_output_created",
    "analysis_created",
    "notification_created",
)


@dataclass(frozen=True, slots=True)
class RunnerResult:
    exit_code: int
    report: dict[str, Any]


@dataclass(frozen=True, slots=True)
class ReplayExecutionResult:
    analysis_requested_event_found: bool
    candidate_current_bundle_confirmed: bool
    evidence_bundle_ready_confirmed: bool
    judge_profile_allowed: bool
    routing_policy_applied: bool
    judge_run_created_or_reused: bool
    judge_call_requested_event_created: bool
    default_model_selected: bool
    prompt_cache_key_created: bool
    checks_failed: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class AnalysisRequestedEvent:
    event_id: UUID
    candidate_group_id: UUID
    bundle_id: UUID
    judge_profile: str
    escalation_allowed: bool


@dataclass(frozen=True, slots=True)
class BundleRouteRecord:
    bundle_id: UUID
    candidate_group_id: UUID
    reroot_count: int
    ready_for_analysis: bool
    token_budget_profile: str | None


@dataclass(frozen=True, slots=True)
class BundleShapeStats:
    member_count: int
    supporting_count: int


@dataclass(frozen=True, slots=True)
class RoutingDecision:
    judge_profile: str
    model: str
    reasoning_effort: str
    prompt_version: str
    schema_version: str
    policy_version: str
    prompt_cache_key: str


class AnalysisRouterReplayExecutor(Protocol):
    def execute(
        self,
        *,
        database_url: str,
        replay_namespace: str,
    ) -> ReplayExecutionResult: ...


class EvidenceBundleReplayRunner(Protocol):
    def run(
        self,
        *,
        database_url: str,
        source_fixture_path: Path,
        github_snapshot_fixture_path: Path,
        replay_namespace: str,
        env: Mapping[str, str],
        repo_root: Path,
    ) -> evidence_bundle_runner.RunnerResult: ...


class DefaultEvidenceBundleReplayRunner:
    def run(
        self,
        *,
        database_url: str,
        source_fixture_path: Path,
        github_snapshot_fixture_path: Path,
        replay_namespace: str,
        env: Mapping[str, str],
        repo_root: Path,
    ) -> evidence_bundle_runner.RunnerResult:
        args = argparse.Namespace(
            database_url=database_url,
            source_fixture=str(source_fixture_path),
            github_snapshot_fixture=str(github_snapshot_fixture_path),
            replay_namespace=replay_namespace,
            confirm_local_test_db=True,
        )
        predecessor_env = dict(env)
        predecessor_env["APP_ENV"] = "test"
        return evidence_bundle_runner.run(args, env=predecessor_env, repo_root=repo_root)


class SqlAlchemyAnalysisRouterReplayExecutor:
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
                return _execute_analysis_router_replay(
                    connection,
                    replay_namespace=replay_namespace,
                )
        finally:
            engine.dispose()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Replay predecessor local/test DB fixtures through analysis.requested, "
            "then deterministically write or reuse a judge_runs row and emit one "
            "namespace-scoped judge.call.requested.v1 event."
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
    executor: AnalysisRouterReplayExecutor | None = None,
    predecessor_runner: EvidenceBundleReplayRunner | None = None,
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

    namespace_ok, namespace_failures = evidence_bundle_runner.source_candidate_runner.validate_replay_namespace(
        args.replay_namespace
    )
    checks_failed.extend(namespace_failures)

    database_ok, database_failures, _ = validate_database_url(args.database_url)
    report["database_url_guard_passed"] = database_ok
    checks_failed.extend(database_failures)

    try:
        evidence_bundle_runner.source_candidate_runner.load_source_fixture(
            Path(args.source_fixture),
            repo_root=root,
        )
    except Exception:  # noqa: BLE001 - operator output must stay sanitized.
        checks_failed.append("source_fixture_load_failed")

    try:
        evidence_bundle_runner.github_snapshot_runner.load_github_snapshot_fixture(
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

    active_predecessor = predecessor_runner or DefaultEvidenceBundleReplayRunner()
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
        checks_failed.append("evidence_bundle_replay_failed")
        return _finish(report, checks_failed)

    if not _predecessor_result_acceptable(predecessor):
        checks_failed.append("evidence_bundle_replay_failed")
        return _finish(report, checks_failed)

    report["source_candidate_replay_confirmed"] = predecessor.report.get("source_candidate_replay_confirmed") is True
    report["artifact_snapshot_replay_confirmed"] = predecessor.report.get("artifact_snapshot_replay_confirmed") is True
    report["evidence_bundle_replay_confirmed"] = _predecessor_evidence_confirmed(predecessor.report)
    if report["source_candidate_replay_confirmed"] is not True:
        checks_failed.append("source_candidate_replay_not_confirmed")
    if report["artifact_snapshot_replay_confirmed"] is not True:
        checks_failed.append("artifact_snapshot_replay_not_confirmed")
    if report["evidence_bundle_replay_confirmed"] is not True:
        checks_failed.append("evidence_bundle_replay_not_confirmed")
    if checks_failed:
        return _finish(report, checks_failed)

    active_executor = executor or SqlAlchemyAnalysisRouterReplayExecutor()
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
            "analysis_requested_event_found": execution.analysis_requested_event_found,
            "candidate_current_bundle_confirmed": execution.candidate_current_bundle_confirmed,
            "evidence_bundle_ready_confirmed": execution.evidence_bundle_ready_confirmed,
            "judge_profile_allowed": execution.judge_profile_allowed,
            "routing_policy_applied": execution.routing_policy_applied,
            "judge_run_created_or_reused": execution.judge_run_created_or_reused,
            "judge_call_requested_event_created": execution.judge_call_requested_event_created,
            "default_model_selected": execution.default_model_selected,
            "prompt_cache_key_created": execution.prompt_cache_key_created,
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
) -> tuple[bool, list[str], evidence_bundle_runner.source_candidate_runner.ParsedDatabaseUrl | None]:
    return evidence_bundle_runner.validate_database_url(database_url)


def is_judge_profile_allowed(judge_profile: str | None) -> bool:
    return (judge_profile or "").strip() in ALLOWED_JUDGE_PROFILES


def prompt_version_for_profile(judge_profile: str) -> str:
    profile = judge_profile.strip()
    if profile not in PROMPT_VERSIONS:
        raise ValueError("judge_profile_not_allowed")
    return PROMPT_VERSIONS[profile]


def build_prompt_cache_key(
    *,
    judge_profile: str,
    prompt_version: str,
    schema_version: str,
    policy_version: str,
) -> str:
    return f"judge:{judge_profile}:{prompt_version}:{schema_version}:{policy_version}"


def select_routing_decision(
    *,
    judge_profile: str,
    escalation_allowed: bool,
    reroot_count: int,
    supporting_count: int,
    token_budget_profile: str | None,
    enable_model_escalation: bool = False,
) -> RoutingDecision:
    prompt_version = prompt_version_for_profile(judge_profile)
    use_escalation = (
        enable_model_escalation
        and escalation_allowed
        and (
            reroot_count > 0
            or supporting_count >= 3
            or token_budget_profile in {"large", "xlarge"}
        )
    )
    model = ESCALATION_MODEL if use_escalation else DEFAULT_MODEL
    reasoning_effort = ESCALATION_REASONING_EFFORT if use_escalation else DEFAULT_REASONING_EFFORT
    prompt_cache_key = build_prompt_cache_key(
        judge_profile=judge_profile,
        prompt_version=prompt_version,
        schema_version=SCHEMA_OUTPUT_VERSION,
        policy_version=POLICY_VERSION,
    )
    return RoutingDecision(
        judge_profile=judge_profile,
        model=model,
        reasoning_effort=reasoning_effort,
        prompt_version=prompt_version,
        schema_version=SCHEMA_OUTPUT_VERSION,
        policy_version=POLICY_VERSION,
        prompt_cache_key=prompt_cache_key,
    )


def build_judge_call_requested_dedupe_key(
    *,
    replay_namespace: str,
    bundle_id: UUID,
    model: str,
    reasoning_effort: str,
    prompt_version: str,
) -> str:
    return (
        f"local-db-analysis-router:{replay_namespace}:judge.call.requested:"
        f"{bundle_id}:{model}:{reasoning_effort}:{prompt_version}"
    )


def _execute_analysis_router_replay(
    connection: Any,
    *,
    replay_namespace: str,
) -> ReplayExecutionResult:
    checks_failed: list[str] = []

    event = _load_analysis_requested_event(connection, replay_namespace=replay_namespace)
    analysis_event_found = event is not None
    if event is None:
        checks_failed.append("analysis_requested_event_missing_or_invalid")
        return _execution_result(
            analysis_requested_event_found=analysis_event_found,
            checks_failed=checks_failed,
        )

    current_bundle_id = _load_candidate_current_bundle_id(connection, event.candidate_group_id)
    candidate_current_confirmed = current_bundle_id == event.bundle_id
    if not candidate_current_confirmed:
        checks_failed.append("candidate_current_bundle_mismatch")
        return _execution_result(
            analysis_requested_event_found=analysis_event_found,
            candidate_current_bundle_confirmed=candidate_current_confirmed,
            checks_failed=checks_failed,
        )

    bundle = _load_bundle(connection, event.bundle_id)
    shape = _load_bundle_shape(connection, event.bundle_id)
    evidence_ready_confirmed = (
        bundle is not None
        and bundle.candidate_group_id == event.candidate_group_id
        and bundle.ready_for_analysis
        and shape.member_count >= 1
    )
    if bundle is None:
        checks_failed.append("evidence_bundle_missing")
    elif bundle.candidate_group_id != event.candidate_group_id:
        checks_failed.append("evidence_bundle_candidate_mismatch")
    elif not bundle.ready_for_analysis:
        checks_failed.append("evidence_bundle_not_ready")
    if shape.member_count < 1:
        checks_failed.append("evidence_bundle_members_missing")
    if not evidence_ready_confirmed:
        return _execution_result(
            analysis_requested_event_found=analysis_event_found,
            candidate_current_bundle_confirmed=candidate_current_confirmed,
            evidence_bundle_ready_confirmed=evidence_ready_confirmed,
            checks_failed=checks_failed,
        )

    judge_profile_allowed = is_judge_profile_allowed(event.judge_profile)
    if not judge_profile_allowed:
        checks_failed.append("judge_profile_not_allowed")
        return _execution_result(
            analysis_requested_event_found=analysis_event_found,
            candidate_current_bundle_confirmed=candidate_current_confirmed,
            evidence_bundle_ready_confirmed=evidence_ready_confirmed,
            judge_profile_allowed=judge_profile_allowed,
            checks_failed=checks_failed,
        )

    if event.judge_profile != "github_primary" or event.escalation_allowed is not True:
        checks_failed.append("analysis_requested_fixture_payload_unexpected")
        return _execution_result(
            analysis_requested_event_found=analysis_event_found,
            candidate_current_bundle_confirmed=candidate_current_confirmed,
            evidence_bundle_ready_confirmed=evidence_ready_confirmed,
            judge_profile_allowed=judge_profile_allowed,
            checks_failed=checks_failed,
        )

    decision = select_routing_decision(
        judge_profile=event.judge_profile,
        escalation_allowed=event.escalation_allowed,
        reroot_count=bundle.reroot_count,
        supporting_count=shape.supporting_count,
        token_budget_profile=bundle.token_budget_profile,
        enable_model_escalation=False,
    )
    default_model_selected = (
        decision.model == DEFAULT_MODEL
        and decision.reasoning_effort == DEFAULT_REASONING_EFFORT
    )
    prompt_cache_key_created = bool(decision.prompt_cache_key)

    judge_run_id = _insert_or_reuse_judge_run(
        connection,
        bundle_id=event.bundle_id,
        decision=decision,
    )
    _insert_or_reuse_judge_call_requested_outbox(
        connection,
        replay_namespace=replay_namespace,
        judge_run_id=judge_run_id,
        bundle_id=event.bundle_id,
        decision=decision,
    )
    verification = _verify_judge_handoff(
        connection,
        replay_namespace=replay_namespace,
        judge_run_id=judge_run_id,
        bundle_id=event.bundle_id,
        decision=decision,
    )
    checks_failed.extend(verification["checks_failed"])

    return ReplayExecutionResult(
        analysis_requested_event_found=analysis_event_found,
        candidate_current_bundle_confirmed=candidate_current_confirmed,
        evidence_bundle_ready_confirmed=evidence_ready_confirmed,
        judge_profile_allowed=judge_profile_allowed,
        routing_policy_applied=True,
        judge_run_created_or_reused=verification["judge_run_created_or_reused"],
        judge_call_requested_event_created=verification["judge_call_requested_event_created"],
        default_model_selected=default_model_selected,
        prompt_cache_key_created=prompt_cache_key_created,
        checks_failed=tuple(dict.fromkeys(checks_failed)),
    )


def _load_analysis_requested_event(
    connection: Any,
    *,
    replay_namespace: str,
) -> AnalysisRequestedEvent | None:
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
            "event_type": ANALYSIS_REQUESTED_EVENT_TYPE,
            "dedupe_prefix": f"local-db-evidence-bundle:{replay_namespace}:analysis.requested:%",
        },
    ).mappings().first()
    if row is None:
        return None

    payload = _json_loads(row["payload_json"]) or {}
    candidate_group_id = _uuid_or_none(payload.get("candidate_group_id"))
    bundle_id = _uuid_or_none(payload.get("bundle_id"))
    judge_profile = payload.get("judge_profile")
    if candidate_group_id is None or bundle_id is None:
        return None
    if not isinstance(judge_profile, str) or not judge_profile.strip():
        return None
    if "escalation_allowed" not in payload:
        return None
    return AnalysisRequestedEvent(
        event_id=UUID(str(row["event_id"])),
        candidate_group_id=candidate_group_id,
        bundle_id=bundle_id,
        judge_profile=judge_profile.strip(),
        escalation_allowed=bool(payload["escalation_allowed"]),
    )


def _load_candidate_current_bundle_id(connection: Any, candidate_group_id: UUID) -> UUID | None:
    import sqlalchemy as sa

    row = connection.execute(
        sa.text(
            """
            SELECT current_bundle_id
            FROM candidate_group_proposals
            WHERE candidate_group_id = CAST(:candidate_group_id AS uuid)
            """
        ),
        {"candidate_group_id": str(candidate_group_id)},
    ).mappings().first()
    if row is None:
        return None
    return _uuid_or_none(row["current_bundle_id"])


def _load_bundle(connection: Any, bundle_id: UUID) -> BundleRouteRecord | None:
    import sqlalchemy as sa

    row = connection.execute(
        sa.text(
            """
            SELECT bundle_id, candidate_group_id, reroot_count,
                   ready_for_analysis, token_budget_profile
            FROM candidate_evidence_bundles
            WHERE bundle_id = CAST(:bundle_id AS uuid)
            """
        ),
        {"bundle_id": str(bundle_id)},
    ).mappings().first()
    if row is None:
        return None
    return BundleRouteRecord(
        bundle_id=UUID(str(row["bundle_id"])),
        candidate_group_id=UUID(str(row["candidate_group_id"])),
        reroot_count=int(row["reroot_count"]),
        ready_for_analysis=bool(row["ready_for_analysis"]),
        token_budget_profile=str(row["token_budget_profile"]) if row["token_budget_profile"] else None,
    )


def _load_bundle_shape(connection: Any, bundle_id: UUID) -> BundleShapeStats:
    import sqlalchemy as sa

    row = connection.execute(
        sa.text(
            """
            SELECT
                COUNT(*) AS member_count,
                COUNT(*) FILTER (WHERE member_role = 'supporting') AS supporting_count
            FROM candidate_evidence_members
            WHERE bundle_id = CAST(:bundle_id AS uuid)
            """
        ),
        {"bundle_id": str(bundle_id)},
    ).mappings().one()
    return BundleShapeStats(
        member_count=int(row["member_count"]),
        supporting_count=int(row["supporting_count"]),
    )


def _insert_or_reuse_judge_run(
    connection: Any,
    *,
    bundle_id: UUID,
    decision: RoutingDecision,
) -> UUID:
    import sqlalchemy as sa

    result = connection.execute(
        sa.text(
            """
            INSERT INTO judge_runs (
                bundle_id,
                judge_profile,
                model,
                reasoning_effort,
                prompt_version,
                schema_version,
                policy_version,
                prompt_cache_key,
                status
            ) VALUES (
                CAST(:bundle_id AS uuid),
                :judge_profile,
                :model,
                :reasoning_effort,
                :prompt_version,
                :schema_version,
                :policy_version,
                :prompt_cache_key,
                'pending'
            )
            ON CONFLICT ON CONSTRAINT uq_judge_runs_bundle_prompt_model_effort
            DO NOTHING
            RETURNING judge_run_id
            """
        ),
        {
            "bundle_id": str(bundle_id),
            "judge_profile": decision.judge_profile,
            "model": decision.model,
            "reasoning_effort": decision.reasoning_effort,
            "prompt_version": decision.prompt_version,
            "schema_version": decision.schema_version,
            "policy_version": decision.policy_version,
            "prompt_cache_key": decision.prompt_cache_key,
        },
    )
    judge_run_id = result.scalar_one_or_none()
    if judge_run_id:
        return UUID(str(judge_run_id))

    row = connection.execute(
        sa.text(
            """
            SELECT judge_run_id
            FROM judge_runs
            WHERE bundle_id = CAST(:bundle_id AS uuid)
              AND prompt_version = :prompt_version
              AND model = :model
              AND reasoning_effort = :reasoning_effort
            LIMIT 1
            """
        ),
        {
            "bundle_id": str(bundle_id),
            "prompt_version": decision.prompt_version,
            "model": decision.model,
            "reasoning_effort": decision.reasoning_effort,
        },
    ).scalar_one_or_none()
    if row is None:
        raise RuntimeError("judge_run_reuse_missing")
    return UUID(str(row))


def _insert_or_reuse_judge_call_requested_outbox(
    connection: Any,
    *,
    replay_namespace: str,
    judge_run_id: UUID,
    bundle_id: UUID,
    decision: RoutingDecision,
) -> None:
    import sqlalchemy as sa

    payload = {
        "judge_run_id": str(judge_run_id),
        "bundle_id": str(bundle_id),
        "model": decision.model,
        "reasoning_effort": decision.reasoning_effort,
        "prompt_version": decision.prompt_version,
        "prompt_cache_key": decision.prompt_cache_key,
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
            "event_type": JUDGE_CALL_REQUESTED_EVENT_TYPE,
            "judge_run_id": str(judge_run_id),
            "dedupe_key": build_judge_call_requested_dedupe_key(
                replay_namespace=replay_namespace,
                bundle_id=bundle_id,
                model=decision.model,
                reasoning_effort=decision.reasoning_effort,
                prompt_version=decision.prompt_version,
            ),
            "payload_json": _json_dumps(payload),
        },
    )


def _verify_judge_handoff(
    connection: Any,
    *,
    replay_namespace: str,
    judge_run_id: UUID,
    bundle_id: UUID,
    decision: RoutingDecision,
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
                  AND bundle_id = CAST(:bundle_id AS uuid)
                  AND judge_profile = :judge_profile
                  AND model = :model
                  AND reasoning_effort = :reasoning_effort
                  AND prompt_version = :prompt_version
                  AND schema_version = :schema_version
                  AND policy_version = :policy_version
                  AND prompt_cache_key = :prompt_cache_key
                  AND status = 'pending'
              ) AS judge_run_exists,
              EXISTS (
                SELECT 1
                FROM event_outbox
                WHERE event_type = :event_type
                  AND aggregate_type = 'judge_run'
                  AND aggregate_id = CAST(:judge_run_id AS uuid)
                  AND dedupe_key = :dedupe_key
              ) AS judge_call_event_exists
            """
        ),
        {
            "judge_run_id": str(judge_run_id),
            "bundle_id": str(bundle_id),
            "judge_profile": decision.judge_profile,
            "model": decision.model,
            "reasoning_effort": decision.reasoning_effort,
            "prompt_version": decision.prompt_version,
            "schema_version": decision.schema_version,
            "policy_version": decision.policy_version,
            "prompt_cache_key": decision.prompt_cache_key,
            "event_type": JUDGE_CALL_REQUESTED_EVENT_TYPE,
            "dedupe_key": build_judge_call_requested_dedupe_key(
                replay_namespace=replay_namespace,
                bundle_id=bundle_id,
                model=decision.model,
                reasoning_effort=decision.reasoning_effort,
                prompt_version=decision.prompt_version,
            ),
        },
    ).mappings().one()
    checks = {
        "judge_run_created_or_reused": bool(row["judge_run_exists"]),
        "judge_call_requested_event_created": bool(row["judge_call_event_exists"]),
    }
    failures = [f"{key}:missing" for key, value in checks.items() if value is not True]
    return {**checks, "checks_failed": failures}


def _predecessor_result_acceptable(predecessor: evidence_bundle_runner.RunnerResult) -> bool:
    if predecessor.report.get("status") == "pass" and predecessor.exit_code == 0:
        return True
    allowed_successor_failures = {
        "judge_run_created:unexpected",
        "judge_run_created_or_reused:unexpected",
        "judge_call_requested_event_created:unexpected",
        "judge_call_requested_event_exists:unexpected",
    }
    checks_failed = set(predecessor.report.get("checks_failed") or [])
    return bool(checks_failed) and checks_failed.issubset(allowed_successor_failures)


def _predecessor_evidence_confirmed(report: Mapping[str, Any]) -> bool:
    return all(
        report.get(key) is True
        for key in (
            "evidence_bundle_created_or_reused",
            "candidate_evidence_members_created_or_reused",
            "candidate_current_bundle_updated",
            "analysis_requested_event_created",
            "ready_for_analysis",
        )
    )


def _execution_result(
    *,
    analysis_requested_event_found: bool = False,
    candidate_current_bundle_confirmed: bool = False,
    evidence_bundle_ready_confirmed: bool = False,
    judge_profile_allowed: bool = False,
    checks_failed: list[str] | tuple[str, ...],
) -> ReplayExecutionResult:
    return ReplayExecutionResult(
        analysis_requested_event_found=analysis_requested_event_found,
        candidate_current_bundle_confirmed=candidate_current_bundle_confirmed,
        evidence_bundle_ready_confirmed=evidence_bundle_ready_confirmed,
        judge_profile_allowed=judge_profile_allowed,
        routing_policy_applied=False,
        judge_run_created_or_reused=False,
        judge_call_requested_event_created=False,
        default_model_selected=False,
        prompt_cache_key_created=False,
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
        "analysis_requested_event_found": False,
        "candidate_current_bundle_confirmed": False,
        "evidence_bundle_ready_confirmed": False,
        "judge_profile_allowed": False,
        "routing_policy_applied": False,
        "judge_run_created_or_reused": False,
        "judge_call_requested_event_created": False,
        "default_model_selected": False,
        "prompt_cache_key_created": False,
        "production_db_write": False,
        "live_github_called": False,
        "live_telegram_called": False,
        "openai_called": False,
        "workers_started": False,
        "redis_mutation": False,
        "judge_output_created": False,
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


def _uuid_or_none(value: Any) -> UUID | None:
    if value is None:
        return None
    try:
        return UUID(str(value))
    except (TypeError, ValueError, AttributeError):
        return None


def _safe_failure_code(exc: Exception) -> str:
    message = str(exc)
    if message in {"precondition_missing"}:
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
