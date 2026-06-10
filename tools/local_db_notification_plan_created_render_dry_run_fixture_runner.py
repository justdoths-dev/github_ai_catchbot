from __future__ import annotations

import argparse
import json
import os
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping, Protocol, Sequence
from urllib.parse import parse_qsl, urlparse
from uuid import UUID

from tools import local_db_judge_output_ready_policy_apply_fixture_runner as upstream_runner
from tools import local_db_notifier_fixture_replay_runner as notifier_base


SCHEMA_VERSION = "local_db_notification_plan_created_render_dry_run_fixture_runner_v1"
NOTIFICATION_PLAN_CREATED_EVENT_TYPE = "notification.plan.created.v1"
NOTIFICATION_DELIVERY_RESULT_EVENT_TYPE = "notification.delivery.result.v1"
DRY_RUN_REASON_CODE = "dry_run_skip_transport"
DELIVERY_FROM_STATE = "rendered"
DELIVERY_TO_STATE = "suppressed"
MAX_TELEGRAM_TEXT_CHARS = 4096
DRY_RUN_RESPONSE = {
    "noop": True,
    "dry_run": True,
    "local_fixture": True,
    "transport_skipped": True,
    "reason_code": DRY_RUN_REASON_CODE,
}
SAFE_EXCEPTION_MESSAGES = {
    "notification_plan_created_event_missing_or_invalid",
    "notification_plan_created_event_ambiguous",
    "notification_intent_payload_invalid",
    "notification_plan_created_event_aggregate_mismatch",
    "suppress_delivery_decision_refused",
    "analysis_missing",
    "judge_output_missing",
    "candidate_group_missing",
    "primary_artifact_missing",
    "notifier_context_invalid",
    "notification_plan_material_conflict",
    "notification_plan_intent_mismatch",
}
TRUE_RESULT_KEYS = (
    "database_url_guard_passed",
    "notification_plan_created_event_found",
    "analysis_loaded",
    "judge_output_loaded",
    "candidate_group_loaded",
    "primary_artifact_loaded",
    "notification_plan_concretized",
    "notification_render_created",
    "render_length_within_limit",
    "render_hash_stable",
    "dry_run_delivery_record_created",
    "notification_state_transition_recorded",
    "notification_delivery_result_event_created",
)
FALSE_RESULT_KEYS = (
    "verdict_recomputed",
    "delivery_decision_overridden",
    "openai_called",
    "telegram_called",
    "live_github_called",
    "workers_started",
    "redis_mutation",
    "production_db_write",
    "alembic_or_ddl_ran",
)

source_candidate_runner = upstream_runner.source_candidate_runner
github_snapshot_runner = upstream_runner.github_snapshot_runner


@dataclass(frozen=True, slots=True)
class RunnerResult:
    exit_code: int
    report: dict[str, Any]


@dataclass(frozen=True, slots=True)
class PlanEventResolutionResult:
    notification_plan_created_event_id: UUID | None
    notification_plan_created_event_found: bool
    delivery_dedupe_namespace: str | None
    upstream_fixture_replayed: bool = False
    checks_failed: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class PlanEvent:
    event_id: UUID
    aggregate_type: str | None
    aggregate_id: UUID | None
    payload: notifier_base.NotificationPlanIntent


@dataclass(frozen=True, slots=True)
class RenderDryRunExecutionResult:
    notification_plan_created_event_found: bool
    analysis_loaded: bool
    judge_output_loaded: bool
    candidate_group_loaded: bool
    primary_artifact_loaded: bool
    notification_plan_concretized: bool
    notification_render_created: bool
    render_length_within_limit: bool
    render_hash_stable: bool
    dry_run_delivery_record_created: bool
    notification_state_transition_recorded: bool
    notification_delivery_result_event_created: bool
    verdict_recomputed: bool = False
    delivery_decision_overridden: bool = False
    checks_failed: tuple[str, ...] = ()


class PlanEventResolver(Protocol):
    def resolve(
        self,
        *,
        database_url: str,
        source_fixture_path: Path,
        github_snapshot_fixture_path: Path,
        replay_namespace: str,
        env: Mapping[str, str],
        repo_root: Path,
    ) -> PlanEventResolutionResult: ...


class UpstreamPolicyApplyRunner(Protocol):
    def run(
        self,
        *,
        database_url: str,
        source_fixture_path: Path,
        github_snapshot_fixture_path: Path,
        replay_namespace: str,
        env: Mapping[str, str],
        repo_root: Path,
    ) -> upstream_runner.RunnerResult: ...


class RenderDryRunExecutor(Protocol):
    def execute(
        self,
        *,
        database_url: str,
        notification_plan_created_event_id: UUID,
        delivery_dedupe_namespace: str,
    ) -> RenderDryRunExecutionResult: ...


class DefaultUpstreamPolicyApplyRunner:
    def run(
        self,
        *,
        database_url: str,
        source_fixture_path: Path,
        github_snapshot_fixture_path: Path,
        replay_namespace: str,
        env: Mapping[str, str],
        repo_root: Path,
    ) -> upstream_runner.RunnerResult:
        args = argparse.Namespace(
            database_url=database_url,
            notification_plan_created_event_id=None,
            judge_output_ready_event_id=None,
            source_fixture=str(source_fixture_path),
            github_snapshot_fixture=str(github_snapshot_fixture_path),
            replay_namespace=replay_namespace,
            confirm_local_test_db=True,
        )
        predecessor_env = dict(env)
        predecessor_env["APP_ENV"] = "test"
        return upstream_runner.run(args, env=predecessor_env, repo_root=repo_root)


class DefaultPlanEventResolver:
    def __init__(self, *, upstream: UpstreamPolicyApplyRunner | None = None) -> None:
        self._upstream = upstream or DefaultUpstreamPolicyApplyRunner()

    def resolve(
        self,
        *,
        database_url: str,
        source_fixture_path: Path,
        github_snapshot_fixture_path: Path,
        replay_namespace: str,
        env: Mapping[str, str],
        repo_root: Path,
    ) -> PlanEventResolutionResult:
        before = _find_non_suppress_plan_event_ids_by_namespace(
            database_url=database_url,
            replay_namespace=replay_namespace,
        )
        if len(before) > 1:
            return PlanEventResolutionResult(
                notification_plan_created_event_id=None,
                notification_plan_created_event_found=False,
                delivery_dedupe_namespace=replay_namespace,
                checks_failed=("notification_plan_created_event_ambiguous",),
            )
        if len(before) == 1:
            return PlanEventResolutionResult(
                notification_plan_created_event_id=before[0],
                notification_plan_created_event_found=True,
                delivery_dedupe_namespace=replay_namespace,
            )

        predecessor = self._upstream.run(
            database_url=database_url,
            source_fixture_path=source_fixture_path,
            github_snapshot_fixture_path=github_snapshot_fixture_path,
            replay_namespace=replay_namespace,
            env=env,
            repo_root=repo_root,
        )
        if not _upstream_fixture_result_acceptable(predecessor):
            return PlanEventResolutionResult(
                notification_plan_created_event_id=None,
                notification_plan_created_event_found=False,
                delivery_dedupe_namespace=replay_namespace,
                upstream_fixture_replayed=True,
                checks_failed=("upstream_policy_apply_fixture_failed",),
            )

        after = _find_non_suppress_plan_event_ids_by_namespace(
            database_url=database_url,
            replay_namespace=replay_namespace,
        )
        if len(after) > 1:
            return PlanEventResolutionResult(
                notification_plan_created_event_id=None,
                notification_plan_created_event_found=False,
                delivery_dedupe_namespace=replay_namespace,
                upstream_fixture_replayed=True,
                checks_failed=("notification_plan_created_event_ambiguous",),
            )
        if len(after) != 1:
            return PlanEventResolutionResult(
                notification_plan_created_event_id=None,
                notification_plan_created_event_found=False,
                delivery_dedupe_namespace=replay_namespace,
                upstream_fixture_replayed=True,
                checks_failed=("notification_plan_created_event_missing_or_invalid",),
            )
        return PlanEventResolutionResult(
            notification_plan_created_event_id=after[0],
            notification_plan_created_event_found=True,
            delivery_dedupe_namespace=replay_namespace,
            upstream_fixture_replayed=True,
        )


class SqlAlchemyRenderDryRunExecutor:
    def execute(
        self,
        *,
        database_url: str,
        notification_plan_created_event_id: UUID,
        delivery_dedupe_namespace: str,
    ) -> RenderDryRunExecutionResult:
        notifier_base._bootstrap_repo_imports()  # noqa: SLF001 - local tool reuse.
        import sqlalchemy as sa

        engine = sa.create_engine(database_url, future=True)
        try:
            with engine.begin() as connection:
                return _execute_render_dry_run(
                    connection,
                    notification_plan_created_event_id=notification_plan_created_event_id,
                    delivery_dedupe_namespace=delivery_dedupe_namespace,
                )
        finally:
            engine.dispose()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Consume one local/test DB notification.plan.created.v1 plan-intent, "
            "concretize notifier-owned rows, record a dry-run suppressed delivery "
            "result, and emit one notification.delivery.result.v1 event without "
            "Telegram transport."
        )
    )
    parser.add_argument("--database-url", required=True)
    parser.add_argument("--notification-plan-created-event-id")
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
    resolver: PlanEventResolver | None = None,
    executor: RenderDryRunExecutor | None = None,
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

    explicit_event_id = _uuid_or_none(getattr(args, "notification_plan_created_event_id", None))
    if getattr(args, "notification_plan_created_event_id", None) and explicit_event_id is None:
        checks_failed.append("notification_plan_created_event_id_invalid")

    source_fixture = _path_or_none(getattr(args, "source_fixture", None))
    github_fixture = _path_or_none(getattr(args, "github_snapshot_fixture", None))
    replay_namespace = _string_or_none(getattr(args, "replay_namespace", None))
    fixture_selector_supplied = source_fixture is not None or github_fixture is not None

    if explicit_event_id is not None and fixture_selector_supplied:
        checks_failed.append("selector_mode_ambiguous")

    if explicit_event_id is None:
        if source_fixture is None or github_fixture is None or replay_namespace is None:
            checks_failed.append("fixture_selector_required")
        if fixture_selector_supplied and (source_fixture is None or github_fixture is None):
            checks_failed.append("fixture_path_pair_required")

    if replay_namespace is not None:
        namespace_ok, namespace_failures = source_candidate_runner.validate_replay_namespace(replay_namespace)
        checks_failed.extend(namespace_failures)
        if not namespace_ok:
            replay_namespace = None

    if explicit_event_id is None and source_fixture is not None and github_fixture is not None:
        try:
            source_candidate_runner.load_source_fixture(source_fixture, repo_root=root)
        except Exception:  # noqa: BLE001 - sanitized operator result only.
            checks_failed.append("source_fixture_load_failed")
        try:
            github_snapshot_runner.load_github_snapshot_fixture(github_fixture, repo_root=root)
        except Exception:  # noqa: BLE001 - sanitized operator result only.
            checks_failed.append("github_snapshot_fixture_load_failed")

    if checks_failed:
        return _finish(report, checks_failed)

    if explicit_event_id is not None:
        delivery_dedupe_namespace = replay_namespace or _namespace_for_explicit_plan_event(explicit_event_id)
        resolved_event_id = explicit_event_id
    else:
        if source_fixture is None or github_fixture is None or replay_namespace is None:
            return _finish(report, ["fixture_selector_required"])
        active_resolver = resolver or DefaultPlanEventResolver()
        try:
            resolution = active_resolver.resolve(
                database_url=args.database_url,
                source_fixture_path=source_fixture,
                github_snapshot_fixture_path=github_fixture,
                replay_namespace=replay_namespace,
                env=effective_env,
                repo_root=root,
            )
        except Exception:  # noqa: BLE001 - never echo DB errors or URLs.
            return _finish(report, ["notification_plan_created_event_resolution_failed"])
        report["notification_plan_created_event_found"] = resolution.notification_plan_created_event_found
        checks_failed.extend(resolution.checks_failed)
        resolved_event_id = resolution.notification_plan_created_event_id
        delivery_dedupe_namespace = resolution.delivery_dedupe_namespace or replay_namespace
        if resolved_event_id is None:
            checks_failed.append("notification_plan_created_event_missing_or_invalid")
        if checks_failed:
            return _finish(report, checks_failed)

    active_executor = executor or SqlAlchemyRenderDryRunExecutor()
    try:
        execution = active_executor.execute(
            database_url=args.database_url,
            notification_plan_created_event_id=resolved_event_id,
            delivery_dedupe_namespace=delivery_dedupe_namespace,
        )
    except Exception as exc:  # noqa: BLE001 - never echo DB errors or URLs.
        return _finish(report, [_safe_failure_code(exc)])

    report.update(
        {
            "notification_plan_created_event_found": execution.notification_plan_created_event_found,
            "analysis_loaded": execution.analysis_loaded,
            "judge_output_loaded": execution.judge_output_loaded,
            "candidate_group_loaded": execution.candidate_group_loaded,
            "primary_artifact_loaded": execution.primary_artifact_loaded,
            "notification_plan_concretized": execution.notification_plan_concretized,
            "notification_render_created": execution.notification_render_created,
            "render_length_within_limit": execution.render_length_within_limit,
            "render_hash_stable": execution.render_hash_stable,
            "dry_run_delivery_record_created": execution.dry_run_delivery_record_created,
            "notification_state_transition_recorded": execution.notification_state_transition_recorded,
            "notification_delivery_result_event_created": execution.notification_delivery_result_event_created,
            "verdict_recomputed": execution.verdict_recomputed,
            "delivery_decision_overridden": execution.delivery_decision_overridden,
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


def validate_database_url(database_url: str | None):
    return upstream_runner.validate_database_url(database_url)


def build_notification_render(
    *,
    intent: notifier_base.NotificationPlanIntent,
    analysis: notifier_base.AnalysisRecord,
    judge_output: notifier_base.JudgeOutputRecord,
    candidate: notifier_base.CandidateContext,
) -> notifier_base.NotificationRender:
    payload = judge_output.payload_json
    headline = (
        _string_or_none(payload.get("headline"))
        or _string_or_none(payload.get("title"))
        or _string_or_none(candidate.primary_canonical_id)
        or "Untitled candidate"
    )
    first_lines = [
        f"Urgency: {intent.urgency_profile}",
        f"Primary source: {_string_or_none(candidate.primary_artifact_type) or 'unknown'}",
        f"Headline: {_single_line(headline, max_chars=220)}",
        f"Final verdict: {analysis.verdict}",
        f"Delivery decision: {analysis.delivery_decision}",
    ]
    optional_lines = [
        ("Summary", _string_or_none(payload.get("summary_one_line_ko"))),
        ("Skeptical take", _string_or_none(payload.get("skeptical_take_ko"))),
        ("Recommended action", _coerce_text(analysis.recommended_action_ko)),
        ("Evidence limitations", _coerce_text(analysis.evidence_limitations_ko)),
        ("Risk flags", _coerce_text(payload.get("red_flags_ko"))),
        ("Why it might matter", _string_or_none(payload.get("why_it_might_matter_ko"))),
        ("Reason codes", ", ".join(analysis.reason_codes_json) if analysis.reason_codes_json else None),
        ("Freshness", _coerce_text(analysis.freshness_note_ko)),
        ("Subject", _string_or_none(candidate.primary_canonical_id)),
    ]
    message_text = _budget_message(first_lines, optional_lines, max_chars=MAX_TELEGRAM_TEXT_CHARS)
    reply_markup = _build_inline_keyboard(primary_url=candidate.primary_canonical_url)
    link_preview_options = {"is_disabled": True}
    entities: list[dict[str, Any]] = []
    render_hash_payload = {
        "notification_plan_id": str(intent.notification_plan_id),
        "message_text": message_text,
        "entities_json": entities,
        "reply_markup_json": reply_markup,
        "link_preview_options_json": link_preview_options,
        "disable_notification": intent.urgency_profile != "high",
        "protect_content": False,
        "parse_strategy": "entities",
    }
    render_hash = stable_render_hash(render_hash_payload)
    return notifier_base.NotificationRender(
        notification_plan_id=intent.notification_plan_id,
        message_text=message_text,
        entities_json=entities,
        reply_markup_json=reply_markup,
        link_preview_options_json=link_preview_options,
        disable_notification=intent.urgency_profile != "high",
        protect_content=False,
        parse_strategy="entities",
        render_hash=render_hash,
    )


def stable_render_hash(payload: Mapping[str, Any]) -> str:
    return notifier_base.stable_render_hash(payload)


def build_delivery_result_payload(
    *,
    notification_plan_id: UUID,
    notification_delivery_record_id: UUID,
    telegram_chat_id: int | None,
) -> dict[str, Any]:
    return {
        "notification_plan_id": str(notification_plan_id),
        "delivery_status": DELIVERY_TO_STATE,
        "telegram_chat_id": telegram_chat_id,
        "telegram_message_id": None,
        "notification_delivery_record_id": str(notification_delivery_record_id),
        "attempt_count": 0,
        "transport_error_code": DRY_RUN_REASON_CODE,
        "transport_error_class": None,
        "edited": False,
        **DRY_RUN_RESPONSE,
    }


def build_delivery_result_event_dedupe_key(
    *,
    delivery_dedupe_namespace: str,
    notification_plan_id: UUID | str,
    notification_delivery_record_id: UUID | str,
) -> str:
    return (
        "local-db-notification-render-dry-run:"
        f"{delivery_dedupe_namespace}:notification.delivery.result:"
        f"{notification_plan_id}:{notification_delivery_record_id}"
    )


def context_failure_codes(
    *,
    intent: notifier_base.NotificationPlanIntent,
    analysis: notifier_base.AnalysisRecord,
    judge_output: notifier_base.JudgeOutputRecord,
) -> tuple[str, ...]:
    failures = notifier_base._notifier_context_failures(  # noqa: SLF001 - local fixture helper reuse.
        intent=intent,
        analysis=analysis,
        judge_output=judge_output,
    )
    return tuple(failures)


def _execute_render_dry_run(
    connection: Any,
    *,
    notification_plan_created_event_id: UUID,
    delivery_dedupe_namespace: str,
) -> RenderDryRunExecutionResult:
    checks_failed: list[str] = []
    event = _load_plan_event_by_id(connection, notification_plan_created_event_id)
    if event is None:
        return _execution_result(
            notification_plan_created_event_found=False,
            checks_failed=("notification_plan_created_event_missing_or_invalid",),
        )

    intent = event.payload
    if event.aggregate_type != "analysis" or event.aggregate_id != intent.analysis_id:
        return _execution_result(
            notification_plan_created_event_found=True,
            checks_failed=("notification_plan_created_event_aggregate_mismatch",),
        )
    if intent.delivery_decision == "suppress":
        return _execution_result(
            notification_plan_created_event_found=True,
            checks_failed=("suppress_delivery_decision_refused",),
        )

    analysis = notifier_base._load_analysis(connection, intent.analysis_id)  # noqa: SLF001
    analysis_loaded = analysis is not None
    if analysis is None:
        return _execution_result(
            notification_plan_created_event_found=True,
            analysis_loaded=False,
            checks_failed=("analysis_missing",),
        )

    judge_output = notifier_base._load_judge_output(connection, analysis.judge_output_id)  # noqa: SLF001
    candidate = notifier_base._load_candidate_context(connection, intent.candidate_group_id)  # noqa: SLF001
    judge_output_loaded = judge_output is not None
    candidate_loaded = candidate is not None
    primary_artifact_loaded = bool(
        candidate
        and candidate.current_primary_artifact_id is not None
        and candidate.primary_artifact_type is not None
    )
    if judge_output is None:
        checks_failed.append("judge_output_missing")
    if candidate is None:
        checks_failed.append("candidate_group_missing")
    elif not primary_artifact_loaded:
        checks_failed.append("primary_artifact_missing")
    if checks_failed:
        return _execution_result(
            notification_plan_created_event_found=True,
            analysis_loaded=analysis_loaded,
            judge_output_loaded=judge_output_loaded,
            candidate_group_loaded=candidate_loaded,
            primary_artifact_loaded=primary_artifact_loaded,
            checks_failed=checks_failed,
        )
    assert judge_output is not None
    assert candidate is not None

    context_failures = context_failure_codes(intent=intent, analysis=analysis, judge_output=judge_output)
    if context_failures:
        return _execution_result(
            notification_plan_created_event_found=True,
            analysis_loaded=True,
            judge_output_loaded=True,
            candidate_group_loaded=True,
            primary_artifact_loaded=primary_artifact_loaded,
            checks_failed=context_failures,
        )

    analysis_before = notifier_base._load_analysis_digest(connection, analysis.analysis_id)  # noqa: SLF001
    judge_output_before = notifier_base._load_judge_output_digest(connection, judge_output.judge_output_id)  # noqa: SLF001
    candidate_before = notifier_base._load_candidate_digest(connection, candidate.candidate_group_id)  # noqa: SLF001

    plan_id = notifier_base._insert_or_reuse_notification_plan(connection, intent=intent)  # noqa: SLF001
    if plan_id != intent.notification_plan_id:
        return _execution_result(
            notification_plan_created_event_found=True,
            analysis_loaded=True,
            judge_output_loaded=True,
            candidate_group_loaded=True,
            primary_artifact_loaded=primary_artifact_loaded,
            checks_failed=("notification_plan_material_conflict",),
        )
    if not notifier_base._notification_plan_matches_intent(connection, intent=intent):  # noqa: SLF001
        checks_failed.append("notification_plan_intent_mismatch")

    render = build_notification_render(intent=intent, analysis=analysis, judge_output=judge_output, candidate=candidate)
    render_id = notifier_base._insert_or_reuse_notification_render(connection, render=render)  # noqa: SLF001
    _mark_plan_status(connection, notification_plan_id=intent.notification_plan_id, status=DELIVERY_FROM_STATE)
    delivery_record_id = _insert_or_reuse_dry_run_delivery_record(connection, intent=intent)
    _mark_plan_status(connection, notification_plan_id=intent.notification_plan_id, status=DELIVERY_TO_STATE)
    _insert_or_reuse_delivery_state_transition(connection, notification_plan_id=intent.notification_plan_id)
    _insert_or_reuse_delivery_result_event(
        connection,
        delivery_dedupe_namespace=delivery_dedupe_namespace,
        notification_plan_id=intent.notification_plan_id,
        notification_delivery_record_id=delivery_record_id,
        telegram_chat_id=intent.target_chat_id,
    )

    verification = _verify_success(
        connection,
        delivery_dedupe_namespace=delivery_dedupe_namespace,
        intent=intent,
        analysis=analysis,
        judge_output=judge_output,
        candidate=candidate,
        render=render,
        render_id=render_id,
        delivery_record_id=delivery_record_id,
        analysis_before=analysis_before,
        judge_output_before=judge_output_before,
        candidate_before=candidate_before,
    )
    checks_failed.extend(verification["checks_failed"])

    return RenderDryRunExecutionResult(
        notification_plan_created_event_found=True,
        analysis_loaded=True,
        judge_output_loaded=True,
        candidate_group_loaded=True,
        primary_artifact_loaded=primary_artifact_loaded,
        notification_plan_concretized=verification["notification_plan_concretized"],
        notification_render_created=verification["notification_render_created"],
        render_length_within_limit=verification["render_length_within_limit"],
        render_hash_stable=verification["render_hash_stable"],
        dry_run_delivery_record_created=verification["dry_run_delivery_record_created"],
        notification_state_transition_recorded=verification["notification_state_transition_recorded"],
        notification_delivery_result_event_created=verification["notification_delivery_result_event_created"],
        verdict_recomputed=verification["verdict_recomputed"],
        delivery_decision_overridden=verification["delivery_decision_overridden"],
        checks_failed=tuple(dict.fromkeys(checks_failed)),
    )


def _load_plan_event_by_id(connection: Any, event_id: UUID) -> PlanEvent | None:
    import sqlalchemy as sa

    row = connection.execute(
        sa.text(
            """
            SELECT event_id, event_type, aggregate_type, aggregate_id, payload_json
            FROM event_outbox
            WHERE event_id = CAST(:event_id AS uuid)
            """
        ),
        {"event_id": str(event_id)},
    ).mappings().first()
    if row is None or str(row["event_type"]) != NOTIFICATION_PLAN_CREATED_EVENT_TYPE:
        return None
    payload = notifier_base._json_loads(row["payload_json"]) or {}  # noqa: SLF001
    valid, _failures = notifier_base.validate_notification_intent_payload(payload)
    if not valid:
        return None
    return PlanEvent(
        event_id=UUID(str(row["event_id"])),
        aggregate_type=_string_or_none(row["aggregate_type"]),
        aggregate_id=_uuid_or_none(row["aggregate_id"]),
        payload=notifier_base.notification_plan_intent_from_payload(payload),
    )


def _find_non_suppress_plan_event_ids_by_namespace(
    *,
    database_url: str,
    replay_namespace: str,
) -> list[UUID]:
    notifier_base._bootstrap_repo_imports()  # noqa: SLF001
    import sqlalchemy as sa

    engine = sa.create_engine(database_url, future=True)
    try:
        with engine.begin() as connection:
            rows = connection.execute(
                sa.text(
                    """
                    SELECT event_id
                    FROM event_outbox
                    WHERE event_type = :event_type
                      AND dedupe_key LIKE :dedupe_prefix
                      AND COALESCE(payload_json ->> 'delivery_decision', '') <> 'suppress'
                    ORDER BY created_at, event_id
                    """
                ),
                {
                    "event_type": NOTIFICATION_PLAN_CREATED_EVENT_TYPE,
                    "dedupe_prefix": f"local-db-policy-engine:{replay_namespace}:notification.plan.created:%",
                },
            ).scalars().all()
            return [UUID(str(row)) for row in rows]
    finally:
        engine.dispose()


def _mark_plan_status(connection: Any, *, notification_plan_id: UUID, status: str) -> None:
    import sqlalchemy as sa

    connection.execute(
        sa.text(
            """
            UPDATE notification_plans
            SET status = CAST(:status AS notification_status_enum)
            WHERE notification_plan_id = CAST(:notification_plan_id AS uuid)
            """
        ),
        {"notification_plan_id": str(notification_plan_id), "status": status},
    )


def _insert_or_reuse_dry_run_delivery_record(
    connection: Any,
    *,
    intent: notifier_base.NotificationPlanIntent,
) -> UUID:
    import sqlalchemy as sa

    existing = _load_dry_run_delivery_record_id(connection, notification_plan_id=intent.notification_plan_id)
    if existing is not None:
        return existing
    result = connection.execute(
        sa.text(
            """
            INSERT INTO notification_delivery_records (
                notification_plan_id,
                telegram_chat_id,
                telegram_message_id,
                delivery_status,
                sent_at,
                edited_at,
                attempt_count,
                transport_error_code,
                transport_error_class,
                telegram_response_json,
                created_at
            ) VALUES (
                CAST(:notification_plan_id AS uuid),
                :telegram_chat_id,
                NULL,
                'suppressed'::notification_status_enum,
                NULL,
                NULL,
                0,
                :transport_error_code,
                NULL,
                CAST(:telegram_response_json AS jsonb),
                now()
            )
            RETURNING notification_delivery_record_id
            """
        ),
        {
            "notification_plan_id": str(intent.notification_plan_id),
            "telegram_chat_id": intent.target_chat_id,
            "transport_error_code": DRY_RUN_REASON_CODE,
            "telegram_response_json": _json_dumps(DRY_RUN_RESPONSE),
        },
    )
    return UUID(str(result.scalar_one()))


def _load_dry_run_delivery_record_id(connection: Any, *, notification_plan_id: UUID) -> UUID | None:
    import sqlalchemy as sa

    value = connection.execute(
        sa.text(
            """
            SELECT notification_delivery_record_id
            FROM notification_delivery_records
            WHERE notification_plan_id = CAST(:notification_plan_id AS uuid)
              AND delivery_status = 'suppressed'::notification_status_enum
              AND telegram_message_id IS NULL
              AND attempt_count = 0
              AND transport_error_code = :transport_error_code
              AND telegram_response_json @> CAST(:telegram_response_json AS jsonb)
            ORDER BY created_at, notification_delivery_record_id
            LIMIT 1
            """
        ),
        {
            "notification_plan_id": str(notification_plan_id),
            "transport_error_code": DRY_RUN_REASON_CODE,
            "telegram_response_json": _json_dumps(
                {"noop": True, "dry_run": True, "reason_code": DRY_RUN_REASON_CODE}
            ),
        },
    ).scalar_one_or_none()
    return UUID(str(value)) if value is not None else None


def _insert_or_reuse_delivery_state_transition(connection: Any, *, notification_plan_id: UUID) -> None:
    import sqlalchemy as sa

    existing = connection.execute(
        sa.text(
            """
            SELECT 1
            FROM state_transitions
            WHERE object_type = 'notification_plan'
              AND object_id = CAST(:notification_plan_id AS uuid)
              AND from_state = :from_state
              AND to_state = :to_state
              AND reason_code = :reason_code
            LIMIT 1
            """
        ),
        {
            "notification_plan_id": str(notification_plan_id),
            "from_state": DELIVERY_FROM_STATE,
            "to_state": DELIVERY_TO_STATE,
            "reason_code": DRY_RUN_REASON_CODE,
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
                'notification_plan',
                CAST(:notification_plan_id AS uuid),
                :from_state,
                :to_state,
                :reason_code,
                now()
            )
            """
        ),
        {
            "notification_plan_id": str(notification_plan_id),
            "from_state": DELIVERY_FROM_STATE,
            "to_state": DELIVERY_TO_STATE,
            "reason_code": DRY_RUN_REASON_CODE,
        },
    )


def _insert_or_reuse_delivery_result_event(
    connection: Any,
    *,
    delivery_dedupe_namespace: str,
    notification_plan_id: UUID,
    notification_delivery_record_id: UUID,
    telegram_chat_id: int | None,
) -> None:
    import sqlalchemy as sa

    payload = build_delivery_result_payload(
        notification_plan_id=notification_plan_id,
        notification_delivery_record_id=notification_delivery_record_id,
        telegram_chat_id=telegram_chat_id,
    )
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
                'notification_plan',
                CAST(:notification_plan_id AS uuid),
                :dedupe_key,
                CAST(:payload_json AS jsonb),
                'pending'::outbox_status_enum,
                now()
            )
            ON CONFLICT ON CONSTRAINT uq_event_outbox_dedupe_key
            DO NOTHING
            """
        ),
        {
            "event_type": NOTIFICATION_DELIVERY_RESULT_EVENT_TYPE,
            "notification_plan_id": str(notification_plan_id),
            "dedupe_key": build_delivery_result_event_dedupe_key(
                delivery_dedupe_namespace=delivery_dedupe_namespace,
                notification_plan_id=notification_plan_id,
                notification_delivery_record_id=notification_delivery_record_id,
            ),
            "payload_json": _json_dumps(payload),
        },
    )


def _verify_success(
    connection: Any,
    *,
    delivery_dedupe_namespace: str,
    intent: notifier_base.NotificationPlanIntent,
    analysis: notifier_base.AnalysisRecord,
    judge_output: notifier_base.JudgeOutputRecord,
    candidate: notifier_base.CandidateContext,
    render: notifier_base.NotificationRender,
    render_id: UUID,
    delivery_record_id: UUID,
    analysis_before: str,
    judge_output_before: str,
    candidate_before: str,
) -> dict[str, Any]:
    import sqlalchemy as sa

    counts = connection.execute(
        sa.text(
            """
            SELECT
              (SELECT count(*) FROM notification_plans
               WHERE notification_plan_id = CAST(:notification_plan_id AS uuid)
                 AND analysis_id = CAST(:analysis_id AS uuid)
                 AND candidate_group_id = CAST(:candidate_group_id AS uuid)
                 AND delivery_decision = CAST(:delivery_decision AS delivery_decision_enum)
                 AND urgency_profile = CAST(:urgency_profile AS urgency_profile_enum)
                 AND target_chat_id = :target_chat_id
                 AND material_change_hash = :material_change_hash
                 AND status = 'suppressed'::notification_status_enum) AS notification_plans,
              (SELECT count(*) FROM notification_renders
               WHERE notification_render_id = CAST(:notification_render_id AS uuid)
                 AND notification_plan_id = CAST(:notification_plan_id AS uuid)
                 AND message_text = :message_text
                 AND render_hash = :render_hash
                 AND link_preview_options_json = CAST(:link_preview_options_json AS jsonb)) AS notification_renders,
              (SELECT count(*) FROM notification_delivery_records
               WHERE notification_delivery_record_id = CAST(:notification_delivery_record_id AS uuid)
                 AND notification_plan_id = CAST(:notification_plan_id AS uuid)
                 AND delivery_status = 'suppressed'::notification_status_enum
                 AND telegram_chat_id = :telegram_chat_id
                 AND telegram_message_id IS NULL
                 AND attempt_count = 0
                 AND transport_error_code = :reason_code
                 AND transport_error_class IS NULL
                 AND telegram_response_json @> CAST(:telegram_response_json AS jsonb)) AS delivery_records,
              (SELECT count(*) FROM state_transitions
               WHERE object_type = 'notification_plan'
                 AND object_id = CAST(:notification_plan_id AS uuid)
                 AND from_state = :from_state
                 AND to_state = :to_state
                 AND reason_code = :reason_code) AS state_transitions,
              (SELECT count(*) FROM event_outbox
               WHERE event_type = :event_type
                 AND aggregate_type = 'notification_plan'
                 AND aggregate_id = CAST(:notification_plan_id AS uuid)
                 AND dedupe_key = :dedupe_key
                 AND payload_json = CAST(:payload_json AS jsonb)) AS delivery_result_events
            """
        ),
        {
            "notification_plan_id": str(intent.notification_plan_id),
            "analysis_id": str(intent.analysis_id),
            "candidate_group_id": str(intent.candidate_group_id),
            "delivery_decision": intent.delivery_decision,
            "urgency_profile": intent.urgency_profile,
            "target_chat_id": intent.target_chat_id,
            "telegram_chat_id": intent.target_chat_id,
            "material_change_hash": intent.material_change_hash,
            "notification_render_id": str(render_id),
            "message_text": render.message_text,
            "render_hash": render.render_hash,
            "link_preview_options_json": _json_dumps({"is_disabled": True}),
            "notification_delivery_record_id": str(delivery_record_id),
            "reason_code": DRY_RUN_REASON_CODE,
            "telegram_response_json": _json_dumps(
                {"noop": True, "dry_run": True, "reason_code": DRY_RUN_REASON_CODE}
            ),
            "from_state": DELIVERY_FROM_STATE,
            "to_state": DELIVERY_TO_STATE,
            "event_type": NOTIFICATION_DELIVERY_RESULT_EVENT_TYPE,
            "dedupe_key": build_delivery_result_event_dedupe_key(
                delivery_dedupe_namespace=delivery_dedupe_namespace,
                notification_plan_id=intent.notification_plan_id,
                notification_delivery_record_id=delivery_record_id,
            ),
            "payload_json": _json_dumps(
                build_delivery_result_payload(
                    notification_plan_id=intent.notification_plan_id,
                    notification_delivery_record_id=delivery_record_id,
                    telegram_chat_id=intent.target_chat_id,
                )
            ),
        },
    ).mappings().one()
    rerendered = build_notification_render(
        intent=intent,
        analysis=analysis,
        judge_output=judge_output,
        candidate=candidate,
    )
    verdict_recomputed = notifier_base._load_analysis_digest(connection, analysis.analysis_id) != analysis_before  # noqa: SLF001
    delivery_decision_overridden = verdict_recomputed
    judge_output_mutated = notifier_base._load_judge_output_digest(  # noqa: SLF001
        connection, judge_output.judge_output_id
    ) != judge_output_before
    candidate_group_mutated = notifier_base._load_candidate_digest(  # noqa: SLF001
        connection, candidate.candidate_group_id
    ) != candidate_before
    checks = {
        "notification_plan_concretized": int(counts["notification_plans"]) == 1,
        "notification_render_created": int(counts["notification_renders"]) == 1,
        "render_length_within_limit": len(render.message_text) <= MAX_TELEGRAM_TEXT_CHARS,
        "render_hash_stable": render.render_hash == rerendered.render_hash,
        "dry_run_delivery_record_created": int(counts["delivery_records"]) == 1,
        "notification_state_transition_recorded": int(counts["state_transitions"]) == 1,
        "notification_delivery_result_event_created": int(counts["delivery_result_events"]) == 1,
        "verdict_recomputed": verdict_recomputed,
        "delivery_decision_overridden": delivery_decision_overridden,
        "judge_output_mutated": judge_output_mutated,
        "candidate_group_mutated": candidate_group_mutated,
    }
    failures: list[str] = []
    for key in (
        "notification_plan_concretized",
        "notification_render_created",
        "render_length_within_limit",
        "render_hash_stable",
        "dry_run_delivery_record_created",
        "notification_state_transition_recorded",
        "notification_delivery_result_event_created",
    ):
        if checks[key] is not True:
            failures.append(f"{key}:missing")
    for key in (
        "verdict_recomputed",
        "delivery_decision_overridden",
        "judge_output_mutated",
        "candidate_group_mutated",
    ):
        if checks[key] is not False:
            failures.append(f"{key}:unexpected")
    return {**checks, "checks_failed": failures}


def _upstream_fixture_result_acceptable(predecessor: upstream_runner.RunnerResult) -> bool:
    if predecessor.exit_code != 0 or predecessor.report.get("status") != "pass":
        return False
    expected_true = (
        "database_url_guard_passed",
        "judge_output_ready_event_found",
        "judge_run_loaded",
        "judge_output_loaded",
        "evidence_bundle_loaded",
        "candidate_group_loaded",
        "analysis_validator_passed",
        "analysis_policy_apply_event_created",
        "policy_engine_applied",
        "analysis_created",
        "notification_plan_created_event_created",
    )
    expected_false = (
        "openai_called",
        "telegram_called",
        "live_github_called",
        "workers_started",
        "redis_mutation",
        "production_db_write",
        "alembic_or_ddl_ran",
        "notification_created",
    )
    return all(predecessor.report.get(key) is True for key in expected_true) and all(
        predecessor.report.get(key) is False for key in expected_false
    )


def _base_report() -> dict[str, Any]:
    return {
        "schema_version": SCHEMA_VERSION,
        "status": "pass",
        "database_url_guard_passed": False,
        "notification_plan_created_event_found": False,
        "analysis_loaded": False,
        "judge_output_loaded": False,
        "candidate_group_loaded": False,
        "primary_artifact_loaded": False,
        "notification_plan_concretized": False,
        "notification_render_created": False,
        "render_length_within_limit": False,
        "render_hash_stable": False,
        "dry_run_delivery_record_created": False,
        "notification_state_transition_recorded": False,
        "notification_delivery_result_event_created": False,
        "verdict_recomputed": False,
        "delivery_decision_overridden": False,
        "openai_called": False,
        "telegram_called": False,
        "live_github_called": False,
        "workers_started": False,
        "redis_mutation": False,
        "production_db_write": False,
        "alembic_or_ddl_ran": False,
        "checks_failed": [],
    }


def _execution_result(
    *,
    notification_plan_created_event_found: bool = False,
    analysis_loaded: bool = False,
    judge_output_loaded: bool = False,
    candidate_group_loaded: bool = False,
    primary_artifact_loaded: bool = False,
    notification_plan_concretized: bool = False,
    notification_render_created: bool = False,
    render_length_within_limit: bool = False,
    render_hash_stable: bool = False,
    dry_run_delivery_record_created: bool = False,
    notification_state_transition_recorded: bool = False,
    notification_delivery_result_event_created: bool = False,
    verdict_recomputed: bool = False,
    delivery_decision_overridden: bool = False,
    checks_failed: Sequence[str],
) -> RenderDryRunExecutionResult:
    return RenderDryRunExecutionResult(
        notification_plan_created_event_found=notification_plan_created_event_found,
        analysis_loaded=analysis_loaded,
        judge_output_loaded=judge_output_loaded,
        candidate_group_loaded=candidate_group_loaded,
        primary_artifact_loaded=primary_artifact_loaded,
        notification_plan_concretized=notification_plan_concretized,
        notification_render_created=notification_render_created,
        render_length_within_limit=render_length_within_limit,
        render_hash_stable=render_hash_stable,
        dry_run_delivery_record_created=dry_run_delivery_record_created,
        notification_state_transition_recorded=notification_state_transition_recorded,
        notification_delivery_result_event_created=notification_delivery_result_event_created,
        verdict_recomputed=verdict_recomputed,
        delivery_decision_overridden=delivery_decision_overridden,
        checks_failed=tuple(dict.fromkeys(checks_failed)),
    )


def _finish(report: dict[str, Any], checks_failed: Sequence[str]) -> RunnerResult:
    normalized_failures = list(dict.fromkeys(checks_failed))
    report["checks_failed"] = normalized_failures
    report["status"] = "fail" if normalized_failures else "pass"
    return RunnerResult(exit_code=0 if report["status"] == "pass" else 1, report=report)


def _build_inline_keyboard(*, primary_url: str | None) -> dict[str, Any] | None:
    safe_primary = _safe_url_or_none(primary_url)
    if safe_primary is None:
        return None
    return {"inline_keyboard": [[{"text": "Primary Link", "url": safe_primary}]]}


def _safe_url_or_none(value: Any) -> str | None:
    text = _string_or_none(value)
    if text is None or any(ord(char) < 32 for char in text):
        return None
    parsed = urlparse(text)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        return None
    if parsed.username or parsed.password:
        return None
    sensitive_keys = {"token", "api_key", "apikey", "key", "secret", "password"}
    query_keys = {key.lower() for key, _value in parse_qsl(parsed.query, keep_blank_values=True)}
    if sensitive_keys.intersection(query_keys):
        return None
    return text


def _budget_message(
    first_lines: Sequence[str],
    optional_lines: Sequence[tuple[str, str | None]],
    *,
    max_chars: int,
) -> str:
    selected = [line for line in first_lines if _string_or_none(line)]
    for label, value in optional_lines:
        text = _coerce_text(value)
        if text is None:
            continue
        candidate = selected + [f"{label}: {_single_line(text, max_chars=900)}"]
        if len("\n".join(candidate)) <= max_chars:
            selected = candidate
    message = "\n".join(selected)
    if len(message) <= max_chars:
        return message
    suffix = "\n[truncated]"
    return message[: max(0, max_chars - len(suffix))].rstrip() + suffix


def _single_line(value: str, *, max_chars: int) -> str:
    normalized = " ".join(str(value).split())
    if len(normalized) <= max_chars:
        return normalized
    return normalized[: max(0, max_chars - 3)].rstrip() + "..."


def _coerce_text(value: Any) -> str | None:
    if isinstance(value, list):
        joined = "; ".join(str(item).strip() for item in value if str(item).strip())
        return joined or None
    return _string_or_none(value)


def _json_dumps(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, default=_json_default)


def _json_default(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, UUID):
        return str(value)
    if isinstance(value, datetime):
        return _as_utc(value).isoformat()
    return str(value)


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _uuid_or_none(value: Any) -> UUID | None:
    if value is None:
        return None
    try:
        return UUID(str(value))
    except (TypeError, ValueError, AttributeError):
        return None


def _path_or_none(value: Any) -> Path | None:
    text = _string_or_none(value)
    return Path(text) if text is not None else None


def _string_or_none(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text if text else None


def _safe_failure_code(exc: Exception) -> str:
    message = str(exc)
    if message in SAFE_EXCEPTION_MESSAGES:
        return message
    return exc.__class__.__name__


def _namespace_for_explicit_plan_event(event_id: UUID) -> str:
    return f"event-{event_id}"


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    result = run(args)
    sys.stdout.write(render_json(result.report))
    return result.exit_code


if __name__ == "__main__":
    raise SystemExit(main())
