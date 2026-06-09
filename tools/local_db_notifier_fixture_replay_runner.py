from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping, Protocol, Sequence
from uuid import UUID

from tools import local_db_policy_engine_fixture_replay_runner as policy_runner


SCHEMA_VERSION = "local_db_notifier_fixture_replay_v1"
NOTIFICATION_PLAN_CREATED_EVENT_TYPE = "notification.plan.created.v1"
NOTIFICATION_DELIVERY_RESULT_EVENT_TYPE = "notification.delivery.result.v1"
DELIVERY_STATE_REASON_CODE = "dry_run_skip_transport"
DELIVERY_FROM_STATE = "rendered"
DELIVERY_TO_STATE = "suppressed"
DRY_RUN_RESPONSE = {
    "dry_run": True,
    "local_fixture": True,
    "transport_skipped": True,
    "reason_code": DELIVERY_STATE_REASON_CODE,
}
REQUIRED_NOTIFICATION_INTENT_KEYS = (
    "notification_plan_id",
    "analysis_id",
    "candidate_group_id",
    "delivery_decision",
    "urgency_profile",
    "target_chat_id",
    "target_thread_id",
    "render_profile",
    "dedupe_subject_key",
    "material_change_hash",
    "send_after",
    "suppress_reason_code",
)
ALLOWED_DELIVERY_DECISIONS = frozenset({"send_now", "send_digest", "suppress"})
ALLOWED_URGENCY_PROFILES = frozenset({"high", "normal_silent", "digest", "suppressed"})
SAFE_EXCEPTION_MESSAGES = {
    "notification_plan_created_event_missing_or_invalid",
    "notification_plan_created_event_count_invalid",
    "notification_intent_payload_invalid",
    "analysis_missing",
    "judge_output_missing",
    "candidate_context_missing",
    "primary_artifact_context_missing",
    "notifier_context_invalid",
    "notification_plan_intent_mismatch",
    "notification_plan_material_conflict",
}
TRUE_RESULT_KEYS = (
    "database_url_guard_passed",
    "source_candidate_replay_confirmed",
    "artifact_snapshot_replay_confirmed",
    "evidence_bundle_replay_confirmed",
    "analysis_router_replay_confirmed",
    "judge_output_replay_confirmed",
    "analysis_validator_replay_confirmed",
    "policy_engine_replay_confirmed",
    "notification_plan_created_event_found",
    "analysis_loaded",
    "judge_output_loaded",
    "candidate_context_loaded",
    "notification_plan_created",
    "notification_render_created",
    "notification_delivery_record_created",
    "notification_delivery_state_transition_recorded",
    "notification_delivery_result_event_created",
)
FALSE_RESULT_KEYS = (
    "analysis_mutated",
    "judge_output_mutated",
    "candidate_group_mutated",
    "telegram_called",
    "send_message_called",
    "edit_message_called",
    "production_db_write",
    "live_github_called",
    "live_telegram_called",
    "openai_called",
    "workers_started",
    "redis_mutation",
)


@dataclass(frozen=True, slots=True)
class RunnerResult:
    exit_code: int
    report: dict[str, Any]


@dataclass(frozen=True, slots=True)
class ReplayExecutionResult:
    notification_plan_created_event_found: bool
    analysis_loaded: bool
    judge_output_loaded: bool
    candidate_context_loaded: bool
    notification_plan_created: bool
    notification_render_created: bool
    notification_delivery_record_created: bool
    notification_delivery_state_transition_recorded: bool
    notification_delivery_result_event_created: bool
    analysis_mutated: bool = False
    judge_output_mutated: bool = False
    candidate_group_mutated: bool = False
    telegram_called: bool = False
    send_message_called: bool = False
    edit_message_called: bool = False
    checks_failed: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class NotificationPlanCreatedEvent:
    event_id: UUID
    payload: "NotificationPlanIntent"


@dataclass(frozen=True, slots=True)
class NotificationPlanIntent:
    notification_plan_id: UUID
    analysis_id: UUID
    candidate_group_id: UUID
    delivery_decision: str
    urgency_profile: str
    target_chat_id: int
    target_thread_id: int | None
    render_profile: str
    dedupe_subject_key: str
    material_change_hash: str
    send_after: datetime | None
    suppress_reason_code: str | None


@dataclass(frozen=True, slots=True)
class AnalysisRecord:
    analysis_id: UUID
    candidate_group_id: UUID
    judge_output_id: UUID
    verdict: str
    delivery_decision: str
    reason_codes_json: list[str]
    evidence_limitations_ko: str | None
    recommended_action_ko: str | None
    freshness_note_ko: str | None
    policy_reconciled_flag: bool


@dataclass(frozen=True, slots=True)
class JudgeOutputRecord:
    judge_output_id: UUID
    candidate_group_id: UUID
    payload_json: dict[str, Any]
    model_proposed_verdict: str | None
    model_confidence_band: str | None


@dataclass(frozen=True, slots=True)
class CandidateContext:
    candidate_group_id: UUID
    current_primary_artifact_id: UUID | None
    primary_artifact_type: str | None
    primary_canonical_id: str | None
    primary_canonical_url: str | None


@dataclass(frozen=True, slots=True)
class NotificationRender:
    notification_plan_id: UUID
    message_text: str
    entities_json: list[dict[str, Any]]
    link_preview_options_json: dict[str, Any]
    reply_markup_json: dict[str, Any] | None
    disable_notification: bool
    protect_content: bool
    parse_strategy: str
    render_hash: str


@dataclass(frozen=True, slots=True)
class DeliveryResultPayload:
    notification_plan_id: UUID
    delivery_status: str
    telegram_chat_id: int | None
    telegram_message_id: int | None
    notification_delivery_record_id: UUID
    attempt_count: int
    transport_error_code: str | None
    transport_error_class: str | None
    edited: bool
    dry_run: bool = True
    local_fixture: bool = True
    reason_code: str = DELIVERY_STATE_REASON_CODE


class NotifierReplayExecutor(Protocol):
    def execute(
        self,
        *,
        database_url: str,
        replay_namespace: str,
    ) -> ReplayExecutionResult: ...


class PolicyEngineReplayRunner(Protocol):
    def run(
        self,
        *,
        database_url: str,
        source_fixture_path: Path,
        github_snapshot_fixture_path: Path,
        replay_namespace: str,
        env: Mapping[str, str],
        repo_root: Path,
    ) -> policy_runner.RunnerResult: ...


class DefaultPolicyEngineReplayRunner:
    def run(
        self,
        *,
        database_url: str,
        source_fixture_path: Path,
        github_snapshot_fixture_path: Path,
        replay_namespace: str,
        env: Mapping[str, str],
        repo_root: Path,
    ) -> policy_runner.RunnerResult:
        args = argparse.Namespace(
            database_url=database_url,
            source_fixture=str(source_fixture_path),
            github_snapshot_fixture=str(github_snapshot_fixture_path),
            replay_namespace=replay_namespace,
            confirm_local_test_db=True,
        )
        predecessor_env = dict(env)
        predecessor_env["APP_ENV"] = "test"
        return policy_runner.run(args, env=predecessor_env, repo_root=repo_root)


class SqlAlchemyNotifierReplayExecutor:
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
                return _execute_notifier_replay(connection, replay_namespace=replay_namespace)
        finally:
            engine.dispose()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Replay predecessor local/test DB fixtures through notification.plan.created.v1, "
            "then concretize notifier-owned fixture rows and emit one dry-run "
            "notification.delivery.result.v1 event without Telegram transport."
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
    executor: NotifierReplayExecutor | None = None,
    predecessor_runner: PolicyEngineReplayRunner | None = None,
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
        policy_runner.validator_runner.fake_judge_runner.analysis_router_runner.evidence_bundle_runner.source_candidate_runner.validate_replay_namespace(
            args.replay_namespace
        )
    )
    checks_failed.extend(namespace_failures)

    database_ok, database_failures, _ = validate_database_url(args.database_url)
    report["database_url_guard_passed"] = database_ok
    checks_failed.extend(database_failures)

    try:
        policy_runner.validator_runner.fake_judge_runner.analysis_router_runner.evidence_bundle_runner.source_candidate_runner.load_source_fixture(
            Path(args.source_fixture),
            repo_root=root,
        )
    except Exception:  # noqa: BLE001 - operator output must stay sanitized.
        checks_failed.append("source_fixture_load_failed")

    try:
        policy_runner.validator_runner.fake_judge_runner.analysis_router_runner.evidence_bundle_runner.github_snapshot_runner.load_github_snapshot_fixture(
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

    active_predecessor = predecessor_runner or DefaultPolicyEngineReplayRunner()
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
        checks_failed.append("policy_engine_replay_failed")
        return _finish(report, checks_failed)

    report["source_candidate_replay_confirmed"] = predecessor.report.get("source_candidate_replay_confirmed") is True
    report["artifact_snapshot_replay_confirmed"] = predecessor.report.get("artifact_snapshot_replay_confirmed") is True
    report["evidence_bundle_replay_confirmed"] = predecessor.report.get("evidence_bundle_replay_confirmed") is True
    report["analysis_router_replay_confirmed"] = predecessor.report.get("analysis_router_replay_confirmed") is True
    report["judge_output_replay_confirmed"] = predecessor.report.get("judge_output_replay_confirmed") is True
    report["analysis_validator_replay_confirmed"] = predecessor.report.get("analysis_validator_replay_confirmed") is True
    report["policy_engine_replay_confirmed"] = _predecessor_policy_confirmed(predecessor)
    for key in (
        "source_candidate_replay_confirmed",
        "artifact_snapshot_replay_confirmed",
        "evidence_bundle_replay_confirmed",
        "analysis_router_replay_confirmed",
        "judge_output_replay_confirmed",
        "analysis_validator_replay_confirmed",
        "policy_engine_replay_confirmed",
    ):
        if report[key] is not True:
            checks_failed.append(f"{key}:missing")
    if checks_failed:
        return _finish(report, checks_failed)

    active_executor = executor or SqlAlchemyNotifierReplayExecutor()
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
            "notification_plan_created_event_found": execution.notification_plan_created_event_found,
            "analysis_loaded": execution.analysis_loaded,
            "judge_output_loaded": execution.judge_output_loaded,
            "candidate_context_loaded": execution.candidate_context_loaded,
            "notification_plan_created": execution.notification_plan_created,
            "notification_render_created": execution.notification_render_created,
            "notification_delivery_record_created": execution.notification_delivery_record_created,
            "notification_delivery_state_transition_recorded": execution.notification_delivery_state_transition_recorded,
            "notification_delivery_result_event_created": execution.notification_delivery_result_event_created,
            "analysis_mutated": execution.analysis_mutated,
            "judge_output_mutated": execution.judge_output_mutated,
            "candidate_group_mutated": execution.candidate_group_mutated,
            "telegram_called": execution.telegram_called,
            "send_message_called": execution.send_message_called,
            "edit_message_called": execution.edit_message_called,
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
    return policy_runner.validate_database_url(database_url)


def validate_notification_intent_payload(payload: Mapping[str, Any]) -> tuple[bool, tuple[str, ...]]:
    if not isinstance(payload, Mapping):
        return False, ("payload_not_object",)
    failures: list[str] = []
    for key in REQUIRED_NOTIFICATION_INTENT_KEYS:
        if key not in payload:
            failures.append(f"missing:{key}")

    for key in ("notification_plan_id", "analysis_id", "candidate_group_id"):
        if key in payload and _uuid_or_none(payload.get(key)) is None:
            failures.append(f"{key}:invalid_uuid")

    target_chat_id = _int_or_none(payload.get("target_chat_id"))
    if "target_chat_id" in payload and target_chat_id is None:
        failures.append("target_chat_id:invalid_integer")
    if "target_thread_id" in payload and payload.get("target_thread_id") is not None:
        if _int_or_none(payload.get("target_thread_id")) is None:
            failures.append("target_thread_id:invalid_integer")

    delivery_decision = _string_or_none(payload.get("delivery_decision"))
    if "delivery_decision" in payload and delivery_decision not in ALLOWED_DELIVERY_DECISIONS:
        failures.append("delivery_decision:invalid")
    urgency_profile = _string_or_none(payload.get("urgency_profile"))
    if "urgency_profile" in payload and urgency_profile not in ALLOWED_URGENCY_PROFILES:
        failures.append("urgency_profile:invalid")
    for key in ("render_profile", "dedupe_subject_key", "material_change_hash"):
        if key in payload and _string_or_none(payload.get(key)) is None:
            failures.append(f"{key}:blank")
    if "send_after" in payload and payload.get("send_after") is not None:
        if _datetime_or_none(payload.get("send_after")) is None:
            failures.append("send_after:invalid_datetime")
    if "suppress_reason_code" in payload and payload.get("suppress_reason_code") is not None:
        if _string_or_none(payload.get("suppress_reason_code")) is None:
            failures.append("suppress_reason_code:blank")
    return not failures, tuple(failures)


def notification_plan_intent_from_payload(payload: Mapping[str, Any]) -> NotificationPlanIntent:
    valid, failures = validate_notification_intent_payload(payload)
    if not valid:
        raise ValueError("notification_intent_payload_invalid")
    return NotificationPlanIntent(
        notification_plan_id=UUID(str(payload["notification_plan_id"])),
        analysis_id=UUID(str(payload["analysis_id"])),
        candidate_group_id=UUID(str(payload["candidate_group_id"])),
        delivery_decision=str(payload["delivery_decision"]),
        urgency_profile=str(payload["urgency_profile"]),
        target_chat_id=int(str(payload["target_chat_id"])),
        target_thread_id=_int_or_none(payload.get("target_thread_id")),
        render_profile=str(payload["render_profile"]),
        dedupe_subject_key=str(payload["dedupe_subject_key"]),
        material_change_hash=str(payload["material_change_hash"]),
        send_after=_datetime_or_none(payload.get("send_after")),
        suppress_reason_code=_string_or_none(payload.get("suppress_reason_code")),
    )


def notification_plan_payload(intent: NotificationPlanIntent) -> dict[str, Any]:
    return {
        "notification_plan_id": str(intent.notification_plan_id),
        "analysis_id": str(intent.analysis_id),
        "candidate_group_id": str(intent.candidate_group_id),
        "delivery_decision": intent.delivery_decision,
        "urgency_profile": intent.urgency_profile,
        "target_chat_id": intent.target_chat_id,
        "target_thread_id": intent.target_thread_id,
        "render_profile": intent.render_profile,
        "dedupe_subject_key": intent.dedupe_subject_key,
        "material_change_hash": intent.material_change_hash,
        "send_after": _json_default(intent.send_after) if intent.send_after else None,
        "suppress_reason_code": intent.suppress_reason_code,
    }


def build_notification_render(
    *,
    intent: NotificationPlanIntent,
    analysis: AnalysisRecord,
    judge_output: JudgeOutputRecord,
    candidate: CandidateContext,
) -> NotificationRender:
    headline = (
        _string_or_none(judge_output.payload_json.get("headline"))
        or _string_or_none(analysis.recommended_action_ko)
        or _string_or_none(candidate.primary_canonical_id)
        or "candidate"
    )
    reason_codes = ", ".join(analysis.reason_codes_json) if analysis.reason_codes_json else "none"
    subject = _string_or_none(candidate.primary_canonical_id) or str(candidate.current_primary_artifact_id or "")
    lines = [
        f"[GitHub AI] {analysis.verdict} / {analysis.delivery_decision}",
        _single_line(headline, max_chars=180),
        f"Reason: {_single_line(reason_codes, max_chars=240)}",
    ]
    if subject:
        lines.append(f"Subject: {_single_line(subject, max_chars=160)}")
    message_text = "\n".join(lines)
    entities: list[dict[str, Any]] = []
    link_preview_options = {"is_disabled": True}
    reply_markup = None
    render_hash = stable_render_hash(
        {
            "notification_plan_id": str(intent.notification_plan_id),
            "message_text": message_text,
            "entities_json": entities,
            "link_preview_options_json": link_preview_options,
            "reply_markup_json": reply_markup,
            "disable_notification": intent.urgency_profile != "high",
            "protect_content": False,
            "parse_strategy": "entities",
        }
    )
    return NotificationRender(
        notification_plan_id=intent.notification_plan_id,
        message_text=message_text,
        entities_json=entities,
        link_preview_options_json=link_preview_options,
        reply_markup_json=reply_markup,
        disable_notification=intent.urgency_profile != "high",
        protect_content=False,
        parse_strategy="entities",
        render_hash=render_hash,
    )


def stable_render_hash(payload: Mapping[str, Any]) -> str:
    encoded = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"), default=_json_default)
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def build_delivery_result_event_dedupe_key(
    *,
    replay_namespace: str,
    notification_plan_id: UUID | str,
    notification_delivery_record_id: UUID | str,
) -> str:
    return (
        "local-db-notifier:"
        f"{replay_namespace}:notification.delivery.result:{notification_plan_id}:{notification_delivery_record_id}"
    )


def build_delivery_result_payload(result: DeliveryResultPayload) -> dict[str, Any]:
    return {
        "notification_plan_id": str(result.notification_plan_id),
        "delivery_status": result.delivery_status,
        "telegram_chat_id": result.telegram_chat_id,
        "telegram_message_id": result.telegram_message_id,
        "notification_delivery_record_id": str(result.notification_delivery_record_id),
        "attempt_count": result.attempt_count,
        "transport_error_code": result.transport_error_code,
        "transport_error_class": result.transport_error_class,
        "edited": result.edited,
        "dry_run": result.dry_run,
        "local_fixture": result.local_fixture,
        "reason_code": result.reason_code,
    }


def transport_would_be_called_for_fixture(*, intent: NotificationPlanIntent) -> bool:
    return False


def _execute_notifier_replay(connection: Any, *, replay_namespace: str) -> ReplayExecutionResult:
    checks_failed: list[str] = []
    event_count = _count_notification_plan_events(connection, replay_namespace=replay_namespace)
    if event_count != 1:
        checks_failed.append("notification_plan_created_event_count_invalid")

    event = _load_notification_plan_created_event(connection, replay_namespace=replay_namespace)
    event_found = event is not None
    if event is None:
        checks_failed.append("notification_plan_created_event_missing_or_invalid")
        return _execution_result(
            notification_plan_created_event_found=event_found,
            checks_failed=checks_failed,
        )

    intent = event.payload
    analysis = _load_analysis(connection, intent.analysis_id)
    analysis_loaded = analysis is not None
    if analysis is None:
        checks_failed.append("analysis_missing")
        return _execution_result(
            notification_plan_created_event_found=event_found,
            analysis_loaded=analysis_loaded,
            checks_failed=checks_failed,
        )

    judge_output = _load_judge_output(connection, analysis.judge_output_id)
    candidate = _load_candidate_context(connection, intent.candidate_group_id)
    judge_output_loaded = judge_output is not None
    candidate_loaded = candidate is not None
    if judge_output is None:
        checks_failed.append("judge_output_missing")
    if candidate is None:
        checks_failed.append("candidate_context_missing")
    elif candidate.current_primary_artifact_id is None or candidate.primary_artifact_type is None:
        checks_failed.append("primary_artifact_context_missing")

    if checks_failed:
        return _execution_result(
            notification_plan_created_event_found=event_found,
            analysis_loaded=analysis_loaded,
            judge_output_loaded=judge_output_loaded,
            candidate_context_loaded=candidate_loaded,
            checks_failed=checks_failed,
        )
    assert judge_output is not None
    assert candidate is not None

    context_failures = _notifier_context_failures(intent=intent, analysis=analysis, judge_output=judge_output)
    if context_failures:
        checks_failed.extend(context_failures)
        return _execution_result(
            notification_plan_created_event_found=event_found,
            analysis_loaded=analysis_loaded,
            judge_output_loaded=judge_output_loaded,
            candidate_context_loaded=candidate_loaded,
            checks_failed=checks_failed,
        )

    analysis_before = _load_analysis_digest(connection, analysis.analysis_id)
    judge_output_before = _load_judge_output_digest(connection, judge_output.judge_output_id)
    candidate_before = _load_candidate_digest(connection, candidate.candidate_group_id)

    plan_id = _insert_or_reuse_notification_plan(connection, intent=intent)
    if plan_id != intent.notification_plan_id:
        checks_failed.append("notification_plan_material_conflict")
        return _execution_result(
            notification_plan_created_event_found=event_found,
            analysis_loaded=analysis_loaded,
            judge_output_loaded=judge_output_loaded,
            candidate_context_loaded=candidate_loaded,
            checks_failed=checks_failed,
        )
    plan_matches = _notification_plan_matches_intent(connection, intent=intent)
    if not plan_matches:
        checks_failed.append("notification_plan_intent_mismatch")

    render = build_notification_render(intent=intent, analysis=analysis, judge_output=judge_output, candidate=candidate)
    render_id = _insert_or_reuse_notification_render(connection, render=render)
    _mark_plan_suppressed(connection, notification_plan_id=intent.notification_plan_id)
    delivery_record_id = _insert_or_reuse_dry_run_delivery_record(connection, intent=intent)
    _insert_or_reuse_delivery_state_transition(connection, notification_plan_id=intent.notification_plan_id)
    delivery_payload = DeliveryResultPayload(
        notification_plan_id=intent.notification_plan_id,
        delivery_status=DELIVERY_TO_STATE,
        telegram_chat_id=intent.target_chat_id,
        telegram_message_id=None,
        notification_delivery_record_id=delivery_record_id,
        attempt_count=0,
        transport_error_code=None,
        transport_error_class=None,
        edited=False,
    )
    _insert_or_reuse_delivery_result_event(
        connection,
        replay_namespace=replay_namespace,
        payload=delivery_payload,
    )

    verification = _verify_success(
        connection,
        replay_namespace=replay_namespace,
        intent=intent,
        render=render,
        render_id=render_id,
        delivery_record_id=delivery_record_id,
        delivery_payload=delivery_payload,
        analysis_before=analysis_before,
        judge_output_before=judge_output_before,
        candidate_before=candidate_before,
    )
    checks_failed.extend(verification["checks_failed"])

    return ReplayExecutionResult(
        notification_plan_created_event_found=event_found,
        analysis_loaded=analysis_loaded,
        judge_output_loaded=judge_output_loaded,
        candidate_context_loaded=candidate_loaded,
        notification_plan_created=verification["notification_plan_created"],
        notification_render_created=verification["notification_render_created"],
        notification_delivery_record_created=verification["notification_delivery_record_created"],
        notification_delivery_state_transition_recorded=verification["notification_delivery_state_transition_recorded"],
        notification_delivery_result_event_created=verification["notification_delivery_result_event_created"],
        analysis_mutated=verification["analysis_mutated"],
        judge_output_mutated=verification["judge_output_mutated"],
        candidate_group_mutated=verification["candidate_group_mutated"],
        telegram_called=False,
        send_message_called=False,
        edit_message_called=False,
        checks_failed=tuple(dict.fromkeys(checks_failed)),
    )


def _load_notification_plan_created_event(
    connection: Any,
    *,
    replay_namespace: str,
) -> NotificationPlanCreatedEvent | None:
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
            "event_type": NOTIFICATION_PLAN_CREATED_EVENT_TYPE,
            "dedupe_prefix": _notification_plan_event_dedupe_prefix(replay_namespace),
        },
    ).mappings().first()
    if row is None:
        return None
    payload = _json_loads(row["payload_json"]) or {}
    valid, _failures = validate_notification_intent_payload(payload)
    if not valid:
        return None
    return NotificationPlanCreatedEvent(
        event_id=UUID(str(row["event_id"])),
        payload=notification_plan_intent_from_payload(payload),
    )


def _count_notification_plan_events(connection: Any, *, replay_namespace: str) -> int:
    import sqlalchemy as sa

    return int(
        connection.execute(
            sa.text(
                """
                SELECT count(*)
                FROM event_outbox
                WHERE event_type = :event_type
                  AND dedupe_key LIKE :dedupe_prefix
                """
            ),
            {
                "event_type": NOTIFICATION_PLAN_CREATED_EVENT_TYPE,
                "dedupe_prefix": _notification_plan_event_dedupe_prefix(replay_namespace),
            },
        ).scalar_one()
    )


def _load_analysis(connection: Any, analysis_id: UUID) -> AnalysisRecord | None:
    import sqlalchemy as sa

    row = connection.execute(
        sa.text(
            """
            SELECT analysis_id, candidate_group_id, judge_output_id, verdict, delivery_decision,
                   reason_codes_json, evidence_limitations_ko, recommended_action_ko,
                   freshness_note_ko, policy_reconciled_flag
            FROM analyses
            WHERE analysis_id = CAST(:analysis_id AS uuid)
            """
        ),
        {"analysis_id": str(analysis_id)},
    ).mappings().first()
    if row is None:
        return None
    return AnalysisRecord(
        analysis_id=UUID(str(row["analysis_id"])),
        candidate_group_id=UUID(str(row["candidate_group_id"])),
        judge_output_id=UUID(str(row["judge_output_id"])),
        verdict=str(row["verdict"]),
        delivery_decision=str(row["delivery_decision"]),
        reason_codes_json=_string_list(_json_loads(row["reason_codes_json"])),
        evidence_limitations_ko=_string_or_none(row["evidence_limitations_ko"]),
        recommended_action_ko=_string_or_none(row["recommended_action_ko"]),
        freshness_note_ko=_string_or_none(row["freshness_note_ko"]),
        policy_reconciled_flag=bool(row["policy_reconciled_flag"]),
    )


def _load_judge_output(connection: Any, judge_output_id: UUID) -> JudgeOutputRecord | None:
    import sqlalchemy as sa

    row = connection.execute(
        sa.text(
            """
            SELECT judge_output_id, candidate_group_id, payload_json,
                   model_proposed_verdict, model_confidence_band
            FROM judge_outputs
            WHERE judge_output_id = CAST(:judge_output_id AS uuid)
            """
        ),
        {"judge_output_id": str(judge_output_id)},
    ).mappings().first()
    if row is None:
        return None
    payload = _json_loads(row["payload_json"])
    return JudgeOutputRecord(
        judge_output_id=UUID(str(row["judge_output_id"])),
        candidate_group_id=UUID(str(row["candidate_group_id"])),
        payload_json=payload if isinstance(payload, dict) else {},
        model_proposed_verdict=_string_or_none(row["model_proposed_verdict"]),
        model_confidence_band=_string_or_none(row["model_confidence_band"]),
    )


def _load_candidate_context(connection: Any, candidate_group_id: UUID) -> CandidateContext | None:
    import sqlalchemy as sa

    row = connection.execute(
        sa.text(
            """
            SELECT cgp.candidate_group_id, cgp.current_primary_artifact_id,
                   ar.artifact_type AS primary_artifact_type,
                   ar.canonical_id AS primary_canonical_id,
                   ar.canonical_url AS primary_canonical_url
            FROM candidate_group_proposals cgp
            LEFT JOIN artifact_registry ar
              ON ar.artifact_id = cgp.current_primary_artifact_id
            WHERE cgp.candidate_group_id = CAST(:candidate_group_id AS uuid)
            """
        ),
        {"candidate_group_id": str(candidate_group_id)},
    ).mappings().first()
    if row is None:
        return None
    return CandidateContext(
        candidate_group_id=UUID(str(row["candidate_group_id"])),
        current_primary_artifact_id=_uuid_or_none(row["current_primary_artifact_id"]),
        primary_artifact_type=_string_or_none(row["primary_artifact_type"]),
        primary_canonical_id=_string_or_none(row["primary_canonical_id"]),
        primary_canonical_url=_string_or_none(row["primary_canonical_url"]),
    )


def _notifier_context_failures(
    *,
    intent: NotificationPlanIntent,
    analysis: AnalysisRecord,
    judge_output: JudgeOutputRecord,
) -> list[str]:
    failures: list[str] = []
    if analysis.candidate_group_id != intent.candidate_group_id:
        failures.append("analysis_candidate_mismatch")
    if analysis.delivery_decision != intent.delivery_decision:
        failures.append("analysis_delivery_decision_mismatch")
    if judge_output.judge_output_id != analysis.judge_output_id:
        failures.append("judge_output_analysis_mismatch")
    if judge_output.candidate_group_id != intent.candidate_group_id:
        failures.append("judge_output_candidate_mismatch")
    return failures


def _insert_or_reuse_notification_plan(connection: Any, *, intent: NotificationPlanIntent) -> UUID:
    import sqlalchemy as sa

    existing = _load_notification_plan_id(connection, intent.notification_plan_id)
    if existing is not None:
        return existing

    material_existing = _load_notification_plan_id_by_material(connection, intent=intent)
    if material_existing is not None:
        return material_existing

    result = connection.execute(
        sa.text(
            """
            INSERT INTO notification_plans (
                notification_plan_id,
                analysis_id,
                candidate_group_id,
                delivery_decision,
                urgency_profile,
                target_chat_id,
                target_thread_id,
                render_profile,
                dedupe_subject_key,
                material_change_hash,
                send_after,
                suppress_reason_code,
                status,
                created_at
            ) VALUES (
                CAST(:notification_plan_id AS uuid),
                CAST(:analysis_id AS uuid),
                CAST(:candidate_group_id AS uuid),
                CAST(:delivery_decision AS delivery_decision_enum),
                CAST(:urgency_profile AS urgency_profile_enum),
                :target_chat_id,
                :target_thread_id,
                :render_profile,
                :dedupe_subject_key,
                :material_change_hash,
                :send_after,
                :suppress_reason_code,
                'planned'::notification_status_enum,
                now()
            )
            ON CONFLICT ON CONSTRAINT uq_notification_plans_analysis_target_material
            DO NOTHING
            RETURNING notification_plan_id
            """
        ),
        {
            "notification_plan_id": str(intent.notification_plan_id),
            "analysis_id": str(intent.analysis_id),
            "candidate_group_id": str(intent.candidate_group_id),
            "delivery_decision": intent.delivery_decision,
            "urgency_profile": intent.urgency_profile,
            "target_chat_id": intent.target_chat_id,
            "target_thread_id": intent.target_thread_id,
            "render_profile": intent.render_profile,
            "dedupe_subject_key": intent.dedupe_subject_key,
            "material_change_hash": intent.material_change_hash,
            "send_after": intent.send_after,
            "suppress_reason_code": intent.suppress_reason_code,
        },
    )
    inserted = result.scalar_one_or_none()
    if inserted is not None:
        return UUID(str(inserted))

    existing = _load_notification_plan_id(connection, intent.notification_plan_id)
    if existing is not None:
        return existing
    material_existing = _load_notification_plan_id_by_material(connection, intent=intent)
    if material_existing is None:
        raise RuntimeError("notification plan insert conflicted but existing row was not found")
    return material_existing


def _load_notification_plan_id(connection: Any, notification_plan_id: UUID) -> UUID | None:
    import sqlalchemy as sa

    value = connection.execute(
        sa.text(
            """
            SELECT notification_plan_id
            FROM notification_plans
            WHERE notification_plan_id = CAST(:notification_plan_id AS uuid)
            """
        ),
        {"notification_plan_id": str(notification_plan_id)},
    ).scalar_one_or_none()
    return UUID(str(value)) if value is not None else None


def _load_notification_plan_id_by_material(connection: Any, *, intent: NotificationPlanIntent) -> UUID | None:
    import sqlalchemy as sa

    value = connection.execute(
        sa.text(
            """
            SELECT notification_plan_id
            FROM notification_plans
            WHERE analysis_id = CAST(:analysis_id AS uuid)
              AND target_chat_id = :target_chat_id
              AND material_change_hash = :material_change_hash
            """
        ),
        {
            "analysis_id": str(intent.analysis_id),
            "target_chat_id": intent.target_chat_id,
            "material_change_hash": intent.material_change_hash,
        },
    ).scalar_one_or_none()
    return UUID(str(value)) if value is not None else None


def _notification_plan_matches_intent(connection: Any, *, intent: NotificationPlanIntent) -> bool:
    import sqlalchemy as sa

    row = connection.execute(
        sa.text(
            """
            SELECT notification_plan_id, analysis_id, candidate_group_id, delivery_decision,
                   urgency_profile, target_chat_id, target_thread_id, render_profile,
                   dedupe_subject_key, material_change_hash, send_after, suppress_reason_code
            FROM notification_plans
            WHERE notification_plan_id = CAST(:notification_plan_id AS uuid)
            """
        ),
        {"notification_plan_id": str(intent.notification_plan_id)},
    ).mappings().first()
    if row is None:
        return False
    stored_send_after = row["send_after"]
    if isinstance(stored_send_after, datetime):
        stored_send_after = _as_utc(stored_send_after)
    return {
        "notification_plan_id": str(row["notification_plan_id"]),
        "analysis_id": str(row["analysis_id"]),
        "candidate_group_id": str(row["candidate_group_id"]),
        "delivery_decision": str(row["delivery_decision"]),
        "urgency_profile": str(row["urgency_profile"]),
        "target_chat_id": int(row["target_chat_id"]),
        "target_thread_id": _int_or_none(row["target_thread_id"]),
        "render_profile": row["render_profile"],
        "dedupe_subject_key": row["dedupe_subject_key"],
        "material_change_hash": row["material_change_hash"],
        "send_after": stored_send_after,
        "suppress_reason_code": row["suppress_reason_code"],
    } == {
        "notification_plan_id": str(intent.notification_plan_id),
        "analysis_id": str(intent.analysis_id),
        "candidate_group_id": str(intent.candidate_group_id),
        "delivery_decision": intent.delivery_decision,
        "urgency_profile": intent.urgency_profile,
        "target_chat_id": intent.target_chat_id,
        "target_thread_id": intent.target_thread_id,
        "render_profile": intent.render_profile,
        "dedupe_subject_key": intent.dedupe_subject_key,
        "material_change_hash": intent.material_change_hash,
        "send_after": _as_utc(intent.send_after) if intent.send_after else None,
        "suppress_reason_code": intent.suppress_reason_code,
    }


def _insert_or_reuse_notification_render(connection: Any, *, render: NotificationRender) -> UUID:
    import sqlalchemy as sa

    existing = _load_notification_render_id(connection, render=render)
    if existing is not None:
        return existing
    result = connection.execute(
        sa.text(
            """
            INSERT INTO notification_renders (
                notification_plan_id,
                message_text,
                entities_json,
                link_preview_options_json,
                reply_markup_json,
                disable_notification,
                protect_content,
                parse_strategy,
                render_hash,
                created_at
            ) VALUES (
                CAST(:notification_plan_id AS uuid),
                :message_text,
                CAST(:entities_json AS jsonb),
                CAST(:link_preview_options_json AS jsonb),
                CAST(:reply_markup_json AS jsonb),
                :disable_notification,
                :protect_content,
                :parse_strategy,
                :render_hash,
                now()
            )
            ON CONFLICT ON CONSTRAINT uq_notification_renders_plan_render_hash
            DO NOTHING
            RETURNING notification_render_id
            """
        ),
        {
            "notification_plan_id": str(render.notification_plan_id),
            "message_text": render.message_text,
            "entities_json": _json_dumps(render.entities_json),
            "link_preview_options_json": _json_dumps(render.link_preview_options_json),
            "reply_markup_json": _json_dumps(render.reply_markup_json),
            "disable_notification": render.disable_notification,
            "protect_content": render.protect_content,
            "parse_strategy": render.parse_strategy,
            "render_hash": render.render_hash,
        },
    )
    inserted = result.scalar_one_or_none()
    if inserted is not None:
        return UUID(str(inserted))
    existing = _load_notification_render_id(connection, render=render)
    if existing is None:
        raise RuntimeError("notification render insert conflicted but existing row was not found")
    return existing


def _load_notification_render_id(connection: Any, *, render: NotificationRender) -> UUID | None:
    import sqlalchemy as sa

    value = connection.execute(
        sa.text(
            """
            SELECT notification_render_id
            FROM notification_renders
            WHERE notification_plan_id = CAST(:notification_plan_id AS uuid)
              AND render_hash = :render_hash
            """
        ),
        {"notification_plan_id": str(render.notification_plan_id), "render_hash": render.render_hash},
    ).scalar_one_or_none()
    return UUID(str(value)) if value is not None else None


def _mark_plan_suppressed(connection: Any, *, notification_plan_id: UUID) -> None:
    import sqlalchemy as sa

    connection.execute(
        sa.text(
            """
            UPDATE notification_plans
            SET status = 'suppressed'::notification_status_enum
            WHERE notification_plan_id = CAST(:notification_plan_id AS uuid)
            """
        ),
        {"notification_plan_id": str(notification_plan_id)},
    )


def _insert_or_reuse_dry_run_delivery_record(connection: Any, *, intent: NotificationPlanIntent) -> UUID:
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
            "transport_error_code": DELIVERY_STATE_REASON_CODE,
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
            "transport_error_code": DELIVERY_STATE_REASON_CODE,
            "telegram_response_json": _json_dumps({"dry_run": True, "local_fixture": True}),
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
            "reason_code": DELIVERY_STATE_REASON_CODE,
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
            "reason_code": DELIVERY_STATE_REASON_CODE,
        },
    )


def _insert_or_reuse_delivery_result_event(
    connection: Any,
    *,
    replay_namespace: str,
    payload: DeliveryResultPayload,
) -> None:
    import sqlalchemy as sa

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
            "notification_plan_id": str(payload.notification_plan_id),
            "dedupe_key": build_delivery_result_event_dedupe_key(
                replay_namespace=replay_namespace,
                notification_plan_id=payload.notification_plan_id,
                notification_delivery_record_id=payload.notification_delivery_record_id,
            ),
            "payload_json": _json_dumps(build_delivery_result_payload(payload)),
        },
    )


def _verify_success(
    connection: Any,
    *,
    replay_namespace: str,
    intent: NotificationPlanIntent,
    render: NotificationRender,
    render_id: UUID,
    delivery_record_id: UUID,
    delivery_payload: DeliveryResultPayload,
    analysis_before: str,
    judge_output_before: str,
    candidate_before: str,
) -> dict[str, Any]:
    import sqlalchemy as sa

    plan_created = bool(
        connection.execute(
            sa.text(
                """
                SELECT 1
                FROM notification_plans
                WHERE notification_plan_id = CAST(:notification_plan_id AS uuid)
                  AND analysis_id = CAST(:analysis_id AS uuid)
                  AND candidate_group_id = CAST(:candidate_group_id AS uuid)
                  AND delivery_decision = CAST(:delivery_decision AS delivery_decision_enum)
                  AND urgency_profile = CAST(:urgency_profile AS urgency_profile_enum)
                  AND target_chat_id = :target_chat_id
                  AND material_change_hash = :material_change_hash
                  AND status = 'suppressed'::notification_status_enum
                LIMIT 1
                """
            ),
            {
                "notification_plan_id": str(intent.notification_plan_id),
                "analysis_id": str(intent.analysis_id),
                "candidate_group_id": str(intent.candidate_group_id),
                "delivery_decision": intent.delivery_decision,
                "urgency_profile": intent.urgency_profile,
                "target_chat_id": intent.target_chat_id,
                "material_change_hash": intent.material_change_hash,
            },
        ).scalar_one_or_none()
    )
    render_created = bool(
        connection.execute(
            sa.text(
                """
                SELECT 1
                FROM notification_renders
                WHERE notification_render_id = CAST(:notification_render_id AS uuid)
                  AND notification_plan_id = CAST(:notification_plan_id AS uuid)
                  AND message_text = :message_text
                  AND render_hash = :render_hash
                LIMIT 1
                """
            ),
            {
                "notification_render_id": str(render_id),
                "notification_plan_id": str(intent.notification_plan_id),
                "message_text": render.message_text,
                "render_hash": render.render_hash,
            },
        ).scalar_one_or_none()
    )
    delivery_record_created = bool(
        connection.execute(
            sa.text(
                """
                SELECT 1
                FROM notification_delivery_records
                WHERE notification_delivery_record_id = CAST(:notification_delivery_record_id AS uuid)
                  AND notification_plan_id = CAST(:notification_plan_id AS uuid)
                  AND delivery_status = 'suppressed'::notification_status_enum
                  AND telegram_chat_id = :telegram_chat_id
                  AND telegram_message_id IS NULL
                  AND attempt_count = 0
                  AND transport_error_code = :reason_code
                  AND transport_error_class IS NULL
                  AND telegram_response_json @> CAST(:telegram_response_json AS jsonb)
                LIMIT 1
                """
            ),
            {
                "notification_delivery_record_id": str(delivery_record_id),
                "notification_plan_id": str(intent.notification_plan_id),
                "telegram_chat_id": intent.target_chat_id,
                "reason_code": DELIVERY_STATE_REASON_CODE,
                "telegram_response_json": _json_dumps({"dry_run": True, "local_fixture": True}),
            },
        ).scalar_one_or_none()
    )
    transition_recorded = bool(
        connection.execute(
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
                "notification_plan_id": str(intent.notification_plan_id),
                "from_state": DELIVERY_FROM_STATE,
                "to_state": DELIVERY_TO_STATE,
                "reason_code": DELIVERY_STATE_REASON_CODE,
            },
        ).scalar_one_or_none()
    )
    event_payload = build_delivery_result_payload(delivery_payload)
    delivery_event_created = bool(
        connection.execute(
            sa.text(
                """
                SELECT 1
                FROM event_outbox
                WHERE event_type = :event_type
                  AND aggregate_type = 'notification_plan'
                  AND aggregate_id = CAST(:notification_plan_id AS uuid)
                  AND dedupe_key = :dedupe_key
                  AND payload_json = CAST(:payload_json AS jsonb)
                LIMIT 1
                """
            ),
            {
                "event_type": NOTIFICATION_DELIVERY_RESULT_EVENT_TYPE,
                "notification_plan_id": str(intent.notification_plan_id),
                "dedupe_key": build_delivery_result_event_dedupe_key(
                    replay_namespace=replay_namespace,
                    notification_plan_id=intent.notification_plan_id,
                    notification_delivery_record_id=delivery_record_id,
                ),
                "payload_json": _json_dumps(event_payload),
            },
        ).scalar_one_or_none()
    )
    analysis_mutated = _load_analysis_digest(connection, intent.analysis_id) != analysis_before
    judge_output_mutated = _load_judge_output_digest_by_analysis(connection, intent.analysis_id) != judge_output_before
    candidate_group_mutated = _load_candidate_digest(connection, intent.candidate_group_id) != candidate_before

    counts = _load_notifier_terminal_counts(
        connection,
        replay_namespace=replay_namespace,
        notification_plan_id=intent.notification_plan_id,
    )
    checks = {
        "notification_plan_created": plan_created and counts["notification_plans"] == 1,
        "notification_render_created": render_created and counts["notification_renders"] == 1,
        "notification_delivery_record_created": delivery_record_created and counts["notification_delivery_records"] == 1,
        "notification_delivery_state_transition_recorded": transition_recorded and counts["notification_state_transitions"] == 1,
        "notification_delivery_result_event_created": delivery_event_created and counts["delivery_result_events"] == 1,
        "analysis_mutated": analysis_mutated,
        "judge_output_mutated": judge_output_mutated,
        "candidate_group_mutated": candidate_group_mutated,
    }
    failures = []
    for key, value in checks.items():
        if key in {
            "notification_plan_created",
            "notification_render_created",
            "notification_delivery_record_created",
            "notification_delivery_state_transition_recorded",
            "notification_delivery_result_event_created",
        } and value is not True:
            failures.append(f"{key}:missing")
        if key in {"analysis_mutated", "judge_output_mutated", "candidate_group_mutated"} and value is not False:
            failures.append(f"{key}:unexpected")
    return {**checks, "checks_failed": failures}


def _load_notifier_terminal_counts(
    connection: Any,
    *,
    replay_namespace: str,
    notification_plan_id: UUID,
) -> dict[str, int]:
    import sqlalchemy as sa

    row = connection.execute(
        sa.text(
            """
            SELECT
              (SELECT count(*) FROM notification_plans
               WHERE notification_plan_id = CAST(:notification_plan_id AS uuid)) AS notification_plans,
              (SELECT count(*) FROM notification_renders
               WHERE notification_plan_id = CAST(:notification_plan_id AS uuid)) AS notification_renders,
              (SELECT count(*) FROM notification_delivery_records
               WHERE notification_plan_id = CAST(:notification_plan_id AS uuid)
                 AND delivery_status = 'suppressed'::notification_status_enum
                 AND transport_error_code = :reason_code
                 AND telegram_response_json @> CAST(:telegram_response_json AS jsonb)) AS notification_delivery_records,
              (SELECT count(*) FROM state_transitions
               WHERE object_type = 'notification_plan'
                 AND object_id = CAST(:notification_plan_id AS uuid)
                 AND from_state = :from_state
                 AND to_state = :to_state
                 AND reason_code = :reason_code) AS notification_state_transitions,
              (SELECT count(*) FROM event_outbox
               WHERE event_type = :event_type
                 AND dedupe_key LIKE :delivery_dedupe_prefix) AS delivery_result_events
            """
        ),
        {
            "notification_plan_id": str(notification_plan_id),
            "reason_code": DELIVERY_STATE_REASON_CODE,
            "telegram_response_json": _json_dumps({"dry_run": True, "local_fixture": True}),
            "from_state": DELIVERY_FROM_STATE,
            "to_state": DELIVERY_TO_STATE,
            "event_type": NOTIFICATION_DELIVERY_RESULT_EVENT_TYPE,
            "delivery_dedupe_prefix": f"local-db-notifier:{replay_namespace}:notification.delivery.result:%",
        },
    ).mappings().one()
    return {key: int(row[key]) for key in row.keys()}


def _load_analysis_digest(connection: Any, analysis_id: UUID) -> str:
    import sqlalchemy as sa

    row = connection.execute(
        sa.text(
            """
            SELECT analysis_id, candidate_group_id, judge_output_id, schema_version,
                   policy_version, prompt_version, delivery_policy_version, verdict,
                   delivery_decision, scores_json, reason_codes_json,
                   evidence_limitations_ko, recommended_action_ko, freshness_note_ko,
                   model_proposed_verdict, policy_reconciled_flag
            FROM analyses
            WHERE analysis_id = CAST(:analysis_id AS uuid)
            """
        ),
        {"analysis_id": str(analysis_id)},
    ).mappings().first()
    return _canonical_json(dict(row)) if row is not None else ""


def _load_judge_output_digest(connection: Any, judge_output_id: UUID) -> str:
    import sqlalchemy as sa

    row = connection.execute(
        sa.text(
            """
            SELECT judge_output_id, judge_run_id, candidate_group_id, judge_schema_version,
                   payload_json, model_proposed_verdict, model_confidence_band
            FROM judge_outputs
            WHERE judge_output_id = CAST(:judge_output_id AS uuid)
            """
        ),
        {"judge_output_id": str(judge_output_id)},
    ).mappings().first()
    return _canonical_json(dict(row)) if row is not None else ""


def _load_judge_output_digest_by_analysis(connection: Any, analysis_id: UUID) -> str:
    import sqlalchemy as sa

    value = connection.execute(
        sa.text(
            """
            SELECT judge_output_id
            FROM analyses
            WHERE analysis_id = CAST(:analysis_id AS uuid)
            """
        ),
        {"analysis_id": str(analysis_id)},
    ).scalar_one_or_none()
    if value is None:
        return ""
    return _load_judge_output_digest(connection, UUID(str(value)))


def _load_candidate_digest(connection: Any, candidate_group_id: UUID) -> str:
    import sqlalchemy as sa

    row = connection.execute(
        sa.text(
            """
            SELECT candidate_group_id, current_bundle_id, current_analysis_id, current_primary_artifact_id
            FROM candidate_group_proposals
            WHERE candidate_group_id = CAST(:candidate_group_id AS uuid)
            """
        ),
        {"candidate_group_id": str(candidate_group_id)},
    ).mappings().first()
    return _canonical_json(dict(row)) if row is not None else ""


def _predecessor_policy_confirmed(predecessor: policy_runner.RunnerResult) -> bool:
    expected_true = (
        "analysis_policy_apply_event_found",
        "judge_run_succeeded_confirmed",
        "judge_output_loaded",
        "bundle_context_confirmed",
        "analysis_validation_state_transition_found",
        "analysis_created",
        "policy_state_transition_recorded",
        "notification_plan_intent_event_created",
    )
    if not all(predecessor.report.get(key) is True for key in expected_true):
        return False
    if predecessor.report.get("status") == "pass" and predecessor.exit_code == 0:
        return True
    allowed_successor_failures = {
        "notification_plan_created:unexpected",
        "notification_render_created:unexpected",
        "notification_delivery_created:unexpected",
    }
    checks_failed = set(predecessor.report.get("checks_failed") or [])
    return bool(checks_failed) and checks_failed.issubset(allowed_successor_failures)


def _execution_result(
    *,
    notification_plan_created_event_found: bool = False,
    analysis_loaded: bool = False,
    judge_output_loaded: bool = False,
    candidate_context_loaded: bool = False,
    notification_plan_created: bool = False,
    notification_render_created: bool = False,
    notification_delivery_record_created: bool = False,
    notification_delivery_state_transition_recorded: bool = False,
    notification_delivery_result_event_created: bool = False,
    analysis_mutated: bool = False,
    judge_output_mutated: bool = False,
    candidate_group_mutated: bool = False,
    telegram_called: bool = False,
    send_message_called: bool = False,
    edit_message_called: bool = False,
    checks_failed: list[str] | tuple[str, ...],
) -> ReplayExecutionResult:
    return ReplayExecutionResult(
        notification_plan_created_event_found=notification_plan_created_event_found,
        analysis_loaded=analysis_loaded,
        judge_output_loaded=judge_output_loaded,
        candidate_context_loaded=candidate_context_loaded,
        notification_plan_created=notification_plan_created,
        notification_render_created=notification_render_created,
        notification_delivery_record_created=notification_delivery_record_created,
        notification_delivery_state_transition_recorded=notification_delivery_state_transition_recorded,
        notification_delivery_result_event_created=notification_delivery_result_event_created,
        analysis_mutated=analysis_mutated,
        judge_output_mutated=judge_output_mutated,
        candidate_group_mutated=candidate_group_mutated,
        telegram_called=telegram_called,
        send_message_called=send_message_called,
        edit_message_called=edit_message_called,
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
        "analysis_validator_replay_confirmed": False,
        "policy_engine_replay_confirmed": False,
        "notification_plan_created_event_found": False,
        "analysis_loaded": False,
        "judge_output_loaded": False,
        "candidate_context_loaded": False,
        "notification_plan_created": False,
        "notification_render_created": False,
        "notification_delivery_record_created": False,
        "notification_delivery_state_transition_recorded": False,
        "notification_delivery_result_event_created": False,
        "analysis_mutated": False,
        "judge_output_mutated": False,
        "candidate_group_mutated": False,
        "telegram_called": False,
        "send_message_called": False,
        "edit_message_called": False,
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


def _notification_plan_event_dedupe_prefix(replay_namespace: str) -> str:
    return f"local-db-policy-engine:{replay_namespace}:notification.plan.created:%"


def _single_line(value: str, *, max_chars: int) -> str:
    normalized = " ".join(str(value).split())
    if len(normalized) <= max_chars:
        return normalized
    return normalized[: max(0, max_chars - 3)].rstrip() + "..."


def _json_loads(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, str):
        return json.loads(value)
    return value


def _json_default(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, UUID):
        return str(value)
    if isinstance(value, datetime):
        return _as_utc(value).isoformat()
    return str(value)


def _json_dumps(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, default=_json_default)


def _canonical_json(value: Mapping[str, Any]) -> str:
    normalized = _json_loads(value) or value
    return json.dumps(normalized, ensure_ascii=False, sort_keys=True, separators=(",", ":"), default=_json_default)


def _uuid_or_none(value: Any) -> UUID | None:
    if value is None:
        return None
    try:
        return UUID(str(value))
    except (TypeError, ValueError, AttributeError):
        return None


def _int_or_none(value: Any) -> int | None:
    if value in {None, ""}:
        return None
    try:
        return int(str(value))
    except (TypeError, ValueError):
        return None


def _datetime_or_none(value: Any) -> datetime | None:
    if value in {None, ""}:
        return None
    if isinstance(value, datetime):
        return _as_utc(value)
    try:
        text = str(value).replace("Z", "+00:00")
        return _as_utc(datetime.fromisoformat(text))
    except ValueError:
        return None


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _string_or_none(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text if text else None


def _string_list(value: Any) -> list[str]:
    if isinstance(value, list):
        return [str(item) for item in value if str(item)]
    if isinstance(value, str) and value:
        return [value]
    return []


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
