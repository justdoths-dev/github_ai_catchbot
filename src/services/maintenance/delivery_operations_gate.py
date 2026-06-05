from __future__ import annotations

from typing import Protocol, TypeAlias

from .config import MaintenanceConfig
from .models import DeliveryGateMetric, DeliveryGateReportV1, DeliveryGateSnapshot, GateMode, GateStatus


DeliveryGateMode: TypeAlias = GateMode
DeliveryGateStatus: TypeAlias = GateStatus
DeliveryOperationsGateReport: TypeAlias = DeliveryGateReportV1

FULL_DLQ_OLDEST_AGE_THRESHOLD_SEC = 3600.0

ALLOWED_DELIVERY_DLQ_LAST_ERROR_CODES = frozenset(
    {
        "max_notification_retry_attempts_exceeded",
        "notify_transport_terminal_chat_access",
        "notify_transport_terminal_edit_forbidden",
        "notify_render_invalid_payload",
        "delivery_replay_env_guard_rejected",
        "delivery_replay_unsupported_request",
        "maintenance_due_retry_emit_failed",
    }
)
ALLOWED_DELIVERY_DLQ_NEXT_MANUAL_ACTIONS = frozenset(
    {
        "request_explicit_delivery_replay",
        "fix_chat_access_then_delivery_replay",
        "disable_edits_then_delivery_replay",
        "fix_template_then_delivery_replay",
        "acknowledge_and_close_no_recovery",
        "fix_env_guard_then_retry_replay_request",
    }
)
ALLOWED_DELIVERY_DLQ_REPLAY_HINTS = frozenset({"delivery_replay_from_notification_plan"})


class DeliveryOperationsGateRepository(Protocol):
    async def load_delivery_gate_snapshot(self) -> DeliveryGateSnapshot: ...


class DeliveryOperationsGate:
    def __init__(
        self,
        config: MaintenanceConfig,
        *,
        repository: DeliveryOperationsGateRepository,
    ) -> None:
        self._config = config
        self._repository = repository

    async def run(
        self,
        *,
        mode: DeliveryGateMode,
        operator_review_passed: bool | None = None,
    ) -> DeliveryOperationsGateReport:
        snapshot = await self._repository.load_delivery_gate_snapshot()
        return evaluate_delivery_operations_gate(
            config=self._config,
            snapshot=snapshot,
            mode=mode,
            operator_review_passed=operator_review_passed,
        )


def evaluate_delivery_operations_gate(
    *,
    config: MaintenanceConfig,
    snapshot: DeliveryGateSnapshot,
    mode: DeliveryGateMode,
    operator_review_passed: bool | None = None,
) -> DeliveryOperationsGateReport:
    metrics: list[DeliveryGateMetric] = []
    blocking_reason_codes: list[str] = []
    warning_reason_codes: list[str] = []

    def add_metric(
        *,
        metric_name: str,
        observed_value: float | int | str | None,
        threshold: float | int | str | None,
        comparator: str,
        passed: bool,
        reason_code: str | None = None,
        severity: str = "block",
    ) -> None:
        metrics.append(
            DeliveryGateMetric(
                metric_name=metric_name,
                observed_value=observed_value,
                threshold=threshold,
                comparator=comparator,
                passed=passed,
                severity=severity,
            )
        )
        if passed or reason_code is None:
            return
        if severity == "warn":
            warning_reason_codes.append(reason_code)
        else:
            blocking_reason_codes.append(reason_code)

    add_metric(
        metric_name="success_rate_1h",
        observed_value=snapshot.success_rate_1h,
        threshold=config.delivery_gate_min_success_rate_1h,
        comparator=">=",
        passed=_gte(snapshot.success_rate_1h, config.delivery_gate_min_success_rate_1h),
        reason_code=(
            "delivery_gate_success_rate_missing"
            if snapshot.success_rate_1h is None
            else "delivery_gate_success_rate_below_threshold"
        ),
    )
    add_metric(
        metric_name="high_source_to_delivery_p95_sec",
        observed_value=snapshot.high_source_to_delivery_p95_sec,
        threshold=config.delivery_gate_max_high_source_to_delivery_p95_sec,
        comparator="<=",
        passed=_lte(
            snapshot.high_source_to_delivery_p95_sec,
            config.delivery_gate_max_high_source_to_delivery_p95_sec,
        ),
        reason_code="delivery_gate_high_e2e_p95_too_high",
    )
    add_metric(
        metric_name="due_retry_oldest_lag_sec",
        observed_value=snapshot.due_retry_oldest_lag_sec,
        threshold=config.delivery_gate_max_due_retry_lag_sec,
        comparator="<=",
        passed=_none_or_lte(snapshot.due_retry_oldest_lag_sec, config.delivery_gate_max_due_retry_lag_sec),
        reason_code="delivery_gate_due_retry_lag_too_high",
    )
    add_metric(
        metric_name="open_delivery_dlq_count",
        observed_value=snapshot.open_delivery_dlq_count,
        threshold=config.delivery_gate_max_open_dlq_count,
        comparator="<=",
        passed=snapshot.open_delivery_dlq_count <= config.delivery_gate_max_open_dlq_count,
        reason_code="delivery_gate_open_dlq_present",
    )
    add_metric(
        metric_name="unexpected_send_disabled_count",
        observed_value=snapshot.unexpected_send_disabled_count,
        threshold=config.delivery_gate_max_send_disabled_count,
        comparator="<=",
        passed=snapshot.unexpected_send_disabled_count <= config.delivery_gate_max_send_disabled_count,
        reason_code="delivery_gate_unexpected_send_disabled_rows_present",
    )

    if mode == "full":
        add_metric(
            metric_name="success_rate_24h",
            observed_value=snapshot.success_rate_24h,
            threshold=config.delivery_gate_min_success_rate_24h,
            comparator=">=",
            passed=_gte(snapshot.success_rate_24h, config.delivery_gate_min_success_rate_24h),
            reason_code="delivery_gate_24h_success_rate_below_threshold",
        )
        add_metric(
            metric_name="replay_guard_reject_count_24h",
            observed_value=snapshot.replay_guard_reject_count_24h,
            threshold=config.delivery_gate_max_replay_guard_reject_count,
            comparator="<=",
            passed=snapshot.replay_guard_reject_count_24h <= config.delivery_gate_max_replay_guard_reject_count,
            reason_code="delivery_gate_prod_replay_guard_rejects_present",
        )
        add_metric(
            metric_name="retry_ceiling_exceeded_count_24h",
            observed_value=snapshot.retry_ceiling_exceeded_count_24h,
            threshold=0,
            comparator="==",
            passed=snapshot.retry_ceiling_exceeded_count_24h == 0,
            reason_code="delivery_gate_retry_ceiling_exceeded_rows_present",
        )
        add_metric(
            metric_name="oldest_delivery_dlq_age_sec",
            observed_value=snapshot.oldest_delivery_dlq_age_sec,
            threshold=FULL_DLQ_OLDEST_AGE_THRESHOLD_SEC,
            comparator="<",
            passed=(
                snapshot.open_delivery_dlq_count <= 0
                or _lt(snapshot.oldest_delivery_dlq_age_sec, FULL_DLQ_OLDEST_AGE_THRESHOLD_SEC)
            ),
            reason_code="delivery_gate_delivery_dlq_oldest_age_too_high",
        )
        duplicate_noop_warn = (
            snapshot.duplicate_noop_ratio_1h is not None and snapshot.duplicate_noop_ratio_1h > 0
        )
        add_metric(
            metric_name="duplicate_noop_ratio_1h",
            observed_value=snapshot.duplicate_noop_ratio_1h,
            threshold=None,
            comparator="review",
            passed=not duplicate_noop_warn,
            reason_code="delivery_gate_duplicate_noop_ratio_review_required",
            severity="warn",
        )
        if config.delivery_gate_require_operator_review_for_full and operator_review_passed is not True:
            warning_reason_codes.append("delivery_gate_operator_review_required")

    blocking_reason_codes = _dedupe_stable(blocking_reason_codes)
    warning_reason_codes = _dedupe_stable(warning_reason_codes)
    if blocking_reason_codes:
        gate_status: DeliveryGateStatus = "fail"
    elif warning_reason_codes:
        gate_status = "warn"
    else:
        gate_status = "pass"

    return DeliveryGateReportV1(
        mode=mode,
        gate_status=gate_status,
        blocking_reason_codes=blocking_reason_codes,
        warning_reason_codes=warning_reason_codes,
        metrics=metrics,
        operator_review_required=mode == "full" and config.delivery_gate_require_operator_review_for_full,
        operator_review_passed=operator_review_passed,
        recommended_flag_patch=_recommended_flag_patch(gate_status),
    )


def _gte(value: float | None, threshold: float) -> bool:
    return value is not None and value >= threshold


def _lte(value: float | None, threshold: float) -> bool:
    return value is not None and value <= threshold


def _lt(value: float | None, threshold: float) -> bool:
    return value is not None and value < threshold


def _none_or_lte(value: float | None, threshold: float) -> bool:
    return value is None or value <= threshold


def _dedupe_stable(values: list[str]) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for value in values:
        if value in seen:
            continue
        seen.add(value)
        result.append(value)
    return result


def _recommended_flag_patch(gate_status: DeliveryGateStatus) -> dict[str, object]:
    if gate_status == "fail":
        return {
            "ENABLE_NOTIFICATION_SEND": False,
            "MAINTENANCE_ENABLE_NOTIFICATION_RETRY_PROMOTION": False,
            "NOTIFIER_TELEGRAM_DRY_RUN": False,
        }
    return {
        "ENABLE_NOTIFICATION_SEND": True,
        "MAINTENANCE_ENABLE_NOTIFICATION_RETRY_PROMOTION": True,
        "NOTIFIER_TELEGRAM_DRY_RUN": False,
    }
