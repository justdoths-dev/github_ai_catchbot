from __future__ import annotations

from typing import Protocol

from .config import MaintenanceConfig
from .models import DeliveryGateMetric, DeliveryGateReportV1, DeliveryGateSnapshot, GateMode


FULL_DLQ_OLDEST_AGE_THRESHOLD_SEC = 3600.0


class DeliveryGateRepository(Protocol):
    async def load_delivery_gate_snapshot(self) -> DeliveryGateSnapshot: ...


class DeliveryGateRunner:
    def __init__(self, config: MaintenanceConfig, *, repository: DeliveryGateRepository) -> None:
        self._config = config
        self._repository = repository

    async def run(
        self,
        *,
        mode: GateMode,
        operator_review_passed: bool | None = None,
    ) -> DeliveryGateReportV1:
        snapshot = await self._repository.load_delivery_gate_snapshot()
        metrics: list[DeliveryGateMetric] = []
        blocking_reason_codes: list[str] = []
        warning_reason_codes: list[str] = []

        def add_metric(
            *,
            metric_name: str,
            observed_value: float | int | str | bool | None,
            threshold: float | int | str | bool | None,
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

        if not self._config.enable_notification_send:
            blocking_reason_codes.append("delivery_gate_flag_send_disabled")
        if self._config.notifier_telegram_dry_run:
            blocking_reason_codes.append("delivery_gate_dry_run_enabled")
        if not self._config.enable_delivery_retry_promotion:
            blocking_reason_codes.append("delivery_gate_retry_promotion_disabled")

        add_metric(
            metric_name="success_rate_1h",
            observed_value=snapshot.success_rate_1h,
            threshold=self._config.delivery_gate_min_success_rate_1h,
            comparator=">=",
            passed=_gte(snapshot.success_rate_1h, self._config.delivery_gate_min_success_rate_1h),
            reason_code="delivery_gate_success_rate_below_threshold",
        )
        add_metric(
            metric_name="high_source_to_delivery_p95_sec",
            observed_value=snapshot.high_source_to_delivery_p95_sec,
            threshold=self._config.delivery_gate_max_high_source_to_delivery_p95_sec,
            comparator="<=",
            passed=_none_or_lte(
                snapshot.high_source_to_delivery_p95_sec,
                self._config.delivery_gate_max_high_source_to_delivery_p95_sec,
            ),
            reason_code="delivery_gate_high_e2e_p95_too_high",
        )
        add_metric(
            metric_name="plan_to_transport_p95_sec",
            observed_value=snapshot.plan_to_transport_p95_sec,
            threshold=self._config.delivery_gate_max_plan_to_transport_p95_sec,
            comparator="<=",
            passed=_none_or_lte(
                snapshot.plan_to_transport_p95_sec,
                self._config.delivery_gate_max_plan_to_transport_p95_sec,
            ),
            reason_code="delivery_gate_plan_to_transport_p95_too_high",
        )
        add_metric(
            metric_name="due_retry_oldest_lag_sec",
            observed_value=snapshot.due_retry_oldest_lag_sec,
            threshold=self._config.delivery_gate_max_due_retry_lag_sec,
            comparator="<=",
            passed=_none_or_lte(snapshot.due_retry_oldest_lag_sec, self._config.delivery_gate_max_due_retry_lag_sec),
            reason_code="delivery_gate_due_retry_lag_too_high",
        )
        add_metric(
            metric_name="open_delivery_dlq_count",
            observed_value=snapshot.open_delivery_dlq_count,
            threshold=self._config.delivery_gate_max_open_dlq_count,
            comparator="<=",
            passed=snapshot.open_delivery_dlq_count <= self._config.delivery_gate_max_open_dlq_count,
            reason_code="delivery_gate_open_dlq_present",
        )
        add_metric(
            metric_name="unexpected_send_disabled_count",
            observed_value=snapshot.unexpected_send_disabled_count,
            threshold=self._config.delivery_gate_max_send_disabled_count,
            comparator="<=",
            passed=snapshot.unexpected_send_disabled_count <= self._config.delivery_gate_max_send_disabled_count,
            reason_code="delivery_gate_unexpected_send_disabled_rows_present",
        )

        if mode == "full":
            add_metric(
                metric_name="success_rate_24h",
                observed_value=snapshot.success_rate_24h,
                threshold=self._config.delivery_gate_min_success_rate_24h,
                comparator=">=",
                passed=_gte(snapshot.success_rate_24h, self._config.delivery_gate_min_success_rate_24h),
                reason_code="delivery_gate_24h_success_rate_below_threshold",
            )
            add_metric(
                metric_name="replay_guard_reject_count_24h",
                observed_value=snapshot.replay_guard_reject_count_24h,
                threshold=self._config.delivery_gate_max_replay_guard_reject_count,
                comparator="<=",
                passed=(
                    snapshot.replay_guard_reject_count_24h
                    <= self._config.delivery_gate_max_replay_guard_reject_count
                ),
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
                passed=_none_or_lt(snapshot.oldest_delivery_dlq_age_sec, FULL_DLQ_OLDEST_AGE_THRESHOLD_SEC),
                reason_code="delivery_gate_oldest_dlq_too_old",
            )
            add_metric(
                metric_name="duplicate_noop_ratio_1h",
                observed_value=snapshot.duplicate_noop_ratio_1h,
                threshold=None,
                comparator="report-only",
                passed=True,
                severity="warn",
            )
            if self._config.delivery_gate_require_operator_review_for_full and operator_review_passed is not True:
                warning_reason_codes.append("delivery_gate_operator_review_required")

        blocking_reason_codes = _dedupe_stable(blocking_reason_codes)
        warning_reason_codes = _dedupe_stable(warning_reason_codes)
        if blocking_reason_codes:
            gate_status = "fail"
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
            operator_review_required=(
                mode == "full" and self._config.delivery_gate_require_operator_review_for_full
            ),
            operator_review_passed=operator_review_passed,
            recommended_flag_patch=_recommended_flag_patch(gate_status),
        )


def _gte(value: float | None, threshold: float) -> bool:
    return value is not None and value >= threshold


def _none_or_lte(value: float | None, threshold: float) -> bool:
    return value is None or value <= threshold


def _none_or_lt(value: float | None, threshold: float) -> bool:
    return value is None or value < threshold


def _dedupe_stable(values: list[str]) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for value in values:
        if value not in seen:
            seen.add(value)
            result.append(value)
    return result


def _recommended_flag_patch(gate_status: str) -> dict[str, object]:
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
