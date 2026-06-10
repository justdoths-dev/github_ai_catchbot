from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol, TypeAlias

from .config import MaintenanceConfig
from .models import DeliveryGateMetric, DeliveryGateReportV1, DeliveryGateSnapshot, GateMode, GateStatus


DeliveryGateMode: TypeAlias = GateMode
DeliveryGateStatus: TypeAlias = GateStatus
DeliveryGateReport: TypeAlias = DeliveryGateReportV1

FULL_DLQ_OLDEST_AGE_THRESHOLD_SEC = 3600.0
DUPLICATE_NOOP_RATIO_WARN_THRESHOLD = 0.0


@dataclass(frozen=True, slots=True)
class DeliveryGateThresholds:
    min_success_rate_1h: float = 0.99
    min_success_rate_24h: float = 0.99
    max_high_source_to_delivery_p95_sec: float = 120.0
    max_due_retry_lag_sec: float = 120.0
    max_open_dlq_count: int = 0
    max_send_disabled_count: int = 0
    max_replay_guard_reject_count_24h: int = 0
    max_retry_ceiling_exceeded_count_24h: int = 0
    max_oldest_delivery_dlq_age_sec: float = FULL_DLQ_OLDEST_AGE_THRESHOLD_SEC
    duplicate_noop_ratio_warn_threshold: float = DUPLICATE_NOOP_RATIO_WARN_THRESHOLD
    require_operator_review_for_full: bool = True

    @classmethod
    def from_config(cls, config: MaintenanceConfig) -> "DeliveryGateThresholds":
        return cls(
            min_success_rate_1h=config.delivery_gate_min_success_rate_1h,
            min_success_rate_24h=config.delivery_gate_min_success_rate_24h,
            max_high_source_to_delivery_p95_sec=config.delivery_gate_max_high_source_to_delivery_p95_sec,
            max_due_retry_lag_sec=config.delivery_gate_max_due_retry_lag_sec,
            max_open_dlq_count=config.delivery_gate_max_open_dlq_count,
            max_send_disabled_count=config.delivery_gate_max_send_disabled_count,
            max_replay_guard_reject_count_24h=config.delivery_gate_max_replay_guard_reject_count,
            require_operator_review_for_full=config.delivery_gate_require_operator_review_for_full,
        )


class DeliveryGateRepository(Protocol):
    async def load_delivery_gate_snapshot(self) -> DeliveryGateSnapshot: ...


class DeliveryGate:
    def __init__(
        self,
        config: MaintenanceConfig,
        *,
        repository: DeliveryGateRepository,
    ) -> None:
        self._config = config
        self._repository = repository

    async def run(
        self,
        *,
        mode: DeliveryGateMode,
        operator_review_passed: bool | None = None,
    ) -> DeliveryGateReport:
        snapshot = await self._repository.load_delivery_gate_snapshot()
        return evaluate_delivery_gate(
            config=self._config,
            snapshot=snapshot,
            mode=mode,
            operator_review_passed=operator_review_passed,
        )


def evaluate_delivery_gate(
    *,
    config: MaintenanceConfig,
    snapshot: DeliveryGateSnapshot,
    mode: DeliveryGateMode,
    operator_review_passed: bool | None = None,
    thresholds: DeliveryGateThresholds | None = None,
) -> DeliveryGateReport:
    if mode not in {"restricted", "full"}:
        raise ValueError(f"unsupported delivery gate mode: {mode!r}")

    gate_thresholds = thresholds or DeliveryGateThresholds.from_config(config)
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
            return
        blocking_reason_codes.append(reason_code)

    _add_restricted_metrics(
        add_metric=add_metric,
        config=config,
        snapshot=snapshot,
        thresholds=gate_thresholds,
    )
    if mode == "full":
        _add_full_metrics(
            add_metric=add_metric,
            snapshot=snapshot,
            thresholds=gate_thresholds,
        )
        if gate_thresholds.require_operator_review_for_full and operator_review_passed is not True:
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
        operator_review_required=mode == "full" and gate_thresholds.require_operator_review_for_full,
        operator_review_passed=operator_review_passed,
        recommended_flag_patch=_recommended_flag_patch(gate_status),
    )


def _add_restricted_metrics(
    *,
    add_metric,
    config: MaintenanceConfig,
    snapshot: DeliveryGateSnapshot,
    thresholds: DeliveryGateThresholds,
) -> None:
    add_metric(
        metric_name="enable_notification_send",
        observed_value=config.enable_notification_send,
        threshold=True,
        comparator="==",
        passed=config.enable_notification_send is True,
        reason_code="delivery_gate_flag_send_disabled",
    )
    add_metric(
        metric_name="notifier_telegram_dry_run",
        observed_value=config.notifier_telegram_dry_run,
        threshold=False,
        comparator="==",
        passed=config.notifier_telegram_dry_run is False,
        reason_code="delivery_gate_dry_run_enabled",
    )
    add_metric(
        metric_name="maintenance_retry_promotion",
        observed_value=config.enable_delivery_retry_promotion,
        threshold=True,
        comparator="==",
        passed=config.enable_delivery_retry_promotion is True,
        reason_code="delivery_gate_retry_promotion_disabled",
    )
    add_metric(
        metric_name="success_rate_1h",
        observed_value=snapshot.success_rate_1h,
        threshold=thresholds.min_success_rate_1h,
        comparator=">=",
        passed=_gte(snapshot.success_rate_1h, thresholds.min_success_rate_1h),
        reason_code=(
            "delivery_gate_success_rate_missing"
            if snapshot.success_rate_1h is None
            else "delivery_gate_success_rate_below_threshold"
        ),
    )
    add_metric(
        metric_name="high_source_to_delivery_p95_sec",
        observed_value=snapshot.high_source_to_delivery_p95_sec,
        threshold=thresholds.max_high_source_to_delivery_p95_sec,
        comparator="<=",
        passed=_none_or_lte(
            snapshot.high_source_to_delivery_p95_sec,
            thresholds.max_high_source_to_delivery_p95_sec,
        ),
        reason_code="delivery_gate_high_e2e_p95_too_high",
    )
    add_metric(
        metric_name="due_retry_oldest_lag_sec",
        observed_value=snapshot.due_retry_oldest_lag_sec,
        threshold=thresholds.max_due_retry_lag_sec,
        comparator="<=",
        passed=_none_or_lte(snapshot.due_retry_oldest_lag_sec, thresholds.max_due_retry_lag_sec),
        reason_code="delivery_gate_due_retry_lag_too_high",
    )
    add_metric(
        metric_name="open_delivery_dlq_count",
        observed_value=snapshot.open_delivery_dlq_count,
        threshold=thresholds.max_open_dlq_count,
        comparator="<=",
        passed=snapshot.open_delivery_dlq_count <= thresholds.max_open_dlq_count,
        reason_code="delivery_gate_open_dlq_present",
    )
    add_metric(
        metric_name="unexpected_send_disabled_count",
        observed_value=snapshot.unexpected_send_disabled_count,
        threshold=thresholds.max_send_disabled_count,
        comparator="<=",
        passed=snapshot.unexpected_send_disabled_count <= thresholds.max_send_disabled_count,
        reason_code="delivery_gate_unexpected_send_disabled_rows_present",
    )


def _add_full_metrics(
    *,
    add_metric,
    snapshot: DeliveryGateSnapshot,
    thresholds: DeliveryGateThresholds,
) -> None:
    add_metric(
        metric_name="success_rate_24h",
        observed_value=snapshot.success_rate_24h,
        threshold=thresholds.min_success_rate_24h,
        comparator=">=",
        passed=_gte(snapshot.success_rate_24h, thresholds.min_success_rate_24h),
        reason_code=(
            "delivery_gate_24h_success_rate_missing"
            if snapshot.success_rate_24h is None
            else "delivery_gate_24h_success_rate_below_threshold"
        ),
    )
    add_metric(
        metric_name="replay_guard_reject_count_24h",
        observed_value=snapshot.replay_guard_reject_count_24h,
        threshold=thresholds.max_replay_guard_reject_count_24h,
        comparator="<=",
        passed=snapshot.replay_guard_reject_count_24h <= thresholds.max_replay_guard_reject_count_24h,
        reason_code="delivery_gate_replay_guard_rejects_present",
    )
    add_metric(
        metric_name="retry_ceiling_exceeded_count_24h",
        observed_value=snapshot.retry_ceiling_exceeded_count_24h,
        threshold=thresholds.max_retry_ceiling_exceeded_count_24h,
        comparator="<=",
        passed=snapshot.retry_ceiling_exceeded_count_24h <= thresholds.max_retry_ceiling_exceeded_count_24h,
        reason_code="delivery_gate_retry_ceiling_exceeded_rows_present",
    )
    add_metric(
        metric_name="oldest_delivery_dlq_age_sec",
        observed_value=snapshot.oldest_delivery_dlq_age_sec,
        threshold=thresholds.max_oldest_delivery_dlq_age_sec,
        comparator="<",
        passed=_dlq_oldest_age_passes(snapshot, thresholds),
        reason_code="delivery_gate_delivery_dlq_oldest_age_too_high",
    )
    add_metric(
        metric_name="duplicate_noop_ratio_1h",
        observed_value=snapshot.duplicate_noop_ratio_1h,
        threshold=thresholds.duplicate_noop_ratio_warn_threshold,
        comparator="<=",
        passed=_none_or_lte(snapshot.duplicate_noop_ratio_1h, thresholds.duplicate_noop_ratio_warn_threshold),
        reason_code="delivery_gate_duplicate_noop_ratio_review_required",
        severity="warn",
    )


def _gte(value: float | None, threshold: float) -> bool:
    return value is not None and value >= threshold


def _lt(value: float | None, threshold: float) -> bool:
    return value is not None and value < threshold


def _none_or_lte(value: float | None, threshold: float) -> bool:
    return value is None or value <= threshold


def _dlq_oldest_age_passes(snapshot: DeliveryGateSnapshot, thresholds: DeliveryGateThresholds) -> bool:
    if snapshot.open_delivery_dlq_count <= 0 and snapshot.oldest_delivery_dlq_age_sec is None:
        return True
    return _lt(snapshot.oldest_delivery_dlq_age_sec, thresholds.max_oldest_delivery_dlq_age_sec)


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
