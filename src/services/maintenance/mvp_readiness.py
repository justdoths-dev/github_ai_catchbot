from __future__ import annotations

from collections.abc import Mapping
from typing import Protocol

from .config import MaintenanceConfig
from .models import (
    DeliveryGateReportV1,
    MvpReadinessCheck,
    MvpReadinessCheckStatus,
    MvpReadinessReportV1,
    MvpReadinessStatus,
)


SCHEMA_VERSION = "mvp_readiness_report_v1"
COMPONENT_STATUS_UNKNOWN = "mvp_readiness_component_status_unknown"

UPSTREAM_HOT_PATH_CHECKS = (
    "collector_telegram_ready",
    "router_normalizer_ready",
    "enrichers_ready",
    "evidence_assembler_ready",
    "analysis_router_ready",
    "judge_openai_ready",
    "analysis_validator_ready",
    "policy_engine_ready",
    "notifier_telegram_ready",
    "maintenance_recovery_ready",
)

REQUIRED_RECOVERY_CLI_CHECKS = (
    "batch_recovery_replay_selected_operator_confirmed",
    "batch_recovery_retry_selected_due_confirm_write",
    "delivery_gate_restricted_mode",
)


class RestrictedDeliveryGateRunner(Protocol):
    async def run(
        self,
        *,
        mode: str,
        operator_review_passed: bool | None = None,
    ) -> DeliveryGateReportV1: ...


async def run_restricted_live_mvp_readiness(
    *,
    config: MaintenanceConfig,
    delivery_gate_runner: RestrictedDeliveryGateRunner,
    recovery_cli_surface: Mapping[str, bool],
    upstream_component_statuses: Mapping[str, str] | None = None,
) -> MvpReadinessReportV1:
    delivery_gate_report = await delivery_gate_runner.run(mode="restricted")
    return build_restricted_live_mvp_readiness_report(
        config=config,
        delivery_gate_report=delivery_gate_report,
        recovery_cli_surface=recovery_cli_surface,
        upstream_component_statuses=upstream_component_statuses,
    )


def build_restricted_live_mvp_readiness_report(
    *,
    config: MaintenanceConfig,
    delivery_gate_report: DeliveryGateReportV1,
    recovery_cli_surface: Mapping[str, bool],
    upstream_component_statuses: Mapping[str, str] | None = None,
) -> MvpReadinessReportV1:
    checks: list[MvpReadinessCheck] = []

    checks.extend(_runtime_flag_checks(config))
    checks.extend(_delivery_gate_checks(delivery_gate_report))
    checks.extend(_recovery_cli_checks(recovery_cli_surface))
    checks.extend(_upstream_hot_path_checks(upstream_component_statuses or {}))

    blocking_reason_codes = _dedupe_stable(
        [
            check.reason_code
            for check in checks
            if check.reason_code is not None and check.status == "fail" and check.severity == "block"
        ]
    )
    warning_reason_codes = _dedupe_stable(
        [
            check.reason_code
            for check in checks
            if check.reason_code is not None
            and (check.severity == "warn" or check.status in {"warn", "unknown"})
            and check.reason_code not in blocking_reason_codes
        ]
    )

    readiness_status = _readiness_status(blocking_reason_codes, warning_reason_codes)
    return MvpReadinessReportV1(
        schema_version=SCHEMA_VERSION,
        mode="restricted",
        readiness_status=readiness_status,
        blocking_reason_codes=blocking_reason_codes,
        warning_reason_codes=warning_reason_codes,
        checks=checks,
        recommended_next_action=_recommended_next_action(readiness_status, checks),
        recommended_flag_patch=dict(delivery_gate_report.recommended_flag_patch),
    )


def _runtime_flag_checks(config: MaintenanceConfig) -> list[MvpReadinessCheck]:
    return [
        _bool_check(
            check_name="notification_send_enabled_for_restricted_live",
            observed_value=config.enable_notification_send,
            expected_value=True,
            reason_code="mvp_readiness_notification_send_disabled",
        ),
        _bool_check(
            check_name="notifier_dry_run_disabled_for_restricted_live",
            observed_value=config.notifier_telegram_dry_run,
            expected_value=False,
            reason_code="mvp_readiness_notifier_dry_run_enabled",
        ),
        _bool_check(
            check_name="retry_promotion_enabled_for_restricted_live",
            observed_value=config.enable_delivery_retry_promotion,
            expected_value=True,
            reason_code="mvp_readiness_retry_promotion_disabled",
        ),
    ]


def _delivery_gate_checks(delivery_gate_report: DeliveryGateReportV1) -> list[MvpReadinessCheck]:
    reason_by_metric = _delivery_gate_reason_codes(delivery_gate_report)
    checks: list[MvpReadinessCheck] = []
    for metric in delivery_gate_report.metrics:
        reason_code = reason_by_metric.get(metric.metric_name)
        checks.append(
            MvpReadinessCheck(
                check_name=metric.metric_name,
                status="pass" if metric.passed else "fail",
                severity="warn" if metric.severity == "warn" else "block",
                reason_code=None if metric.passed else reason_code,
                observed_value=metric.observed_value,
                expected_value=metric.threshold,
            )
        )
    return checks


def _delivery_gate_reason_codes(delivery_gate_report: DeliveryGateReportV1) -> dict[str, str]:
    reason_by_metric: dict[str, str] = {}
    for metric in delivery_gate_report.metrics:
        if metric.metric_name == "success_rate_1h":
            reason_by_metric[metric.metric_name] = (
                "delivery_gate_success_rate_missing"
                if metric.observed_value is None
                else "delivery_gate_success_rate_below_threshold"
            )
        elif metric.metric_name == "high_source_to_delivery_p95_sec":
            reason_by_metric[metric.metric_name] = "delivery_gate_high_e2e_p95_too_high"
        elif metric.metric_name == "due_retry_oldest_lag_sec":
            reason_by_metric[metric.metric_name] = "delivery_gate_due_retry_lag_too_high"
        elif metric.metric_name == "open_delivery_dlq_count":
            reason_by_metric[metric.metric_name] = "delivery_gate_open_dlq_present"
        elif metric.metric_name == "unexpected_send_disabled_count":
            reason_by_metric[metric.metric_name] = "delivery_gate_unexpected_send_disabled_rows_present"
        elif metric.metric_name == "success_rate_24h":
            reason_by_metric[metric.metric_name] = "delivery_gate_24h_success_rate_below_threshold"
        elif metric.metric_name == "replay_guard_reject_count_24h":
            reason_by_metric[metric.metric_name] = "delivery_gate_prod_replay_guard_rejects_present"
        elif metric.metric_name == "retry_ceiling_exceeded_count_24h":
            reason_by_metric[metric.metric_name] = "delivery_gate_retry_ceiling_exceeded_rows_present"
        elif metric.metric_name == "oldest_delivery_dlq_age_sec":
            reason_by_metric[metric.metric_name] = "delivery_gate_delivery_dlq_oldest_age_too_high"
        elif metric.metric_name == "duplicate_noop_ratio_1h":
            reason_by_metric[metric.metric_name] = "delivery_gate_duplicate_noop_ratio_review_required"
    return reason_by_metric


def _recovery_cli_checks(recovery_cli_surface: Mapping[str, bool]) -> list[MvpReadinessCheck]:
    checks: list[MvpReadinessCheck] = []
    for check_name in REQUIRED_RECOVERY_CLI_CHECKS:
        observed = bool(recovery_cli_surface.get(check_name, False))
        checks.append(
            _bool_check(
                check_name=check_name,
                observed_value=observed,
                expected_value=True,
                reason_code="mvp_readiness_recovery_cli_missing",
            )
        )
    return checks


def _upstream_hot_path_checks(component_statuses: Mapping[str, str]) -> list[MvpReadinessCheck]:
    checks: list[MvpReadinessCheck] = []
    for check_name in UPSTREAM_HOT_PATH_CHECKS:
        raw_status = component_statuses.get(check_name, "unknown")
        status = _component_status(raw_status)
        if status == "pass":
            checks.append(
                MvpReadinessCheck(
                    check_name=check_name,
                    status="pass",
                    severity="info",
                    reason_code=None,
                    observed_value="pass",
                    expected_value="pass",
                )
            )
        elif status == "fail":
            checks.append(
                MvpReadinessCheck(
                    check_name=check_name,
                    status="fail",
                    severity="block",
                    reason_code="mvp_readiness_component_status_fail",
                    observed_value=raw_status,
                    expected_value="pass",
                )
            )
        elif status == "warn":
            checks.append(
                MvpReadinessCheck(
                    check_name=check_name,
                    status="warn",
                    severity="warn",
                    reason_code="mvp_readiness_component_status_warn",
                    observed_value=raw_status,
                    expected_value="pass",
                )
            )
        else:
            checks.append(
                MvpReadinessCheck(
                    check_name=check_name,
                    status="unknown",
                    severity="warn",
                    reason_code=COMPONENT_STATUS_UNKNOWN,
                    observed_value="unknown",
                    expected_value="pass",
                )
            )
    return checks


def _bool_check(
    *,
    check_name: str,
    observed_value: bool,
    expected_value: bool,
    reason_code: str,
) -> MvpReadinessCheck:
    passed = observed_value is expected_value
    return MvpReadinessCheck(
        check_name=check_name,
        status="pass" if passed else "fail",
        severity="block",
        reason_code=None if passed else reason_code,
        observed_value=observed_value,
        expected_value=expected_value,
    )


def _component_status(value: str) -> MvpReadinessCheckStatus:
    if value == "pass":
        return "pass"
    if value == "fail":
        return "fail"
    if value == "warn":
        return "warn"
    return "unknown"


def _readiness_status(
    blocking_reason_codes: list[str],
    warning_reason_codes: list[str],
) -> MvpReadinessStatus:
    if blocking_reason_codes:
        return "fail"
    if warning_reason_codes:
        return "warn"
    return "pass"


def _recommended_next_action(
    readiness_status: MvpReadinessStatus,
    checks: list[MvpReadinessCheck],
) -> str:
    if readiness_status == "fail":
        return "fix_delivery_readiness_blockers"
    if any(check.status == "unknown" for check in checks):
        return "proceed_to_upstream_hot_path_acceptance"
    return "eligible_for_restricted_live_operator_review"


def _dedupe_stable(values: list[str | None]) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for value in values:
        if value is None or value in seen:
            continue
        seen.add(value)
        result.append(value)
    return result
