from __future__ import annotations

import json
from dataclasses import asdict

from services.maintenance.delivery_operations_gate import evaluate_delivery_operations_gate
from services.maintenance.mvp_readiness import (
    REQUIRED_RECOVERY_CLI_CHECKS,
    UPSTREAM_HOT_PATH_CHECKS,
    build_restricted_live_mvp_readiness_report,
)
from tests.unit.services.maintenance.test_delivery_gate_runner import _config, _snapshot


def _recovery_cli_surface(**overrides: bool) -> dict[str, bool]:
    values = {check_name: True for check_name in REQUIRED_RECOVERY_CLI_CHECKS}
    values.update(overrides)
    return values


def _upstream_statuses(status: str = "pass") -> dict[str, str]:
    return {check_name: status for check_name in UPSTREAM_HOT_PATH_CHECKS}


def _delivery_gate_report(snapshot=None, *, config=None):
    return evaluate_delivery_operations_gate(
        config=config or _config(),
        snapshot=snapshot or _snapshot(),
        mode="restricted",
    )


def _report(*, config=None, snapshot=None, upstream_status: str = "pass", recovery_cli_surface=None):
    cfg = config or _config()
    return build_restricted_live_mvp_readiness_report(
        config=cfg,
        delivery_gate_report=_delivery_gate_report(snapshot=snapshot, config=cfg),
        recovery_cli_surface=recovery_cli_surface or _recovery_cli_surface(),
        upstream_component_statuses=_upstream_statuses(upstream_status),
    )


def test_all_pass_yields_pass_readiness() -> None:
    report = _report()

    assert report.schema_version == "mvp_readiness_report_v1"
    assert report.mode == "restricted"
    assert report.readiness_status == "pass"
    assert report.blocking_reason_codes == []
    assert report.warning_reason_codes == []
    assert report.recommended_next_action == "eligible_for_restricted_live_operator_review"


def test_one_hard_delivery_blocker_yields_fail() -> None:
    report = _report(snapshot=_snapshot(open_delivery_dlq_count=1))

    assert report.readiness_status == "fail"
    assert report.blocking_reason_codes == ["delivery_gate_open_dlq_present"]
    assert report.recommended_next_action == "fix_delivery_readiness_blockers"


def test_upstream_unknown_without_hard_blocker_yields_warn() -> None:
    report = build_restricted_live_mvp_readiness_report(
        config=_config(),
        delivery_gate_report=_delivery_gate_report(),
        recovery_cli_surface=_recovery_cli_surface(),
        upstream_component_statuses={},
    )

    assert report.readiness_status == "warn"
    assert report.blocking_reason_codes == []
    assert report.warning_reason_codes == ["mvp_readiness_component_status_unknown"]
    assert report.recommended_next_action == "proceed_to_upstream_hot_path_acceptance"
    assert {check.status for check in report.checks if check.check_name.endswith("_ready")} == {"unknown"}


def test_recommended_flag_patch_is_output_only() -> None:
    config = _config()
    report = _report(config=config)

    assert report.recommended_flag_patch == {
        "ENABLE_NOTIFICATION_SEND": True,
        "MAINTENANCE_ENABLE_NOTIFICATION_RETRY_PROMOTION": True,
        "NOTIFIER_TELEGRAM_DRY_RUN": False,
    }
    assert config.enable_notification_send is True
    assert config.enable_delivery_retry_promotion is True
    assert config.notifier_telegram_dry_run is False


def test_reason_code_ordering_is_deterministic() -> None:
    report = _report(
        config=_config(send_enabled=False, dry_run=True, retry_promotion_enabled=False),
        snapshot=_snapshot(
            success_rate_1h=0.5,
            high_source_to_delivery_p95_sec=130,
            due_retry_oldest_lag_sec=150,
            open_delivery_dlq_count=1,
            unexpected_send_disabled_count=1,
        ),
        upstream_status="fail",
        recovery_cli_surface=_recovery_cli_surface(
            batch_recovery_replay_selected_operator_confirmed=False,
            batch_recovery_retry_selected_due_confirm_write=False,
        ),
    )

    assert report.blocking_reason_codes == [
        "mvp_readiness_notification_send_disabled",
        "mvp_readiness_notifier_dry_run_enabled",
        "mvp_readiness_retry_promotion_disabled",
        "delivery_gate_success_rate_below_threshold",
        "delivery_gate_high_e2e_p95_too_high",
        "delivery_gate_due_retry_lag_too_high",
        "delivery_gate_open_dlq_present",
        "delivery_gate_unexpected_send_disabled_rows_present",
        "mvp_readiness_recovery_cli_missing",
        "mvp_readiness_component_status_fail",
    ]


def test_json_output_does_not_include_secret_or_runtime_locator_values() -> None:
    config = _config()
    report = _report(config=config)
    payload = json.dumps(asdict(report), ensure_ascii=False, sort_keys=True, default=str)

    assert config.database_url not in payload
    assert config.redis_url not in payload
    assert "DATABASE_URL" not in payload
    assert "REDIS_URL" not in payload
    assert "TELEGRAM_BOT_TOKEN" not in payload
    assert "OPENAI_API_KEY" not in payload
