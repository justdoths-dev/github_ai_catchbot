from __future__ import annotations

import ast
from pathlib import Path

import pytest

from services.maintenance.config import MaintenanceConfig
from services.maintenance.delivery_gate import (
    DeliveryGate,
    DeliveryGateThresholds,
    evaluate_delivery_gate,
)
from services.maintenance.models import DeliveryGateSnapshot


ROOT = Path(__file__).resolve().parents[4]
GATE_SOURCE = ROOT / "src" / "services" / "maintenance" / "delivery_gate.py"
EXPECTED_FLAG_PATCH_KEYS = [
    "ENABLE_NOTIFICATION_SEND",
    "MAINTENANCE_ENABLE_NOTIFICATION_RETRY_PROMOTION",
    "NOTIFIER_TELEGRAM_DRY_RUN",
]
RESTRICTED_METRIC_ORDER = [
    "enable_notification_send",
    "notifier_telegram_dry_run",
    "maintenance_retry_promotion",
    "success_rate_1h",
    "high_source_to_delivery_p95_sec",
    "due_retry_oldest_lag_sec",
    "open_delivery_dlq_count",
    "unexpected_send_disabled_count",
]
FULL_METRIC_ORDER = RESTRICTED_METRIC_ORDER + [
    "success_rate_24h",
    "replay_guard_reject_count_24h",
    "retry_ceiling_exceeded_count_24h",
    "oldest_delivery_dlq_age_sec",
    "duplicate_noop_ratio_1h",
]


class FakeGateRepository:
    def __init__(self, snapshot: DeliveryGateSnapshot) -> None:
        self.snapshot = snapshot
        self.load_count = 0

    async def load_delivery_gate_snapshot(self) -> DeliveryGateSnapshot:
        self.load_count += 1
        return self.snapshot


def _config(
    *,
    send_enabled: bool = True,
    dry_run: bool = False,
    retry_promotion_enabled: bool = True,
) -> MaintenanceConfig:
    return MaintenanceConfig(
        app_env="test",
        database_url="postgresql+psycopg://example",
        redis_url="redis://example",
        maintenance_queue_name="q.maintenance",
        maintenance_consumer_group="maintenance",
        maintenance_consumer_name="test",
        replay_queue_name="q.replay",
        replay_consumer_group="maintenance-replay",
        replay_consumer_name="test",
        batch_size=50,
        block_ms=100,
        retry_scan_poll_sec=30,
        delivery_retry_max_attempts=3,
        enable_notification_send=send_enabled,
        notifier_telegram_dry_run=dry_run,
        enable_delivery_retry_promotion=retry_promotion_enabled,
        enable_replay_to_prod_db=False,
        delivery_gate_min_success_rate_1h=0.99,
        delivery_gate_min_success_rate_24h=0.99,
        delivery_gate_max_high_source_to_delivery_p95_sec=120,
        delivery_gate_max_plan_to_transport_p95_sec=120,
        delivery_gate_max_due_retry_lag_sec=120,
        delivery_gate_max_open_dlq_count=0,
        delivery_gate_max_send_disabled_count=0,
        delivery_gate_max_replay_guard_reject_count=0,
        delivery_gate_require_operator_review_for_full=True,
        log_level="INFO",
    )


def _snapshot(**overrides) -> DeliveryGateSnapshot:
    values = {
        "success_rate_1h": 1.0,
        "success_rate_24h": 1.0,
        "high_source_to_delivery_p95_sec": 10.0,
        "plan_to_transport_p95_sec": 8.0,
        "due_retry_oldest_lag_sec": None,
        "open_delivery_dlq_count": 0,
        "oldest_delivery_dlq_age_sec": None,
        "unexpected_send_disabled_count": 0,
        "replay_guard_reject_count_24h": 0,
        "retry_ceiling_exceeded_count_24h": 0,
        "duplicate_noop_ratio_1h": 0.0,
    }
    values.update(overrides)
    return DeliveryGateSnapshot(**values)


def _report(
    *,
    config: MaintenanceConfig | None = None,
    snapshot: DeliveryGateSnapshot | None = None,
    mode: str = "restricted",
    operator_review_passed: bool | None = None,
    thresholds: DeliveryGateThresholds | None = None,
):
    return evaluate_delivery_gate(
        config=config or _config(),
        snapshot=snapshot or _snapshot(),
        mode=mode,  # type: ignore[arg-type]
        operator_review_passed=operator_review_passed,
        thresholds=thresholds,
    )


def test_restricted_gate_passes_with_all_hard_metrics_healthy() -> None:
    report = _report()

    assert report.mode == "restricted"
    assert report.gate_status == "pass"
    assert report.blocking_reason_codes == []
    assert report.warning_reason_codes == []
    assert [metric.metric_name for metric in report.metrics] == RESTRICTED_METRIC_ORDER


def test_restricted_gate_fails_when_enable_notification_send_false() -> None:
    report = _report(config=_config(send_enabled=False))

    assert report.gate_status == "fail"
    assert report.blocking_reason_codes == ["delivery_gate_flag_send_disabled"]


def test_restricted_gate_fails_when_dry_run_is_enabled() -> None:
    report = _report(config=_config(dry_run=True))

    assert report.gate_status == "fail"
    assert report.blocking_reason_codes == ["delivery_gate_dry_run_enabled"]


def test_restricted_gate_fails_when_retry_promotion_is_disabled() -> None:
    report = _report(config=_config(retry_promotion_enabled=False))

    assert report.gate_status == "fail"
    assert report.blocking_reason_codes == ["delivery_gate_retry_promotion_disabled"]


def test_restricted_gate_fails_when_1h_success_rate_is_below_threshold() -> None:
    report = _report(snapshot=_snapshot(success_rate_1h=0.98))

    assert report.gate_status == "fail"
    assert report.blocking_reason_codes == ["delivery_gate_success_rate_below_threshold"]


def test_restricted_gate_fails_when_high_p95_is_above_threshold() -> None:
    report = _report(snapshot=_snapshot(high_source_to_delivery_p95_sec=121.0))

    assert report.gate_status == "fail"
    assert report.blocking_reason_codes == ["delivery_gate_high_e2e_p95_too_high"]


def test_restricted_gate_fails_when_due_retry_lag_is_above_threshold() -> None:
    report = _report(snapshot=_snapshot(due_retry_oldest_lag_sec=121.0))

    assert report.gate_status == "fail"
    assert report.blocking_reason_codes == ["delivery_gate_due_retry_lag_too_high"]


def test_restricted_gate_fails_when_open_delivery_dlq_exists() -> None:
    report = _report(snapshot=_snapshot(open_delivery_dlq_count=1))

    assert report.gate_status == "fail"
    assert report.blocking_reason_codes == ["delivery_gate_open_dlq_present"]


def test_restricted_gate_fails_when_unexpected_send_disabled_rows_exist() -> None:
    report = _report(snapshot=_snapshot(unexpected_send_disabled_count=1))

    assert report.gate_status == "fail"
    assert report.blocking_reason_codes == ["delivery_gate_unexpected_send_disabled_rows_present"]


def test_full_gate_includes_restricted_checks() -> None:
    report = _report(mode="full", operator_review_passed=True)

    assert [metric.metric_name for metric in report.metrics[: len(RESTRICTED_METRIC_ORDER)]] == RESTRICTED_METRIC_ORDER
    assert [metric.metric_name for metric in report.metrics] == FULL_METRIC_ORDER


def test_full_gate_fails_when_24h_success_rate_is_below_threshold() -> None:
    report = _report(mode="full", operator_review_passed=True, snapshot=_snapshot(success_rate_24h=0.98))

    assert report.gate_status == "fail"
    assert report.blocking_reason_codes == ["delivery_gate_24h_success_rate_below_threshold"]


def test_full_gate_fails_when_replay_guard_rejects_exist() -> None:
    report = _report(mode="full", operator_review_passed=True, snapshot=_snapshot(replay_guard_reject_count_24h=1))

    assert report.gate_status == "fail"
    assert report.blocking_reason_codes == ["delivery_gate_replay_guard_rejects_present"]


def test_full_gate_fails_when_retry_ceiling_exceeded_rows_exist() -> None:
    report = _report(
        mode="full",
        operator_review_passed=True,
        snapshot=_snapshot(retry_ceiling_exceeded_count_24h=1),
    )

    assert report.gate_status == "fail"
    assert report.blocking_reason_codes == ["delivery_gate_retry_ceiling_exceeded_rows_present"]


def test_full_gate_fails_when_oldest_dlq_age_exceeds_threshold() -> None:
    report = _report(
        mode="full",
        operator_review_passed=True,
        snapshot=_snapshot(open_delivery_dlq_count=0, oldest_delivery_dlq_age_sec=3600.0),
    )

    assert report.gate_status == "fail"
    assert report.blocking_reason_codes == ["delivery_gate_delivery_dlq_oldest_age_too_high"]


def test_full_gate_returns_warn_not_fail_when_only_operator_review_is_missing() -> None:
    report = _report(mode="full")

    assert report.gate_status == "warn"
    assert report.blocking_reason_codes == []
    assert report.warning_reason_codes == ["delivery_gate_operator_review_required"]
    assert report.operator_review_required is True
    assert report.operator_review_passed is None


def test_duplicate_noop_spike_is_warning_only() -> None:
    report = _report(
        mode="full",
        operator_review_passed=True,
        snapshot=_snapshot(duplicate_noop_ratio_1h=0.5),
    )

    assert report.gate_status == "warn"
    assert report.blocking_reason_codes == []
    assert report.warning_reason_codes == ["delivery_gate_duplicate_noop_ratio_review_required"]


def test_report_metric_order_is_deterministic() -> None:
    report = _report(mode="full", operator_review_passed=True)

    assert [metric.metric_name for metric in report.metrics] == FULL_METRIC_ORDER


def test_blocking_and_warning_reason_order_is_deterministic() -> None:
    report = _report(
        config=_config(send_enabled=False, dry_run=True, retry_promotion_enabled=False),
        mode="full",
        snapshot=_snapshot(
            success_rate_1h=0.1,
            high_source_to_delivery_p95_sec=130.0,
            due_retry_oldest_lag_sec=130.0,
            open_delivery_dlq_count=1,
            oldest_delivery_dlq_age_sec=3600.0,
            unexpected_send_disabled_count=1,
            success_rate_24h=0.2,
            replay_guard_reject_count_24h=1,
            retry_ceiling_exceeded_count_24h=1,
            duplicate_noop_ratio_1h=0.5,
        ),
    )

    assert report.blocking_reason_codes == [
        "delivery_gate_flag_send_disabled",
        "delivery_gate_dry_run_enabled",
        "delivery_gate_retry_promotion_disabled",
        "delivery_gate_success_rate_below_threshold",
        "delivery_gate_high_e2e_p95_too_high",
        "delivery_gate_due_retry_lag_too_high",
        "delivery_gate_open_dlq_present",
        "delivery_gate_unexpected_send_disabled_rows_present",
        "delivery_gate_24h_success_rate_below_threshold",
        "delivery_gate_replay_guard_rejects_present",
        "delivery_gate_retry_ceiling_exceeded_rows_present",
        "delivery_gate_delivery_dlq_oldest_age_too_high",
    ]
    assert report.warning_reason_codes == [
        "delivery_gate_duplicate_noop_ratio_review_required",
        "delivery_gate_operator_review_required",
    ]


@pytest.mark.parametrize(
    "report",
    [
        _report(),
        _report(snapshot=_snapshot(open_delivery_dlq_count=1)),
        _report(mode="full"),
    ],
)
def test_recommended_flag_patch_contains_exactly_three_allowed_keys(report) -> None:
    assert list(report.recommended_flag_patch) == EXPECTED_FLAG_PATCH_KEYS
    assert set(report.recommended_flag_patch) == set(EXPECTED_FLAG_PATCH_KEYS)


@pytest.mark.asyncio
async def test_gate_runner_does_not_expose_mutating_methods() -> None:
    runner = DeliveryGate(_config(), repository=FakeGateRepository(_snapshot()))
    public_callables = {
        name
        for name in dir(runner)
        if not name.startswith("_") and callable(getattr(runner, name))
    }
    report = await runner.run(mode="restricted")

    assert public_callables == {"run"}
    assert report.gate_status == "pass"


def test_source_import_check_blocks_network_runtime_and_worker_dependencies() -> None:
    tree = ast.parse(GATE_SOURCE.read_text(encoding="utf-8"))
    banned_roots = {"redis", "openai", "telegram", "docker", "systemd", "requests", "httpx", "aiohttp"}
    imported_roots: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imported_roots.update(alias.name.split(".", 1)[0] for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            imported_roots.add(node.module.split(".", 1)[0])

    assert imported_roots.isdisjoint(banned_roots)


def test_delivery_gate_source_contains_no_ddl_or_mutation_strings() -> None:
    text = GATE_SOURCE.read_text(encoding="utf-8").lower()
    banned_fragments = [
        "create table",
        "alter table",
        "drop table",
        "truncate ",
        "insert into",
        "update ",
        "delete from",
        "redis.from_url",
        "create_async_engine",
    ]

    for fragment in banned_fragments:
        assert fragment not in text


def test_threshold_defaults_match_contract() -> None:
    thresholds = DeliveryGateThresholds()

    assert thresholds.min_success_rate_1h == 0.99
    assert thresholds.min_success_rate_24h == 0.99
    assert thresholds.max_high_source_to_delivery_p95_sec == 120.0
    assert thresholds.max_due_retry_lag_sec == 120.0
    assert thresholds.max_open_dlq_count == 0
    assert thresholds.max_send_disabled_count == 0
    assert thresholds.max_replay_guard_reject_count_24h == 0
    assert thresholds.max_retry_ceiling_exceeded_count_24h == 0
    assert thresholds.max_oldest_delivery_dlq_age_sec == 3600.0
