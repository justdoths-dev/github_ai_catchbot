from __future__ import annotations

import json
from dataclasses import asdict
from typing import Awaitable, Callable, cast

from .config import MaintenanceConfig
from .delivery_gate import DeliveryGate, DeliveryGateRepository
from .models import DeliveryGateReportV1, GateMode


SCHEMA_VERSION = "delivery_gate_preflight_report_v1"
SUPPORTED_OUTPUT_FORMAT = "json"
SUPPORTED_MODES = frozenset({"restricted", "full"})
RECOMMENDED_FLAG_PATCH_KEYS = (
    "ENABLE_NOTIFICATION_SEND",
    "MAINTENANCE_ENABLE_NOTIFICATION_RETRY_PROMOTION",
    "NOTIFIER_TELEGRAM_DRY_RUN",
)
AUTHORITY = {
    "telegram_called": False,
    "openai_called": False,
    "github_called": False,
    "redis_mutation": False,
    "workers_started": False,
    "database_mutation": False,
    "production_db_write": False,
    "env_file_mutated": False,
    "feature_flags_applied": False,
    "alembic_or_ddl_ran": False,
}

DELIVERY_GATE_PREFLIGHT_CONFIG_LOAD_FAILED = "delivery_gate_preflight_config_load_failed"
DELIVERY_GATE_PREFLIGHT_SNAPSHOT_LOAD_FAILED = "delivery_gate_preflight_snapshot_load_failed"
DELIVERY_GATE_PREFLIGHT_UNSUPPORTED_MODE = "delivery_gate_preflight_unsupported_mode"
DELIVERY_GATE_PREFLIGHT_OUTPUT_FORMAT_UNSUPPORTED = "delivery_gate_preflight_output_format_unsupported"

ConfigLoader = Callable[[], MaintenanceConfig]
ReportLoader = Callable[[MaintenanceConfig, GateMode, bool | None], Awaitable[DeliveryGateReportV1]]


async def load_delivery_gate_preflight_report(
    config: MaintenanceConfig,
    repository: DeliveryGateRepository,
    *,
    mode: GateMode,
    operator_review_passed: bool | None = None,
) -> DeliveryGateReportV1:
    gate = DeliveryGate(config, repository=repository)
    return await gate.run(mode=mode, operator_review_passed=operator_review_passed)


async def run_delivery_gate_preflight(
    *,
    mode: str,
    output: str,
    operator_review_passed: bool | None,
    load_config: ConfigLoader,
    load_report: ReportLoader,
    emit_json: Callable[[str], None] = print,
) -> int:
    if output != SUPPORTED_OUTPUT_FORMAT:
        emit_json(_to_json(_error_payload(reason_code=DELIVERY_GATE_PREFLIGHT_OUTPUT_FORMAT_UNSUPPORTED)))
        return 1
    if mode not in SUPPORTED_MODES:
        emit_json(_to_json(_error_payload(reason_code=DELIVERY_GATE_PREFLIGHT_UNSUPPORTED_MODE)))
        return 1

    gate_mode = cast(GateMode, mode)
    try:
        config = load_config()
    except Exception:
        emit_json(
            _to_json(
                _error_payload(
                    mode=gate_mode,
                    operator_review_passed=operator_review_passed,
                    reason_code=DELIVERY_GATE_PREFLIGHT_CONFIG_LOAD_FAILED,
                )
            )
        )
        return 1

    try:
        report = await load_report(config, gate_mode, operator_review_passed)
    except Exception:
        emit_json(
            _to_json(
                _error_payload(
                    mode=gate_mode,
                    operator_review_passed=operator_review_passed,
                    reason_code=DELIVERY_GATE_PREFLIGHT_SNAPSHOT_LOAD_FAILED,
                )
            )
        )
        return 1

    emit_json(_to_json(_report_payload(report)))
    return 2 if report.gate_status == "fail" else 0


def _report_payload(report: DeliveryGateReportV1) -> dict[str, object]:
    payload = asdict(report)
    return {
        "schema_version": SCHEMA_VERSION,
        "mode": payload["mode"],
        "gate_status": payload["gate_status"],
        "blocking_reason_codes": payload["blocking_reason_codes"],
        "warning_reason_codes": payload["warning_reason_codes"],
        "metrics": payload["metrics"],
        "operator_review_required": payload["operator_review_required"],
        "operator_review_passed": payload["operator_review_passed"],
        "recommended_flag_patch": _ordered_flag_patch(payload["recommended_flag_patch"]),
        "authority": dict(AUTHORITY),
    }


def _error_payload(
    *,
    reason_code: str,
    mode: GateMode | None = None,
    operator_review_passed: bool | None = None,
) -> dict[str, object]:
    return {
        "schema_version": SCHEMA_VERSION,
        "mode": mode,
        "gate_status": "fail",
        "blocking_reason_codes": [reason_code],
        "warning_reason_codes": [],
        "metrics": [],
        "operator_review_required": False,
        "operator_review_passed": operator_review_passed,
        "recommended_flag_patch": _ordered_flag_patch(
            {
                "ENABLE_NOTIFICATION_SEND": False,
                "MAINTENANCE_ENABLE_NOTIFICATION_RETRY_PROMOTION": False,
                "NOTIFIER_TELEGRAM_DRY_RUN": False,
            }
        ),
        "authority": dict(AUTHORITY),
    }


def _ordered_flag_patch(values: dict[str, object]) -> dict[str, object]:
    return {key: values[key] for key in RECOMMENDED_FLAG_PATCH_KEYS}


def _to_json(value: object) -> str:
    return json.dumps(value, ensure_ascii=False, default=str, indent=2, sort_keys=True)
