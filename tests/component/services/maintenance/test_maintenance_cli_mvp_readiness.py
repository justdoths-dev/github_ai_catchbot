from __future__ import annotations

import json

import pytest

from services.maintenance import main as maintenance_main
from services.maintenance.delivery_operations_gate import DeliveryOperationsGate
from services.maintenance.mvp_readiness import REQUIRED_RECOVERY_CLI_CHECKS, UPSTREAM_HOT_PATH_CHECKS
from services.maintenance.models import DeliveryGateSnapshot
from tests.unit.services.maintenance.test_delivery_gate_runner import FakeGateRepository, _config, _snapshot


def _recovery_cli_surface() -> dict[str, bool]:
    return {check_name: True for check_name in REQUIRED_RECOVERY_CLI_CHECKS}


def _upstream_statuses(status: str = "pass") -> dict[str, str]:
    return {check_name: status for check_name in UPSTREAM_HOT_PATH_CHECKS}


def _runner(snapshot: DeliveryGateSnapshot | None = None, *, config=None):
    return DeliveryOperationsGate(config or _config(), repository=FakeGateRepository(snapshot or _snapshot()))


def test_parser_accepts_mvp_readiness_json_command() -> None:
    args = maintenance_main.build_parser().parse_args(["mvp-readiness", "--mode", "restricted", "--format", "json"])

    assert args.command == "mvp-readiness"
    assert args.mode == "restricted"
    assert args.format == "json"


@pytest.mark.asyncio
async def test_mvp_readiness_command_returns_deterministic_json_without_live_db() -> None:
    emitted: list[str] = []

    exit_code = await maintenance_main.run_mvp_readiness(
        _config(),
        maintenance_main.build_parser().parse_args(["mvp-readiness", "--mode", "restricted", "--format", "json"]),
        _runner(),
        recovery_cli_surface=_recovery_cli_surface(),
        upstream_component_statuses=_upstream_statuses(),
        emit_json=emitted.append,
    )
    first_payload = json.loads(emitted[0])
    emitted.clear()
    second_exit_code = await maintenance_main.run_mvp_readiness(
        _config(),
        maintenance_main.build_parser().parse_args(["mvp-readiness", "--mode", "restricted", "--format", "json"]),
        _runner(),
        recovery_cli_surface=_recovery_cli_surface(),
        upstream_component_statuses=_upstream_statuses(),
        emit_json=emitted.append,
    )

    assert exit_code == 0
    assert second_exit_code == 0
    assert json.loads(emitted[0]) == first_payload
    assert first_payload["schema_version"] == "mvp_readiness_report_v1"
    assert first_payload["readiness_status"] == "pass"


@pytest.mark.asyncio
async def test_mvp_readiness_cli_path_does_not_call_batch_recovery(monkeypatch) -> None:
    calls: list[str] = []

    async def fake_run_mvp_readiness(config, args):
        calls.append(args.command)
        return 3

    async def fail_batch_recovery(config, args):
        raise AssertionError("mvp-readiness must not call batch recovery")

    monkeypatch.setattr(maintenance_main.MaintenanceConfig, "from_env", classmethod(lambda cls: _config()))
    monkeypatch.setattr(maintenance_main, "_run_mvp_readiness", fake_run_mvp_readiness)
    monkeypatch.setattr(maintenance_main, "_run_batch_recovery", fail_batch_recovery)

    exit_code = await maintenance_main._run(["mvp-readiness", "--mode", "restricted", "--format", "json"])

    assert exit_code == 3
    assert calls == ["mvp-readiness"]


@pytest.mark.asyncio
async def test_mvp_readiness_does_not_call_notifier_or_redis_publisher(monkeypatch) -> None:
    from services.notifier_telegram import worker as notifier_worker
    from services.outbox_relay import redis_streams as outbox_redis_streams

    def fail_notifier(*args, **kwargs):
        raise AssertionError("mvp-readiness must not instantiate notifier worker")

    def fail_publisher(*args, **kwargs):
        raise AssertionError("mvp-readiness must not instantiate Redis publisher")

    monkeypatch.setattr(notifier_worker, "NotifierTelegramWorker", fail_notifier)
    monkeypatch.setattr(outbox_redis_streams, "RedisStreamsPublisher", fail_publisher)
    emitted: list[str] = []

    exit_code = await maintenance_main.run_mvp_readiness(
        _config(),
        maintenance_main.build_parser().parse_args(["mvp-readiness", "--mode", "restricted", "--format", "json"]),
        _runner(),
        recovery_cli_surface=_recovery_cli_surface(),
        upstream_component_statuses=_upstream_statuses(),
        emit_json=emitted.append,
    )

    assert exit_code == 0
    assert json.loads(emitted[0])["readiness_status"] == "pass"


@pytest.mark.asyncio
async def test_mvp_readiness_exit_codes_map_to_pass_warn_fail() -> None:
    common_args = maintenance_main.build_parser().parse_args(["mvp-readiness", "--mode", "restricted", "--format", "json"])

    pass_code = await maintenance_main.run_mvp_readiness(
        _config(),
        common_args,
        _runner(),
        recovery_cli_surface=_recovery_cli_surface(),
        upstream_component_statuses=_upstream_statuses(),
        emit_json=lambda _: None,
    )
    warn_code = await maintenance_main.run_mvp_readiness(
        _config(),
        common_args,
        _runner(),
        recovery_cli_surface=_recovery_cli_surface(),
        upstream_component_statuses={},
        emit_json=lambda _: None,
    )
    fail_code = await maintenance_main.run_mvp_readiness(
        _config(),
        common_args,
        _runner(_snapshot(open_delivery_dlq_count=1)),
        recovery_cli_surface=_recovery_cli_surface(),
        upstream_component_statuses=_upstream_statuses(),
        emit_json=lambda _: None,
    )

    assert pass_code == 0
    assert warn_code == 3
    assert fail_code == 2
