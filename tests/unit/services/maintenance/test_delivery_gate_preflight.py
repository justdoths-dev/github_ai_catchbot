from __future__ import annotations

import ast
import json
from dataclasses import replace
from pathlib import Path

import pytest

from services.maintenance import delivery_gate_preflight as preflight
from services.maintenance import main as maintenance_main
from services.maintenance.delivery_gate_preflight import (
    AUTHORITY,
    RECOMMENDED_FLAG_PATCH_KEYS,
    load_delivery_gate_preflight_report,
    run_delivery_gate_preflight,
)
from services.maintenance.models import DeliveryGateReportV1, DeliveryGateSnapshot
from tests.unit.services.maintenance.test_delivery_gate_runner import FakeGateRepository, _config, _snapshot


ROOT = Path(__file__).resolve().parents[4]
PREFLIGHT_SOURCE = ROOT / "src" / "services" / "maintenance" / "delivery_gate_preflight.py"
RAW_DATABASE_URL = "opaque-preflight-database-source-sentinel-db-secret"
RAW_REDIS_URL = "opaque-preflight-redis-source-sentinel-redis-secret"
TELEGRAM_TOKEN = "sentinel-telegram-token"
OPENAI_KEY = "sentinel-openai-key"
GITHUB_TOKEN = "sentinel-github-token"


async def _invoke_preflight(
    *,
    mode: str = "restricted",
    output: str = "json",
    operator_review_passed: bool | None = None,
    config=None,
    snapshot: DeliveryGateSnapshot | None = None,
    load_config=None,
    load_report=None,
) -> tuple[int, dict, str]:
    emitted: list[str] = []
    cfg = config or _config()
    snap = snapshot or _snapshot()

    async def default_load_report(config, gate_mode, review_passed) -> DeliveryGateReportV1:
        return await load_delivery_gate_preflight_report(
            config,
            FakeGateRepository(snap),
            mode=gate_mode,
            operator_review_passed=review_passed,
        )

    exit_code = await run_delivery_gate_preflight(
        mode=mode,
        output=output,
        operator_review_passed=operator_review_passed,
        load_config=load_config or (lambda: cfg),
        load_report=load_report or default_load_report,
        emit_json=emitted.append,
    )
    assert len(emitted) == 1
    return exit_code, json.loads(emitted[0]), emitted[0]


def _assert_no_sensitive_output(output: str) -> None:
    forbidden = [
        RAW_DATABASE_URL,
        RAW_REDIS_URL,
        "sentinel-db-secret",
        "sentinel-redis-secret",
        TELEGRAM_TOKEN,
        OPENAI_KEY,
        GITHUB_TOKEN,
        "Traceback",
        "RuntimeError",
        "raw exception",
    ]
    for value in forbidden:
        assert value not in output


def _assert_patch_keys(payload: dict) -> None:
    assert tuple(payload["recommended_flag_patch"]) == RECOMMENDED_FLAG_PATCH_KEYS
    assert set(payload["recommended_flag_patch"]) == set(RECOMMENDED_FLAG_PATCH_KEYS)


@pytest.mark.asyncio
async def test_restricted_preflight_pass_returns_deterministic_json_and_exit_zero() -> None:
    exit_code, payload, output = await _invoke_preflight()
    second_exit_code, second_payload, second_output = await _invoke_preflight()

    assert exit_code == 0
    assert second_exit_code == 0
    assert payload == second_payload
    assert output == second_output
    assert payload["schema_version"] == "delivery_gate_preflight_report_v1"
    assert payload["mode"] == "restricted"
    assert payload["gate_status"] == "pass"
    assert payload["blocking_reason_codes"] == []
    assert payload["warning_reason_codes"] == []
    assert payload["operator_review_required"] is False
    assert payload["operator_review_passed"] is None


@pytest.mark.asyncio
async def test_restricted_preflight_fail_returns_json_and_nonzero_exit() -> None:
    exit_code, payload, _ = await _invoke_preflight(snapshot=_snapshot(open_delivery_dlq_count=1))

    assert exit_code == 2
    assert payload["gate_status"] == "fail"
    assert payload["blocking_reason_codes"] == ["delivery_gate_open_dlq_present"]


@pytest.mark.asyncio
async def test_full_preflight_warn_from_missing_operator_review_exits_zero() -> None:
    exit_code, payload, _ = await _invoke_preflight(mode="full")

    assert exit_code == 0
    assert payload["gate_status"] == "warn"
    assert payload["blocking_reason_codes"] == []
    assert payload["warning_reason_codes"] == ["delivery_gate_operator_review_required"]
    assert payload["operator_review_required"] is True
    assert payload["operator_review_passed"] is None


@pytest.mark.asyncio
async def test_operator_review_passed_clears_full_warning_when_metrics_are_healthy() -> None:
    exit_code, payload, _ = await _invoke_preflight(mode="full", operator_review_passed=True)

    assert exit_code == 0
    assert payload["gate_status"] == "pass"
    assert payload["warning_reason_codes"] == []
    assert payload["operator_review_passed"] is True


@pytest.mark.asyncio
async def test_unsupported_mode_fails_closed_with_stable_reason_without_config_load() -> None:
    def fail_config():
        raise AssertionError("config must not load for unsupported mode")

    exit_code, payload, _ = await _invoke_preflight(mode="unbounded", load_config=fail_config)

    assert exit_code == 1
    assert payload["mode"] is None
    assert payload["gate_status"] == "fail"
    assert payload["blocking_reason_codes"] == ["delivery_gate_preflight_unsupported_mode"]


@pytest.mark.asyncio
async def test_unsupported_output_format_fails_closed_with_stable_reason_without_config_load() -> None:
    def fail_config():
        raise AssertionError("config must not load for unsupported output")

    exit_code, payload, _ = await _invoke_preflight(output="yaml", load_config=fail_config)

    assert exit_code == 1
    assert payload["gate_status"] == "fail"
    assert payload["blocking_reason_codes"] == ["delivery_gate_preflight_output_format_unsupported"]


@pytest.mark.asyncio
async def test_config_load_failure_is_sanitized() -> None:
    def fail_config():
        raise RuntimeError(f"raw exception {RAW_DATABASE_URL} {RAW_REDIS_URL} {TELEGRAM_TOKEN}")

    async def fail_report(config, mode, operator_review_passed):
        raise AssertionError("snapshot must not load after config failure")

    exit_code, payload, output = await _invoke_preflight(load_config=fail_config, load_report=fail_report)

    assert exit_code == 1
    assert payload["blocking_reason_codes"] == ["delivery_gate_preflight_config_load_failed"]
    _assert_no_sensitive_output(output)


@pytest.mark.asyncio
async def test_snapshot_load_failure_is_sanitized() -> None:
    async def fail_report(config, mode, operator_review_passed):
        raise RuntimeError(f"raw exception {RAW_DATABASE_URL} {OPENAI_KEY} {GITHUB_TOKEN}")

    exit_code, payload, output = await _invoke_preflight(
        config=replace(_config(), database_url=RAW_DATABASE_URL, redis_url=RAW_REDIS_URL),
        load_report=fail_report,
    )

    assert exit_code == 1
    assert payload["blocking_reason_codes"] == ["delivery_gate_preflight_snapshot_load_failed"]
    _assert_no_sensitive_output(output)


@pytest.mark.asyncio
async def test_output_omits_database_urls_tokens_passwords_and_tracebacks() -> None:
    exit_code, payload, output = await _invoke_preflight(
        config=replace(_config(), database_url=RAW_DATABASE_URL, redis_url=RAW_REDIS_URL),
    )

    assert exit_code == 0
    assert payload["gate_status"] == "pass"
    _assert_no_sensitive_output(output)


@pytest.mark.asyncio
async def test_recommended_flag_patch_contains_exactly_three_allowed_keys() -> None:
    pass_code, pass_payload, _ = await _invoke_preflight()
    fail_code, fail_payload, _ = await _invoke_preflight(snapshot=_snapshot(open_delivery_dlq_count=1))

    assert pass_code == 0
    assert fail_code == 2
    _assert_patch_keys(pass_payload)
    _assert_patch_keys(fail_payload)


@pytest.mark.asyncio
async def test_authority_booleans_are_all_false() -> None:
    exit_code, payload, _ = await _invoke_preflight()

    assert exit_code == 0
    assert payload["authority"] == AUTHORITY
    assert all(value is False for value in payload["authority"].values())


@pytest.mark.asyncio
async def test_preflight_report_loader_calls_canonical_delivery_gate(monkeypatch) -> None:
    calls: list[tuple[object, object, str, bool | None]] = []
    expected_report = await load_delivery_gate_preflight_report(
        _config(),
        FakeGateRepository(_snapshot()),
        mode="restricted",
    )

    class FakeDeliveryGate:
        def __init__(self, config, *, repository) -> None:
            self.config = config
            self.repository = repository

        async def run(self, *, mode, operator_review_passed=None):
            calls.append((self.config, self.repository, mode, operator_review_passed))
            return expected_report

    config = _config()
    repository = FakeGateRepository(_snapshot())
    monkeypatch.setattr(preflight, "DeliveryGate", FakeDeliveryGate)

    report = await load_delivery_gate_preflight_report(
        config,
        repository,
        mode="restricted",
        operator_review_passed=True,
    )

    assert report is expected_report
    assert calls == [(config, repository, "restricted", True)]


def test_no_public_preflight_method_applies_flags() -> None:
    tree = ast.parse(PREFLIGHT_SOURCE.read_text(encoding="utf-8"))
    public_functions = {
        node.name
        for node in ast.walk(tree)
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and not node.name.startswith("_")
    }

    assert public_functions == {"load_delivery_gate_preflight_report", "run_delivery_gate_preflight"}
    assert all("apply" not in name and "flag" not in name for name in public_functions)


def test_source_import_check_blocks_runtime_network_and_worker_dependencies() -> None:
    tree = ast.parse(PREFLIGHT_SOURCE.read_text(encoding="utf-8"))
    banned_roots = {"redis", "openai", "telegram", "docker", "systemd", "requests", "httpx", "aiohttp"}
    imported_roots: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imported_roots.update(alias.name.split(".", 1)[0] for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            imported_roots.add(node.module.split(".", 1)[0])

    assert imported_roots.isdisjoint(banned_roots)


def test_preflight_source_contains_no_ddl_or_mutation_strings() -> None:
    text = PREFLIGHT_SOURCE.read_text(encoding="utf-8").lower()
    banned_fragments = [
        "create table",
        "alter table",
        "drop table",
        "truncate",
        "insert into",
        "update ",
        "delete from",
    ]

    for fragment in banned_fragments:
        assert fragment not in text


def test_parser_accepts_delivery_gate_preflight_json_command() -> None:
    args = maintenance_main.build_parser().parse_args(
        ["delivery-gate-preflight", "--mode", "restricted", "--output", "json"]
    )

    assert args.command == "delivery-gate-preflight"
    assert args.mode == "restricted"
    assert args.output == "json"
    assert args.operator_review_passed is False


def test_parser_accepts_operator_review_passed_flag_and_unsupported_values_for_json_fail_closed() -> None:
    args = maintenance_main.build_parser().parse_args(
        [
            "delivery-gate-preflight",
            "--mode",
            "unbounded",
            "--operator-review-passed",
            "--output",
            "yaml",
        ]
    )

    assert args.command == "delivery-gate-preflight"
    assert args.mode == "unbounded"
    assert args.output == "yaml"
    assert args.operator_review_passed is True


@pytest.mark.asyncio
async def test_cli_entrypoint_dispatches_preflight_before_runtime_config_load(monkeypatch) -> None:
    calls: list[str] = []

    async def fake_preflight(args):
        calls.append(args.command)
        return 0

    def fail_from_env(cls):
        raise AssertionError("preflight dispatch must own config load")

    monkeypatch.setattr(maintenance_main.MaintenanceConfig, "from_env", classmethod(fail_from_env))
    monkeypatch.setattr(maintenance_main, "_run_delivery_gate_preflight", fake_preflight)

    exit_code = await maintenance_main._run(["delivery-gate-preflight", "--mode", "restricted", "--output", "json"])

    assert exit_code == 0
    assert calls == ["delivery-gate-preflight"]


def test_delivery_gate_preflight_config_loader_does_not_require_redis_env(monkeypatch) -> None:
    for key in maintenance_main.ONE_SHOT_RUNTIME_ENV_KEYS:
        monkeypatch.delenv(key, raising=False)
    monkeypatch.setenv("DATABASE_URL", "opaque-local-test-database-source")
    monkeypatch.setenv("ENABLE_NOTIFICATION_SEND", "true")
    monkeypatch.setenv("NOTIFIER_TELEGRAM_DRY_RUN", "false")
    monkeypatch.setenv("MAINTENANCE_ENABLE_NOTIFICATION_RETRY_PROMOTION", "true")

    args = maintenance_main.build_parser().parse_args(
        ["delivery-gate-preflight", "--mode", "restricted", "--output", "json"]
    )
    config = maintenance_main._load_delivery_gate_preflight_config(args)

    assert config.database_url == "opaque-local-test-database-source"
    assert config.redis_url == "redis://127.0.0.1:6379/0"
