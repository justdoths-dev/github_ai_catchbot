from __future__ import annotations

import ast
import json
from pathlib import Path

import pytest

from services.maintenance import main as maintenance_main
from services.maintenance.delivery_gate_preflight import (
    AUTHORITY as PREFLIGHT_AUTHORITY,
    RECOMMENDED_FLAG_PATCH_KEYS,
    SCHEMA_VERSION as PREFLIGHT_SCHEMA_VERSION,
)
from services.maintenance.delivery_gate_preflight_invocation import (
    AUTHORITY,
    CapturedPreflightInvocation,
    run_delivery_gate_preflight_invocation_proof,
)


ROOT = Path(__file__).resolve().parents[4]
SOURCE = ROOT / "src" / "services" / "maintenance" / "delivery_gate_preflight_invocation.py"
SENSITIVE_SENTINELS = [
    "DATABASE_URL",
    "REDIS_URL",
    "TELEGRAM_BOT_TOKEN",
    "OPENAI_API_KEY",
    "GITHUB_TOKEN",
    "password",
    "Traceback",
    "RuntimeError",
    "raw exception",
]


def _preflight_report(
    *,
    mode: str = "restricted",
    gate_status: str = "pass",
    schema_version: str = PREFLIGHT_SCHEMA_VERSION,
    authority: dict[str, bool] | None = None,
    recommended_flag_patch: dict[str, object] | None = None,
    blocking_reason_codes: list[str] | None = None,
    warning_reason_codes: list[str] | None = None,
) -> dict[str, object]:
    return {
        "schema_version": schema_version,
        "mode": mode,
        "gate_status": gate_status,
        "blocking_reason_codes": blocking_reason_codes or [],
        "warning_reason_codes": warning_reason_codes or [],
        "metrics": [],
        "operator_review_required": mode == "full",
        "operator_review_passed": None,
        "recommended_flag_patch": recommended_flag_patch
        or {
            "ENABLE_NOTIFICATION_SEND": gate_status == "pass",
            "MAINTENANCE_ENABLE_NOTIFICATION_RETRY_PROMOTION": gate_status == "pass",
            "NOTIFIER_TELEGRAM_DRY_RUN": False,
        },
        "authority": dict(PREFLIGHT_AUTHORITY if authority is None else authority),
    }


def _json_report(report: dict[str, object]) -> str:
    return json.dumps(report, ensure_ascii=False, sort_keys=True)


def _invoker(
    *,
    report: dict[str, object] | None = None,
    stdout: str | None = None,
    stderr: str = "",
    exit_code: int | None = 0,
    command_raised: bool = False,
    calls: list[list[str]] | None = None,
):
    async def invoke(argv: list[str]) -> CapturedPreflightInvocation:
        if calls is not None:
            calls.append(argv)
        return CapturedPreflightInvocation(
            exit_code=exit_code,
            stdout=_json_report(report or _preflight_report()) if stdout is None else stdout,
            stderr=stderr,
            command_raised=command_raised,
        )

    return invoke


async def _run_proof(
    *,
    mode: str = "restricted",
    output: str = "json",
    require_gate_status: str | None = None,
    operator_review_passed: bool = False,
    invoke_preflight=None,
) -> tuple[int, dict[str, object], str]:
    emitted: list[str] = []
    exit_code = await run_delivery_gate_preflight_invocation_proof(
        mode=mode,
        output=output,
        require_gate_status=require_gate_status,
        operator_review_passed=operator_review_passed,
        invoke_preflight=invoke_preflight or _invoker(),
        emit_json=emitted.append,
    )
    assert len(emitted) == 1
    return exit_code, json.loads(emitted[0]), emitted[0]


@pytest.mark.asyncio
async def test_restricted_invocation_proof_accepts_valid_preflight_pass() -> None:
    exit_code, payload, _ = await _run_proof(invoke_preflight=_invoker(report=_preflight_report()))

    assert exit_code == 0
    assert payload["schema_version"] == "delivery_gate_preflight_invocation_proof_v1"
    assert payload["proof_status"] == "pass"
    assert payload["proof_reason_codes"] == []
    assert payload["mode"] == "restricted"
    assert payload["preflight_exit_code"] == 0
    assert payload["preflight_gate_status"] == "pass"
    assert payload["preflight_json_valid"] is True
    assert payload["preflight_authority_all_false"] is True
    assert payload["preflight_recommended_flag_patch_keys_valid"] is True
    assert payload["preflight_output_sanitized"] is True


@pytest.mark.asyncio
async def test_restricted_invocation_proof_accepts_valid_preflight_fail_by_default() -> None:
    report = _preflight_report(
        gate_status="fail",
        blocking_reason_codes=["delivery_gate_flag_send_disabled"],
    )

    exit_code, payload, _ = await _run_proof(invoke_preflight=_invoker(report=report, exit_code=2))

    assert exit_code == 0
    assert payload["proof_status"] == "pass"
    assert payload["preflight_exit_code"] == 2
    assert payload["preflight_gate_status"] == "fail"
    assert payload["preflight_blocking_reason_codes"] == ["delivery_gate_flag_send_disabled"]


@pytest.mark.asyncio
async def test_full_invocation_proof_accepts_valid_preflight_warn() -> None:
    report = _preflight_report(
        mode="full",
        gate_status="warn",
        warning_reason_codes=["delivery_gate_operator_review_required"],
    )

    exit_code, payload, _ = await _run_proof(mode="full", invoke_preflight=_invoker(report=report))

    assert exit_code == 0
    assert payload["proof_status"] == "pass"
    assert payload["preflight_gate_status"] == "warn"
    assert payload["preflight_warning_reason_codes"] == ["delivery_gate_operator_review_required"]


@pytest.mark.asyncio
async def test_require_gate_status_fails_when_actual_gate_status_differs() -> None:
    report = _preflight_report(gate_status="fail")

    exit_code, payload, _ = await _run_proof(
        require_gate_status="pass",
        invoke_preflight=_invoker(report=report, exit_code=2),
    )

    assert exit_code == 1
    assert payload["proof_status"] == "fail"
    assert payload["proof_reason_codes"] == ["delivery_gate_preflight_invocation_gate_status_mismatch"]


@pytest.mark.asyncio
async def test_invalid_require_gate_status_fails_closed() -> None:
    calls: list[list[str]] = []

    exit_code, payload, _ = await _run_proof(
        require_gate_status="paas",
        invoke_preflight=_invoker(calls=calls),
    )

    assert exit_code == 1
    assert calls == []
    assert payload["proof_status"] == "fail"
    assert payload["proof_reason_codes"] == [
        "delivery_gate_preflight_invocation_unsupported_required_gate_status"
    ]
    assert payload["preflight_gate_status"] is None
    assert payload["preflight_report"] == {}


@pytest.mark.asyncio
async def test_invalid_non_json_preflight_output_fails_closed() -> None:
    exit_code, payload, _ = await _run_proof(invoke_preflight=_invoker(stdout="not json"))

    assert exit_code == 1
    assert payload["proof_reason_codes"] == ["delivery_gate_preflight_invocation_invalid_json"]
    assert payload["preflight_json_valid"] is False
    assert payload["preflight_report"] == {}


@pytest.mark.asyncio
async def test_schema_mismatch_fails_closed() -> None:
    report = _preflight_report(schema_version="wrong_schema")

    exit_code, payload, _ = await _run_proof(invoke_preflight=_invoker(report=report))

    assert exit_code == 1
    assert payload["proof_reason_codes"] == ["delivery_gate_preflight_invocation_schema_mismatch"]
    assert payload["preflight_schema_version"] == "wrong_schema"


@pytest.mark.asyncio
async def test_missing_authority_fails_closed() -> None:
    report = _preflight_report()
    report.pop("authority")

    exit_code, payload, _ = await _run_proof(invoke_preflight=_invoker(report=report))

    assert exit_code == 1
    assert payload["proof_reason_codes"] == ["delivery_gate_preflight_invocation_authority_missing"]
    assert payload["preflight_authority_all_false"] is False


@pytest.mark.asyncio
async def test_any_true_authority_boolean_fails_closed() -> None:
    authority = dict(PREFLIGHT_AUTHORITY)
    authority["telegram_called"] = True

    exit_code, payload, _ = await _run_proof(invoke_preflight=_invoker(report=_preflight_report(authority=authority)))

    assert exit_code == 1
    assert payload["proof_reason_codes"] == ["delivery_gate_preflight_invocation_authority_opened"]
    assert payload["preflight_authority_all_false"] is False


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "patch",
    [
        {
            "ENABLE_NOTIFICATION_SEND": True,
            "MAINTENANCE_ENABLE_NOTIFICATION_RETRY_PROMOTION": True,
        },
        {
            "ENABLE_NOTIFICATION_SEND": True,
            "MAINTENANCE_ENABLE_NOTIFICATION_RETRY_PROMOTION": True,
            "NOTIFIER_TELEGRAM_DRY_RUN": False,
            "EXTRA_FLAG": True,
        },
    ],
)
async def test_recommended_flag_patch_extra_or_missing_key_fails_closed(patch: dict[str, object]) -> None:
    report = _preflight_report(recommended_flag_patch=patch)

    exit_code, payload, _ = await _run_proof(invoke_preflight=_invoker(report=report))

    assert exit_code == 1
    assert payload["proof_reason_codes"] == ["delivery_gate_preflight_invocation_flag_patch_invalid"]
    assert payload["preflight_recommended_flag_patch_keys_valid"] is False


@pytest.mark.asyncio
@pytest.mark.parametrize("sentinel", SENSITIVE_SENTINELS)
async def test_sensitive_sentinel_output_fails_closed_without_echoing_secret(sentinel: str) -> None:
    report = _preflight_report()

    exit_code, payload, proof_output = await _run_proof(
        invoke_preflight=_invoker(report=report, stderr=f"leaked {sentinel} value"),
    )

    assert exit_code == 1
    assert payload["proof_reason_codes"] == ["delivery_gate_preflight_invocation_sensitive_output_detected"]
    assert payload["preflight_output_sanitized"] is False
    assert payload["preflight_report"] == {}
    assert sentinel not in proof_output


@pytest.mark.asyncio
async def test_command_exception_before_json_fails_closed_without_traceback() -> None:
    exit_code, payload, proof_output = await _run_proof(
        invoke_preflight=_invoker(stdout="", command_raised=True, exit_code=None),
    )

    assert exit_code == 1
    assert payload["proof_reason_codes"] == ["delivery_gate_preflight_invocation_command_failed_without_json"]
    assert payload["preflight_exit_code"] is None
    assert "Traceback" not in proof_output
    assert "RuntimeError" not in proof_output


@pytest.mark.asyncio
async def test_unsupported_mode_fails_closed_without_invoking_preflight() -> None:
    calls: list[list[str]] = []

    exit_code, payload, _ = await _run_proof(mode="unbounded", invoke_preflight=_invoker(calls=calls))

    assert exit_code == 1
    assert calls == []
    assert payload["proof_reason_codes"] == ["delivery_gate_preflight_invocation_unsupported_mode"]


@pytest.mark.asyncio
async def test_unsupported_output_fails_closed_without_invoking_preflight() -> None:
    calls: list[list[str]] = []

    exit_code, payload, _ = await _run_proof(output="yaml", invoke_preflight=_invoker(calls=calls))

    assert exit_code == 1
    assert calls == []
    assert payload["proof_reason_codes"] == ["delivery_gate_preflight_invocation_unsupported_output"]


@pytest.mark.asyncio
async def test_proof_output_contains_all_required_authority_booleans_false() -> None:
    _, payload, _ = await _run_proof()

    assert payload["authority"] == AUTHORITY
    assert all(value is False for value in payload["authority"].values())


@pytest.mark.asyncio
async def test_invocation_proof_output_is_deterministic_for_same_preflight_report() -> None:
    report = _preflight_report(gate_status="fail", blocking_reason_codes=["delivery_gate_flag_send_disabled"])

    first = await _run_proof(invoke_preflight=_invoker(report=report, exit_code=2))
    second = await _run_proof(invoke_preflight=_invoker(report=report, exit_code=2))

    assert first == second


@pytest.mark.asyncio
async def test_default_invoker_calls_existing_preflight_cli_path(monkeypatch) -> None:
    calls: list[list[str]] = []
    emitted: list[str] = []

    async def fake_run(argv):
        calls.append(argv)
        print(_json_report(_preflight_report()))
        return 0

    monkeypatch.setattr(maintenance_main, "_run", fake_run)

    exit_code = await run_delivery_gate_preflight_invocation_proof(
        mode="restricted",
        output="json",
        emit_json=emitted.append,
    )
    payload = json.loads(emitted[0])

    assert exit_code == 0
    assert payload["proof_status"] == "pass"
    assert calls == [["delivery-gate-preflight", "--mode", "restricted", "--output", "json"]]


def test_proof_source_does_not_duplicate_gate_metric_logic() -> None:
    text = SOURCE.read_text(encoding="utf-8")

    assert '"delivery-gate-preflight"' in text
    assert "success_rate_1h" not in text
    assert "open_delivery_dlq_count" not in text
    assert "DeliveryGate(" not in text
    assert "evaluate_delivery_gate" not in text
    assert "load_delivery_gate_snapshot" not in text


def test_no_public_invocation_method_applies_flags() -> None:
    tree = ast.parse(SOURCE.read_text(encoding="utf-8"))
    public_functions = {
        node.name
        for node in ast.walk(tree)
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and not node.name.startswith("_")
    }

    assert public_functions == {"run_delivery_gate_preflight_invocation_proof"}
    assert all("apply" not in name and "flag" not in name for name in public_functions)


def test_source_import_check_blocks_runtime_network_and_worker_dependencies() -> None:
    tree = ast.parse(SOURCE.read_text(encoding="utf-8"))
    banned_roots = {"redis", "openai", "telegram", "docker", "systemd", "requests", "httpx", "aiohttp"}
    imported_roots: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imported_roots.update(alias.name.split(".", 1)[0] for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            imported_roots.add(node.module.split(".", 1)[0])

    assert imported_roots.isdisjoint(banned_roots)


def test_proof_source_contains_no_ddl_or_mutation_strings() -> None:
    text = SOURCE.read_text(encoding="utf-8").lower()
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


def test_parser_accepts_delivery_gate_preflight_invocation_proof_command() -> None:
    args = maintenance_main.build_parser().parse_args(
        [
            "delivery-gate-preflight-invocation-proof",
            "--mode",
            "restricted",
            "--output",
            "json",
            "--require-gate-status",
            "pass",
        ]
    )

    assert args.command == "delivery-gate-preflight-invocation-proof"
    assert args.mode == "restricted"
    assert args.output == "json"
    assert args.require_gate_status == "pass"
    assert args.operator_review_passed is False


def test_parser_rejects_invalid_require_gate_status() -> None:
    with pytest.raises(SystemExit):
        maintenance_main.build_parser().parse_args(
            [
                "delivery-gate-preflight-invocation-proof",
                "--mode",
                "restricted",
                "--output",
                "json",
                "--require-gate-status",
                "paas",
            ]
        )


@pytest.mark.asyncio
async def test_cli_entrypoint_dispatches_invocation_proof_before_runtime_config_load(monkeypatch) -> None:
    calls: list[tuple[str, str, str | None, bool]] = []

    async def fake_invocation_proof(*, mode, output, require_gate_status=None, operator_review_passed=False):
        calls.append((mode, output, require_gate_status, operator_review_passed))
        return 0

    def fail_from_env(cls):
        raise AssertionError("invocation proof dispatch must own nested preflight config load")

    monkeypatch.setattr(maintenance_main.MaintenanceConfig, "from_env", classmethod(fail_from_env))
    monkeypatch.setattr(maintenance_main, "run_delivery_gate_preflight_invocation_proof", fake_invocation_proof)

    exit_code = await maintenance_main._run(
        [
            "delivery-gate-preflight-invocation-proof",
            "--mode",
            "full",
            "--output",
            "json",
            "--operator-review-passed",
            "--require-gate-status",
            "warn",
        ]
    )

    assert exit_code == 0
    assert calls == [("full", "json", "warn", True)]


def test_expected_recommended_flag_patch_key_order_matches_preflight_contract() -> None:
    assert tuple(_preflight_report()["recommended_flag_patch"]) == RECOMMENDED_FLAG_PATCH_KEYS
