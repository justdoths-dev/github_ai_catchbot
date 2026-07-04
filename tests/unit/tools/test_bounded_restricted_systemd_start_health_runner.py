from __future__ import annotations

import ast
import json
from pathlib import Path
from typing import Any

from src.services.maintenance.systemd_rollout import (
    DIAGNOSTIC_SCHEMA_VERSION,
    SCHEMA_VERSION as SYSTEMD_ROLLOUT_SCHEMA_VERSION,
    SERVICE_NAME,
    SystemdDiagnosticReport,
    SystemdRolloutReport,
)
from tools import bounded_restricted_systemd_install_context_runner as install_runner
from tools import bounded_restricted_systemd_start_health_runner as runner


ROOT = Path(__file__).resolve().parents[3]
TOOL_PATH = ROOT / "tools/bounded_restricted_systemd_start_health_runner.py"
UNSAFE_STDERR = "raw " + "stderr sentinel"
UNSAFE_EXCEPTION_BODY = "raw " + "exception body sentinel"
UNSAFE_INVOCATION_ID = "0123456789abcdef" + "0123456789abcdef"
UNSAFE_INVOCATION_KEY = "Invocation" + "ID="
EXEC_START_KEY = "Exec" + "Start="
WORKING_DIRECTORY_KEY = "Working" + "Directory="
ENVIRONMENT_FILE_KEY = "Environment" + "File="
PRIVATE_REPO_PATH = "/private" + "/repo"


def _paths(tmp_path: Path) -> dict[str, Path]:
    repo_root = tmp_path / "repo-secret-sentinel"
    bootstrap = repo_root / "src/services/maintenance/worker_bootstrap.py"
    bootstrap.parent.mkdir(parents=True)
    bootstrap.write_text("raise SystemExit(0)\n", encoding="utf-8")
    python_executable = tmp_path / "python-secret-sentinel"
    python_executable.write_text("#!/bin/sh\n", encoding="utf-8")
    runtime_env_file = tmp_path / "runtime-secret-sentinel.env"
    runtime_env_file.write_text("placeholder-runtime-env-line\n", encoding="utf-8")
    systemd_user_dir = tmp_path / "systemd-secret-sentinel"
    systemd_user_dir.mkdir()
    return {
        "repo_root": repo_root.resolve(),
        "python_executable": python_executable.resolve(),
        "runtime_env_file": runtime_env_file.resolve(),
        "systemd_user_dir": systemd_user_dir.resolve(),
    }


def _argv(paths: dict[str, Path], mode: str) -> list[str]:
    return [
        "--mode",
        mode,
        "--repo-root",
        str(paths["repo_root"]),
        "--python-executable",
        str(paths["python_executable"]),
        "--runtime-env-file",
        str(paths["runtime_env_file"]),
        "--systemd-user-dir",
        str(paths["systemd_user_dir"]),
    ]


def _rollout_report(
    *,
    mode: str,
    status: str = "pass",
    reason_code: str | None = None,
    start_attempted: bool = False,
    proof_attempted: bool = False,
    rollback_attempted: bool = False,
    service_active: bool = False,
) -> SystemdRolloutReport:
    return SystemdRolloutReport(
        schema_version=SYSTEMD_ROLLOUT_SCHEMA_VERSION,
        mode=mode,
        target="maintenance-worker",
        status=status,
        reason_code=reason_code,
        service_name=SERVICE_NAME,
        timer_name=None,
        unit_plan_created=True,
        install_attempted=False,
        start_attempted=start_attempted,
        enable_attempted=False,
        rollback_attempted=rollback_attempted,
        proof_attempted=proof_attempted,
        service_file_present=mode != "rollback",
        timer_file_present=False,
        service_enabled=mode in {"start", "proof"},
        service_active=service_active,
        rollback_plan_available=mode != "rollback",
        redactions_applied={"runtime_env_path_omitted": True},
    )


def _diagnostic_report() -> SystemdDiagnosticReport:
    return SystemdDiagnosticReport(
        schema_version=DIAGNOSTIC_SCHEMA_VERSION,
        target="maintenance-worker",
        service_name=SERVICE_NAME,
        status="pass",
        reason_code=None,
        service_file_present=True,
        service_enabled=True,
        service_active=True,
        load_state="loaded",
        active_state="active",
        sub_state="running",
        result="success",
        exec_main_code=0,
        exec_main_status=0,
        n_restarts=0,
        unit_file_state="enabled",
        current_invocation_fingerprint="abcdef0123456789",
        restart_likely=False,
        exited_after_start_likely=False,
    )


def _parse_output(capsys: Any) -> dict[str, Any]:
    return json.loads(capsys.readouterr().out)


def _fail_rollout(_request):
    raise AssertionError("run_systemd_rollout must not be called")


def _fail_diagnostic(_request):
    raise AssertionError("run_systemd_diagnostic must not be called")


def test_start_requires_both_confirmation_flags(tmp_path: Path, capsys, monkeypatch) -> None:
    paths = _paths(tmp_path)
    monkeypatch.setattr(runner, "run_systemd_rollout", _fail_rollout)

    for extra in (
        [],
        ["--confirm-start"],
        ["--i-understand-this-starts-maintenance-worker"],
    ):
        assert runner.main([*_argv(paths, "start"), *extra]) == 1
        parsed = _parse_output(capsys)
        assert parsed["reason_code"] == "start_confirmation_flags_missing"
        assert parsed["authority"]["systemd_start_attempted"] is False
        assert parsed["authority"]["worker_process_start_attempted"] is False
        assert parsed["authority"]["downstream_runtime_authority_may_open"] is False


def test_start_rejects_rollback_confirmation_flags(tmp_path: Path, capsys, monkeypatch) -> None:
    paths = _paths(tmp_path)
    monkeypatch.setattr(runner, "run_systemd_rollout", _fail_rollout)

    assert (
        runner.main(
            [
                *_argv(paths, "start"),
                "--confirm-start",
                "--i-understand-this-starts-maintenance-worker",
                "--confirm-rollback",
            ]
        )
        == 1
    )
    parsed = _parse_output(capsys)

    assert parsed["reason_code"] == "rollback_confirmation_not_allowed_for_start_mode"
    assert parsed["systemd_start_or_health_readback"]["lower_status"] == "not_dispatched"
    assert parsed["authority"]["systemd_start_attempted"] is False


def test_start_with_both_confirmations_delegates_to_existing_rollout_path(
    tmp_path: Path,
    capsys,
    monkeypatch,
) -> None:
    paths = _paths(tmp_path)
    captured = {}

    def fake_rollout(request):
        captured["mode"] = request.mode
        captured["confirm_start"] = request.confirm_start
        captured["confirm_install"] = request.confirm_install
        captured["confirm_rollback"] = request.confirm_rollback
        captured["dry_run"] = request.dry_run
        captured["target"] = request.target
        captured["service_name"] = request.service_name
        captured["timer_name"] = request.timer_name
        return _rollout_report(mode="start", start_attempted=True, service_active=True)

    monkeypatch.setattr(runner, "run_systemd_rollout", fake_rollout)

    assert (
        runner.main(
            [
                *_argv(paths, "start"),
                "--confirm-start",
                "--i-understand-this-starts-maintenance-worker",
            ]
        )
        == 0
    )
    parsed = _parse_output(capsys)

    assert captured == {
        "mode": "start",
        "confirm_start": True,
        "confirm_install": False,
        "confirm_rollback": False,
        "dry_run": False,
        "target": "maintenance-worker",
        "service_name": SERVICE_NAME,
        "timer_name": None,
    }
    assert parsed["systemd_start_or_health_readback"]["lower_report_kind"] == "rollout"
    assert parsed["systemd_start_or_health_readback"]["start_attempted_by_rollout_service"] is True
    assert parsed["authority"]["systemd_start_attempted"] is True
    assert parsed["authority"]["worker_process_start_attempted"] is True
    assert parsed["authority"]["worker_process_active_after_stability_readback"] is True
    assert parsed["authority"]["downstream_runtime_authority_may_open"] is True
    _assert_direct_runner_clients_false(parsed["authority"])
    _assert_output_redacted(json.dumps(parsed, sort_keys=True), paths)


def test_proof_delegates_to_existing_rollout_path_as_read_only_health_readback(
    tmp_path: Path,
    capsys,
    monkeypatch,
) -> None:
    paths = _paths(tmp_path)
    captured = {}

    def fake_rollout(request):
        captured["mode"] = request.mode
        captured["confirm_start"] = request.confirm_start
        captured["confirm_install"] = request.confirm_install
        captured["confirm_rollback"] = request.confirm_rollback
        captured["dry_run"] = request.dry_run
        return _rollout_report(mode="proof", proof_attempted=True, service_active=True)

    monkeypatch.setattr(runner, "run_systemd_rollout", fake_rollout)

    assert runner.main(_argv(paths, "proof")) == 0
    parsed = _parse_output(capsys)

    assert captured == {
        "mode": "proof",
        "confirm_start": False,
        "confirm_install": False,
        "confirm_rollback": False,
        "dry_run": True,
    }
    assert parsed["systemd_start_or_health_readback"]["proof_attempted_by_rollout_service"] is True
    assert parsed["authority"]["systemd_start_attempted"] is False
    assert parsed["authority"]["downstream_runtime_authority_may_open"] is False


def test_diagnose_delegates_to_existing_diagnostic_path(tmp_path: Path, capsys, monkeypatch) -> None:
    paths = _paths(tmp_path)
    captured = {}

    def fake_diagnostic(request):
        captured["target"] = request.target
        captured["runtime_env_file"] = request.runtime_env_file
        captured["systemd_user_dir"] = request.systemd_user_dir
        captured["service_name"] = request.service_name
        return _diagnostic_report()

    monkeypatch.setattr(runner, "run_systemd_diagnostic", fake_diagnostic)

    assert runner.main(_argv(paths, "diagnose")) == 0
    parsed = _parse_output(capsys)

    assert captured == {
        "target": "maintenance-worker",
        "runtime_env_file": paths["runtime_env_file"],
        "systemd_user_dir": paths["systemd_user_dir"],
        "service_name": SERVICE_NAME,
    }
    assert parsed["systemd_start_or_health_readback"]["lower_report_kind"] == "diagnostic"
    assert parsed["systemd_start_or_health_readback"]["current_invocation_fingerprint_present"] is True
    assert "abcdef0123456789" not in json.dumps(parsed, sort_keys=True)
    assert parsed["authority"]["runtime_env_values_read_by_runner"] is False


def test_rollback_requires_both_confirmations_and_delegates_to_existing_rollout_path(
    tmp_path: Path,
    capsys,
    monkeypatch,
) -> None:
    paths = _paths(tmp_path)
    monkeypatch.setattr(runner, "run_systemd_rollout", _fail_rollout)

    assert runner.main(_argv(paths, "rollback")) == 1
    parsed = _parse_output(capsys)
    assert parsed["reason_code"] == "rollback_confirmation_flags_missing"

    assert runner.main([*_argv(paths, "rollback"), "--confirm-rollback"]) == 1
    parsed = _parse_output(capsys)
    assert parsed["reason_code"] == "rollback_confirmation_flags_missing"

    captured = {}

    def fake_rollout(request):
        captured["mode"] = request.mode
        captured["confirm_start"] = request.confirm_start
        captured["confirm_install"] = request.confirm_install
        captured["confirm_rollback"] = request.confirm_rollback
        captured["dry_run"] = request.dry_run
        return _rollout_report(mode="rollback", rollback_attempted=True, service_active=False)

    monkeypatch.setattr(runner, "run_systemd_rollout", fake_rollout)

    assert (
        runner.main(
            [
                *_argv(paths, "rollback"),
                "--confirm-rollback",
                "--i-understand-this-stops-and-removes-user-systemd-unit",
            ]
        )
        == 0
    )
    parsed = _parse_output(capsys)

    assert captured == {
        "mode": "rollback",
        "confirm_start": False,
        "confirm_install": False,
        "confirm_rollback": True,
        "dry_run": False,
    }
    assert parsed["systemd_start_or_health_readback"]["rollback_attempted_by_rollout_service"] is True
    assert parsed["authority"]["systemd_stop_attempted"] is True
    assert parsed["authority"]["systemd_disable_attempted"] is True
    assert parsed["authority"]["unit_file_remove_attempted"] is True
    assert parsed["authority"]["daemon_reload_attempted"] is True
    assert parsed["authority"]["systemd_start_attempted"] is False


def test_rollback_rejects_start_confirmation_flags(tmp_path: Path, capsys, monkeypatch) -> None:
    paths = _paths(tmp_path)
    monkeypatch.setattr(runner, "run_systemd_rollout", _fail_rollout)

    assert (
        runner.main(
            [
                *_argv(paths, "rollback"),
                "--confirm-rollback",
                "--i-understand-this-stops-and-removes-user-systemd-unit",
                "--confirm-start",
            ]
        )
        == 1
    )
    parsed = _parse_output(capsys)

    assert parsed["reason_code"] == "start_confirmation_not_allowed_for_rollback_mode"
    assert parsed["authority"]["unit_file_remove_attempted"] is False


def test_read_only_modes_reject_all_confirmation_flags(tmp_path: Path, capsys, monkeypatch) -> None:
    paths = _paths(tmp_path)
    monkeypatch.setattr(runner, "run_systemd_rollout", _fail_rollout)
    monkeypatch.setattr(runner, "run_systemd_diagnostic", _fail_diagnostic)

    for mode in ("proof", "diagnose"):
        for flag in (
            "--confirm-start",
            "--i-understand-this-starts-maintenance-worker",
            "--confirm-rollback",
            "--i-understand-this-stops-and-removes-user-systemd-unit",
        ):
            assert runner.main([*_argv(paths, mode), flag]) == 1
            parsed = _parse_output(capsys)
            assert parsed["mode"] == mode
            assert parsed["reason_code"] == "confirmation_not_allowed_for_read_only_mode"
            assert parsed["systemd_start_or_health_readback"]["lower_status"] == "not_dispatched"
            assert parsed["authority"]["systemd_start_attempted"] is False
            assert parsed["authority"]["unit_file_remove_attempted"] is False


def test_unsupported_modes_reject_with_sanitized_compact_json_only(
    tmp_path: Path,
    capsys,
    monkeypatch,
) -> None:
    paths = _paths(tmp_path)
    monkeypatch.setattr(runner, "run_systemd_rollout", _fail_rollout)
    monkeypatch.setattr(runner, "run_systemd_diagnostic", _fail_diagnostic)

    for mode in ("install", "plan", "context-proof", "collector", "worker", "queue"):
        assert runner.main(_argv(paths, mode)) == 1
        captured = capsys.readouterr()
        parsed = json.loads(captured.out)
        assert captured.err == ""
        assert captured.out.count("\n") == 1
        assert "usage:" not in captured.out
        assert parsed["mode"] == mode
        assert parsed["reason_code"] == "mode_not_allowed"
        assert parsed["authority"]["runtime_env_values_read_by_runner"] is False
        _assert_output_redacted(captured.out, paths)


def test_outputs_redact_paths_unit_content_stderr_exception_body_and_invocation_id(
    tmp_path: Path,
    capsys,
    monkeypatch,
) -> None:
    paths = _paths(tmp_path)
    unsafe_reason = f"{UNSAFE_STDERR} {PRIVATE_REPO_PATH} {EXEC_START_KEY}secret"

    monkeypatch.setattr(
        runner,
        "run_systemd_rollout",
        lambda _request: _rollout_report(
            mode="start",
            status="blocked",
            reason_code=unsafe_reason,
            start_attempted=True,
            service_active=False,
        ),
    )

    assert (
        runner.main(
            [
                *_argv(paths, "start"),
                "--confirm-start",
                "--i-understand-this-starts-maintenance-worker",
            ]
        )
        == 1
    )
    output = capsys.readouterr().out
    _assert_output_redacted(output, paths)
    _assert_forbidden_runtime_material_absent(output)

    def fake_raise(_request):
        raise RuntimeError(
            f"{UNSAFE_EXCEPTION_BODY} {PRIVATE_REPO_PATH} "
            f"{UNSAFE_INVOCATION_KEY}{UNSAFE_INVOCATION_ID}"
        )

    monkeypatch.setattr(runner, "run_systemd_rollout", fake_raise)

    assert (
        runner.main(
            [
                *_argv(paths, "start"),
                "--confirm-start",
                "--i-understand-this-starts-maintenance-worker",
            ]
        )
        == 1
    )
    output = capsys.readouterr().out
    _assert_output_redacted(output, paths)
    _assert_forbidden_runtime_material_absent(output)


def test_relative_paths_reject_before_dispatch(tmp_path: Path, capsys, monkeypatch) -> None:
    paths = _paths(tmp_path)
    monkeypatch.setattr(runner, "run_systemd_rollout", _fail_rollout)
    argv = _argv(paths, "proof")
    argv[argv.index("--repo-root") + 1] = "relative/repo"

    assert runner.main(argv) == 1
    parsed = _parse_output(capsys)

    assert parsed["reason_code"] == "repo_root_not_absolute"
    assert parsed["systemd_start_or_health_readback"]["lower_status"] == "not_dispatched"


def test_static_ast_guard_rejects_forbidden_imports_calls_and_runtime_clients() -> None:
    tree = ast.parse(TOOL_PATH.read_text(encoding="utf-8"))
    imported_roots: set[str] = set()
    call_names: set[str] = set()
    names: set[str] = set()

    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imported_roots.update(alias.name.split(".", 1)[0] for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            imported_roots.add(node.module.split(".", 1)[0])
        elif isinstance(node, ast.Call):
            if isinstance(node.func, ast.Name):
                call_names.add(node.func.id)
            elif isinstance(node.func, ast.Attribute):
                call_names.add(node.func.attr)
        elif isinstance(node, ast.Name):
            names.add(node.id)

    assert {
        "redis",
        "openai",
        "telegram",
        "docker",
        "requests",
        "httpx",
        "aiohttp",
        "urllib",
        "subprocess",
    }.isdisjoint(imported_roots)
    assert {
        "create_engine",
        "create_async_engine",
        "async_sessionmaker",
        "sessionmaker",
        "xadd",
        "xack",
        "xgroup_create",
        "xreadgroup",
        "send_message",
        "edit_message_text",
    }.isdisjoint(call_names | names)


def test_existing_install_context_runner_still_rejects_start(tmp_path: Path, capsys) -> None:
    paths = _paths(tmp_path)

    assert install_runner.main(_argv(paths, "start")) == 1
    parsed = _parse_output(capsys)

    assert parsed["runner_name"] == "bounded_restricted_systemd_install_context_runner"
    assert parsed["reason_code"] == "mode_not_allowed"
    assert parsed["authority"]["systemd_start_attempted"] is False
    assert parsed["authority"]["worker_started"] is False


def _assert_direct_runner_clients_false(authority: dict[str, Any]) -> None:
    for field in (
        "direct_runner_db_client_constructed",
        "direct_runner_redis_client_constructed",
        "direct_runner_telegram_client_constructed",
        "direct_runner_openai_client_constructed",
        "direct_runner_github_client_constructed",
        "direct_runner_x_client_constructed",
        "direct_runner_web_client_constructed",
    ):
        assert authority[field] is False
    assert authority["runtime_env_values_read_by_runner"] is False
    assert authority["secrets_output"] is False


def _assert_output_redacted(output: str, paths: dict[str, Path]) -> None:
    for forbidden in (
        str(paths["repo_root"]),
        str(paths["python_executable"]),
        str(paths["runtime_env_file"]),
        str(paths["systemd_user_dir"]),
        "repo-secret-sentinel",
        "python-secret-sentinel",
        "runtime-secret-sentinel.env",
        "systemd-secret-sentinel",
    ):
        assert forbidden not in output


def _assert_forbidden_runtime_material_absent(output: str) -> None:
    for forbidden in (
        "placeholder-runtime-env-line",
        EXEC_START_KEY,
        WORKING_DIRECTORY_KEY,
        ENVIRONMENT_FILE_KEY,
        UNSAFE_STDERR,
        UNSAFE_EXCEPTION_BODY,
        UNSAFE_INVOCATION_ID,
        UNSAFE_INVOCATION_KEY,
        PRIVATE_REPO_PATH,
    ):
        assert forbidden not in output
