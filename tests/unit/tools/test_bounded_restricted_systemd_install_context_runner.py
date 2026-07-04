from __future__ import annotations

import ast
import json
from pathlib import Path
from typing import Any

from src.services.maintenance.systemd_rollout import (
    CONTEXT_PROOF_SCHEMA_VERSION,
    DIAGNOSTIC_SCHEMA_VERSION,
    SCHEMA_VERSION as SYSTEMD_ROLLOUT_SCHEMA_VERSION,
    SERVICE_NAME,
    SystemdContextProofReport,
    SystemdDiagnosticReport,
    SystemdRolloutReport,
)
from tools import bounded_restricted_systemd_install_context_runner as runner


ROOT = Path(__file__).resolve().parents[3]
TOOL_PATH = ROOT / "tools/bounded_restricted_systemd_install_context_runner.py"


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
    install_attempted: bool = False,
    enable_attempted: bool = False,
    rollback_attempted: bool = False,
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
        install_attempted=install_attempted,
        start_attempted=False,
        enable_attempted=enable_attempted,
        rollback_attempted=rollback_attempted,
        proof_attempted=False,
        service_file_present=mode != "rollback",
        timer_file_present=False,
        service_enabled=mode == "install",
        service_active=False,
        rollback_plan_available=mode != "rollback",
        redactions_applied={"runtime_env_path_omitted": True},
    )


def _context_report() -> SystemdContextProofReport:
    return SystemdContextProofReport(
        schema_version=CONTEXT_PROOF_SCHEMA_VERSION,
        mode="context-proof",
        target="maintenance-worker",
        status="pass",
        reason_code=None,
        service_name_matches_expected=True,
        service_file_present=True,
        service_enabled=True,
        service_active=False,
        unit_load_state="loaded",
        unit_file_state="enabled",
        exec_start_matches_expected=True,
        working_directory_matches_expected=True,
        environment_file_matches_expected=True,
        restart_policy_matches_expected=True,
        restart_sec_matches_expected=True,
        expected_python_executable_present=True,
        expected_repo_root_present=True,
        expected_runtime_env_file_present=True,
        unit_context_matches_expected=True,
        mismatched_context_fields=[],
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


def test_plan_mode_succeeds_with_existing_rollout_path_and_sanitized_json(tmp_path: Path, capsys) -> None:
    paths = _paths(tmp_path)

    exit_code = runner.main(_argv(paths, "plan"))
    output = capsys.readouterr().out
    parsed = json.loads(output)

    assert exit_code == 0
    assert parsed["schema_version"] == "restricted_systemd_install_context_runner_v1"
    assert parsed["runner_name"] == "bounded_restricted_systemd_install_context_runner"
    assert parsed["mode"] == "plan"
    assert parsed["ok"] is True
    assert parsed["systemd_plan_or_readback"]["lower_schema_version"] == SYSTEMD_ROLLOUT_SCHEMA_VERSION
    assert parsed["systemd_plan_or_readback"]["unit_plan_created"] is True
    assert parsed["authority"] == {
        "systemd_write_attempted": False,
        "systemd_start_attempted": False,
        "systemd_stop_attempted": False,
        "systemd_enable_attempted": False,
        "systemd_disable_attempted": False,
        "unit_file_write_attempted": False,
        "unit_file_remove_attempted": False,
        "daemon_reload_attempted": False,
        "worker_started": False,
        "redis_consume_attempted": False,
        "redis_ack_attempted": False,
        "redis_xadd_attempted": False,
        "redis_group_mutation_attempted": False,
        "db_read_attempted": False,
        "db_write_attempted": False,
        "telegram_attempted": False,
        "openai_attempted": False,
        "github_attempted": False,
        "x_attempted": False,
        "web_attempted": False,
        "docker_attempted": False,
        "migration_attempted": False,
        "runtime_env_values_read": False,
        "secrets_output": False,
    }
    _assert_output_redacted(output, paths)


def test_install_mode_requires_both_confirmation_flags(tmp_path: Path, capsys) -> None:
    paths = _paths(tmp_path)

    assert runner.main(_argv(paths, "install")) == 1
    parsed = _parse_output(capsys)
    assert parsed["reason_code"] == "install_confirmation_flags_missing"
    assert parsed["authority"]["systemd_write_attempted"] is False

    assert runner.main([*_argv(paths, "install"), "--confirm-install"]) == 1
    parsed = _parse_output(capsys)
    assert parsed["reason_code"] == "install_confirmation_flags_missing"
    assert parsed["authority"]["unit_file_write_attempted"] is False


def test_rollback_mode_requires_both_confirmation_flags(tmp_path: Path, capsys) -> None:
    paths = _paths(tmp_path)

    assert runner.main(_argv(paths, "rollback")) == 1
    parsed = _parse_output(capsys)
    assert parsed["reason_code"] == "rollback_confirmation_flags_missing"
    assert parsed["authority"]["systemd_write_attempted"] is False

    assert runner.main([*_argv(paths, "rollback"), "--confirm-rollback"]) == 1
    parsed = _parse_output(capsys)
    assert parsed["reason_code"] == "rollback_confirmation_flags_missing"
    assert parsed["authority"]["unit_file_remove_attempted"] is False


def test_read_only_modes_reject_write_confirmation_flags(tmp_path: Path, capsys) -> None:
    paths = _paths(tmp_path)
    for mode in ("plan", "context-proof", "diagnose"):
        assert runner.main([*_argv(paths, mode), "--confirm-install"]) == 1
        parsed = _parse_output(capsys)
        assert parsed["mode"] == mode
        assert parsed["reason_code"] == "write_confirmation_not_allowed_for_read_only_mode"
        assert parsed["authority"]["systemd_write_attempted"] is False


def test_start_mode_is_not_accepted_or_implemented(tmp_path: Path, capsys) -> None:
    paths = _paths(tmp_path)

    assert runner.main(_argv(paths, "start")) == 1
    parsed = _parse_output(capsys)

    assert parsed["mode"] == "start"
    assert parsed["reason_code"] == "mode_not_allowed"
    assert parsed["systemd_plan_or_readback"]["lower_status"] == "not_dispatched"
    assert parsed["authority"]["systemd_start_attempted"] is False
    assert parsed["authority"]["worker_started"] is False


def test_install_mode_calls_existing_rollout_path_with_install_but_never_start(
    tmp_path: Path,
    capsys,
    monkeypatch,
) -> None:
    paths = _paths(tmp_path)
    captured = {}

    def fake_rollout(request):
        captured["mode"] = request.mode
        captured["confirm_install"] = request.confirm_install
        captured["confirm_start"] = request.confirm_start
        captured["dry_run"] = request.dry_run
        return _rollout_report(mode="install", install_attempted=True, enable_attempted=True)

    monkeypatch.setattr(runner, "run_systemd_rollout", fake_rollout)

    exit_code = runner.main(
        [
            *_argv(paths, "install"),
            "--confirm-install",
            "--i-understand-this-writes-user-systemd-unit",
        ]
    )
    parsed = _parse_output(capsys)

    assert exit_code == 0
    assert captured == {
        "mode": "install",
        "confirm_install": True,
        "confirm_start": False,
        "dry_run": False,
    }
    assert parsed["authority"]["systemd_write_attempted"] is True
    assert parsed["authority"]["unit_file_write_attempted"] is True
    assert parsed["authority"]["daemon_reload_attempted"] is True
    assert parsed["authority"]["systemd_enable_attempted"] is True
    assert parsed["authority"]["systemd_start_attempted"] is False
    assert parsed["authority"]["worker_started"] is False
    assert parsed["systemd_plan_or_readback"]["start_attempted_by_lower_runner"] is False


def test_rollback_mode_calls_existing_rollout_path_with_rollback_but_never_start(
    tmp_path: Path,
    capsys,
    monkeypatch,
) -> None:
    paths = _paths(tmp_path)
    captured = {}

    def fake_rollout(request):
        captured["mode"] = request.mode
        captured["confirm_rollback"] = request.confirm_rollback
        captured["confirm_start"] = request.confirm_start
        captured["dry_run"] = request.dry_run
        return _rollout_report(mode="rollback", rollback_attempted=True)

    monkeypatch.setattr(runner, "run_systemd_rollout", fake_rollout)

    exit_code = runner.main(
        [
            *_argv(paths, "rollback"),
            "--confirm-rollback",
            "--i-understand-this-removes-user-systemd-unit",
        ]
    )
    parsed = _parse_output(capsys)

    assert exit_code == 0
    assert captured == {
        "mode": "rollback",
        "confirm_rollback": True,
        "confirm_start": False,
        "dry_run": False,
    }
    assert parsed["authority"]["systemd_write_attempted"] is True
    assert parsed["authority"]["systemd_stop_attempted"] is True
    assert parsed["authority"]["systemd_disable_attempted"] is True
    assert parsed["authority"]["unit_file_remove_attempted"] is True
    assert parsed["authority"]["daemon_reload_attempted"] is True
    assert parsed["authority"]["systemd_start_attempted"] is False
    assert parsed["authority"]["worker_started"] is False
    assert parsed["systemd_plan_or_readback"]["start_attempted_by_lower_runner"] is False


def test_context_proof_mode_consumes_existing_context_proof_function(
    tmp_path: Path,
    capsys,
    monkeypatch,
) -> None:
    paths = _paths(tmp_path)
    captured = {}

    def fake_context(request):
        captured["repo_root"] = request.repo_root
        captured["service_name"] = request.service_name
        return _context_report()

    monkeypatch.setattr(runner, "run_systemd_context_proof", fake_context)

    assert runner.main(_argv(paths, "context-proof")) == 0
    parsed = _parse_output(capsys)

    assert captured == {"repo_root": paths["repo_root"], "service_name": SERVICE_NAME}
    assert parsed["systemd_plan_or_readback"]["lower_report_kind"] == "context_proof"
    assert parsed["systemd_plan_or_readback"]["exec_start_matches_expected"] is True
    assert parsed["systemd_plan_or_readback"]["working_directory_matches_expected"] is True
    assert parsed["systemd_plan_or_readback"]["environment_file_matches_expected"] is True
    assert parsed["authority"]["systemd_write_attempted"] is False


def test_diagnose_mode_consumes_existing_diagnostic_function(tmp_path: Path, capsys, monkeypatch) -> None:
    paths = _paths(tmp_path)
    captured = {}

    def fake_diagnostic(request):
        captured["runtime_env_file"] = request.runtime_env_file
        captured["systemd_user_dir"] = request.systemd_user_dir
        return _diagnostic_report()

    monkeypatch.setattr(runner, "run_systemd_diagnostic", fake_diagnostic)

    assert runner.main(_argv(paths, "diagnose")) == 0
    parsed = _parse_output(capsys)

    assert captured == {
        "runtime_env_file": paths["runtime_env_file"],
        "systemd_user_dir": paths["systemd_user_dir"],
    }
    assert parsed["systemd_plan_or_readback"]["lower_report_kind"] == "diagnostic"
    assert parsed["systemd_plan_or_readback"]["current_invocation_fingerprint_present"] is True
    assert parsed["authority"]["systemd_write_attempted"] is False
    assert parsed["authority"]["runtime_env_values_read"] is False


def test_outputs_redact_paths_unit_content_exec_start_env_path_and_stderr(
    tmp_path: Path,
    capsys,
    monkeypatch,
) -> None:
    paths = _paths(tmp_path)
    monkeypatch.setattr(
        runner,
        "run_systemd_rollout",
        lambda request: _rollout_report(mode=request.mode, install_attempted=True, enable_attempted=True),
    )

    assert (
        runner.main(
            [
                *_argv(paths, "install"),
                "--confirm-install",
                "--i-understand-this-writes-user-systemd-unit",
            ]
        )
        == 0
    )
    output = capsys.readouterr().out

    _assert_output_redacted(output, paths)
    for forbidden in (
        "ExecStart=",
        "WorkingDirectory=",
        "EnvironmentFile=",
        "placeholder-runtime-env-line",
        "raw stderr sentinel",
        str(paths["repo_root"] / "src/services/maintenance/worker_bootstrap.py"),
    ):
        assert forbidden not in output


def test_authority_booleans_match_modes(tmp_path: Path, capsys, monkeypatch) -> None:
    paths = _paths(tmp_path)
    monkeypatch.setattr(
        runner,
        "run_systemd_rollout",
        lambda request: _rollout_report(
            mode=request.mode,
            install_attempted=request.mode == "install",
            enable_attempted=request.mode == "install",
            rollback_attempted=request.mode == "rollback",
        ),
    )
    monkeypatch.setattr(runner, "run_systemd_context_proof", lambda request: _context_report())
    monkeypatch.setattr(runner, "run_systemd_diagnostic", lambda request: _diagnostic_report())

    mode_args = {
        "plan": [],
        "context-proof": [],
        "diagnose": [],
        "install": ["--confirm-install", "--i-understand-this-writes-user-systemd-unit"],
        "rollback": ["--confirm-rollback", "--i-understand-this-removes-user-systemd-unit"],
    }

    for mode, extra in mode_args.items():
        assert runner.main([*_argv(paths, mode), *extra]) == 0
        authority = _parse_output(capsys)["authority"]
        if mode == "install":
            assert authority["systemd_write_attempted"] is True
            assert authority["unit_file_write_attempted"] is True
            assert authority["systemd_enable_attempted"] is True
            assert authority["daemon_reload_attempted"] is True
            assert authority["systemd_stop_attempted"] is False
            assert authority["unit_file_remove_attempted"] is False
        elif mode == "rollback":
            assert authority["systemd_write_attempted"] is True
            assert authority["systemd_stop_attempted"] is True
            assert authority["systemd_disable_attempted"] is True
            assert authority["unit_file_remove_attempted"] is True
            assert authority["daemon_reload_attempted"] is True
            assert authority["unit_file_write_attempted"] is False
        else:
            assert authority["systemd_write_attempted"] is False
            assert authority["unit_file_write_attempted"] is False
            assert authority["unit_file_remove_attempted"] is False
            assert authority["daemon_reload_attempted"] is False
        assert authority["systemd_start_attempted"] is False
        assert authority["worker_started"] is False
        assert authority["redis_consume_attempted"] is False
        assert authority["redis_ack_attempted"] is False
        assert authority["db_write_attempted"] is False
        assert authority["telegram_attempted"] is False
        assert authority["openai_attempted"] is False
        assert authority["runtime_env_values_read"] is False
        assert authority["secrets_output"] is False


def test_static_ast_guard_rejects_forbidden_imports_calls_start_unit_and_start_rollout_mode() -> None:
    tree = ast.parse(TOOL_PATH.read_text(encoding="utf-8"))
    imported_roots: set[str] = set()
    call_names: set[str] = set()
    names: set[str] = set()
    start_rollout_mode_literals: list[str] = []

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
            if isinstance(node.func, ast.Name) and node.func.id == "SystemdRolloutRequest":
                for keyword in node.keywords:
                    if keyword.arg == "mode" and isinstance(keyword.value, ast.Constant):
                        start_rollout_mode_literals.append(str(keyword.value.value))
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
        "create_async_engine",
        "async_sessionmaker",
        "sessionmaker",
        "from_env",
        "xadd",
        "xack",
        "xgroup_create",
        "xreadgroup",
        "run_forever",
        "send_message",
        "edit_message_text",
        "start_unit",
    }.isdisjoint(call_names | names)
    assert "start" not in start_rollout_mode_literals


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
