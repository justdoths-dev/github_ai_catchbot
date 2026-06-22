from __future__ import annotations

import hashlib
import json
from dataclasses import asdict
from pathlib import Path

import pytest

from services.maintenance import worker_bootstrap, worker_runtime_crash_probe
from services.maintenance.systemd_rollout import (
    START_STABILITY_INTERVAL_SEC,
    SYSTEMD_DIAGNOSTIC_ALLOWED_PROPERTIES,
    SYSTEMD_CONTEXT_ALLOWED_PROPERTIES,
    SERVICE_NAME,
    LocalUserSystemdAdapter,
    SystemdAdapterError,
    SystemdContextProofRequest,
    SystemdContextState,
    SystemdDiagnosticRequest,
    SystemdDiagnosticState,
    SystemdReadback,
    SystemdRolloutRequest,
    SystemdUnitPlan,
    build_systemd_unit_plan,
    parse_service_unit_directives,
    parse_systemd_context_properties,
    parse_systemd_show_properties,
    run_systemd_context_proof,
    run_systemd_diagnostic,
    run_systemd_rollout,
)


SAFE_INVOCATION_ID = "0123456789abcdef0123456789abcdef"


class FakeSystemdAdapter:
    def __init__(self, systemd_user_dir: Path) -> None:
        self.systemd_user_dir = systemd_user_dir
        self.calls: list[tuple[str, str | None]] = []
        self.enabled: set[str] = set()
        self.active: set[str] = set()
        self.readback_service_active_overrides: list[bool] = []

    def write_unit_file(self, unit_name: str, content: str) -> None:
        self.calls.append(("write_unit_file", unit_name))
        (self.systemd_user_dir / unit_name).write_text(content, encoding="utf-8")

    def remove_unit_file(self, unit_name: str) -> None:
        self.calls.append(("remove_unit_file", unit_name))
        path = self.systemd_user_dir / unit_name
        if path.exists():
            path.unlink()

    def daemon_reload(self) -> None:
        self.calls.append(("daemon_reload", None))

    def enable_unit(self, unit_name: str) -> None:
        self.calls.append(("enable_unit", unit_name))
        self.enabled.add(unit_name)

    def disable_unit(self, unit_name: str) -> None:
        self.calls.append(("disable_unit", unit_name))
        self.enabled.discard(unit_name)

    def start_unit(self, unit_name: str) -> None:
        self.calls.append(("start_unit", unit_name))
        self.active.add(unit_name)

    def stop_unit(self, unit_name: str) -> None:
        self.calls.append(("stop_unit", unit_name))
        self.active.discard(unit_name)

    def readback(self, plan: SystemdUnitPlan) -> SystemdReadback:
        self.calls.append(("readback", plan.service_name))
        service_file_present = plan.service_unit_path.is_file()
        timer_file_present = bool(plan.timer_unit_path and plan.timer_unit_path.is_file())
        return SystemdReadback(
            service_file_present=service_file_present,
            timer_file_present=timer_file_present,
            service_enabled=plan.service_name in self.enabled,
            service_active=self.readback_service_active_overrides.pop(0)
            if self.readback_service_active_overrides
            else plan.service_name in self.active,
            rollback_plan_available=service_file_present or timer_file_present,
        )


class FakeDiagnosticAdapter:
    def __init__(self, state: SystemdDiagnosticState) -> None:
        self.state = state
        self.calls: list[str] = []

    def diagnostic_state(self, unit_name: str) -> SystemdDiagnosticState:
        self.calls.append(unit_name)
        return self.state


class FakeContextProofAdapter:
    def __init__(self, state: SystemdContextState) -> None:
        self.state = state
        self.calls: list[str] = []

    def context_state(self, plan: SystemdUnitPlan) -> SystemdContextState:
        self.calls.append(plan.service_name)
        return self.state


class FailingDiagnosticAdapter:
    def diagnostic_state(self, unit_name: str) -> SystemdDiagnosticState:
        raise SystemdAdapterError("systemctl_show_failed")


def _request(
    tmp_path: Path,
    *,
    mode: str = "plan",
    target: str = "maintenance-worker",
    confirm_install: bool = False,
    confirm_start: bool = False,
    confirm_rollback: bool = False,
    env_file_name: str = "runtime.env",
) -> SystemdRolloutRequest:
    repo_root = tmp_path / "repo"
    maintenance_dir = repo_root / "src/services/maintenance"
    maintenance_dir.mkdir(parents=True, exist_ok=True)
    (maintenance_dir / "main.py").write_text("# maintenance entrypoint\n", encoding="utf-8")
    (maintenance_dir / "worker_bootstrap.py").write_text("# maintenance bootstrap entrypoint\n", encoding="utf-8")
    python_executable = tmp_path / "venv/bin/python"
    python_executable.parent.mkdir(parents=True, exist_ok=True)
    python_executable.write_text("# python shim\n", encoding="utf-8")
    runtime_env_file = tmp_path / env_file_name
    runtime_env_file.write_text(
        "DATABASE_URL=sentinel-db-secret\nREDIS_URL=sentinel-redis-secret\n",
        encoding="utf-8",
    )
    systemd_user_dir = tmp_path / "user-systemd"
    systemd_user_dir.mkdir(parents=True, exist_ok=True)
    return SystemdRolloutRequest(
        mode=mode,
        target=target,
        confirm_install=confirm_install,
        confirm_start=confirm_start,
        confirm_rollback=confirm_rollback,
        repo_root=repo_root.resolve(),
        python_executable=python_executable.resolve(),
        runtime_env_file=runtime_env_file.resolve(),
        systemd_user_dir=systemd_user_dir.resolve(),
        dry_run=mode in {"plan", "proof"},
    )


def _diagnostic_request(tmp_path: Path, *, target: str = "maintenance-worker") -> SystemdDiagnosticRequest:
    runtime_env_file = tmp_path / "sentinel-runtime-secret.env"
    systemd_user_dir = tmp_path / "user-systemd"
    systemd_user_dir.mkdir(parents=True, exist_ok=True)
    return SystemdDiagnosticRequest(
        target=target,
        runtime_env_file=runtime_env_file.resolve(),
        systemd_user_dir=systemd_user_dir.resolve(),
    )


def _context_request(tmp_path: Path, *, target: str = "maintenance-worker") -> SystemdContextProofRequest:
    request = _request(tmp_path, mode="plan", target=target)
    return SystemdContextProofRequest(
        target=target,
        repo_root=request.repo_root,
        python_executable=request.python_executable,
        runtime_env_file=request.runtime_env_file,
        systemd_user_dir=request.systemd_user_dir,
    )


def test_plan_builds_deterministic_service_unit_without_writes(tmp_path: Path) -> None:
    request = _request(tmp_path, mode="plan")
    adapter = FakeSystemdAdapter(request.systemd_user_dir)

    plan = build_systemd_unit_plan(request)
    report = run_systemd_rollout(request, adapter=adapter)

    assert plan.service_name == SERVICE_NAME
    assert plan.timer_name is None
    assert plan.timer_unit_content is None
    bootstrap_script = request.repo_root / "src/services/maintenance/worker_bootstrap.py"
    assert plan.exec_start_argv == (
        str(request.python_executable),
        "-I",
        str(bootstrap_script),
    )
    assert "-m" not in plan.exec_start_argv
    assert plan.service_unit_content == "\n".join(
        [
            "[Unit]",
            "Description=github_ai_catchbot maintenance worker",
            "After=network-online.target",
            "Wants=network-online.target",
            "",
            "[Service]",
            "Type=simple",
            f"WorkingDirectory={request.repo_root}",
            f"EnvironmentFile={request.runtime_env_file}",
            f"ExecStart={request.python_executable} -I {bootstrap_script}",
            "Restart=on-failure",
            "RestartSec=10",
            "",
            "[Install]",
            "WantedBy=default.target",
            "",
        ]
    )
    assert report.status == "pass"
    assert report.unit_plan_created is True
    assert adapter.calls == []
    assert list(request.systemd_user_dir.iterdir()) == []


def test_plan_report_omits_runtime_env_path_and_secret_values(tmp_path: Path) -> None:
    request = _request(tmp_path, mode="plan", env_file_name="sentinel-runtime-secret.env")

    report = run_systemd_rollout(request, adapter=FakeSystemdAdapter(request.systemd_user_dir))
    output = json.dumps(asdict(report), sort_keys=True)

    assert report.redactions_applied["runtime_env_path_omitted"] is True
    assert str(request.runtime_env_file) not in output
    assert plan_unit_content_absent(output)
    assert "sentinel-runtime-secret" not in output
    assert "sentinel-db-secret" not in output
    assert "sentinel-redis-secret" not in output


def test_invalid_target_blocks_before_env_file_checks(tmp_path: Path) -> None:
    request = _request(tmp_path, mode="plan", target="other-target")
    missing_env = request.runtime_env_file.parent / "missing-runtime.env"
    request = SystemdRolloutRequest(
        mode=request.mode,
        target=request.target,
        confirm_install=request.confirm_install,
        confirm_start=request.confirm_start,
        confirm_rollback=request.confirm_rollback,
        repo_root=request.repo_root,
        python_executable=request.python_executable,
        runtime_env_file=missing_env,
        systemd_user_dir=request.systemd_user_dir,
        dry_run=request.dry_run,
    )

    report = run_systemd_rollout(request, adapter=FakeSystemdAdapter(request.systemd_user_dir))

    assert report.status == "blocked"
    assert report.reason_code == "target_not_allowed"
    assert report.unit_plan_created is False


def test_install_without_confirm_blocks_before_env_file_checks(tmp_path: Path) -> None:
    request = _request(tmp_path, mode="install")
    missing_env = request.runtime_env_file.parent / "missing-runtime.env"
    request = SystemdRolloutRequest(
        mode=request.mode,
        target=request.target,
        confirm_install=False,
        confirm_start=False,
        confirm_rollback=False,
        repo_root=request.repo_root,
        python_executable=request.python_executable,
        runtime_env_file=missing_env,
        systemd_user_dir=request.systemd_user_dir,
        dry_run=False,
    )

    report = run_systemd_rollout(request, adapter=FakeSystemdAdapter(request.systemd_user_dir))

    assert report.status == "blocked"
    assert report.reason_code == "install_confirm_missing"
    assert report.install_attempted is False


def test_start_without_confirm_blocks_before_env_file_checks(tmp_path: Path) -> None:
    request = _request(tmp_path, mode="start")
    missing_env = request.runtime_env_file.parent / "missing-runtime.env"
    request = SystemdRolloutRequest(
        mode=request.mode,
        target=request.target,
        confirm_install=False,
        confirm_start=False,
        confirm_rollback=False,
        repo_root=request.repo_root,
        python_executable=request.python_executable,
        runtime_env_file=missing_env,
        systemd_user_dir=request.systemd_user_dir,
        dry_run=False,
    )

    report = run_systemd_rollout(request, adapter=FakeSystemdAdapter(request.systemd_user_dir))

    assert report.status == "blocked"
    assert report.reason_code == "start_confirm_missing"
    assert report.start_attempted is False


def test_rollback_without_confirm_blocks_before_env_file_checks(tmp_path: Path) -> None:
    request = _request(tmp_path, mode="rollback")
    missing_env = request.runtime_env_file.parent / "missing-runtime.env"
    request = SystemdRolloutRequest(
        mode=request.mode,
        target=request.target,
        confirm_install=False,
        confirm_start=False,
        confirm_rollback=False,
        repo_root=request.repo_root,
        python_executable=request.python_executable,
        runtime_env_file=missing_env,
        systemd_user_dir=request.systemd_user_dir,
        dry_run=False,
    )

    report = run_systemd_rollout(request, adapter=FakeSystemdAdapter(request.systemd_user_dir))

    assert report.status == "blocked"
    assert report.reason_code == "rollback_confirm_missing"
    assert report.rollback_attempted is False


def test_install_writes_only_exact_service_file_and_enables(tmp_path: Path) -> None:
    request = _request(tmp_path, mode="install", confirm_install=True)
    adapter = FakeSystemdAdapter(request.systemd_user_dir)

    report = run_systemd_rollout(request, adapter=adapter)

    assert report.status == "pass"
    assert report.install_attempted is True
    assert report.enable_attempted is True
    assert report.service_file_present is True
    assert report.timer_file_present is False
    assert report.service_enabled is True
    assert sorted(path.name for path in request.systemd_user_dir.iterdir()) == [SERVICE_NAME]
    assert adapter.calls == [
        ("write_unit_file", SERVICE_NAME),
        ("daemon_reload", None),
        ("enable_unit", SERVICE_NAME),
        ("readback", SERVICE_NAME),
    ]


def test_start_records_exact_start_and_stable_active_readback(tmp_path: Path) -> None:
    request = _request(tmp_path, mode="start", confirm_start=True)
    adapter = FakeSystemdAdapter(request.systemd_user_dir)
    plan = build_systemd_unit_plan(request)
    adapter.write_unit_file(plan.service_name, plan.service_unit_content)
    adapter.calls.clear()
    sleeps: list[float] = []

    report = run_systemd_rollout(request, adapter=adapter, sleeper=sleeps.append)

    assert report.status == "pass"
    assert report.reason_code is None
    assert report.start_attempted is True
    assert report.service_active is True
    assert sleeps == [START_STABILITY_INTERVAL_SEC]
    assert adapter.calls == [
        ("start_unit", SERVICE_NAME),
        ("readback", SERVICE_NAME),
        ("readback", SERVICE_NAME),
    ]


def test_start_blocks_when_immediate_readback_inactive(tmp_path: Path) -> None:
    request = _request(tmp_path, mode="start", confirm_start=True)
    adapter = FakeSystemdAdapter(request.systemd_user_dir)
    plan = build_systemd_unit_plan(request)
    adapter.write_unit_file(plan.service_name, plan.service_unit_content)
    adapter.readback_service_active_overrides = [False]
    adapter.calls.clear()
    sleeps: list[float] = []

    report = run_systemd_rollout(request, adapter=adapter, sleeper=sleeps.append)

    assert report.status == "blocked"
    assert report.reason_code == "service_not_active"
    assert report.start_attempted is True
    assert report.service_active is False
    assert sleeps == []
    assert adapter.calls == [
        ("start_unit", SERVICE_NAME),
        ("readback", SERVICE_NAME),
    ]


def test_start_blocks_when_service_exits_after_immediate_active_readback(tmp_path: Path) -> None:
    request = _request(tmp_path, mode="start", confirm_start=True)
    adapter = FakeSystemdAdapter(request.systemd_user_dir)
    plan = build_systemd_unit_plan(request)
    adapter.write_unit_file(plan.service_name, plan.service_unit_content)
    adapter.readback_service_active_overrides = [True, False]
    adapter.calls.clear()
    sleeps: list[float] = []

    report = run_systemd_rollout(request, adapter=adapter, sleeper=sleeps.append)

    assert report.status == "blocked"
    assert report.reason_code == "service_exited_after_start"
    assert report.start_attempted is True
    assert report.service_active is False
    assert sleeps == [START_STABILITY_INTERVAL_SEC]
    assert adapter.calls == [
        ("start_unit", SERVICE_NAME),
        ("readback", SERVICE_NAME),
        ("readback", SERVICE_NAME),
    ]


def test_proof_reads_exact_service_state_from_fake_adapter(tmp_path: Path) -> None:
    request = _request(tmp_path, mode="proof")
    adapter = FakeSystemdAdapter(request.systemd_user_dir)
    plan = build_systemd_unit_plan(request)
    adapter.write_unit_file(plan.service_name, plan.service_unit_content)
    adapter.enable_unit(plan.service_name)
    adapter.start_unit(plan.service_name)
    adapter.calls.clear()

    report = run_systemd_rollout(request, adapter=adapter)

    assert report.status == "pass"
    assert report.proof_attempted is True
    assert report.service_file_present is True
    assert report.timer_file_present is False
    assert report.service_enabled is True
    assert report.service_active is True
    assert report.rollback_plan_available is True
    assert adapter.calls == [("readback", SERVICE_NAME)]


def test_rollback_removes_only_exact_unit_and_proves_absence(tmp_path: Path) -> None:
    request = _request(tmp_path, mode="rollback", confirm_rollback=True)
    adapter = FakeSystemdAdapter(request.systemd_user_dir)
    plan = build_systemd_unit_plan(request)
    adapter.write_unit_file(plan.service_name, plan.service_unit_content)
    unrelated = request.systemd_user_dir / "unrelated.service"
    unrelated.write_text("[Service]\nType=oneshot\n", encoding="utf-8")
    adapter.enable_unit(plan.service_name)
    adapter.start_unit(plan.service_name)
    adapter.calls.clear()

    report = run_systemd_rollout(request, adapter=adapter)

    assert report.status == "pass"
    assert report.rollback_attempted is True
    assert report.service_file_present is False
    assert report.timer_file_present is False
    assert report.service_enabled is False
    assert report.service_active is False
    assert (request.systemd_user_dir / SERVICE_NAME).exists() is False
    assert unrelated.exists() is True
    assert sorted(path.name for path in request.systemd_user_dir.iterdir()) == ["unrelated.service"]
    assert adapter.calls == [
        ("stop_unit", SERVICE_NAME),
        ("disable_unit", SERVICE_NAME),
        ("remove_unit_file", SERVICE_NAME),
        ("daemon_reload", None),
        ("readback", SERVICE_NAME),
    ]


def test_service_unit_uses_environment_file_without_shell_or_broad_permissions(tmp_path: Path) -> None:
    plan = build_systemd_unit_plan(_request(tmp_path, mode="plan"))

    assert "EnvironmentFile=" in plan.service_unit_content
    assert "Environment=PYTHONPATH=" not in plan.service_unit_content
    assert "PYTHONPATH=" not in plan.service_unit_content
    assert "ExecStart=" in plan.service_unit_content
    assert f"{plan.exec_start_argv[0]} -I {plan.exec_start_argv[2]}" in plan.service_unit_content
    assert "/bin/sh" not in plan.service_unit_content
    assert "/bin/bash" not in plan.service_unit_content
    assert "bash -c" not in plan.service_unit_content
    assert " source " not in plan.service_unit_content
    assert " cat " not in plan.service_unit_content
    assert " export " not in plan.service_unit_content
    assert "printenv" not in plan.service_unit_content
    assert "chmod" not in plan.service_unit_content
    assert "chown" not in plan.service_unit_content
    assert "journalctl" not in plan.service_unit_content
    assert "src/services/maintenance/worker_bootstrap.py" in plan.service_unit_content
    assert "-I" in plan.exec_start_argv
    assert "-m" not in plan.exec_start_argv
    assert "-m src.services.maintenance.worker_bootstrap" not in plan.service_unit_content
    assert "src.services.maintenance.main worker" not in plan.service_unit_content
    assert "User=" not in plan.service_unit_content
    assert "Group=" not in plan.service_unit_content


def test_diagnostic_invalid_target_blocks_before_env_file_checks(tmp_path: Path) -> None:
    request = _diagnostic_request(tmp_path, target="other-target")
    adapter = FakeDiagnosticAdapter(
        SystemdDiagnosticState(
            service_file_present=True,
            service_enabled=True,
            service_active=True,
        )
    )

    report = run_systemd_diagnostic(request, adapter=adapter)
    output = json.dumps(asdict(report), sort_keys=True)

    assert report.schema_version == "maintenance_systemd_diagnostic_report_v1"
    assert report.status == "blocked"
    assert report.reason_code == "target_not_allowed"
    assert adapter.calls == []
    assert str(request.runtime_env_file) not in output
    assert "sentinel-runtime-secret" not in output


def test_context_proof_invalid_target_blocks_before_path_checks(tmp_path: Path) -> None:
    request = _context_request(tmp_path, target="other-target")
    missing_env = request.runtime_env_file.parent / "sentinel-missing-runtime.env"
    request = SystemdContextProofRequest(
        target=request.target,
        repo_root=request.repo_root,
        python_executable=request.python_executable,
        runtime_env_file=missing_env,
        systemd_user_dir=request.systemd_user_dir,
    )
    adapter = FakeContextProofAdapter(
        SystemdContextState(
            service_file_present=True,
            service_enabled=True,
            service_active=True,
            service_name_matches_expected=True,
        )
    )

    report = run_systemd_context_proof(request, adapter=adapter)
    output = json.dumps(asdict(report), sort_keys=True)

    assert report.schema_version == "maintenance_systemd_context_proof_report_v1"
    assert report.status == "blocked"
    assert report.reason_code == "target_not_allowed"
    assert adapter.calls == []
    assert str(missing_env) not in output
    assert "sentinel-missing-runtime" not in output


def test_context_proof_compares_expected_unit_context_without_raw_path_output(tmp_path: Path) -> None:
    request = _context_request(tmp_path)
    rollout_request = _request(tmp_path, mode="plan")
    plan = build_systemd_unit_plan(rollout_request)
    adapter = FakeContextProofAdapter(
        SystemdContextState(
            service_file_present=True,
            service_enabled=True,
            service_active=False,
            service_name_matches_expected=True,
            unit_load_state="loaded",
            unit_file_state="enabled",
            exec_start=" ".join(plan.exec_start_argv),
            working_directory=str(request.repo_root),
            environment_file=str(request.runtime_env_file),
            restart_policy="on-failure",
            restart_sec="10s",
        )
    )

    report = run_systemd_context_proof(request, adapter=adapter)
    output = json.dumps(asdict(report), sort_keys=True)

    assert report.status == "pass"
    assert report.unit_context_matches_expected is True
    assert report.mismatched_context_fields == []
    assert report.service_name_matches_expected is True
    assert report.service_file_present is True
    assert report.service_enabled is True
    assert report.service_active is False
    assert report.exec_start_matches_expected is True
    assert report.working_directory_matches_expected is True
    assert report.environment_file_matches_expected is True
    assert report.restart_policy_matches_expected is True
    assert report.restart_sec_matches_expected is True
    assert report.raw_unit_content_omitted is True
    assert report.raw_exec_start_omitted is True
    assert report.raw_paths_omitted is True
    assert str(request.repo_root) not in output
    assert str(request.runtime_env_file) not in output
    assert str(request.python_executable) not in output
    assert "EnvironmentFile=" not in output
    assert "ExecStart=" not in output


def test_context_proof_accepts_exact_systemd_show_argv_equality(tmp_path: Path) -> None:
    request = _context_request(tmp_path)
    plan = build_systemd_unit_plan(_request(tmp_path, mode="plan"))
    adapter = FakeContextProofAdapter(
        SystemdContextState(
            service_file_present=True,
            service_enabled=True,
            service_active=False,
            service_name_matches_expected=True,
            unit_load_state="loaded",
            unit_file_state="enabled",
            exec_start="{ path=/redacted ; argv[]="
            + " ".join(plan.exec_start_argv)
            + " ; ignore_errors=no ; start_time=[n/a] ; stop_time=[n/a] ; pid=0 ; code=(null) ; status=0/0 }",
            working_directory=str(request.repo_root),
            environment_file=str(request.runtime_env_file),
            restart_policy="on-failure",
            restart_sec="10s",
        )
    )

    report = run_systemd_context_proof(request, adapter=adapter)

    assert report.status == "pass"
    assert report.exec_start_matches_expected is True
    assert report.unit_context_matches_expected is True
    assert report.mismatched_context_fields == []


def test_context_proof_rejects_shell_wrapped_expected_exec_start(tmp_path: Path) -> None:
    request = _context_request(tmp_path)
    plan = build_systemd_unit_plan(_request(tmp_path, mode="plan"))
    adapter = FakeContextProofAdapter(
        SystemdContextState(
            service_file_present=True,
            service_enabled=True,
            service_active=False,
            service_name_matches_expected=True,
            unit_load_state="loaded",
            unit_file_state="enabled",
            exec_start='/bin/sh -c "' + " ".join(plan.exec_start_argv) + '"',
            working_directory=str(request.repo_root),
            environment_file=str(request.runtime_env_file),
            restart_policy="on-failure",
            restart_sec="10s",
        )
    )

    report = run_systemd_context_proof(request, adapter=adapter)
    output = json.dumps(asdict(report), sort_keys=True)

    assert report.status == "blocked"
    assert report.reason_code == "unit_context_mismatch"
    assert report.exec_start_matches_expected is False
    assert report.unit_context_matches_expected is False
    assert report.mismatched_context_fields == ["exec_start_matches_expected"]
    assert "ExecStart=" not in output
    assert "/bin/sh" not in output
    assert str(request.python_executable) not in output


def test_context_proof_rejects_direct_main_worker_exec_start(tmp_path: Path) -> None:
    request = _context_request(tmp_path)
    adapter = FakeContextProofAdapter(
        SystemdContextState(
            service_file_present=True,
            service_enabled=True,
            service_active=False,
            service_name_matches_expected=True,
            unit_load_state="loaded",
            unit_file_state="enabled",
            exec_start=f"{request.python_executable} -m src.services.maintenance.main worker",
            working_directory=str(request.repo_root),
            environment_file=str(request.runtime_env_file),
            restart_policy="on-failure",
            restart_sec="10s",
        )
    )

    report = run_systemd_context_proof(request, adapter=adapter)
    output = json.dumps(asdict(report), sort_keys=True)

    assert report.status == "blocked"
    assert report.reason_code == "unit_context_mismatch"
    assert report.exec_start_matches_expected is False
    assert report.unit_context_matches_expected is False
    assert report.mismatched_context_fields == ["exec_start_matches_expected"]
    assert "src.services.maintenance.main" not in output
    assert str(request.python_executable) not in output


def test_context_proof_rejects_direct_script_exec_start_without_isolated_mode(tmp_path: Path) -> None:
    request = _context_request(tmp_path)
    bootstrap_script = request.repo_root / "src/services/maintenance/worker_bootstrap.py"
    adapter = FakeContextProofAdapter(
        SystemdContextState(
            service_file_present=True,
            service_enabled=True,
            service_active=False,
            service_name_matches_expected=True,
            unit_load_state="loaded",
            unit_file_state="enabled",
            exec_start=f"{request.python_executable} {bootstrap_script}",
            working_directory=str(request.repo_root),
            environment_file=str(request.runtime_env_file),
            restart_policy="on-failure",
            restart_sec="10s",
        )
    )

    report = run_systemd_context_proof(request, adapter=adapter)
    output = json.dumps(asdict(report), sort_keys=True)

    assert report.status == "blocked"
    assert report.reason_code == "unit_context_mismatch"
    assert report.exec_start_matches_expected is False
    assert report.unit_context_matches_expected is False
    assert report.mismatched_context_fields == ["exec_start_matches_expected"]
    assert "worker_bootstrap.py" not in output
    assert str(request.python_executable) not in output


def test_context_proof_rejects_old_module_bootstrap_exec_start(tmp_path: Path) -> None:
    request = _context_request(tmp_path)
    adapter = FakeContextProofAdapter(
        SystemdContextState(
            service_file_present=True,
            service_enabled=True,
            service_active=False,
            service_name_matches_expected=True,
            unit_load_state="loaded",
            unit_file_state="enabled",
            exec_start=f"{request.python_executable} -m src.services.maintenance.worker_bootstrap",
            working_directory=str(request.repo_root),
            environment_file=str(request.runtime_env_file),
            restart_policy="on-failure",
            restart_sec="10s",
        )
    )

    report = run_systemd_context_proof(request, adapter=adapter)
    output = json.dumps(asdict(report), sort_keys=True)

    assert report.status == "blocked"
    assert report.reason_code == "unit_context_mismatch"
    assert report.exec_start_matches_expected is False
    assert report.unit_context_matches_expected is False
    assert report.mismatched_context_fields == ["exec_start_matches_expected"]
    assert "src.services.maintenance.worker_bootstrap" not in output
    assert str(request.python_executable) not in output


@pytest.mark.parametrize(
    "placement, systemd_show",
    [
        ("prefix", False),
        ("suffix", False),
        ("prefix", True),
        ("suffix", True),
    ],
)
def test_context_proof_rejects_extra_prefix_or_suffix_exec_start_tokens(
    tmp_path: Path,
    placement: str,
    systemd_show: bool,
) -> None:
    request = _context_request(tmp_path)
    plan = build_systemd_unit_plan(_request(tmp_path, mode="plan"))
    expected = " ".join(plan.exec_start_argv)
    observed = f"/usr/bin/env {expected}" if placement == "prefix" else f"{expected} --unexpected"
    exec_start = f"{{ path=/redacted ; argv[]={observed} ; ignore_errors=no }}" if systemd_show else observed
    adapter = FakeContextProofAdapter(
        SystemdContextState(
            service_file_present=True,
            service_enabled=True,
            service_active=False,
            service_name_matches_expected=True,
            unit_load_state="loaded",
            unit_file_state="enabled",
            exec_start=exec_start,
            working_directory=str(request.repo_root),
            environment_file=str(request.runtime_env_file),
            restart_policy="on-failure",
            restart_sec="10s",
        )
    )

    report = run_systemd_context_proof(request, adapter=adapter)
    output = json.dumps(asdict(report), sort_keys=True)

    assert report.status == "blocked"
    assert report.reason_code == "unit_context_mismatch"
    assert report.exec_start_matches_expected is False
    assert report.unit_context_matches_expected is False
    assert report.mismatched_context_fields == ["exec_start_matches_expected"]
    assert "ExecStart=" not in output
    assert str(request.python_executable) not in output


def test_context_proof_mismatch_reports_field_names_only(tmp_path: Path) -> None:
    request = _context_request(tmp_path)
    adapter = FakeContextProofAdapter(
        SystemdContextState(
            service_file_present=True,
            service_enabled=True,
            service_active=False,
            service_name_matches_expected=True,
            unit_load_state="loaded",
            unit_file_state="enabled",
            exec_start="/sentinel/private/python -m wrong.module sentinel-secret",
            working_directory="/sentinel/private/repo",
            environment_file="/sentinel/private/runtime.env",
            restart_policy="always",
            restart_sec="20s",
        )
    )

    report = run_systemd_context_proof(request, adapter=adapter)
    output = json.dumps(asdict(report), sort_keys=True)

    assert report.status == "blocked"
    assert report.reason_code == "unit_context_mismatch"
    assert report.unit_context_matches_expected is False
    assert report.mismatched_context_fields == [
        "exec_start_matches_expected",
        "working_directory_matches_expected",
        "environment_file_matches_expected",
        "restart_policy_matches_expected",
        "restart_sec_matches_expected",
    ]
    assert "/sentinel/private" not in output
    assert "sentinel-secret" not in output
    assert "wrong.module" not in output


def test_diagnostic_reads_exact_service_only_and_reports_active_state(tmp_path: Path) -> None:
    expected_fingerprint = hashlib.sha256(SAFE_INVOCATION_ID.encode("ascii")).hexdigest()[:16]
    state = SystemdDiagnosticState(
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
        current_invocation_fingerprint=expected_fingerprint,
    )
    adapter = FakeDiagnosticAdapter(state)

    report = run_systemd_diagnostic(_diagnostic_request(tmp_path), adapter=adapter)
    output = json.dumps(asdict(report), sort_keys=True)

    assert report.status == "pass"
    assert report.reason_code is None
    assert report.service_name == SERVICE_NAME
    assert report.service_file_present is True
    assert report.service_enabled is True
    assert report.service_active is True
    assert report.load_state == "loaded"
    assert report.active_state == "active"
    assert report.current_invocation_fingerprint == expected_fingerprint
    assert report.raw_invocation_id_omitted is True
    assert report.redactions_applied["raw_invocation_id_omitted"] is True
    assert report.restart_likely is False
    assert report.exited_after_start_likely is False
    assert adapter.calls == [SERVICE_NAME]
    assert SAFE_INVOCATION_ID not in output


def test_diagnostic_flags_service_that_exited_after_start(tmp_path: Path) -> None:
    state = SystemdDiagnosticState(
        service_file_present=True,
        service_enabled=True,
        service_active=False,
        load_state="loaded",
        active_state="inactive",
        sub_state="dead",
        result="exit-code",
        exec_main_code=1,
        exec_main_status=1,
        n_restarts=2,
        unit_file_state="enabled",
    )

    report = run_systemd_diagnostic(_diagnostic_request(tmp_path), adapter=FakeDiagnosticAdapter(state))

    assert report.status == "blocked"
    assert report.reason_code == "service_exited_after_start"
    assert report.restart_likely is True
    assert report.exited_after_start_likely is True


@pytest.mark.parametrize("bootstrap_status", [21, 22, 23])
def test_diagnostic_preserves_bounded_bootstrap_exec_main_status(
    tmp_path: Path,
    bootstrap_status: int,
) -> None:
    state = SystemdDiagnosticState(
        service_file_present=True,
        service_enabled=True,
        service_active=False,
        load_state="loaded",
        active_state="activating",
        sub_state="auto-restart",
        result="exit-code",
        exec_main_code=1,
        exec_main_status=bootstrap_status,
        n_restarts=3,
        unit_file_state="enabled",
    )

    report = run_systemd_diagnostic(_diagnostic_request(tmp_path), adapter=FakeDiagnosticAdapter(state))
    output = json.dumps(asdict(report), sort_keys=True)

    assert report.status == "blocked"
    assert report.reason_code == "service_exited_after_start"
    assert report.exec_main_status == bootstrap_status
    assert report.restart_likely is True
    assert report.exited_after_start_likely is True
    assert report.redactions_applied["journal_output_omitted"] is True
    assert "sentinel-secret" not in output


def test_diagnostic_redacts_systemctl_failure_output_paths_and_journal_text(tmp_path: Path) -> None:
    request = _diagnostic_request(tmp_path)

    report = run_systemd_diagnostic(request, adapter=FailingDiagnosticAdapter())
    output = json.dumps(asdict(report), sort_keys=True)

    assert report.status == "failed"
    assert report.reason_code == "systemctl_show_failed"
    assert str(request.runtime_env_file) not in output
    assert str(request.systemd_user_dir) not in output
    assert "sentinel-secret" not in output
    assert "journal body sentinel" not in output
    assert report.redactions_applied["systemctl_stderr_omitted"] is True
    assert report.redactions_applied["journal_output_omitted"] is True


def test_parse_systemd_show_properties_allows_only_safe_properties_and_redacts_fragment_path() -> None:
    expected_fingerprint = hashlib.sha256(SAFE_INVOCATION_ID.encode("ascii")).hexdigest()[:16]
    output = "\n".join(
        [
            "LoadState=loaded",
            "ActiveState=inactive",
            "SubState=dead",
            "Result=exit-code",
            "ExecMainCode=1",
            "ExecMainStatus=1",
            "NRestarts=2",
            "UnitFileState=enabled",
            "FragmentPath=/home/dev/private/github-ai-catchbot-maintenance.service",
            f"InvocationID={SAFE_INVOCATION_ID}",
            "Environment=DATABASE_URL=sentinel-secret",
            "ExecStart=/bin/sh -c sentinel-secret",
            "StatusText=journal body sentinel",
        ]
    )

    properties = parse_systemd_show_properties(output)
    serialized = json.dumps(properties, sort_keys=True)

    assert set(properties) == set(SYSTEMD_DIAGNOSTIC_ALLOWED_PROPERTIES)
    assert properties["FragmentPath"] == "present"
    assert properties["InvocationID"] == expected_fingerprint
    assert "/home/dev/private" not in serialized
    assert SAFE_INVOCATION_ID not in serialized
    assert "sentinel-secret" not in serialized
    assert "journal body sentinel" not in serialized


def test_parse_systemd_show_properties_drops_malformed_invocation_id() -> None:
    properties = parse_systemd_show_properties(
        "\n".join(
            [
                "LoadState=loaded",
                "InvocationID=sentinel-secret-invocation-id",
            ]
        )
    )
    serialized = json.dumps(properties, sort_keys=True)

    assert properties["InvocationID"] == ""
    assert "sentinel-secret-invocation-id" not in serialized


def test_parse_systemd_context_properties_allows_only_context_properties_and_redacts_fragment_path() -> None:
    output = "\n".join(
        [
            "LoadState=loaded",
            "ActiveState=activating",
            "UnitFileState=enabled",
            "FragmentPath=/home/dev/private/github-ai-catchbot-maintenance.service",
            "ExecStart=/home/dev/private/python -I /home/dev/private/repo/src/services/maintenance/worker_bootstrap.py",
            "WorkingDirectory=/home/dev/private/repo",
            "EnvironmentFiles=/home/dev/private/runtime.env (ignore_errors=no)",
            "Restart=on-failure",
            "RestartUSec=10s",
            "Environment=DATABASE_URL=sentinel-secret",
            "StatusText=journal body sentinel",
        ]
    )

    properties = parse_systemd_context_properties(output)
    serialized = json.dumps(properties, sort_keys=True)

    assert set(properties) == set(SYSTEMD_CONTEXT_ALLOWED_PROPERTIES)
    assert properties["FragmentPath"] == "present"
    assert "DATABASE_URL=sentinel-secret" not in serialized
    assert "journal body sentinel" not in serialized


def test_parse_service_unit_directives_reads_only_service_context() -> None:
    content = "\n".join(
        [
            "[Unit]",
            "Description=sentinel",
            "[Service]",
            "WorkingDirectory=/repo",
            "EnvironmentFile=/runtime.env",
            "ExecStart=/python -I /repo/src/services/maintenance/worker_bootstrap.py",
            "Restart=on-failure",
            "RestartSec=10",
            "[Install]",
            "WantedBy=default.target",
        ]
    )

    assert parse_service_unit_directives(content) == {
        "WorkingDirectory": "/repo",
        "EnvironmentFile": "/runtime.env",
        "ExecStart": "/python -I /repo/src/services/maintenance/worker_bootstrap.py",
        "Restart": "on-failure",
        "RestartSec": "10",
    }


def test_local_diagnostic_uses_exact_user_systemctl_show_allowlist(monkeypatch, tmp_path: Path) -> None:
    calls: list[list[str]] = []
    expected_fingerprint = hashlib.sha256(SAFE_INVOCATION_ID.encode("ascii")).hexdigest()[:16]

    def fake_run(args, check, stdout, stderr, text):
        del check, stdout, stderr, text
        calls.append(list(args))

        class Result:
            returncode = 0
            stdout = "\n".join(
                [
                    "LoadState=loaded",
                    "ActiveState=active",
                    "SubState=running",
                    "Result=success",
                    "ExecMainCode=0",
                    "ExecMainStatus=0",
                    "NRestarts=0",
                    "UnitFileState=enabled",
                    "FragmentPath=/redacted/path",
                    f"InvocationID={SAFE_INVOCATION_ID}",
                    "Environment=DATABASE_URL=sentinel-secret",
                ]
            )

        return Result()

    monkeypatch.setattr("services.maintenance.systemd_rollout.subprocess.run", fake_run)
    adapter = LocalUserSystemdAdapter(tmp_path)

    state = adapter.diagnostic_state(SERVICE_NAME)

    assert state.service_file_present is True
    assert state.current_invocation_fingerprint == expected_fingerprint
    assert calls[0] == [
        "systemctl",
        "--user",
        "show",
        SERVICE_NAME,
        *(f"--property={property_name}" for property_name in SYSTEMD_DIAGNOSTIC_ALLOWED_PROPERTIES),
    ]
    assert calls[1] == ["systemctl", "--user", "is-enabled", SERVICE_NAME]
    assert calls[2] == ["systemctl", "--user", "is-active", SERVICE_NAME]


def test_invocation_fingerprint_contract_matches_bootstrap_runtime_and_systemd(
    monkeypatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setenv("INVOCATION_ID", SAFE_INVOCATION_ID)
    expected = hashlib.sha256(SAFE_INVOCATION_ID.encode("ascii")).hexdigest()[:16]
    bootstrap_report = worker_bootstrap.build_worker_bootstrap_fatal_report(
        reason_code="worker_bootstrap_main_error",
        phase="bootstrap_main",
    )
    runtime_report = worker_runtime_crash_probe.build_worker_runtime_fatal_report(
        reason_code="worker_runtime_config_error",
        phase="config_load",
        cleanup_completed=True,
    )
    diagnostic_report = run_systemd_diagnostic(
        _diagnostic_request(tmp_path),
        adapter=FakeDiagnosticAdapter(
            SystemdDiagnosticState(
                service_file_present=True,
                service_enabled=True,
                service_active=True,
                current_invocation_fingerprint=parse_systemd_show_properties(
                    f"InvocationID={SAFE_INVOCATION_ID}"
                )["InvocationID"],
            )
        ),
    )

    assert bootstrap_report["invocation_fingerprint"] == expected
    assert runtime_report.invocation_fingerprint == expected
    assert diagnostic_report.current_invocation_fingerprint == expected


def test_local_context_proof_uses_exact_user_systemctl_show_allowlist(monkeypatch, tmp_path: Path) -> None:
    calls: list[list[str]] = []
    request = _request(tmp_path, mode="plan")
    plan = build_systemd_unit_plan(request)
    plan.service_unit_path.write_text(plan.service_unit_content, encoding="utf-8")

    def fake_run(args, check, stdout, stderr, text):
        del check, stdout, stderr, text
        calls.append(list(args))

        class Result:
            returncode = 0
            stdout = "\n".join(
                [
                    "LoadState=loaded",
                    "ActiveState=active",
                    "UnitFileState=enabled",
                    "FragmentPath=/redacted/path",
                    f"ExecStart={' '.join(plan.exec_start_argv)}",
                    f"WorkingDirectory={request.repo_root}",
                    f"EnvironmentFiles={request.runtime_env_file} (ignore_errors=no)",
                    "Restart=on-failure",
                    "RestartUSec=10s",
                    "Environment=DATABASE_URL=sentinel-secret",
                ]
            )

        return Result()

    monkeypatch.setattr("services.maintenance.systemd_rollout.subprocess.run", fake_run)
    adapter = LocalUserSystemdAdapter(tmp_path)

    state = adapter.context_state(plan)

    assert state.service_file_present is True
    assert calls[0] == [
        "systemctl",
        "--user",
        "show",
        SERVICE_NAME,
        *(f"--property={property_name}" for property_name in SYSTEMD_CONTEXT_ALLOWED_PROPERTIES),
    ]
    assert calls[1] == ["systemctl", "--user", "is-enabled", SERVICE_NAME]
    assert calls[2] == ["systemctl", "--user", "is-active", SERVICE_NAME]


def test_local_diagnostic_rejects_non_exact_service_without_systemctl(monkeypatch, tmp_path: Path) -> None:
    def fail_run(*args, **kwargs):
        raise AssertionError("systemctl must not run for non-exact diagnostic unit")

    monkeypatch.setattr("services.maintenance.systemd_rollout.subprocess.run", fail_run)
    adapter = LocalUserSystemdAdapter(tmp_path)

    with pytest.raises(SystemdAdapterError):
        adapter.diagnostic_state("other.service")


def plan_unit_content_absent(output: str) -> bool:
    return "EnvironmentFile=" not in output and "ExecStart=" not in output and "PYTHONPATH=" not in output
