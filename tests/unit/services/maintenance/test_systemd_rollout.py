from __future__ import annotations

import json
from dataclasses import asdict
from pathlib import Path

from services.maintenance.systemd_rollout import (
    SERVICE_NAME,
    SystemdReadback,
    SystemdRolloutRequest,
    SystemdUnitPlan,
    build_systemd_unit_plan,
    run_systemd_rollout,
)


class FakeSystemdAdapter:
    def __init__(self, systemd_user_dir: Path) -> None:
        self.systemd_user_dir = systemd_user_dir
        self.calls: list[tuple[str, str | None]] = []
        self.enabled: set[str] = set()
        self.active: set[str] = set()

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
            service_active=plan.service_name in self.active,
            rollback_plan_available=service_file_present or timer_file_present,
        )


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


def test_plan_builds_deterministic_service_unit_without_writes(tmp_path: Path) -> None:
    request = _request(tmp_path, mode="plan")
    adapter = FakeSystemdAdapter(request.systemd_user_dir)

    plan = build_systemd_unit_plan(request)
    report = run_systemd_rollout(request, adapter=adapter)

    assert plan.service_name == SERVICE_NAME
    assert plan.timer_name is None
    assert plan.timer_unit_content is None
    assert plan.exec_start_argv == (
        str(request.python_executable),
        "-m",
        "src.services.maintenance.main",
        "worker",
    )
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
            f"ExecStart={request.python_executable} -m src.services.maintenance.main worker",
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


def test_start_records_only_exact_start_operation(tmp_path: Path) -> None:
    request = _request(tmp_path, mode="start", confirm_start=True)
    adapter = FakeSystemdAdapter(request.systemd_user_dir)
    plan = build_systemd_unit_plan(request)
    adapter.write_unit_file(plan.service_name, plan.service_unit_content)
    adapter.calls.clear()

    report = run_systemd_rollout(request, adapter=adapter)

    assert report.status == "pass"
    assert report.start_attempted is True
    assert report.service_active is True
    assert adapter.calls == [
        ("start_unit", SERVICE_NAME),
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
    assert "ExecStart=" in plan.service_unit_content
    assert "/bin/sh" not in plan.service_unit_content
    assert " source " not in plan.service_unit_content
    assert "User=" not in plan.service_unit_content
    assert "Group=" not in plan.service_unit_content
