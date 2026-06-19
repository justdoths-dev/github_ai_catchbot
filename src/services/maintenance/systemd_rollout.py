from __future__ import annotations

import subprocess
from dataclasses import dataclass, field
from pathlib import Path
from typing import Protocol


SCHEMA_VERSION = "maintenance_systemd_rollout_report_v1"
TARGET_MAINTENANCE_WORKER = "maintenance-worker"
SERVICE_NAME = "github-ai-catchbot-maintenance.service"
TIMER_NAME = "github-ai-catchbot-maintenance.timer"
WORKER_MODULE = "src.services.maintenance.main"
WORKER_COMMAND = "worker"

_ALLOWED_MODES = {"plan", "install", "start", "proof", "rollback"}
_WRITE_MODES = {"install", "start", "rollback"}
_READ_ONLY_MODES = {"plan", "proof"}


@dataclass(frozen=True, slots=True)
class SystemdRolloutRequest:
    mode: str
    target: str
    confirm_install: bool
    confirm_start: bool
    confirm_rollback: bool
    repo_root: Path
    python_executable: Path
    runtime_env_file: Path
    systemd_user_dir: Path
    service_name: str = SERVICE_NAME
    timer_name: str | None = None
    dry_run: bool = True


@dataclass(frozen=True, slots=True)
class SystemdUnitPlan:
    service_name: str
    timer_name: str | None
    service_unit_path: Path
    timer_unit_path: Path | None
    service_unit_content: str
    timer_unit_content: str | None
    exec_start_argv: tuple[str, ...]
    restart_policy: str
    restart_sec: int


@dataclass(frozen=True, slots=True)
class SystemdReadback:
    service_file_present: bool
    timer_file_present: bool
    service_enabled: bool
    service_active: bool
    rollback_plan_available: bool


@dataclass(frozen=True, slots=True)
class SystemdRolloutReport:
    schema_version: str
    mode: str
    target: str
    status: str
    reason_code: str | None
    service_name: str
    timer_name: str | None
    unit_plan_created: bool
    install_attempted: bool
    start_attempted: bool
    enable_attempted: bool
    rollback_attempted: bool
    proof_attempted: bool
    service_file_present: bool
    timer_file_present: bool
    service_enabled: bool
    service_active: bool
    rollback_plan_available: bool
    redactions_applied: dict[str, bool] = field(default_factory=dict)


class SystemdRolloutAdapter(Protocol):
    def write_unit_file(self, unit_name: str, content: str) -> None: ...
    def remove_unit_file(self, unit_name: str) -> None: ...
    def daemon_reload(self) -> None: ...
    def enable_unit(self, unit_name: str) -> None: ...
    def disable_unit(self, unit_name: str) -> None: ...
    def start_unit(self, unit_name: str) -> None: ...
    def stop_unit(self, unit_name: str) -> None: ...
    def readback(self, plan: SystemdUnitPlan) -> SystemdReadback: ...


class SystemdAdapterError(RuntimeError):
    pass


class LocalUserSystemdAdapter:
    def __init__(self, systemd_user_dir: Path) -> None:
        self._systemd_user_dir = Path(systemd_user_dir)

    def write_unit_file(self, unit_name: str, content: str) -> None:
        unit_path = self._unit_path(unit_name)
        try:
            unit_path.parent.mkdir(parents=True, exist_ok=True)
            unit_path.write_text(content, encoding="utf-8")
        except OSError as exc:
            raise SystemdAdapterError("unit_file_write_failed") from exc

    def remove_unit_file(self, unit_name: str) -> None:
        unit_path = self._unit_path(unit_name)
        try:
            if unit_path.exists():
                unit_path.unlink()
        except OSError as exc:
            raise SystemdAdapterError("unit_file_remove_failed") from exc

    def daemon_reload(self) -> None:
        self._run_systemctl("daemon-reload")

    def enable_unit(self, unit_name: str) -> None:
        self._run_systemctl("enable", self._checked_unit_name(unit_name))

    def disable_unit(self, unit_name: str) -> None:
        self._run_systemctl("disable", self._checked_unit_name(unit_name), allow_failure=True)

    def start_unit(self, unit_name: str) -> None:
        self._run_systemctl("start", self._checked_unit_name(unit_name))

    def stop_unit(self, unit_name: str) -> None:
        self._run_systemctl("stop", self._checked_unit_name(unit_name), allow_failure=True)

    def readback(self, plan: SystemdUnitPlan) -> SystemdReadback:
        service_file_present = plan.service_unit_path.is_file()
        timer_file_present = bool(plan.timer_unit_path and plan.timer_unit_path.is_file())
        return SystemdReadback(
            service_file_present=service_file_present,
            timer_file_present=timer_file_present,
            service_enabled=self._systemctl_bool("is-enabled", plan.service_name),
            service_active=self._systemctl_bool("is-active", plan.service_name),
            rollback_plan_available=service_file_present or timer_file_present,
        )

    def _unit_path(self, unit_name: str) -> Path:
        checked = self._checked_unit_name(unit_name)
        return self._systemd_user_dir / checked

    @staticmethod
    def _checked_unit_name(unit_name: str) -> str:
        if unit_name not in {SERVICE_NAME, TIMER_NAME}:
            raise SystemdAdapterError("unit_name_not_allowed")
        if "/" in unit_name or "\\" in unit_name:
            raise SystemdAdapterError("unit_name_not_allowed")
        return unit_name

    def _systemctl_bool(self, action: str, unit_name: str) -> bool:
        try:
            result = subprocess.run(
                ["systemctl", "--user", action, self._checked_unit_name(unit_name)],
                check=False,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                text=True,
            )
        except (OSError, SystemdAdapterError):
            return False
        return result.returncode == 0

    def _run_systemctl(self, *args: str, allow_failure: bool = False) -> None:
        try:
            result = subprocess.run(
                ["systemctl", "--user", *args],
                check=False,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                text=True,
            )
        except OSError as exc:
            raise SystemdAdapterError("systemctl_invocation_failed") from exc
        if result.returncode != 0 and not allow_failure:
            raise SystemdAdapterError("systemctl_operation_failed")


def build_systemd_unit_plan(request: SystemdRolloutRequest) -> SystemdUnitPlan:
    exec_start = (
        str(request.python_executable),
        "-m",
        WORKER_MODULE,
        WORKER_COMMAND,
    )
    service_content = "\n".join(
        [
            "[Unit]",
            "Description=github_ai_catchbot maintenance worker",
            "After=network-online.target",
            "Wants=network-online.target",
            "",
            "[Service]",
            "Type=simple",
            f"WorkingDirectory={_systemd_value(request.repo_root)}",
            f"EnvironmentFile={_systemd_value(request.runtime_env_file)}",
            "ExecStart=" + " ".join(_systemd_exec_arg(part) for part in exec_start),
            "Restart=on-failure",
            "RestartSec=10",
            "",
            "[Install]",
            "WantedBy=default.target",
            "",
        ]
    )
    return SystemdUnitPlan(
        service_name=request.service_name,
        timer_name=request.timer_name,
        service_unit_path=request.systemd_user_dir / request.service_name,
        timer_unit_path=(request.systemd_user_dir / request.timer_name) if request.timer_name else None,
        service_unit_content=service_content,
        timer_unit_content=None,
        exec_start_argv=exec_start,
        restart_policy="on-failure",
        restart_sec=10,
    )


def run_systemd_rollout(
    request: SystemdRolloutRequest,
    *,
    adapter: SystemdRolloutAdapter | None = None,
) -> SystemdRolloutReport:
    request_error = systemd_rollout_request_error(request)
    if request_error is not None:
        return _report(
            request,
            status="blocked",
            reason_code=request_error,
            unit_plan_created=False,
        )

    plan = build_systemd_unit_plan(request)
    systemd = adapter or LocalUserSystemdAdapter(request.systemd_user_dir)

    if request.mode == "plan":
        return _report(
            request,
            status="pass",
            reason_code=None,
            unit_plan_created=True,
            rollback_plan_available=True,
        )

    try:
        if request.mode == "install":
            systemd.write_unit_file(plan.service_name, plan.service_unit_content)
            if plan.timer_name and plan.timer_unit_content:
                systemd.write_unit_file(plan.timer_name, plan.timer_unit_content)
            systemd.daemon_reload()
            systemd.enable_unit(plan.service_name)
            readback = systemd.readback(plan)
            status = "pass" if readback.service_file_present and readback.service_enabled else "blocked"
            return _report(
                request,
                status=status,
                reason_code=None if status == "pass" else "install_readback_missing",
                unit_plan_created=True,
                install_attempted=True,
                enable_attempted=True,
                readback=readback,
            )

        if request.mode == "start":
            systemd.start_unit(plan.service_name)
            readback = systemd.readback(plan)
            status = "pass" if readback.service_active else "blocked"
            return _report(
                request,
                status=status,
                reason_code=None if status == "pass" else "service_not_active",
                unit_plan_created=True,
                start_attempted=True,
                readback=readback,
            )

        if request.mode == "proof":
            readback = systemd.readback(plan)
            service_ready = readback.service_file_present and readback.service_enabled and readback.service_active
            timer_ready = True if plan.timer_name is None else readback.timer_file_present
            status = "pass" if service_ready and timer_ready else "blocked"
            return _report(
                request,
                status=status,
                reason_code=None if status == "pass" else "service_rollout_incomplete",
                unit_plan_created=True,
                proof_attempted=True,
                readback=readback,
            )

        if request.mode == "rollback":
            if plan.timer_name:
                systemd.stop_unit(plan.timer_name)
                systemd.disable_unit(plan.timer_name)
            systemd.stop_unit(plan.service_name)
            systemd.disable_unit(plan.service_name)
            if plan.timer_name:
                systemd.remove_unit_file(plan.timer_name)
            systemd.remove_unit_file(plan.service_name)
            systemd.daemon_reload()
            readback = systemd.readback(plan)
            rollback_ok = (
                not readback.service_file_present
                and not readback.timer_file_present
                and not readback.service_enabled
                and not readback.service_active
            )
            return _report(
                request,
                status="pass" if rollback_ok else "blocked",
                reason_code=None if rollback_ok else "rollback_readback_failed",
                unit_plan_created=True,
                rollback_attempted=True,
                readback=readback,
            )
    except SystemdAdapterError as exc:
        return _report(
            request,
            status="failed",
            reason_code=_adapter_reason_code(exc),
            unit_plan_created=True,
            install_attempted=request.mode == "install",
            start_attempted=request.mode == "start",
            enable_attempted=request.mode == "install",
            rollback_attempted=request.mode == "rollback",
            proof_attempted=request.mode == "proof",
        )

    return _report(
        request,
        status="blocked",
        reason_code="mode_not_allowed",
        unit_plan_created=True,
    )


def systemd_rollout_request_error(request: SystemdRolloutRequest) -> str | None:
    if request.mode not in _ALLOWED_MODES:
        return "mode_not_allowed"
    if request.target != TARGET_MAINTENANCE_WORKER:
        return "target_not_allowed"
    if request.service_name != SERVICE_NAME:
        return "service_name_not_allowed"
    if request.timer_name not in {None, TIMER_NAME}:
        return "timer_name_not_allowed"
    if request.mode == "install" and not request.confirm_install:
        return "install_confirm_missing"
    if request.mode == "start" and not request.confirm_start:
        return "start_confirm_missing"
    if request.mode == "rollback" and not request.confirm_rollback:
        return "rollback_confirm_missing"
    if request.mode in _READ_ONLY_MODES and (
        request.confirm_install or request.confirm_start or request.confirm_rollback
    ):
        return "confirm_not_allowed_for_read_only"
    if request.mode == "install" and (request.confirm_start or request.confirm_rollback):
        return "confirm_not_allowed_for_mode"
    if request.mode == "start" and (request.confirm_install or request.confirm_rollback):
        return "confirm_not_allowed_for_mode"
    if request.mode == "rollback" and (request.confirm_install or request.confirm_start):
        return "confirm_not_allowed_for_mode"
    if request.mode in _READ_ONLY_MODES and not request.dry_run:
        return "read_only_mode_requires_dry_run"
    if request.mode in _WRITE_MODES and request.dry_run:
        return "write_mode_requires_dry_run_false"
    if not request.repo_root.is_absolute():
        return "repo_root_not_absolute"
    if not request.python_executable.is_absolute():
        return "python_executable_not_absolute"
    if not request.runtime_env_file.is_absolute():
        return "runtime_env_file_not_absolute"
    if not request.systemd_user_dir.is_absolute():
        return "systemd_user_dir_not_absolute"
    if not request.repo_root.is_dir():
        return "repo_root_missing"
    if not (request.repo_root / "src/services/maintenance/main.py").is_file():
        return "maintenance_entrypoint_missing"
    if not request.python_executable.is_file():
        return "python_executable_missing"
    if request.mode in {"plan", "install", "start"} and not request.runtime_env_file.is_file():
        return "env_file_missing"
    return None


def _report(
    request: SystemdRolloutRequest,
    *,
    status: str,
    reason_code: str | None,
    unit_plan_created: bool,
    install_attempted: bool = False,
    start_attempted: bool = False,
    enable_attempted: bool = False,
    rollback_attempted: bool = False,
    proof_attempted: bool = False,
    service_file_present: bool = False,
    timer_file_present: bool = False,
    service_enabled: bool = False,
    service_active: bool = False,
    rollback_plan_available: bool = False,
    readback: SystemdReadback | None = None,
) -> SystemdRolloutReport:
    if readback is not None:
        service_file_present = readback.service_file_present
        timer_file_present = readback.timer_file_present
        service_enabled = readback.service_enabled
        service_active = readback.service_active
        rollback_plan_available = readback.rollback_plan_available
    return SystemdRolloutReport(
        schema_version=SCHEMA_VERSION,
        mode=request.mode,
        target=request.target,
        status=status,
        reason_code=reason_code,
        service_name=request.service_name,
        timer_name=request.timer_name,
        unit_plan_created=unit_plan_created,
        install_attempted=install_attempted,
        start_attempted=start_attempted,
        enable_attempted=enable_attempted,
        rollback_attempted=rollback_attempted,
        proof_attempted=proof_attempted,
        service_file_present=service_file_present,
        timer_file_present=timer_file_present,
        service_enabled=service_enabled,
        service_active=service_active,
        rollback_plan_available=rollback_plan_available,
        redactions_applied={
            "runtime_env_path_omitted": True,
            "runtime_env_values_omitted": True,
            "secret_values_omitted": True,
            "systemd_user_dir_omitted": True,
            "unit_file_content_omitted": True,
            "systemctl_stdout_omitted": True,
            "systemctl_stderr_omitted": True,
            "exception_body_omitted": True,
        },
    )


def _systemd_value(value: Path) -> str:
    text = str(value)
    return _quote_systemd(text) if _needs_systemd_quotes(text) else text


def _systemd_exec_arg(value: str) -> str:
    return _quote_systemd(value) if _needs_systemd_quotes(value) else value


def _needs_systemd_quotes(value: str) -> bool:
    return any(char.isspace() or char in {'"', "\\"} for char in value)


def _quote_systemd(value: str) -> str:
    escaped = value.replace("\\", "\\\\").replace('"', '\\"')
    return f'"{escaped}"'


def _adapter_reason_code(exc: SystemdAdapterError) -> str:
    reason_code = str(exc)
    if reason_code in {
        "unit_file_write_failed",
        "unit_file_remove_failed",
        "systemctl_invocation_failed",
        "systemctl_operation_failed",
        "unit_name_not_allowed",
    }:
        return reason_code
    return "systemd_adapter_failed"
