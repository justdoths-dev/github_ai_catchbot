from __future__ import annotations

import ast
import json
import sys
from dataclasses import asdict
from pathlib import Path

import pytest

from services.maintenance import worker_bootstrap
from services.maintenance.worker_runtime_crash_probe import read_worker_runtime_fatal_report


RAW_DATABASE_URL = "sentinel-database-secret-value"
RAW_REDIS_URL = "sentinel-redis-secret-value"
RAW_RUNTIME_ENV_PATH = "/tmp/sentinel-runtime-secret.env"


def _read_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def _assert_no_secret_leaks(output: str, *extra_values: object) -> None:
    forbidden = [
        RAW_DATABASE_URL,
        RAW_REDIS_URL,
        RAW_RUNTIME_ENV_PATH,
        "DATABASE_URL",
        "REDIS_URL",
        "Traceback",
        *[str(value) for value in extra_values],
    ]
    for value in forbidden:
        if value:
            assert value not in output


def test_bootstrap_top_level_imports_are_stdlib_only() -> None:
    source = Path(worker_bootstrap.__file__).read_text(encoding="utf-8")
    tree = ast.parse(source)
    imported_roots: set[str] = set()
    for node in tree.body:
        if isinstance(node, ast.Import):
            imported_roots.update(alias.name.split(".", 1)[0] for alias in node.names)
        elif isinstance(node, ast.ImportFrom):
            imported_roots.add(node.module.split(".", 1)[0] if node.module else "")

    assert imported_roots <= {"__future__", "asyncio", "importlib", "json", "sys", "datetime", "pathlib"}
    assert "from .worker_runtime_crash_probe" not in source
    assert "from services.maintenance" not in source


def test_bootstrap_exit_status_reason_codes_are_bounded() -> None:
    assert worker_bootstrap.BOOTSTRAP_EXIT_STATUS_REASON_CODES == {
        21: "worker_bootstrap_import_error",
        22: "worker_bootstrap_main_error",
        23: "worker_bootstrap_report_write_failed",
    }


@pytest.mark.asyncio
async def test_bootstrap_adds_repo_root_for_direct_script_import(monkeypatch, tmp_path: Path) -> None:
    report_path = tmp_path / "state/maintenance/worker-runtime-fatal-report.json"
    script_dir = str(worker_bootstrap.REPO_ROOT / "src/services/maintenance")
    monkeypatch.setattr(worker_bootstrap.sys, "path", [script_dir])
    observed_path: list[str] = []

    class FakeMaintenanceMain:
        async def _run(self, argv: list[str]) -> int:
            assert argv == ["worker"]
            return 0

    def import_with_repo_root(module_name: str):
        assert module_name == worker_bootstrap.MAINTENANCE_MAIN_MODULE
        observed_path.extend(sys.path)
        return FakeMaintenanceMain()

    exit_code = await worker_bootstrap.run_bootstrap(
        import_module=import_with_repo_root,
        report_path=report_path,
    )

    assert exit_code == 0
    assert observed_path[0] == str(worker_bootstrap.REPO_ROOT)
    assert observed_path.count(str(worker_bootstrap.REPO_ROOT)) == 1
    assert not report_path.exists()


@pytest.mark.asyncio
async def test_bootstrap_import_failure_writes_redacted_fatal_report(tmp_path: Path, capsys) -> None:
    report_path = tmp_path / "state/maintenance/worker-runtime-fatal-report.json"
    raw_exception = f"Traceback {RAW_DATABASE_URL} {RAW_REDIS_URL} {RAW_RUNTIME_ENV_PATH}"

    def fail_import(module_name: str):
        assert module_name == worker_bootstrap.MAINTENANCE_MAIN_MODULE
        raise RuntimeError(raw_exception)

    exit_code = await worker_bootstrap.run_bootstrap(import_module=fail_import, report_path=report_path)
    output = capsys.readouterr().out
    payload = _read_json(report_path)
    serialized = json.dumps(payload, sort_keys=True)

    assert exit_code == worker_bootstrap.EXIT_WORKER_BOOTSTRAP_IMPORT_ERROR
    assert output == ""
    assert payload["schema_version"] == worker_bootstrap.FATAL_REPORT_SCHEMA_VERSION
    assert payload["reason_code"] == "worker_bootstrap_import_error"
    assert payload["phase"] == "bootstrap_import"
    assert payload["tasks_started"] == []
    assert payload["broad_worker_run_started"] is False
    assert payload["cleanup_completed"] is None
    assert payload["redactions_applied"]["raw_exception_body_omitted"] is True
    assert payload["redactions_applied"]["traceback_omitted"] is True
    assert payload["report_path_omitted"] is True
    _assert_no_secret_leaks(serialized, raw_exception, report_path)


@pytest.mark.asyncio
async def test_bootstrap_main_failure_writes_redacted_fatal_report(tmp_path: Path, capsys) -> None:
    report_path = tmp_path / "state/maintenance/worker-runtime-fatal-report.json"
    raw_exception = f"Traceback {RAW_DATABASE_URL} {RAW_REDIS_URL} {RAW_RUNTIME_ENV_PATH}"
    observed_argv: list[str] = []

    class FakeMaintenanceMain:
        async def _run(self, argv: list[str]) -> int:
            observed_argv.extend(argv)
            raise RuntimeError(raw_exception)

    exit_code = await worker_bootstrap.run_bootstrap(
        import_module=lambda module_name: FakeMaintenanceMain(),
        report_path=report_path,
    )
    output = capsys.readouterr().out
    payload = _read_json(report_path)
    serialized = json.dumps(payload, sort_keys=True)

    assert exit_code == worker_bootstrap.EXIT_WORKER_BOOTSTRAP_MAIN_ERROR
    assert output == ""
    assert observed_argv == ["worker"]
    assert payload["schema_version"] == worker_bootstrap.FATAL_REPORT_SCHEMA_VERSION
    assert payload["reason_code"] == "worker_bootstrap_main_error"
    assert payload["phase"] == "bootstrap_main"
    assert payload["tasks_started"] == []
    assert payload["broad_worker_run_started"] is False
    assert payload["cleanup_completed"] is None
    _assert_no_secret_leaks(serialized, raw_exception, report_path)


@pytest.mark.asyncio
async def test_bootstrap_returns_main_worker_exit_code_without_fatal_report(tmp_path: Path) -> None:
    report_path = tmp_path / "state/maintenance/worker-runtime-fatal-report.json"
    observed_argv: list[str] = []

    class FakeMaintenanceMain:
        async def _run(self, argv: list[str]) -> int:
            observed_argv.extend(argv)
            return 0

    exit_code = await worker_bootstrap.run_bootstrap(
        import_module=lambda module_name: FakeMaintenanceMain(),
        report_path=report_path,
    )

    assert exit_code == 0
    assert observed_argv == ["worker"]
    assert not report_path.exists()


@pytest.mark.asyncio
async def test_bootstrap_nonzero_main_exit_returns_bootstrap_main_status_when_app_report_missing(
    tmp_path: Path,
) -> None:
    report_path = tmp_path / "state/maintenance/worker-runtime-fatal-report.json"

    class FakeMaintenanceMain:
        async def _run(self, argv: list[str]) -> int:
            assert argv == ["worker"]
            return 1

    exit_code = await worker_bootstrap.run_bootstrap(
        import_module=lambda module_name: FakeMaintenanceMain(),
        report_path=report_path,
    )
    payload = _read_json(report_path)

    assert exit_code == worker_bootstrap.EXIT_WORKER_BOOTSTRAP_MAIN_ERROR
    assert payload["reason_code"] == "worker_bootstrap_main_error"
    assert payload["phase"] == "bootstrap_main"


@pytest.mark.asyncio
async def test_bootstrap_nonzero_main_exit_preserves_existing_app_level_report(tmp_path: Path) -> None:
    report_path = tmp_path / "state/maintenance/worker-runtime-fatal-report.json"
    app_report = worker_bootstrap.build_worker_bootstrap_fatal_report(
        reason_code="worker_bootstrap_import_error",
        phase="bootstrap_import",
    )
    app_report["reason_code"] = "worker_runtime_config_error"
    app_report["phase"] = "config_load"
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(json.dumps(app_report, sort_keys=True) + "\n", encoding="utf-8")

    class FakeMaintenanceMain:
        async def _run(self, argv: list[str]) -> int:
            assert argv == ["worker"]
            return 7

    exit_code = await worker_bootstrap.run_bootstrap(
        import_module=lambda module_name: FakeMaintenanceMain(),
        report_path=report_path,
    )
    payload = _read_json(report_path)

    assert exit_code == 7
    assert payload["reason_code"] == "worker_runtime_config_error"
    assert payload["phase"] == "config_load"


@pytest.mark.asyncio
async def test_bootstrap_report_write_failure_returns_bounded_status_without_leaks(
    tmp_path: Path,
    capsys,
) -> None:
    parent_file = tmp_path / "sentinel-secret-report-parent"
    parent_file.write_text("not a directory\n", encoding="utf-8")
    report_path = parent_file / "worker-runtime-fatal-report.json"
    raw_exception = f"Traceback {RAW_DATABASE_URL} {RAW_REDIS_URL} {RAW_RUNTIME_ENV_PATH}"

    def fail_import(module_name: str):
        assert module_name == worker_bootstrap.MAINTENANCE_MAIN_MODULE
        raise RuntimeError(raw_exception)

    exit_code = await worker_bootstrap.run_bootstrap(import_module=fail_import, report_path=report_path)
    output = capsys.readouterr().out

    assert exit_code == worker_bootstrap.EXIT_WORKER_BOOTSTRAP_REPORT_WRITE_FAILED
    assert output == ""
    assert not report_path.exists()
    _assert_no_secret_leaks(output, raw_exception, report_path, parent_file)


@pytest.mark.parametrize(
    ("reason_code", "phase"),
    [
        ("worker_bootstrap_import_error", "bootstrap_import"),
        ("worker_bootstrap_main_error", "bootstrap_main"),
    ],
)
def test_readback_accepts_bootstrap_report_reason_and_phase(
    tmp_path: Path,
    reason_code: str,
    phase: str,
) -> None:
    report_path = tmp_path / "state/maintenance/worker-runtime-fatal-report.json"
    worker_bootstrap.write_worker_bootstrap_fatal_report(
        reason_code=reason_code,
        phase=phase,
        report_path=report_path,
    )

    report = read_worker_runtime_fatal_report(report_path=report_path)
    output = json.dumps(asdict(report), sort_keys=True)

    assert report.status == "pass"
    assert report.reason_code is None
    assert report.latest_report_reason_code == reason_code
    assert report.latest_report_phase == phase
    assert report.latest_report_tasks_started == []
    assert report.latest_report_broad_worker_run_started is False
    _assert_no_secret_leaks(output, report_path)
