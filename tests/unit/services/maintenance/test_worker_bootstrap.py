from __future__ import annotations

import ast
import hashlib
import importlib
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
RAW_MODULE_FILE_PATH = "/tmp/sentinel-module-file/src/services/maintenance/main.py"
RAW_SYS_PATH_ENTRY = "/tmp/sentinel-pythonpath-contamination"
RAW_SYSTEMD_OUTPUT = "sentinel-systemd-stdout-stderr"
RAW_JOURNAL_OUTPUT = "sentinel-journal-output"
SAFE_INVOCATION_ID = "0123456789abcdef0123456789abcdef"
ACTIONABLE_BOOTSTRAP_IMPORT_STAGE_LABELS = tuple(
    stage
    for stage, module_name in worker_bootstrap.BOOTSTRAP_IMPORT_STAGE_SEQUENCE
    if stage == "bootstrap_repo_root_path_ready" or module_name is not None
)


def _read_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def _assert_no_secret_leaks(output: str, *extra_values: object) -> None:
    forbidden = [
        RAW_DATABASE_URL,
        RAW_REDIS_URL,
        RAW_RUNTIME_ENV_PATH,
        RAW_MODULE_FILE_PATH,
        RAW_SYS_PATH_ENTRY,
        RAW_SYSTEMD_OUTPUT,
        RAW_JOURNAL_OUTPUT,
        "DATABASE_URL",
        "REDIS_URL",
        "sys.path",
        "systemctl",
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

    assert imported_roots <= {
        "__future__",
        "asyncio",
        "hashlib",
        "importlib",
        "json",
        "os",
        "sys",
        "sysconfig",
        "datetime",
        "pathlib",
    }
    assert "from .worker_runtime_crash_probe" not in source
    assert "from services.maintenance" not in source


def test_maintenance_package_import_is_side_effect_free(monkeypatch) -> None:
    for module_name in list(sys.modules):
        if module_name == "src.services.maintenance" or module_name.startswith("src.services.maintenance."):
            monkeypatch.delitem(sys.modules, module_name, raising=False)

    package = importlib.import_module("src.services.maintenance")

    assert package.__all__ == ["MaintenanceConfig", "MaintenanceService"]
    assert "src.services.maintenance.config" not in sys.modules
    assert "src.services.maintenance.service" not in sys.modules
    assert "src.services.maintenance.redis_streams" not in sys.modules
    assert "src.services.maintenance.repositories" not in sys.modules
    assert "src.services.maintenance.worker" not in sys.modules


def test_maintenance_repositories_import_is_side_effect_safe(monkeypatch, tmp_path: Path) -> None:
    report_path = tmp_path / "state/maintenance/worker-runtime-fatal-report.json"
    for module_name in list(sys.modules):
        if module_name == "src.services.maintenance" or module_name.startswith("src.services.maintenance."):
            monkeypatch.delitem(sys.modules, module_name, raising=False)

    repositories = importlib.import_module("src.services.maintenance.repositories")

    assert repositories.MaintenanceRepository.__name__ == "MaintenanceRepository"
    assert not report_path.exists()
    assert "src.services.maintenance.main" not in sys.modules
    assert "src.services.maintenance.service" not in sys.modules
    assert "src.services.maintenance.worker" not in sys.modules
    assert "src.services.maintenance.redis_streams" not in sys.modules


def test_bootstrap_exit_status_reason_codes_are_bounded() -> None:
    assert worker_bootstrap.BOOTSTRAP_EXIT_STATUS_REASON_CODES == {
        21: "worker_bootstrap_import_error",
        22: "worker_bootstrap_main_error",
        23: "worker_bootstrap_report_write_failed",
    }


def test_bootstrap_import_stage_labels_are_fixed_allowlist() -> None:
    assert worker_bootstrap.BOOTSTRAP_IMPORT_STAGE_LABELS == (
        "bootstrap_repo_root_path_ready",
        "stdlib_ready",
        "maintenance_package_ready",
        "maintenance_package_init_import",
        "maintenance_config_import",
        "maintenance_redis_streams_import",
        "maintenance_repositories_import",
        "maintenance_repositories_spec_ready",
        "maintenance_repositories_sqlalchemy_import",
        "maintenance_repositories_outbox_eligibility_import",
        "maintenance_repositories_models_import",
        "maintenance_repositories_delivery_retry_import",
        "maintenance_repositories_delivery_replay_import",
        "maintenance_repositories_retry_policy_import",
        "maintenance_repositories_module_import",
        "maintenance_service_import",
        "maintenance_worker_import",
        "maintenance_main_import",
    )
    assert len(set(worker_bootstrap.BOOTSTRAP_IMPORT_STAGE_LABELS)) == len(
        worker_bootstrap.BOOTSTRAP_IMPORT_STAGE_LABELS
    )
    assert worker_bootstrap.BOOTSTRAP_IMPORT_STAGE_REASON_CODES == {
        "repo_root_path_unavailable",
        "stage_import_error",
        "stage_dependency_unavailable",
        "stage_module_unavailable",
        "stage_spec_unavailable",
    }
    assert worker_bootstrap.BOOTSTRAP_SPEC_READY_STAGES == frozenset(
        {
            "maintenance_package_ready",
            "maintenance_repositories_spec_ready",
        }
    )


def _make_pyvenv(tmp_path: Path) -> tuple[Path, Path, Path]:
    venv_root = tmp_path / "venv"
    purelib = venv_root / "lib/python3.12/site-packages"
    platlib = venv_root / "lib64/python3.12/site-packages"
    (venv_root / "bin").mkdir(parents=True)
    purelib.mkdir(parents=True)
    platlib.mkdir(parents=True)
    (venv_root / "pyvenv.cfg").write_text("home = /redacted\n", encoding="utf-8")
    (venv_root / "bin/python").write_text("# python\n", encoding="utf-8")
    (venv_root / "bin/python").chmod(0o755)
    return venv_root.resolve(), purelib.resolve(), platlib.resolve()


def _make_repo_local_pyvenv(repo_root: Path) -> tuple[Path, Path, Path]:
    return _make_pyvenv(repo_root)


def _patch_sysconfig_paths(monkeypatch, *, venv_root: Path, purelib: object, platlib: object) -> None:
    def fake_get_path(path_name: str, *, scheme: str | None = None, vars: dict[str, str] | None = None):
        assert scheme == "venv"
        assert vars == {"base": str(venv_root), "platbase": str(venv_root)}
        if path_name == "purelib":
            return purelib
        if path_name == "platlib":
            return platlib
        raise AssertionError(f"unexpected sysconfig path name {path_name}")

    monkeypatch.setattr(worker_bootstrap.sysconfig, "get_path", fake_get_path)


def _patch_sys_prefix_venv(monkeypatch, *, venv_root: Path) -> None:
    monkeypatch.setattr(worker_bootstrap.sys, "prefix", str(venv_root))
    monkeypatch.setattr(worker_bootstrap.sys, "base_prefix", str(venv_root.parent / "base-python"))
    monkeypatch.setattr(worker_bootstrap.sys, "executable", str(venv_root / "bin/python"))


def _patch_base_interpreter_context(monkeypatch, *, tmp_path: Path) -> None:
    monkeypatch.setattr(worker_bootstrap.sys, "prefix", str(tmp_path / "base-python"))
    monkeypatch.setattr(worker_bootstrap.sys, "base_prefix", str(tmp_path / "base-python"))
    monkeypatch.setattr(worker_bootstrap.sys, "executable", str(tmp_path / "base-python/bin/python"))


def _patch_sqlalchemy_distribution_unknown(monkeypatch) -> None:
    monkeypatch.setattr(worker_bootstrap, "_sqlalchemy_distribution_present", lambda: None)


def test_venv_site_path_repair_places_validated_paths_after_unique_repo_root(
    monkeypatch,
    tmp_path: Path,
) -> None:
    venv_root, purelib, platlib = _make_pyvenv(tmp_path)
    stdlib_entry = str(tmp_path / "stdlib")
    arbitrary_entry = str(tmp_path / "arbitrary-pythonpath-like-entry")
    _patch_sys_prefix_venv(monkeypatch, venv_root=venv_root)
    _patch_sysconfig_paths(monkeypatch, venv_root=venv_root, purelib=str(purelib), platlib=str(platlib))
    _patch_sqlalchemy_distribution_unknown(monkeypatch)
    monkeypatch.setattr(
        worker_bootstrap.sys,
        "path",
        [stdlib_entry, str(worker_bootstrap.REPO_ROOT), str(purelib), str(worker_bootstrap.REPO_ROOT), arbitrary_entry],
    )

    context = worker_bootstrap._repair_venv_site_paths()

    assert context == {
        "venv_context_source": "sys_prefix",
        "venv_context_active": True,
        "venv_site_candidate_present": True,
        "venv_site_on_sys_path_before": False,
        "venv_site_path_repaired": True,
        "venv_site_on_sys_path_after": True,
        "import_caches_invalidated": True,
        "sqlalchemy_distribution_present": None,
    }
    assert sys.path[:3] == [str(worker_bootstrap.REPO_ROOT), str(purelib), str(platlib)]
    assert sys.path.count(str(worker_bootstrap.REPO_ROOT)) == 1
    assert sys.path.count(str(purelib)) == 1
    assert sys.path.count(str(platlib)) == 1
    assert stdlib_entry in sys.path
    assert arbitrary_entry in sys.path


def test_venv_site_path_repair_deduplicates_equal_purelib_and_platlib(
    monkeypatch,
    tmp_path: Path,
) -> None:
    venv_root, purelib, _platlib = _make_pyvenv(tmp_path)
    stdlib_entry = str(tmp_path / "stdlib")
    _patch_sys_prefix_venv(monkeypatch, venv_root=venv_root)
    _patch_sysconfig_paths(monkeypatch, venv_root=venv_root, purelib=str(purelib), platlib=str(purelib))
    _patch_sqlalchemy_distribution_unknown(monkeypatch)
    monkeypatch.setattr(worker_bootstrap.sys, "path", [str(worker_bootstrap.REPO_ROOT), stdlib_entry])

    context = worker_bootstrap._repair_venv_site_paths()

    assert context["venv_site_candidate_present"] is True
    assert context["venv_site_on_sys_path_after"] is True
    assert sys.path[:2] == [str(worker_bootstrap.REPO_ROOT), str(purelib)]
    assert sys.path.count(str(purelib)) == 1
    assert stdlib_entry in sys.path


def test_venv_site_path_repair_does_not_duplicate_existing_validated_entries(
    monkeypatch,
    tmp_path: Path,
) -> None:
    venv_root, purelib, platlib = _make_pyvenv(tmp_path)
    stdlib_entry = str(tmp_path / "stdlib")
    _patch_sys_prefix_venv(monkeypatch, venv_root=venv_root)
    _patch_sysconfig_paths(monkeypatch, venv_root=venv_root, purelib=str(purelib), platlib=str(platlib))
    _patch_sqlalchemy_distribution_unknown(monkeypatch)
    monkeypatch.setattr(
        worker_bootstrap.sys,
        "path",
        [str(worker_bootstrap.REPO_ROOT), str(purelib), str(platlib), stdlib_entry],
    )

    context = worker_bootstrap._repair_venv_site_paths()

    assert context["venv_site_on_sys_path_before"] is True
    assert context["venv_site_path_repaired"] is False
    assert context["venv_site_on_sys_path_after"] is True
    assert sys.path == [str(worker_bootstrap.REPO_ROOT), str(purelib), str(platlib), stdlib_entry]


@pytest.mark.parametrize(
    ("label", "purelib_factory", "access_allowed"),
    [
        ("outside_venv_root", lambda tmp_path, venv_root: tmp_path / "outside-site", True),
        ("relative_path", lambda tmp_path, venv_root: Path("relative-site-packages"), True),
        ("missing_directory", lambda tmp_path, venv_root: venv_root / "missing-site-packages", True),
        ("unreadable_directory", lambda tmp_path, venv_root: venv_root / "unreadable-site-packages", False),
        ("base_interpreter_path", lambda tmp_path, venv_root: tmp_path / "base-python/site-packages", True),
        ("user_site_path", lambda tmp_path, venv_root: tmp_path / "home/.local/lib/python/site-packages", True),
    ],
)
def test_venv_site_path_repair_rejects_unvalidated_site_paths(
    monkeypatch,
    tmp_path: Path,
    label: str,
    purelib_factory,
    access_allowed: bool,
) -> None:
    del label
    venv_root, _purelib, _platlib = _make_pyvenv(tmp_path)
    candidate = purelib_factory(tmp_path, venv_root)
    if access_allowed and candidate.is_absolute() and "missing" not in candidate.name:
        candidate.mkdir(parents=True, exist_ok=True)
    if not access_allowed:
        candidate.mkdir(parents=True, exist_ok=True)
        monkeypatch.setattr(worker_bootstrap.os, "access", lambda path, mode: False)
    stdlib_entry = str(tmp_path / "stdlib")
    _patch_sys_prefix_venv(monkeypatch, venv_root=venv_root)
    _patch_sysconfig_paths(monkeypatch, venv_root=venv_root, purelib=str(candidate), platlib=str(candidate))
    _patch_sqlalchemy_distribution_unknown(monkeypatch)
    monkeypatch.setattr(worker_bootstrap.sys, "path", [str(worker_bootstrap.REPO_ROOT), stdlib_entry])

    context = worker_bootstrap._repair_venv_site_paths()

    assert context["venv_site_candidate_present"] is False
    assert context["venv_site_path_repaired"] is False
    assert context["venv_site_on_sys_path_after"] is False
    assert str(candidate) not in sys.path
    assert sys.path == [str(worker_bootstrap.REPO_ROOT), stdlib_entry]


def test_venv_site_path_repair_uses_executable_adjacent_pyvenv_fallback(
    monkeypatch,
    tmp_path: Path,
) -> None:
    venv_root, purelib, platlib = _make_pyvenv(tmp_path)
    _patch_sysconfig_paths(monkeypatch, venv_root=venv_root, purelib=str(purelib), platlib=str(platlib))
    _patch_sqlalchemy_distribution_unknown(monkeypatch)
    monkeypatch.setattr(worker_bootstrap.sys, "prefix", str(tmp_path / "base-python"))
    monkeypatch.setattr(worker_bootstrap.sys, "base_prefix", str(tmp_path / "base-python"))
    monkeypatch.setattr(worker_bootstrap.sys, "executable", str(venv_root / "bin/python"))
    monkeypatch.setattr(worker_bootstrap.sys, "path", [str(worker_bootstrap.REPO_ROOT)])

    context = worker_bootstrap._repair_venv_site_paths()

    assert context["venv_context_source"] == "executable_pyvenv_cfg"
    assert context["venv_context_active"] is False
    assert sys.path[:3] == [str(worker_bootstrap.REPO_ROOT), str(purelib), str(platlib)]


def test_venv_site_path_repair_rejects_executable_adjacent_root_without_pyvenv_cfg(
    monkeypatch,
    tmp_path: Path,
) -> None:
    repo_root = tmp_path / "repo"
    repo_root.mkdir()
    executable = tmp_path / "venv/bin/python"
    executable.parent.mkdir(parents=True)
    executable.write_text("# python\n", encoding="utf-8")
    executable.chmod(0o755)
    monkeypatch.setattr(worker_bootstrap, "REPO_ROOT", repo_root)
    monkeypatch.setattr(worker_bootstrap.sys, "prefix", str(tmp_path / "base-python"))
    monkeypatch.setattr(worker_bootstrap.sys, "base_prefix", str(tmp_path / "base-python"))
    monkeypatch.setattr(worker_bootstrap.sys, "executable", str(executable))
    monkeypatch.setattr(worker_bootstrap.sys, "path", [str(repo_root)])
    monkeypatch.setattr(
        worker_bootstrap.sysconfig,
        "get_path",
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("sysconfig must not be called")),
    )
    _patch_sqlalchemy_distribution_unknown(monkeypatch)

    context = worker_bootstrap._repair_venv_site_paths()

    assert context["venv_context_source"] == "unavailable"
    assert context["venv_site_candidate_present"] is False
    assert sys.path == [str(repo_root)]


@pytest.mark.asyncio
async def test_repo_local_pyvenv_fallback_repairs_site_paths_and_imports_sqlalchemy(
    monkeypatch,
    tmp_path: Path,
) -> None:
    repo_root = tmp_path / "repo"
    repo_root.mkdir()
    venv_root, purelib, platlib = _make_repo_local_pyvenv(repo_root)
    (purelib / "sqlalchemy").mkdir()
    (purelib / "sqlalchemy/__init__.py").write_text("SENTINEL = 'repo-local-venv'\n", encoding="utf-8")
    report_path = tmp_path / "state/maintenance/worker-runtime-fatal-report.json"
    stdlib_entry = str(tmp_path / "stdlib")
    imported_modules: list[str] = []
    observed_path_at_sqlalchemy_import: list[str] = []
    for module_name in list(sys.modules):
        if module_name == "sqlalchemy" or module_name.startswith("sqlalchemy."):
            monkeypatch.delitem(sys.modules, module_name, raising=False)
    monkeypatch.setattr(worker_bootstrap, "REPO_ROOT", repo_root)
    _patch_base_interpreter_context(monkeypatch, tmp_path=tmp_path)
    _patch_sysconfig_paths(monkeypatch, venv_root=venv_root, purelib=str(purelib), platlib=str(platlib))
    _patch_sqlalchemy_distribution_unknown(monkeypatch)
    monkeypatch.setattr(worker_bootstrap.sys, "path", [str(repo_root), stdlib_entry])

    context = worker_bootstrap._repair_venv_site_paths()
    sqlalchemy = importlib.import_module("sqlalchemy")

    assert context["venv_context_source"] == "repo_local_pyvenv_cfg"
    assert context["venv_context_active"] is False
    assert context["venv_site_candidate_present"] is True
    assert context["venv_site_path_repaired"] is True
    assert context["venv_site_on_sys_path_after"] is True
    assert context["import_caches_invalidated"] is True
    assert sqlalchemy.SENTINEL == "repo-local-venv"
    assert sys.path[:3] == [str(repo_root), str(purelib), str(platlib)]
    assert sys.path.count(str(repo_root)) == 1
    monkeypatch.delitem(sys.modules, "sqlalchemy", raising=False)

    class FakeMaintenanceMain:
        async def _run(self, argv: list[str]) -> int:
            assert argv == ["worker"]
            return 0

    def import_module(module_name: str):
        imported_modules.append(module_name)
        if module_name == "sqlalchemy":
            observed_path_at_sqlalchemy_import.extend(sys.path)
            return importlib.import_module(module_name)
        if module_name == worker_bootstrap.MAINTENANCE_MAIN_MODULE:
            return FakeMaintenanceMain()
        return object()

    exit_code = await worker_bootstrap.run_bootstrap(
        import_module=import_module,
        find_spec=lambda module_name: object(),
        report_path=report_path,
    )

    assert exit_code == 0
    assert imported_modules.index("sqlalchemy") < imported_modules.index("src.services.outbox_relay.eligibility")
    assert observed_path_at_sqlalchemy_import[:3] == [str(repo_root), str(purelib), str(platlib)]
    assert not report_path.exists()


def test_repo_local_pyvenv_fallback_rejects_missing_pyvenv_cfg(
    monkeypatch,
    tmp_path: Path,
) -> None:
    repo_root = tmp_path / "repo"
    python_executable = repo_root / "venv/bin/python"
    python_executable.parent.mkdir(parents=True)
    python_executable.write_text("# python\n", encoding="utf-8")
    python_executable.chmod(0o755)
    monkeypatch.setattr(worker_bootstrap, "REPO_ROOT", repo_root)
    _patch_base_interpreter_context(monkeypatch, tmp_path=tmp_path)
    monkeypatch.setattr(worker_bootstrap.sys, "path", [str(repo_root)])
    monkeypatch.setattr(
        worker_bootstrap.sysconfig,
        "get_path",
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("sysconfig must not be called")),
    )
    _patch_sqlalchemy_distribution_unknown(monkeypatch)

    context = worker_bootstrap._repair_venv_site_paths()

    assert context["venv_context_source"] == "unavailable"
    assert context["venv_site_candidate_present"] is False
    assert sys.path == [str(repo_root)]


@pytest.mark.parametrize("python_state", ["missing", "non_executable"])
def test_repo_local_pyvenv_fallback_rejects_missing_or_non_executable_python(
    monkeypatch,
    tmp_path: Path,
    python_state: str,
) -> None:
    repo_root = tmp_path / "repo"
    venv_root = repo_root / "venv"
    (venv_root / "bin").mkdir(parents=True)
    (venv_root / "pyvenv.cfg").write_text("home = /redacted\n", encoding="utf-8")
    if python_state == "non_executable":
        (venv_root / "bin/python").write_text("# python\n", encoding="utf-8")
        (venv_root / "bin/python").chmod(0o644)
    monkeypatch.setattr(worker_bootstrap, "REPO_ROOT", repo_root)
    _patch_base_interpreter_context(monkeypatch, tmp_path=tmp_path)
    monkeypatch.setattr(worker_bootstrap.sys, "path", [str(repo_root)])
    monkeypatch.setattr(
        worker_bootstrap.sysconfig,
        "get_path",
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("sysconfig must not be called")),
    )
    _patch_sqlalchemy_distribution_unknown(monkeypatch)

    context = worker_bootstrap._repair_venv_site_paths()

    assert context["venv_context_source"] == "unavailable"
    assert context["venv_site_candidate_present"] is False
    assert sys.path == [str(repo_root)]


def test_repo_local_pyvenv_fallback_rejects_outside_site_paths(
    monkeypatch,
    tmp_path: Path,
) -> None:
    repo_root = tmp_path / "repo"
    venv_root, _purelib, _platlib = _make_repo_local_pyvenv(repo_root)
    outside = tmp_path / "outside-site"
    outside.mkdir()
    monkeypatch.setattr(worker_bootstrap, "REPO_ROOT", repo_root)
    _patch_base_interpreter_context(monkeypatch, tmp_path=tmp_path)
    _patch_sysconfig_paths(monkeypatch, venv_root=venv_root, purelib=str(outside), platlib=str(outside))
    _patch_sqlalchemy_distribution_unknown(monkeypatch)
    monkeypatch.setattr(worker_bootstrap.sys, "path", [str(repo_root)])

    context = worker_bootstrap._repair_venv_site_paths()

    assert context["venv_context_source"] == "repo_local_pyvenv_cfg"
    assert context["venv_site_candidate_present"] is False
    assert context["venv_site_path_repaired"] is False
    assert str(outside.resolve()) not in sys.path
    assert sys.path == [str(repo_root)]


def test_repo_local_pyvenv_fallback_does_not_accept_arbitrary_outside_root(
    monkeypatch,
    tmp_path: Path,
) -> None:
    repo_root = tmp_path / "repo"
    repo_root.mkdir()
    outside_root, _purelib, _platlib = _make_pyvenv(tmp_path / "outside")
    monkeypatch.setattr(worker_bootstrap, "REPO_ROOT", repo_root)
    monkeypatch.setattr(worker_bootstrap.sys, "prefix", str(tmp_path / "base-python"))
    monkeypatch.setattr(worker_bootstrap.sys, "base_prefix", str(tmp_path / "base-python"))
    monkeypatch.setattr(worker_bootstrap.sys, "executable", str(outside_root / "Scripts/python"))
    monkeypatch.setattr(worker_bootstrap.sys, "path", [str(repo_root)])
    monkeypatch.setattr(
        worker_bootstrap.sysconfig,
        "get_path",
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("sysconfig must not be called")),
    )
    _patch_sqlalchemy_distribution_unknown(monkeypatch)

    context = worker_bootstrap._repair_venv_site_paths()

    assert context["venv_context_source"] == "unavailable"
    assert context["venv_site_candidate_present"] is False
    assert sys.path == [str(repo_root)]


@pytest.mark.asyncio
async def test_sqlalchemy_import_succeeds_after_validated_venv_site_path_repair(
    monkeypatch,
    tmp_path: Path,
) -> None:
    venv_root, purelib, platlib = _make_pyvenv(tmp_path)
    (purelib / "sqlalchemy").mkdir()
    (purelib / "sqlalchemy/__init__.py").write_text("SENTINEL = 'from-validated-venv'\n", encoding="utf-8")
    report_path = tmp_path / "state/maintenance/worker-runtime-fatal-report.json"
    stdlib_entry = str(tmp_path / "stdlib")
    imported_modules: list[str] = []
    observed_path_at_sqlalchemy_import: list[str] = []
    for module_name in list(sys.modules):
        if module_name == "sqlalchemy" or module_name.startswith("sqlalchemy."):
            monkeypatch.delitem(sys.modules, module_name, raising=False)
    _patch_sys_prefix_venv(monkeypatch, venv_root=venv_root)
    _patch_sysconfig_paths(monkeypatch, venv_root=venv_root, purelib=str(purelib), platlib=str(platlib))
    _patch_sqlalchemy_distribution_unknown(monkeypatch)
    monkeypatch.setattr(worker_bootstrap.sys, "path", [str(worker_bootstrap.REPO_ROOT), stdlib_entry])

    assert importlib.util.find_spec("sqlalchemy") is None

    class FakeMaintenanceMain:
        async def _run(self, argv: list[str]) -> int:
            assert argv == ["worker"]
            return 0

    def import_module(module_name: str):
        imported_modules.append(module_name)
        if module_name == "sqlalchemy":
            observed_path_at_sqlalchemy_import.extend(sys.path)
            return importlib.import_module(module_name)
        if module_name == worker_bootstrap.MAINTENANCE_MAIN_MODULE:
            return FakeMaintenanceMain()
        return object()

    exit_code = await worker_bootstrap.run_bootstrap(
        import_module=import_module,
        find_spec=lambda module_name: object(),
        report_path=report_path,
    )

    assert exit_code == 0
    assert imported_modules.index("sqlalchemy") < imported_modules.index("src.services.outbox_relay.eligibility")
    assert observed_path_at_sqlalchemy_import[:3] == [
        str(worker_bootstrap.REPO_ROOT),
        str(purelib),
        str(platlib),
    ]
    assert observed_path_at_sqlalchemy_import.count(str(worker_bootstrap.REPO_ROOT)) == 1
    assert observed_path_at_sqlalchemy_import.count(str(purelib)) == 1
    assert observed_path_at_sqlalchemy_import.count(str(platlib)) == 1
    assert not report_path.exists()


def test_bootstrap_source_does_not_use_forbidden_path_or_activation_mechanisms() -> None:
    source = Path(worker_bootstrap.__file__).read_text(encoding="utf-8")

    assert "site.addsitedir" not in source
    assert "PYTHONPATH" not in source
    assert " source " not in source
    assert " export " not in source
    assert "printenv" not in source
    assert " -m " not in source
    assert "pip install" not in source
    assert "poetry install" not in source
    assert "uv pip" not in source


@pytest.mark.asyncio
async def test_bootstrap_adds_repo_root_for_direct_script_import(monkeypatch, tmp_path: Path) -> None:
    report_path = tmp_path / "state/maintenance/worker-runtime-fatal-report.json"
    script_dir = str(worker_bootstrap.REPO_ROOT / "src/services/maintenance")
    monkeypatch.setattr(worker_bootstrap.sys, "path", [script_dir])
    observed_path: list[str] = []
    found_specs: list[str] = []
    imported_modules: list[str] = []

    class FakeMaintenanceMain:
        async def _run(self, argv: list[str]) -> int:
            assert argv == ["worker"]
            return 0

    def import_with_repo_root(module_name: str):
        imported_modules.append(module_name)
        if module_name == worker_bootstrap.MAINTENANCE_MAIN_MODULE:
            observed_path.extend(sys.path)
            return FakeMaintenanceMain()
        return object()

    def find_package_spec(module_name: str):
        found_specs.append(module_name)
        return object()

    exit_code = await worker_bootstrap.run_bootstrap(
        import_module=import_with_repo_root,
        find_spec=find_package_spec,
        report_path=report_path,
    )

    expected_modules = [
        module_name
        for stage, module_name in worker_bootstrap.BOOTSTRAP_IMPORT_STAGE_SEQUENCE
        if module_name and stage not in worker_bootstrap.BOOTSTRAP_SPEC_READY_STAGES
    ]
    assert exit_code == 0
    assert found_specs == [
        worker_bootstrap.MAINTENANCE_PACKAGE_MODULE,
        worker_bootstrap.MAINTENANCE_REPOSITORIES_MODULE,
    ]
    assert imported_modules == expected_modules
    assert observed_path[0] == str(worker_bootstrap.REPO_ROOT)
    assert observed_path.count(str(worker_bootstrap.REPO_ROOT)) == 1
    assert not report_path.exists()


@pytest.mark.asyncio
@pytest.mark.parametrize("failed_stage", ACTIONABLE_BOOTSTRAP_IMPORT_STAGE_LABELS)
async def test_bootstrap_import_classifier_reports_exact_failing_allowlisted_stage(
    monkeypatch,
    tmp_path: Path,
    capsys,
    failed_stage: str,
) -> None:
    report_path = tmp_path / "state/maintenance/worker-runtime-fatal-report.json"
    raw_exception = (
        f"Traceback {RAW_DATABASE_URL} {RAW_REDIS_URL} {RAW_RUNTIME_ENV_PATH} "
        f"{RAW_MODULE_FILE_PATH} {RAW_SYSTEMD_OUTPUT} {RAW_JOURNAL_OUTPUT}"
    )
    stage_modules = dict(worker_bootstrap.BOOTSTRAP_IMPORT_STAGE_SEQUENCE)
    found_specs: list[str] = []
    imported_modules: list[str] = []

    def fail_import(module_name: str):
        imported_modules.append(module_name)
        if failed_stage not in worker_bootstrap.BOOTSTRAP_SPEC_READY_STAGES and module_name == stage_modules[failed_stage]:
            raise RuntimeError(raw_exception)
        return object()

    def fail_find_spec(module_name: str):
        found_specs.append(module_name)
        if failed_stage in worker_bootstrap.BOOTSTRAP_SPEC_READY_STAGES and module_name == stage_modules[failed_stage]:
            raise RuntimeError(raw_exception)
        return object()

    if failed_stage == "bootstrap_repo_root_path_ready":
        not_a_directory = tmp_path / "sentinel-repo-root-file"
        not_a_directory.write_text("not a directory\n", encoding="utf-8")
        monkeypatch.setattr(worker_bootstrap, "REPO_ROOT", not_a_directory)

    exit_code = await worker_bootstrap.run_bootstrap(
        import_module=fail_import,
        find_spec=fail_find_spec,
        report_path=report_path,
    )
    output = capsys.readouterr().out
    payload = _read_json(report_path)
    serialized = json.dumps(payload, sort_keys=True)

    assert exit_code == worker_bootstrap.EXIT_WORKER_BOOTSTRAP_IMPORT_ERROR
    assert output == ""
    assert payload["schema_version"] == worker_bootstrap.FATAL_REPORT_SCHEMA_VERSION
    assert payload["reason_code"] == "worker_bootstrap_import_error"
    assert payload["phase"] == "bootstrap_import"
    assert payload["import_stage"] == failed_stage
    assert payload["import_stage_status"] == "failed"
    assert payload["import_stage_index"] == worker_bootstrap.BOOTSTRAP_IMPORT_STAGE_LABELS.index(failed_stage)
    expected_stage_reason = "stage_import_error"
    if failed_stage == "bootstrap_repo_root_path_ready":
        expected_stage_reason = "repo_root_path_unavailable"
    assert payload["import_stage_reason_code"] == expected_stage_reason
    assert payload["tasks_started"] == []
    assert payload["broad_worker_run_started"] is False
    assert payload["cleanup_completed"] is None
    assert payload["redactions_applied"]["raw_exception_body_omitted"] is True
    assert payload["redactions_applied"]["traceback_omitted"] is True
    assert payload["redactions_applied"]["systemd_stdout_stderr_omitted"] is True
    assert payload["redactions_applied"]["journal_output_omitted"] is True
    assert payload["report_path_omitted"] is True
    if failed_stage == "bootstrap_repo_root_path_ready":
        assert imported_modules == []
        assert found_specs == []
    elif failed_stage in worker_bootstrap.BOOTSTRAP_SPEC_READY_STAGES:
        assert found_specs[-1] == stage_modules[failed_stage]
        assert stage_modules[failed_stage] not in imported_modules
        if failed_stage == "maintenance_package_ready":
            assert imported_modules == ["json"]
        if failed_stage == "maintenance_repositories_spec_ready":
            assert found_specs == [
                worker_bootstrap.MAINTENANCE_PACKAGE_MODULE,
                worker_bootstrap.MAINTENANCE_REPOSITORIES_MODULE,
            ]
            assert worker_bootstrap.MAINTENANCE_REPOSITORIES_MODULE not in imported_modules
            assert imported_modules[-1] == "src.services.maintenance.redis_streams"
    else:
        assert imported_modules[-1] == stage_modules[failed_stage]
    _assert_no_secret_leaks(serialized, raw_exception, report_path)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "failed_stage",
    [
        "maintenance_repositories_sqlalchemy_import",
        "maintenance_repositories_outbox_eligibility_import",
        "maintenance_repositories_models_import",
        "maintenance_repositories_delivery_retry_import",
        "maintenance_repositories_delivery_replay_import",
        "maintenance_repositories_retry_policy_import",
        "maintenance_repositories_module_import",
    ],
)
async def test_repository_import_classifier_reports_exact_dependency_substage(
    tmp_path: Path,
    failed_stage: str,
) -> None:
    report_path = tmp_path / "state/maintenance/worker-runtime-fatal-report.json"
    stage_modules = dict(worker_bootstrap.BOOTSTRAP_IMPORT_STAGE_SEQUENCE)

    def fail_import(module_name: str):
        if module_name == stage_modules[failed_stage]:
            raise RuntimeError(f"Traceback {RAW_DATABASE_URL} {RAW_REDIS_URL} {RAW_MODULE_FILE_PATH}")
        return object()

    exit_code = await worker_bootstrap.run_bootstrap(
        import_module=fail_import,
        find_spec=lambda module_name: object(),
        report_path=report_path,
    )
    payload = _read_json(report_path)
    serialized = json.dumps(payload, sort_keys=True)

    assert exit_code == worker_bootstrap.EXIT_WORKER_BOOTSTRAP_IMPORT_ERROR
    assert payload["reason_code"] == "worker_bootstrap_import_error"
    assert payload["phase"] == "bootstrap_import"
    assert payload["import_stage"] == failed_stage
    assert payload["import_stage_status"] == "failed"
    assert payload["import_stage_reason_code"] == "stage_import_error"
    assert payload["import_stage_index"] == worker_bootstrap.BOOTSTRAP_IMPORT_STAGE_LABELS.index(failed_stage)
    assert payload["tasks_started"] == []
    assert payload["broad_worker_run_started"] is False
    _assert_no_secret_leaks(serialized, RAW_DATABASE_URL, RAW_REDIS_URL, RAW_MODULE_FILE_PATH, report_path)


@pytest.mark.asyncio
async def test_repository_spec_unavailable_reports_safe_dependency_stage(tmp_path: Path) -> None:
    report_path = tmp_path / "state/maintenance/worker-runtime-fatal-report.json"

    def find_spec(module_name: str):
        if module_name == worker_bootstrap.MAINTENANCE_REPOSITORIES_MODULE:
            return None
        return object()

    exit_code = await worker_bootstrap.run_bootstrap(
        import_module=lambda module_name: object(),
        find_spec=find_spec,
        report_path=report_path,
    )
    payload = _read_json(report_path)

    assert exit_code == worker_bootstrap.EXIT_WORKER_BOOTSTRAP_IMPORT_ERROR
    assert payload["reason_code"] == "worker_bootstrap_import_error"
    assert payload["phase"] == "bootstrap_import"
    assert payload["import_stage"] == "maintenance_repositories_spec_ready"
    assert payload["import_stage_status"] == "failed"
    assert payload["import_stage_reason_code"] == "stage_spec_unavailable"
    assert payload["import_stage_index"] == worker_bootstrap.BOOTSTRAP_IMPORT_STAGE_LABELS.index(
        "maintenance_repositories_spec_ready"
    )


@pytest.mark.asyncio
async def test_sqlalchemy_import_succeeds_when_injected_spec_is_unavailable(tmp_path: Path) -> None:
    report_path = tmp_path / "state/maintenance/worker-runtime-fatal-report.json"
    imported_modules: list[str] = []

    def find_spec(module_name: str):
        if module_name == "sqlalchemy":
            raise AssertionError("sqlalchemy spec must not gate the real import")
        return object()

    class FakeMaintenanceMain:
        async def _run(self, argv: list[str]) -> int:
            assert argv == ["worker"]
            return 0

    def import_module(module_name: str):
        imported_modules.append(module_name)
        if module_name == worker_bootstrap.MAINTENANCE_MAIN_MODULE:
            return FakeMaintenanceMain()
        return object()

    exit_code = await worker_bootstrap.run_bootstrap(
        import_module=import_module,
        find_spec=find_spec,
        report_path=report_path,
    )

    assert exit_code == 0
    assert "sqlalchemy" in imported_modules
    assert imported_modules.index("sqlalchemy") < imported_modules.index("src.services.outbox_relay.eligibility")
    assert not report_path.exists()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("exception", "expected_reason_code", "raw_exception_name"),
    [
        (
            ModuleNotFoundError("No module named 'sqlalchemy'", name="sqlalchemy"),
            "stage_module_unavailable",
            None,
        ),
        (
            ModuleNotFoundError("No module named 'greenlet'", name="greenlet"),
            "stage_dependency_unavailable",
            "greenlet",
        ),
        (
            RuntimeError(f"Traceback sys.path={RAW_SYS_PATH_ENTRY} {RAW_DATABASE_URL} {RAW_REDIS_URL}"),
            "stage_import_error",
            None,
        ),
    ],
)
async def test_sqlalchemy_import_failure_reports_safe_reason_code(
    tmp_path: Path,
    exception: Exception,
    expected_reason_code: str,
    raw_exception_name: str | None,
) -> None:
    report_path = tmp_path / "state/maintenance/worker-runtime-fatal-report.json"

    def fail_sqlalchemy_import(module_name: str):
        if module_name == "sqlalchemy":
            raise exception
        return object()

    exit_code = await worker_bootstrap.run_bootstrap(
        import_module=fail_sqlalchemy_import,
        find_spec=lambda module_name: object(),
        report_path=report_path,
    )
    payload = _read_json(report_path)
    serialized = json.dumps(payload, sort_keys=True)

    assert exit_code == worker_bootstrap.EXIT_WORKER_BOOTSTRAP_IMPORT_ERROR
    assert payload["reason_code"] == "worker_bootstrap_import_error"
    assert payload["phase"] == "bootstrap_import"
    assert payload["import_stage"] == "maintenance_repositories_sqlalchemy_import"
    assert payload["import_stage_status"] == "failed"
    assert payload["import_stage_reason_code"] == expected_reason_code
    assert payload["import_stage_index"] == worker_bootstrap.BOOTSTRAP_IMPORT_STAGE_LABELS.index(
        "maintenance_repositories_sqlalchemy_import"
    )
    assert "No module named" not in serialized
    _assert_no_secret_leaks(serialized, raw_exception_name, exception, report_path)


def test_readback_accepts_repository_import_substages_without_leaks(tmp_path: Path) -> None:
    report_path = tmp_path / "state/maintenance/worker-runtime-fatal-report.json"
    for stage in [
        "maintenance_repositories_import",
        "maintenance_repositories_spec_ready",
        "maintenance_repositories_sqlalchemy_import",
        "maintenance_repositories_outbox_eligibility_import",
        "maintenance_repositories_models_import",
        "maintenance_repositories_delivery_retry_import",
        "maintenance_repositories_delivery_replay_import",
        "maintenance_repositories_retry_policy_import",
        "maintenance_repositories_module_import",
    ]:
        worker_bootstrap.write_worker_bootstrap_fatal_report(
            reason_code="worker_bootstrap_import_error",
            phase="bootstrap_import",
            import_stage=stage,
            import_stage_status="failed",
            import_stage_reason_code="stage_import_error",
            import_stage_index=worker_bootstrap.BOOTSTRAP_IMPORT_STAGE_LABELS.index(stage),
            report_path=report_path,
        )
        report = read_worker_runtime_fatal_report(report_path=report_path)
        output = json.dumps(asdict(report), sort_keys=True)

        assert report.status == "pass"
        assert report.reason_code is None
        assert report.latest_report_reason_code == "worker_bootstrap_import_error"
        assert report.latest_report_phase == "bootstrap_import"
        assert report.import_stage == stage
        assert report.import_stage_status == "failed"
        assert report.import_stage_reason_code == "stage_import_error"
        assert report.import_stage_index == worker_bootstrap.BOOTSTRAP_IMPORT_STAGE_LABELS.index(stage)
        _assert_no_secret_leaks(output, report_path)


def test_readback_accepts_historical_sqlalchemy_spec_unavailable_without_leaks(tmp_path: Path) -> None:
    report_path = tmp_path / "state/maintenance/worker-runtime-fatal-report.json"
    worker_bootstrap.write_worker_bootstrap_fatal_report(
        reason_code="worker_bootstrap_import_error",
        phase="bootstrap_import",
        import_stage="maintenance_repositories_sqlalchemy_import",
        import_stage_status="failed",
        import_stage_reason_code="stage_spec_unavailable",
        import_stage_index=worker_bootstrap.BOOTSTRAP_IMPORT_STAGE_LABELS.index(
            "maintenance_repositories_sqlalchemy_import"
        ),
        report_path=report_path,
    )

    report = read_worker_runtime_fatal_report(report_path=report_path)
    output = json.dumps(asdict(report), sort_keys=True)

    assert report.status == "pass"
    assert report.reason_code is None
    assert report.latest_report_reason_code == "worker_bootstrap_import_error"
    assert report.latest_report_phase == "bootstrap_import"
    assert report.import_stage == "maintenance_repositories_sqlalchemy_import"
    assert report.import_stage_status == "failed"
    assert report.import_stage_reason_code == "stage_spec_unavailable"
    assert report.import_stage_index == worker_bootstrap.BOOTSTRAP_IMPORT_STAGE_LABELS.index(
        "maintenance_repositories_sqlalchemy_import"
    )
    _assert_no_secret_leaks(output, report_path)


def test_bootstrap_fatal_report_context_fields_round_trip_without_raw_values(tmp_path: Path) -> None:
    report_path = tmp_path / "state/maintenance/worker-runtime-fatal-report.json"
    raw_venv_path = "/tmp/sentinel-venv-secret/lib/python3.12/site-packages"
    raw_package_version = "SQLAlchemy==2.0.sentinel-secret"
    worker_bootstrap.write_worker_bootstrap_fatal_report(
        reason_code="worker_bootstrap_import_error",
        phase="bootstrap_import",
        import_stage="maintenance_repositories_sqlalchemy_import",
        import_stage_status="failed",
        import_stage_reason_code="stage_module_unavailable",
        import_stage_index=worker_bootstrap.BOOTSTRAP_IMPORT_STAGE_LABELS.index(
            "maintenance_repositories_sqlalchemy_import"
        ),
        venv_context={
            "venv_context_source": "repo_local_pyvenv_cfg",
            "venv_context_active": True,
            "venv_site_candidate_present": True,
            "venv_site_on_sys_path_before": False,
            "venv_site_path_repaired": True,
            "venv_site_on_sys_path_after": True,
            "import_caches_invalidated": True,
            "sqlalchemy_distribution_present": True,
            "raw_path": raw_venv_path,
            "package_version": raw_package_version,
        },
        report_path=report_path,
    )

    payload = _read_json(report_path)
    report = read_worker_runtime_fatal_report(report_path=report_path)
    output = json.dumps(asdict(report), sort_keys=True)
    serialized_payload = json.dumps(payload, sort_keys=True)

    assert payload["venv_context_source"] == "repo_local_pyvenv_cfg"
    assert payload["venv_context_active"] is True
    assert payload["venv_site_candidate_present"] is True
    assert payload["venv_site_on_sys_path_before"] is False
    assert payload["venv_site_path_repaired"] is True
    assert payload["venv_site_on_sys_path_after"] is True
    assert payload["import_caches_invalidated"] is True
    assert payload["sqlalchemy_distribution_present"] is True
    assert report.venv_context_source == "repo_local_pyvenv_cfg"
    assert report.venv_context_active is True
    assert report.venv_site_candidate_present is True
    assert report.venv_site_on_sys_path_before is False
    assert report.venv_site_path_repaired is True
    assert report.venv_site_on_sys_path_after is True
    assert report.import_caches_invalidated is True
    assert report.sqlalchemy_distribution_present is True
    _assert_no_secret_leaks(serialized_payload, raw_venv_path, raw_package_version)
    _assert_no_secret_leaks(output, raw_venv_path, raw_package_version, report_path)


def test_bootstrap_fatal_report_uses_bounded_invocation_fingerprint(monkeypatch) -> None:
    monkeypatch.setenv("INVOCATION_ID", SAFE_INVOCATION_ID)
    report = worker_bootstrap.build_worker_bootstrap_fatal_report(
        reason_code="worker_bootstrap_main_error",
        phase="bootstrap_main",
    )
    serialized = json.dumps(report, sort_keys=True)
    expected = hashlib.sha256(SAFE_INVOCATION_ID.encode("ascii")).hexdigest()[:16]

    assert report["invocation_fingerprint"] == expected
    assert report["raw_invocation_id_omitted"] is True
    assert report["redactions_applied"]["raw_invocation_id_omitted"] is True
    assert SAFE_INVOCATION_ID not in serialized


@pytest.mark.parametrize("raw_invocation_id", [None, "", "not-hex", "f" * 31, "g" * 32])
def test_bootstrap_fatal_report_ignores_missing_or_malformed_invocation_id(
    monkeypatch,
    raw_invocation_id: str | None,
) -> None:
    if raw_invocation_id is None:
        monkeypatch.delenv("INVOCATION_ID", raising=False)
    else:
        monkeypatch.setenv("INVOCATION_ID", raw_invocation_id)

    report = worker_bootstrap.build_worker_bootstrap_fatal_report(
        reason_code="worker_bootstrap_main_error",
        phase="bootstrap_main",
    )

    assert report["invocation_fingerprint"] is None
    assert report["raw_invocation_id_omitted"] is True


@pytest.mark.asyncio
async def test_bootstrap_package_init_failure_reports_safe_package_init_stage(
    tmp_path: Path,
    capsys,
) -> None:
    report_path = tmp_path / "state/maintenance/worker-runtime-fatal-report.json"
    raw_exception = (
        f"Traceback {RAW_DATABASE_URL} {RAW_REDIS_URL} {RAW_RUNTIME_ENV_PATH} "
        f"{RAW_MODULE_FILE_PATH} {RAW_SYSTEMD_OUTPUT} {RAW_JOURNAL_OUTPUT}"
    )

    def import_with_package_init_failure(module_name: str):
        if module_name == worker_bootstrap.MAINTENANCE_PACKAGE_MODULE:
            raise RuntimeError(raw_exception)
        return object()

    exit_code = await worker_bootstrap.run_bootstrap(
        import_module=import_with_package_init_failure,
        find_spec=lambda module_name: object(),
        report_path=report_path,
    )
    output = capsys.readouterr().out
    payload = _read_json(report_path)
    serialized = json.dumps(payload, sort_keys=True)

    assert exit_code == worker_bootstrap.EXIT_WORKER_BOOTSTRAP_IMPORT_ERROR
    assert output == ""
    assert payload["reason_code"] == "worker_bootstrap_import_error"
    assert payload["phase"] == "bootstrap_import"
    assert payload["import_stage"] == "maintenance_package_init_import"
    assert payload["import_stage_status"] == "failed"
    assert payload["import_stage_reason_code"] == "stage_import_error"
    assert payload["import_stage_index"] == worker_bootstrap.BOOTSTRAP_IMPORT_STAGE_LABELS.index(
        "maintenance_package_init_import"
    )
    assert payload["tasks_started"] == []
    assert payload["broad_worker_run_started"] is False
    _assert_no_secret_leaks(serialized, raw_exception, report_path)


@pytest.mark.asyncio
async def test_bootstrap_package_spec_unavailable_reports_safe_stage(tmp_path: Path, capsys) -> None:
    report_path = tmp_path / "state/maintenance/worker-runtime-fatal-report.json"

    exit_code = await worker_bootstrap.run_bootstrap(
        import_module=lambda module_name: object(),
        find_spec=lambda module_name: None,
        report_path=report_path,
    )
    output = capsys.readouterr().out
    payload = _read_json(report_path)
    serialized = json.dumps(payload, sort_keys=True)

    assert exit_code == worker_bootstrap.EXIT_WORKER_BOOTSTRAP_IMPORT_ERROR
    assert output == ""
    assert payload["reason_code"] == "worker_bootstrap_import_error"
    assert payload["phase"] == "bootstrap_import"
    assert payload["import_stage"] == "maintenance_package_ready"
    assert payload["import_stage_status"] == "failed"
    assert payload["import_stage_reason_code"] == "stage_spec_unavailable"
    assert payload["import_stage_index"] == worker_bootstrap.BOOTSTRAP_IMPORT_STAGE_LABELS.index(
        "maintenance_package_ready"
    )
    _assert_no_secret_leaks(serialized, report_path)


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
    raw_exception = (
        f"Traceback {RAW_DATABASE_URL} {RAW_REDIS_URL} {RAW_RUNTIME_ENV_PATH} "
        f"{RAW_MODULE_FILE_PATH} {RAW_SYSTEMD_OUTPUT} {RAW_JOURNAL_OUTPUT}"
    )

    def fail_import(module_name: str):
        if module_name == worker_bootstrap.MAINTENANCE_MAIN_MODULE:
            raise RuntimeError(raw_exception)
        return object()

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
        import_stage="maintenance_main_import" if phase == "bootstrap_import" else None,
        import_stage_status="failed" if phase == "bootstrap_import" else None,
        import_stage_reason_code="stage_import_error" if phase == "bootstrap_import" else None,
        import_stage_index=worker_bootstrap.BOOTSTRAP_IMPORT_STAGE_LABELS.index("maintenance_main_import")
        if phase == "bootstrap_import"
        else None,
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
    if phase == "bootstrap_import":
        assert report.import_stage == "maintenance_main_import"
        assert report.import_stage_status == "failed"
        assert report.import_stage_reason_code == "stage_import_error"
        assert report.import_stage_index == worker_bootstrap.BOOTSTRAP_IMPORT_STAGE_LABELS.index(
            "maintenance_main_import"
        )
    else:
        assert report.import_stage is None
        assert report.import_stage_status is None
        assert report.import_stage_reason_code is None
        assert report.import_stage_index is None
    _assert_no_secret_leaks(output, report_path)


def test_readback_accepts_package_init_import_stage_without_leaks(tmp_path: Path) -> None:
    report_path = tmp_path / "state/maintenance/worker-runtime-fatal-report.json"
    worker_bootstrap.write_worker_bootstrap_fatal_report(
        reason_code="worker_bootstrap_import_error",
        phase="bootstrap_import",
        import_stage="maintenance_package_init_import",
        import_stage_status="failed",
        import_stage_reason_code="stage_import_error",
        import_stage_index=worker_bootstrap.BOOTSTRAP_IMPORT_STAGE_LABELS.index("maintenance_package_init_import"),
        report_path=report_path,
    )

    report = read_worker_runtime_fatal_report(report_path=report_path)
    output = json.dumps(asdict(report), sort_keys=True)

    assert report.status == "pass"
    assert report.reason_code is None
    assert report.latest_report_reason_code == "worker_bootstrap_import_error"
    assert report.latest_report_phase == "bootstrap_import"
    assert report.import_stage == "maintenance_package_init_import"
    assert report.import_stage_status == "failed"
    assert report.import_stage_reason_code == "stage_import_error"
    assert report.import_stage_index == worker_bootstrap.BOOTSTRAP_IMPORT_STAGE_LABELS.index(
        "maintenance_package_init_import"
    )
    _assert_no_secret_leaks(output, report_path)
