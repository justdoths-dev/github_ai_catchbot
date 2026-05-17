from __future__ import annotations

import ast
import subprocess
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[4]
SCRIPT = (
    ROOT / "scripts" / "ops" / "dedicated_vps_tdjson_runtime_dependency_preflight.py"
)
RUNBOOK = (
    ROOT
    / "ops"
    / "pipeline"
    / "runbooks"
    / "dedicated_vps_tdjson_runtime_dependency_preflight.md"
)


def _module():
    from scripts.ops import dedicated_vps_tdjson_runtime_dependency_preflight as module

    return module


class FakeTdjsonLibrary:
    td_json_client_create = object()
    td_json_client_send = object()
    td_json_client_receive = object()
    td_json_client_destroy = object()


class FakeMissingSymbolLibrary:
    td_json_client_create = object()
    td_json_client_send = object()
    td_json_client_receive = object()


def test_no_env_path_and_find_library_missing_returns_tdjson_missing() -> None:
    report = _module().run_preflight(
        env={},
        find_library_func=lambda _name: None,
        cdll_loader=lambda candidate: FakeTdjsonLibrary(),
        candidate_paths=(),
    )

    assert report["contract_status"] == "tdjson_missing"
    assert report["tdjson_available"] is False
    assert report["tdjson_loadable"] is False
    assert report["required_symbols_present"] is False
    assert report["tdjson_library_path_env_set"] is False
    assert report["find_library_result_present"] is False
    assert report["candidate_checks"] == []


def test_env_path_candidate_exists_and_fake_cdll_has_all_symbols(
    tmp_path: Path,
) -> None:
    candidate = tmp_path / "libtdjson.so"
    candidate.write_bytes(b"fake")

    report = _module().run_preflight(
        env={"TDJSON_LIBRARY_PATH": str(candidate)},
        find_library_func=lambda _name: None,
        cdll_loader=lambda loaded: FakeTdjsonLibrary(),
        candidate_paths=(),
    )

    assert report["contract_status"] == "tdjson_available"
    assert report["tdjson_available"] is True
    assert report["tdjson_loadable"] is True
    assert report["required_symbols_present"] is True
    assert report["tdjson_library_path_env_set"] is True
    assert report["candidate_checks"][0]["basename"] == "libtdjson.so"
    assert report["candidate_checks"][0]["status"] == "loadable"


def test_candidate_exists_but_fake_cdll_raises_oserror(tmp_path: Path) -> None:
    candidate = tmp_path / "libtdjson.so"
    candidate.write_bytes(b"fake")

    def raise_oserror(_candidate: str) -> object:
        raise OSError("fake load failure")

    report = _module().run_preflight(
        env={"TDJSON_LIBRARY_PATH": str(candidate)},
        find_library_func=lambda _name: None,
        cdll_loader=raise_oserror,
        candidate_paths=(),
    )

    assert report["contract_status"] == "tdjson_load_failed"
    assert report["tdjson_available"] is False
    assert report["tdjson_loadable"] is False
    assert report["required_symbols_present"] is False
    assert report["candidate_checks"][0]["status"] == "load_failed"
    assert report["candidate_checks"][0]["error_class"] == "OSError"


def test_fake_cdll_loads_but_required_symbol_missing(tmp_path: Path) -> None:
    candidate = tmp_path / "libtdjson.so"
    candidate.write_bytes(b"fake")

    report = _module().run_preflight(
        env={"TDJSON_LIBRARY_PATH": str(candidate)},
        find_library_func=lambda _name: None,
        cdll_loader=lambda loaded: FakeMissingSymbolLibrary(),
        candidate_paths=(),
    )

    assert report["contract_status"] == "tdjson_missing_required_symbols"
    assert report["tdjson_available"] is False
    assert report["tdjson_loadable"] is True
    assert report["required_symbols_present"] is False
    assert report["candidate_checks"][0]["status"] == "missing_required_symbols"
    assert report["candidate_checks"][0]["missing_required_symbols"] == [
        "td_json_client_destroy"
    ]


def test_output_always_contains_safety_booleans_and_forbidden_flags_are_false() -> None:
    module = _module()
    report = module.run_preflight(
        env={},
        find_library_func=lambda _name: None,
        cdll_loader=lambda candidate: FakeTdjsonLibrary(),
        candidate_paths=(),
    )

    assert report["boundary_check"] == "pass"
    for key in module.SAFETY_FLAGS:
        assert key in report
        assert report[key] is False


def test_script_and_runbook_do_not_reference_runtime_env_reading_commands() -> None:
    text = SCRIPT.read_text(encoding="utf-8") + "\n" + RUNBOOK.read_text(
        encoding="utf-8"
    )
    forbidden = (
        "cat /etc/github-ai-catchbot/runtime.env",
        "source /etc/github-ai-catchbot/runtime.env",
        ". /etc/github-ai-catchbot/runtime.env",
        "open('/etc/github-ai-catchbot/runtime.env'",
        'open("/etc/github-ai-catchbot/runtime.env"',
    )

    for snippet in forbidden:
        assert snippet not in text


def test_script_source_does_not_import_forbidden_runtime_modules() -> None:
    tree = ast.parse(SCRIPT.read_text(encoding="utf-8"))
    imported: list[str] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imported.extend(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            imported.append(node.module)

    forbidden_fragments = (
        "collector_telegram",
        "CollectorTelegramService",
        "CollectorRuntime",
        "notifier",
        "database",
        "redis",
        "alembic",
        "docker",
        "systemd",
    )
    assert not [
        name
        for name in imported
        if any(fragment in name for fragment in forbidden_fragments)
    ]


def test_tdjson_symbols_are_checked_only_as_attributes() -> None:
    module = _module()
    tree = ast.parse(SCRIPT.read_text(encoding="utf-8"))
    forbidden_calls: list[str] = []

    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        function = node.func
        if isinstance(function, ast.Name) and function.id in module.REQUIRED_SYMBOLS:
            forbidden_calls.append(function.id)
        elif (
            isinstance(function, ast.Attribute)
            and function.attr in module.REQUIRED_SYMBOLS
        ):
            forbidden_calls.append(function.attr)

    assert forbidden_calls == []


def test_cli_outputs_json_without_reading_runtime_env() -> None:
    result = subprocess.run(
        [sys.executable, str(SCRIPT), "--format", "json"],
        check=False,
        capture_output=True,
        text=True,
        timeout=20,
    )

    assert result.returncode == 0
    assert "/etc/github-ai-catchbot/runtime.env" not in result.stdout
