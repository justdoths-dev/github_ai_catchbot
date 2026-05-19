from __future__ import annotations

import ast
import json
import subprocess
import sys
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[4]
SCRIPT = ROOT / "scripts" / "ops" / "dedicated_vps_tdjson_source_build_plan.py"


def _module():
    from scripts.ops import dedicated_vps_tdjson_source_build_plan as module

    return module


class FakeCompletedProcess:
    def __init__(self, args: list[str], returncode: int = 0, stdout: str = "", stderr: str = ""):
        self.args = args
        self.returncode = returncode
        self.stdout = stdout
        self.stderr = stderr


class FakeDiskUsage:
    def __init__(self, free: int = 20 * 1024 * 1024 * 1024):
        self.total = 40 * 1024 * 1024 * 1024
        self.used = self.total - free
        self.free = free


def _available_factory(tools: set[str] | None = None, commands: set[str] | None = None):
    tool_names = tools or {
        "git",
        "cmake",
        "g++",
        "gcc",
        "make",
        "pkg-config",
        "python3",
    }
    command_names = commands or {"dpkg-query", "apt-cache", "uname"}

    def available(command: str) -> str | None:
        if command in command_names or command in tool_names:
            return f"/usr/bin/{command}"
        return None

    return available


def _runner(evidence: dict[str, dict[str, Any]] | None = None):
    package_evidence = evidence or {}

    def runner(args: list[str], **_kwargs: object) -> FakeCompletedProcess:
        package = args[3] if args[:2] == ["dpkg-query", "-W"] else args[2] if len(args) >= 3 else None
        item = package_evidence.get(str(package), {})
        if args[:2] == ["apt-cache", "policy"]:
            candidate = item.get("candidate")
            value = candidate if candidate else "(none)"
            return FakeCompletedProcess(args, stdout=f"{package}:\n  Candidate: {value}\n")
        if args[:2] == ["apt-cache", "show"]:
            if not item.get("show"):
                return FakeCompletedProcess(args, returncode=100, stderr="no packages found")
            return FakeCompletedProcess(args, stdout=str(item["show"]))
        if args[:2] == ["apt-cache", "depends"]:
            return FakeCompletedProcess(args, stdout=str(item.get("depends", "")))
        if args[:2] == ["apt-cache", "search"]:
            lines = []
            for name, package_item in package_evidence.items():
                if package_item.get("search"):
                    lines.append(f"{name} - {package_item['search']}")
            return FakeCompletedProcess(args, stdout="\n".join(lines))
        if args[:3] == ["dpkg-query", "-W", "-f=${binary:Package}\\t${Version}\\t${Status}\\n"]:
            if item.get("installed"):
                return FakeCompletedProcess(
                    args,
                    stdout=f"{package}\t{item.get('installed_version', '1.0')}\tinstall ok installed\n",
                )
            return FakeCompletedProcess(args, returncode=1, stderr="no packages found")
        if args == ["uname", "-m"]:
            return FakeCompletedProcess(args, stdout="x86_64\n")
        raise AssertionError(args)

    return runner


def _installed_required_evidence() -> dict[str, dict[str, Any]]:
    return {
        "git": {"installed": True, "installed_version": "1"},
        "cmake": {"installed": True, "installed_version": "1"},
        "build-essential": {"installed": True, "installed_version": "1"},
        "g++": {"installed": True, "installed_version": "1"},
        "gcc": {"installed": True, "installed_version": "1"},
        "make": {"installed": True, "installed_version": "1"},
        "pkg-config": {"installed": True, "installed_version": "1"},
        "zlib1g-dev": {"installed": True, "installed_version": "1"},
        "libssl-dev": {"installed": True, "installed_version": "1"},
        "gperf": {"installed": True, "installed_version": "1"},
    }


def _candidate_required_evidence() -> dict[str, dict[str, Any]]:
    return {
        package: {"candidate": "1.0-1", "show": f"Package: {package}\nVersion: 1.0-1\n"}
        for package in (
            "git",
            "cmake",
            "build-essential",
            "g++",
            "gcc",
            "make",
            "pkg-config",
            "zlib1g-dev",
            "libssl-dev",
            "gperf",
        )
    }


def _run_report(
    *,
    os_release_text: str = 'ID=ubuntu\nVERSION_ID="24.04"\nPRETTY_NAME="Ubuntu"\n',
    tools: set[str] | None = None,
    evidence: dict[str, dict[str, Any]] | None = None,
    disk_free: int = 20 * 1024 * 1024 * 1024,
    **kwargs: Any,
) -> dict[str, Any]:
    return _module().run_plan(
        env={},
        os_release_text=os_release_text,
        command_available=_available_factory(tools=tools),
        subprocess_runner=_runner(evidence),
        disk_usage_provider=lambda _path: FakeDiskUsage(disk_free),
        cpu_count_provider=lambda: 4,
        machine_provider=lambda: "x86_64",
        platform_provider=lambda: "Linux-test",
        **kwargs,
    )


def test_unsupported_host_defers_manual_review() -> None:
    report = _run_report(os_release_text='ID=fedora\nVERSION_ID="40"\nPRETTY_NAME="Fedora"\n')

    assert report["contract_status"] == "unsupported_host"
    assert report["recommended_next_slice"] == "defer_manual_review"


def test_all_required_tools_and_packages_installed_selects_source_build_operator_execution() -> None:
    report = _run_report(evidence=_installed_required_evidence())

    assert report["contract_status"] == "source_build_plan_ready"
    assert report["recommended_next_slice"] == "tdjson_source_build_operator_execution"
    assert report["selected_plan"]["requires_package_install"] is False
    assert report["source_build_attempted"] is False


def test_candidate_visible_but_not_installed_requires_dependency_install_plan() -> None:
    report = _run_report(evidence=_candidate_required_evidence())

    assert report["contract_status"] == "source_build_plan_requires_dependency_plan"
    assert report["recommended_next_slice"] == "tdjson_source_build_dependency_install_plan"
    assert report["selected_plan"]["requires_package_install"] is True
    assert "zlib1g-dev" in report["selected_plan"]["missing_required_packages"]


def test_missing_required_build_tools_is_inconclusive() -> None:
    report = _run_report(
        tools={"git", "g++", "gcc", "make", "pkg-config", "python3"},
        evidence=_installed_required_evidence(),
    )

    assert report["contract_status"] == "source_build_plan_inconclusive"
    assert report["recommended_next_slice"] == "defer_manual_review"
    assert "cmake" in report["selected_plan"]["missing_required_tools"]
    assert any("Required build tool" in note for note in report["risk_notes"])


def test_prior_prebuilt_manual_evidence_is_not_overridden_without_stronger_source_evidence() -> None:
    report = _run_report(
        evidence=_candidate_required_evidence(),
        prior_manual_evidence_json={
            "contract_status": "manual_package_evidence_inconclusive",
            "recommended_next_slice": "tdjson_prebuilt_library_path_plan",
            "secret_like": "not surfaced",
        },
    )

    assert report["contract_status"] == "source_build_plan_inconclusive"
    assert report["recommended_next_slice"] == "tdjson_prebuilt_library_path_plan"
    assert "not surfaced" not in json.dumps(report)


def test_prior_prebuilt_manual_evidence_can_be_overridden_by_complete_source_build_evidence() -> None:
    report = _run_report(
        evidence=_installed_required_evidence(),
        prior_manual_evidence_json={
            "contract_status": "manual_package_evidence_inconclusive",
            "recommended_next_slice": "tdjson_prebuilt_library_path_plan",
        },
    )

    assert report["contract_status"] == "source_build_plan_ready"
    assert report["recommended_next_slice"] == "tdjson_source_build_operator_execution"


def test_prior_json_is_parsed_only_when_explicitly_provided_and_summarized(tmp_path: Path) -> None:
    preflight = tmp_path / "preflight.json"
    package_decision = tmp_path / "package_decision.json"
    apt_plan = tmp_path / "apt_plan.json"
    manual_evidence = tmp_path / "manual_evidence.json"
    preflight.write_text(
        json.dumps({"contract_status": "tdjson_missing", "tdjson_available": False, "secret_like": "not surfaced"}),
        encoding="utf-8",
    )
    package_decision.write_text(
        json.dumps({"contract_status": "package_decision_ready", "recommended_next_slice": "tdjson_apt_install_plan", "secret_like": "not surfaced"}),
        encoding="utf-8",
    )
    apt_plan.write_text(
        json.dumps({"contract_status": "apt_install_plan_inconclusive", "recommended_next_slice": "defer_manual_review", "selected_plan": {"package_name": "libtdjson-dev"}, "secret_like": "not surfaced"}),
        encoding="utf-8",
    )
    manual_evidence.write_text(
        json.dumps({"contract_status": "manual_package_evidence_inconclusive", "recommended_next_slice": "tdjson_source_build_plan", "secret_like": "not surfaced"}),
        encoding="utf-8",
    )

    without_prior = _run_report(evidence=_installed_required_evidence())
    with_prior = _run_report(
        evidence=_installed_required_evidence(),
        preflight_json=str(preflight),
        package_decision_json=str(package_decision),
        apt_plan_json=str(apt_plan),
        manual_evidence_json=str(manual_evidence),
    )

    assert without_prior["prior_inputs"]["preflight"]["provided"] is False
    assert without_prior["prior_inputs"]["package_decision"]["provided"] is False
    assert without_prior["prior_inputs"]["apt_plan"]["provided"] is False
    assert without_prior["prior_inputs"]["manual_evidence"]["provided"] is False
    assert with_prior["prior_inputs"] == {
        "preflight": {
            "provided": True,
            "contract_status": "tdjson_missing",
            "tdjson_available": False,
        },
        "package_decision": {
            "provided": True,
            "contract_status": "package_decision_ready",
            "recommended_next_slice": "tdjson_apt_install_plan",
        },
        "apt_plan": {
            "provided": True,
            "contract_status": "apt_install_plan_inconclusive",
            "recommended_next_slice": "defer_manual_review",
            "selected_package": "libtdjson-dev",
        },
        "manual_evidence": {
            "provided": True,
            "contract_status": "manual_package_evidence_inconclusive",
            "recommended_next_slice": "tdjson_source_build_plan",
        },
    }
    assert "not surfaced" not in json.dumps(with_prior)


def test_output_always_contains_required_safety_booleans_and_they_remain_false() -> None:
    module = _module()
    report = _run_report(evidence=_installed_required_evidence())

    assert report["boundary_check"] == "pass"
    for key in module.SAFETY_FLAGS:
        assert key in report
        assert report[key] is False


def test_script_source_does_not_read_runtime_env_file() -> None:
    text = SCRIPT.read_text(encoding="utf-8")
    forbidden = (
        "/etc/github-ai-catchbot/runtime.env",
        "runtime.env.read_text",
        "open('/etc/github-ai-catchbot/runtime.env'",
        'open("/etc/github-ai-catchbot/runtime.env"',
    )

    for snippet in forbidden:
        assert snippet not in text


def test_script_source_does_not_include_forbidden_mutation_build_command_execution() -> None:
    tree = ast.parse(SCRIPT.read_text(encoding="utf-8"))
    direct_subprocess_runs: list[ast.Call] = []

    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        if (
            isinstance(node.func, ast.Attribute)
            and isinstance(node.func.value, ast.Name)
            and node.func.value.id == "subprocess"
            and node.func.attr == "run"
        ):
            direct_subprocess_runs.append(node)

    assert direct_subprocess_runs == []


def test_script_source_does_not_import_forbidden_runtime_modules() -> None:
    tree = ast.parse(SCRIPT.read_text(encoding="utf-8"))
    imported: list[str] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imported.extend(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            imported.append(node.module)

    forbidden_fragments = (
        "collector",
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
        if any(fragment in name.lower() for fragment in forbidden_fragments)
    ]


def test_subprocess_execution_is_constrained_by_allowlist() -> None:
    module = _module()
    allowed = [
        ("dpkg-query", "-W", "-f=${binary:Package}\\t${Version}\\t${Status}\\n", "zlib1g-dev"),
        ("apt-cache", "policy", "zlib1g-dev"),
        ("apt-cache", "show", "zlib1g-dev"),
        ("apt-cache", "depends", "zlib1g-dev"),
        ("apt-cache", "search", "tdlib build dependencies"),
        ("apt-cache", "search", "cmake tdlib"),
        ("apt-cache", "search", "libtdjson build"),
        ("uname", "-m"),
    ]
    forbidden = [
        ("sudo", "apt", "install", "libssl-dev"),
        ("apt", "update"),
        ("apt-get", "install", "libssl-dev"),
        ("apt-get", "upgrade"),
        ("apt-get", "remove", "libssl-dev"),
        ("apt", "purge", "libssl-dev"),
        ("dpkg", "-i", "tdlib.deb"),
        ("curl", "https://example.invalid"),
        ("wget", "https://example.invalid/file"),
        ("git", "clone", "https://example.invalid/repo.git"),
        ("cmake", "--build", "build"),
        ("make", "install"),
        ("ninja", "install"),
        ("systemctl", "restart", "service"),
        ("service", "restart", "name"),
        ("docker", "compose", "up"),
    ]

    for argv in allowed:
        module._validate_allowed_command(argv)
    for argv in forbidden:
        try:
            module._validate_allowed_command(argv)
        except ValueError:
            pass
        else:
            raise AssertionError(f"forbidden command was accepted: {argv}")


def test_build_tools_are_checked_with_shutil_which_only_not_executed() -> None:
    calls: list[list[str]] = []

    def runner(args: list[str], **kwargs: object) -> FakeCompletedProcess:
        calls.append(args)
        return _runner(_installed_required_evidence())(args, **kwargs)

    report = _module().run_plan(
        env={},
        os_release_text='ID=ubuntu\nVERSION_ID="24.04"\nPRETTY_NAME="Ubuntu"\n',
        command_available=_available_factory(),
        subprocess_runner=runner,
        disk_usage_provider=lambda _path: FakeDiskUsage(),
        cpu_count_provider=lambda: 4,
        machine_provider=lambda: "x86_64",
        platform_provider=lambda: "Linux-test",
    )

    executed_names = {args[0] for args in calls}
    assert executed_names <= {"dpkg-query", "apt-cache", "uname"}
    assert {"git", "cmake", "make", "ninja"}.isdisjoint(executed_names)
    assert report["inspection"]["build_tool_availability"]["cmake"]["available"] is True


def test_future_commands_appear_only_under_not_run_fields_and_are_never_executed() -> None:
    calls: list[list[str]] = []

    def runner(args: list[str], **kwargs: object) -> FakeCompletedProcess:
        calls.append(args)
        return _runner(_installed_required_evidence())(args, **kwargs)

    report = _module().run_plan(
        env={},
        os_release_text='ID=ubuntu\nVERSION_ID="24.04"\nPRETTY_NAME="Ubuntu"\n',
        command_available=_available_factory(),
        subprocess_runner=runner,
        disk_usage_provider=lambda _path: FakeDiskUsage(),
        cpu_count_provider=lambda: 4,
        machine_provider=lambda: "x86_64",
        platform_provider=lambda: "Linux-test",
    )

    for args in calls:
        _module()._validate_allowed_command(args)
    assert not any(args[0] in {"git", "cmake", "make", "ninja"} for args in calls)

    selected = report["selected_plan"]
    for field in (
        "future_operator_commands_not_run",
        "future_validation_commands_not_run",
        "future_rollback_commands_not_run",
    ):
        assert field in selected
        assert isinstance(selected[field], list)

    outside_not_run_fields = dict(selected)
    for field in (
        "future_operator_commands_not_run",
        "future_validation_commands_not_run",
        "future_rollback_commands_not_run",
    ):
        outside_not_run_fields.pop(field)
    assert "git clone" not in json.dumps(outside_not_run_fields)
    assert "cmake " not in json.dumps(outside_not_run_fields)


def test_cli_emits_valid_json() -> None:
    result = subprocess.run(
        [sys.executable, str(SCRIPT), "--format", "json"],
        check=False,
        capture_output=True,
        text=True,
        timeout=30,
    )

    assert result.returncode == 0
    report = json.loads(result.stdout)
    assert report["schema_version"] == "dedicated_vps_tdjson_source_build_plan_v1"
    assert report["contract_name"] == "dedicated_vps_tdjson_source_build_plan"
    assert report["contract_status"] in {
        "source_build_plan_ready",
        "source_build_plan_requires_dependency_plan",
        "source_build_plan_inconclusive",
        "unsupported_host",
    }
    assert report["recommended_next_slice"] in {
        "tdjson_source_build_operator_execution",
        "tdjson_source_build_dependency_install_plan",
        "tdjson_prebuilt_library_path_plan",
        "defer_manual_review",
    }
    assert report["boundary_check"] == "pass"
