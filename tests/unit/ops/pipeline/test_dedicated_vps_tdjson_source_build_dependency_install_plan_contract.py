from __future__ import annotations

import ast
import json
import subprocess
import sys
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[4]
SCRIPT = ROOT / "scripts" / "ops" / "dedicated_vps_tdjson_source_build_dependency_install_plan.py"


def _module():
    from scripts.ops import dedicated_vps_tdjson_source_build_dependency_install_plan as module

    return module


class FakeCompletedProcess:
    def __init__(self, args: list[str], returncode: int = 0, stdout: str = "", stderr: str = ""):
        self.args = args
        self.returncode = returncode
        self.stdout = stdout
        self.stderr = stderr


def _available(command: str) -> str | None:
    return f"/usr/bin/{command}"


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
            if not item.get("candidate"):
                return FakeCompletedProcess(args, returncode=100, stderr="no packages found")
            return FakeCompletedProcess(
                args,
                stdout=(
                    f"Package: {package}\n"
                    f"Version: {item['candidate']}\n"
                    "Description: source-build dependency\n"
                    "Depends: libc6\n"
                ),
            )
        if args[:2] == ["apt-cache", "depends"]:
            return FakeCompletedProcess(args, stdout=f"{package}\n  Depends: libc6\n")
        if args[:3] == ["dpkg-query", "-W", "-f=${binary:Package}\\t${Version}\\t${Status}\\n"]:
            if item.get("installed"):
                return FakeCompletedProcess(
                    args,
                    stdout=f"{package}\t{item.get('installed_version', item.get('candidate', '1.0'))}\tinstall ok installed\n",
                )
            return FakeCompletedProcess(args, returncode=1, stderr="no packages found")
        if args == ["uname", "-m"]:
            return FakeCompletedProcess(args, stdout="x86_64\n")
        raise AssertionError(args)

    return runner


def _evidence(
    *,
    cmake_candidate: str | None = "3.28.3-1build7",
    gperf_candidate: str | None = "3.1-1build1",
    cmake_installed: bool = False,
    gperf_installed: bool = False,
    optional_candidates: bool = True,
) -> dict[str, dict[str, Any]]:
    data: dict[str, dict[str, Any]] = {
        "cmake": {"candidate": cmake_candidate, "installed": cmake_installed},
        "gperf": {"candidate": gperf_candidate, "installed": gperf_installed},
    }
    if optional_candidates:
        data.update(
            {
                "ninja-build": {"candidate": "1.11.1-2"},
                "clang": {"candidate": "1:18.0-59"},
                "libc++-dev": {"candidate": "1:18.0-59"},
                "libc++abi-dev": {"candidate": "1:18.0-59"},
            }
        )
    return data


def _source_build_plan(
    *,
    missing_tools: list[str] | None = None,
    missing_packages: list[str] | None = None,
) -> dict[str, Any]:
    return {
        "contract_status": "source_build_plan_inconclusive",
        "recommended_next_slice": "defer_manual_review",
        "selected_plan": {
            "missing_required_tools": missing_tools if missing_tools is not None else ["cmake"],
            "missing_required_packages": missing_packages if missing_packages is not None else ["gperf"],
        },
        "secret_like": "not surfaced",
    }


def _run_report(
    *,
    os_release_text: str = 'ID=ubuntu\nVERSION_ID="24.04"\nPRETTY_NAME="Ubuntu 24.04 LTS"\n',
    evidence: dict[str, dict[str, Any]] | None = None,
    source_build_plan: dict[str, Any] | None = None,
    **kwargs: Any,
) -> dict[str, Any]:
    return _module().run_plan(
        env={},
        os_release_text=os_release_text,
        command_available=_available,
        subprocess_runner=_runner(evidence or _evidence()),
        prior_source_build_plan_json=source_build_plan
        if source_build_plan is not None
        else _source_build_plan(),
        machine_provider=lambda: "x86_64",
        platform_provider=lambda: "Linux-test",
        **kwargs,
    )


def test_unsupported_host_defers_manual_review() -> None:
    report = _run_report(os_release_text='ID=fedora\nVERSION_ID="40"\nPRETTY_NAME="Fedora"\n')

    assert report["contract_status"] == "unsupported_host"
    assert report["recommended_next_slice"] == "defer_manual_review"


def test_missing_cmake_gperf_with_visible_candidates_selects_both_packages() -> None:
    report = _run_report()

    assert report["contract_status"] == "dependency_install_plan_ready"
    assert report["recommended_next_slice"] == "tdjson_source_build_dependency_install_operator_execution"
    assert report["selected_plan"]["packages_to_install"] == ["cmake", "gperf"]
    assert report["selected_plan"]["future_operator_commands_not_run"] == ["sudo apt install cmake gperf"]


def test_cmake_already_installed_plans_only_gperf() -> None:
    report = _run_report(evidence=_evidence(cmake_installed=True))

    assert report["contract_status"] == "dependency_install_plan_ready"
    assert report["selected_plan"]["packages_to_install"] == ["gperf"]
    assert report["selected_plan"]["future_operator_commands_not_run"] == ["sudo apt install gperf"]


def test_both_cmake_and_gperf_installed_recommends_source_build_plan_recheck() -> None:
    report = _run_report(evidence=_evidence(cmake_installed=True, gperf_installed=True))

    assert report["contract_status"] == "dependency_install_plan_ready"
    assert report["recommended_next_slice"] == "tdjson_source_build_plan_recheck"
    assert report["selected_plan"]["packages_to_install"] == []


def test_missing_apt_candidate_is_inconclusive_and_defers_manual_review() -> None:
    report = _run_report(evidence=_evidence(cmake_candidate=None))

    assert report["contract_status"] == "dependency_install_plan_inconclusive"
    assert report["recommended_next_slice"] == "defer_manual_review"
    assert any("Missing apt candidate(s): cmake." in note for note in report["risk_notes"])


def test_optional_packages_are_not_selected_without_explicit_requirement() -> None:
    report = _run_report(evidence=_evidence(optional_candidates=True))

    assert report["selected_plan"]["packages_to_install"] == ["cmake", "gperf"]
    assert report["selected_plan"]["packages_excluded_as_optional"] == [
        "ninja-build",
        "clang",
        "libc++-dev",
        "libc++abi-dev",
    ]
    assert "ninja-build" not in " ".join(report["selected_plan"]["future_operator_commands_not_run"])


def test_prior_jsons_are_parsed_only_when_explicitly_provided_and_summarized(tmp_path: Path) -> None:
    source_build = tmp_path / "source_build.json"
    preflight = tmp_path / "preflight.json"
    package_decision = tmp_path / "package_decision.json"
    apt_plan = tmp_path / "apt_plan.json"
    manual_evidence = tmp_path / "manual_evidence.json"
    source_build.write_text(json.dumps(_source_build_plan()), encoding="utf-8")
    preflight.write_text(
        json.dumps({"contract_status": "tdjson_missing", "tdjson_available": False, "secret_like": "not surfaced"}),
        encoding="utf-8",
    )
    package_decision.write_text(
        json.dumps({"contract_status": "package_decision_ready", "recommended_next_slice": "tdjson_apt_install_plan", "secret_like": "not surfaced"}),
        encoding="utf-8",
    )
    apt_plan.write_text(
        json.dumps({"contract_status": "apt_install_plan_inconclusive", "recommended_next_slice": "defer_manual_review", "secret_like": "not surfaced"}),
        encoding="utf-8",
    )
    manual_evidence.write_text(
        json.dumps({"contract_status": "manual_package_evidence_inconclusive", "recommended_next_slice": "tdjson_source_build_plan", "secret_like": "not surfaced"}),
        encoding="utf-8",
    )

    without_prior = _module().run_plan(
        env={},
        os_release_text='ID=ubuntu\nVERSION_ID="24.04"\nPRETTY_NAME="Ubuntu"\n',
        command_available=_available,
        subprocess_runner=_runner(_evidence()),
        machine_provider=lambda: "x86_64",
        platform_provider=lambda: "Linux-test",
    )
    with_prior = _module().run_plan(
        env={},
        os_release_text='ID=ubuntu\nVERSION_ID="24.04"\nPRETTY_NAME="Ubuntu"\n',
        command_available=_available,
        subprocess_runner=_runner(_evidence()),
        source_build_plan_json=str(source_build),
        preflight_json=str(preflight),
        package_decision_json=str(package_decision),
        apt_plan_json=str(apt_plan),
        manual_evidence_json=str(manual_evidence),
        machine_provider=lambda: "x86_64",
        platform_provider=lambda: "Linux-test",
    )

    assert without_prior["prior_inputs"]["source_build_plan"]["provided"] is False
    assert without_prior["prior_inputs"]["preflight"]["provided"] is False
    assert with_prior["prior_inputs"] == {
        "source_build_plan": {
            "provided": True,
            "contract_status": "source_build_plan_inconclusive",
            "recommended_next_slice": "defer_manual_review",
            "missing_required_tools": ["cmake"],
            "missing_required_packages": ["gperf"],
        },
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
    report = _run_report()

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
        ("dpkg-query", "-W", "-f=${binary:Package}\\t${Version}\\t${Status}\\n", "cmake"),
        ("apt-cache", "policy", "cmake"),
        ("apt-cache", "show", "cmake"),
        ("apt-cache", "depends", "cmake"),
        ("uname", "-m"),
    ]
    forbidden = [
        ("sudo", "apt", "install", "cmake", "gperf"),
        ("apt", "update"),
        ("apt-get", "install", "cmake"),
        ("apt-get", "upgrade"),
        ("apt-get", "remove", "cmake"),
        ("apt", "purge", "cmake"),
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


def test_future_install_and_rollback_commands_are_only_not_run_fields_and_never_executed() -> None:
    calls: list[list[str]] = []

    def runner(args: list[str], **kwargs: object) -> FakeCompletedProcess:
        calls.append(args)
        return _runner(_evidence())(args, **kwargs)

    report = _module().run_plan(
        env={},
        os_release_text='ID=ubuntu\nVERSION_ID="24.04"\nPRETTY_NAME="Ubuntu"\n',
        command_available=_available,
        subprocess_runner=runner,
        prior_source_build_plan_json=_source_build_plan(),
        machine_provider=lambda: "x86_64",
        platform_provider=lambda: "Linux-test",
    )

    for args in calls:
        _module()._validate_allowed_command(args)
    assert not any(args[0] in {"sudo", "apt", "apt-get", "git", "cmake", "make", "ninja"} for args in calls)
    assert report["selected_plan"]["future_operator_commands_not_run"] == ["sudo apt install cmake gperf"]
    assert report["selected_plan"]["future_rollback_commands_not_run"] == ["sudo apt remove cmake gperf"]

    selected_without_not_run = dict(report["selected_plan"])
    for field in (
        "future_operator_commands_not_run",
        "future_validation_commands_not_run",
        "future_rollback_commands_not_run",
    ):
        selected_without_not_run.pop(field)
    assert "sudo apt install" not in json.dumps(selected_without_not_run)
    assert "sudo apt remove" not in json.dumps(selected_without_not_run)


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
    assert report["schema_version"] == "dedicated_vps_tdjson_source_build_dependency_install_plan_v1"
    assert report["contract_name"] == "dedicated_vps_tdjson_source_build_dependency_install_plan"
    assert report["contract_status"] in {
        "dependency_install_plan_ready",
        "dependency_install_plan_inconclusive",
        "unsupported_host",
    }
    assert report["recommended_next_slice"] in {
        "tdjson_source_build_dependency_install_operator_execution",
        "tdjson_source_build_plan_recheck",
        "defer_manual_review",
    }
    assert report["boundary_check"] == "pass"
