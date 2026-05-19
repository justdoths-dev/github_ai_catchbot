from __future__ import annotations

import ast
import json
import subprocess
import sys
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[4]
SCRIPT = ROOT / "scripts" / "ops" / "dedicated_vps_tdjson_manual_package_evidence_review.py"


def _module():
    from scripts.ops import dedicated_vps_tdjson_manual_package_evidence_review as module

    return module


class FakeCompletedProcess:
    def __init__(self, args: list[str], returncode: int = 0, stdout: str = "", stderr: str = ""):
        self.args = args
        self.returncode = returncode
        self.stdout = stdout
        self.stderr = stderr


def _available(command: str) -> str | None:
    return f"/usr/bin/{command}"


def _runner(evidence: dict[str, dict[str, Any]] | None = None, *, ldconfig: str = ""):
    package_evidence = evidence or {}

    def runner(args: list[str], **_kwargs: object) -> FakeCompletedProcess:
        package = args[2] if len(args) >= 3 else None
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
        if args[:2] == ["apt-cache", "madison"]:
            return FakeCompletedProcess(args, stdout=str(item.get("madison", "")))
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
        if args == ["ldconfig", "-p"]:
            return FakeCompletedProcess(args, stdout=ldconfig)
        if args == ["uname", "-m"]:
            return FakeCompletedProcess(args, stdout="x86_64\n")
        raise AssertionError(args)

    return runner


def _run_report(
    *,
    os_release_text: str = 'ID=ubuntu\nVERSION_ID="24.04"\nPRETTY_NAME="Ubuntu"\n',
    evidence: dict[str, dict[str, Any]] | None = None,
    ldconfig: str = "",
    **kwargs: Any,
) -> dict[str, Any]:
    return _module().run_review(
        env={},
        os_release_text=os_release_text,
        command_available=_available,
        subprocess_runner=_runner(evidence, ldconfig=ldconfig),
        machine_provider=lambda: "x86_64",
        platform_provider=lambda: "Linux-test",
        **kwargs,
    )


def test_unsupported_host_defers_manual_review() -> None:
    report = _run_report(os_release_text='ID=fedora\nVERSION_ID="40"\nPRETTY_NAME="Fedora"\n')

    assert report["contract_status"] == "unsupported_host"
    assert report["recommended_next_slice"] == "defer_manual_review"


def test_no_visible_apt_evidence_on_ubuntu_debian_is_inconclusive_source_build_plan() -> None:
    report = _run_report()

    assert report["contract_status"] == "manual_package_evidence_inconclusive"
    assert report["recommended_next_slice"] in {"tdjson_source_build_plan", "defer_manual_review"}


def test_generic_tdlib_only_package_evidence_is_inconclusive_with_libtdjson_risk() -> None:
    report = _run_report(
        evidence={
            "tdlib": {
                "candidate": "1.8.0-1",
                "show": "Package: tdlib\nVersion: 1.8.0-1\nDescription: Telegram Database library\n",
                "depends": "tdlib\n  Depends: libc6\n",
            }
        }
    )

    assert report["contract_status"] == "manual_package_evidence_inconclusive"
    assert any("may not provide libtdjson.so" in note for note in report["risk_notes"])


def test_libtdjson_policy_show_evidence_recommends_apt_plan_recheck_not_operator_execution() -> None:
    report = _run_report(
        evidence={
            "libtdjson": {
                "candidate": "1.8.0-1",
                "show": (
                    "Package: libtdjson\n"
                    "Version: 1.8.0-1\n"
                    "Source: tdlib\n"
                    "Description: TDLib JSON runtime library\n"
                    "Provides: libtdjson.so\n"
                ),
                "depends": "libtdjson\n  Depends: libc6\n",
            }
        }
    )

    assert report["contract_status"] == "manual_package_evidence_ready"
    assert report["recommended_next_slice"] == "tdjson_apt_install_plan_recheck"
    assert "operator_execution" not in report["recommended_next_slice"]
    assert report["apt_install_attempted"] is False


def test_ldconfig_tdjson_hint_without_apt_package_uses_prebuilt_plan_or_manual_review() -> None:
    report = _run_report(
        ldconfig="libtdjson.so (libc6,x86-64) => /usr/local/lib/libtdjson.so\n"
    )

    assert report["contract_status"] == "manual_package_evidence_inconclusive"
    assert report["recommended_next_slice"] in {"tdjson_prebuilt_library_path_plan", "defer_manual_review"}
    assert report["apt_install_attempted"] is False


def test_prior_json_is_parsed_only_when_explicitly_provided_and_summarized(tmp_path: Path) -> None:
    preflight = tmp_path / "preflight.json"
    package_decision = tmp_path / "package_decision.json"
    apt_plan = tmp_path / "apt_plan.json"
    preflight.write_text(
        json.dumps(
            {
                "contract_status": "tdjson_missing",
                "tdjson_available": False,
                "secret_like": "not surfaced",
            }
        ),
        encoding="utf-8",
    )
    package_decision.write_text(
        json.dumps(
            {
                "contract_status": "package_decision_ready",
                "recommended_next_slice": "tdjson_apt_install_plan",
                "secret_like": "not surfaced",
            }
        ),
        encoding="utf-8",
    )
    apt_plan.write_text(
        json.dumps(
            {
                "contract_status": "apt_install_plan_inconclusive",
                "recommended_next_slice": "defer_manual_review",
                "selected_plan": {"package_name": "libtdjson-dev"},
                "secret_like": "not surfaced",
            }
        ),
        encoding="utf-8",
    )

    without_prior = _run_report()
    with_prior = _run_report(
        preflight_json=str(preflight),
        package_decision_json=str(package_decision),
        apt_plan_json=str(apt_plan),
    )

    assert without_prior["prior_inputs"]["preflight"]["provided"] is False
    assert without_prior["prior_inputs"]["package_decision"]["provided"] is False
    assert without_prior["prior_inputs"]["apt_plan"]["provided"] is False
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
    }
    assert "not surfaced" not in json.dumps(with_prior)


def test_output_always_contains_required_safety_booleans_and_they_remain_false() -> None:
    module = _module()
    report = _run_report(
        evidence={
            "libtdjson": {
                "candidate": "1.8.0-1",
                "show": "Package: libtdjson\nVersion: 1.8.0-1\nDescription: TDLib JSON runtime library\n",
            }
        }
    )

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


def test_script_source_does_not_include_forbidden_mutation_command_execution() -> None:
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
        ("dpkg-query", "-W", "-f=${binary:Package}\\t${Version}\\t${Status}\\n", "libtdjson"),
        ("apt-cache", "policy", "libtdjson"),
        ("apt-cache", "show", "libtdjson"),
        ("apt-cache", "depends", "libtdjson"),
        ("apt-cache", "madison", "libtdjson"),
        ("apt-cache", "search", "tdlib json"),
        ("ldconfig", "-p"),
        ("uname", "-m"),
    ]
    forbidden = [
        ("sudo", "apt", "install", "libtdjson"),
        ("apt", "update"),
        ("apt-get", "install", "libtdjson"),
        ("apt-get", "remove", "libtdjson"),
        ("dpkg", "-i", "libtdjson.deb"),
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


def test_future_commands_appear_only_under_not_run_fields_and_are_never_executed() -> None:
    calls: list[list[str]] = []

    def runner(args: list[str], **kwargs: object) -> FakeCompletedProcess:
        calls.append(args)
        return _runner({})(args, **kwargs)

    report = _module().run_review(
        env={},
        os_release_text='ID=ubuntu\nVERSION_ID="24.04"\nPRETTY_NAME="Ubuntu"\n',
        command_available=_available,
        subprocess_runner=runner,
        machine_provider=lambda: "x86_64",
        platform_provider=lambda: "Linux-test",
    )

    for args in calls:
        _module()._validate_allowed_command(args)
    assert not any("sudo" in part or "cmake" in part for args in calls for part in args)
    assert report["candidate_next_actions"]
    assert all(
        "future_operator_commands_not_run" in action
        and "future_validation_commands_not_run" in action
        and "future_rollback_commands_not_run" in action
        for action in report["candidate_next_actions"]
    )


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
    assert report["schema_version"] == "dedicated_vps_tdjson_manual_package_evidence_review_v1"
    assert report["contract_name"] == "dedicated_vps_tdjson_manual_package_evidence_review"
    assert report["contract_status"] in {
        "manual_package_evidence_ready",
        "manual_package_evidence_inconclusive",
        "unsupported_host",
    }
    assert report["recommended_next_slice"] in {
        "tdjson_source_build_plan",
        "tdjson_prebuilt_library_path_plan",
        "tdjson_apt_install_plan_recheck",
        "defer_manual_review",
    }
    assert report["boundary_check"] == "pass"
