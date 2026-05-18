from __future__ import annotations

import ast
import json
import subprocess
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[4]
SCRIPT = (
    ROOT
    / "scripts"
    / "ops"
    / "dedicated_vps_tdjson_runtime_dependency_package_decision.py"
)


def _module():
    from scripts.ops import dedicated_vps_tdjson_runtime_dependency_package_decision as module

    return module


class FakeCompletedProcess:
    def __init__(self, args: list[str], returncode: int = 0, stdout: str = "", stderr: str = ""):
        self.args = args
        self.returncode = returncode
        self.stdout = stdout
        self.stderr = stderr


def _available(command: str) -> str | None:
    return f"/usr/bin/{command}"


def _runner_with_apt_candidate(args: list[str], **_kwargs: object) -> FakeCompletedProcess:
    if args[:2] == ["apt-cache", "policy"] and args[2] == "libtdjson":
        return FakeCompletedProcess(args, stdout="libtdjson:\n  Candidate: 1.8.0-1\n")
    if args[:2] == ["apt-cache", "policy"]:
        return FakeCompletedProcess(args, stdout=f"{args[2]}:\n  Candidate: (none)\n")
    if args[:2] == ["apt-cache", "search"]:
        return FakeCompletedProcess(args, stdout="")
    if args[:3] == ["dpkg-query", "-W", "-f=${binary:Package}\\t${Version}\\t${Status}\\n"]:
        return FakeCompletedProcess(args, returncode=1, stderr="no packages found")
    if args == ["ldconfig", "-p"]:
        return FakeCompletedProcess(args, stdout="")
    if args == ["uname", "-m"]:
        return FakeCompletedProcess(args, stdout="x86_64\n")
    raise AssertionError(args)


def _runner_without_apt_candidate(args: list[str], **_kwargs: object) -> FakeCompletedProcess:
    if args[:2] == ["apt-cache", "policy"]:
        return FakeCompletedProcess(args, stdout=f"{args[2]}:\n  Candidate: (none)\n")
    if args[:2] == ["apt-cache", "search"]:
        return FakeCompletedProcess(args, stdout="")
    if args[:3] == ["dpkg-query", "-W", "-f=${binary:Package}\\t${Version}\\t${Status}\\n"]:
        return FakeCompletedProcess(args, returncode=1, stderr="no packages found")
    if args == ["ldconfig", "-p"]:
        return FakeCompletedProcess(args, stdout="")
    if args == ["uname", "-m"]:
        return FakeCompletedProcess(args, stdout="x86_64\n")
    raise AssertionError(args)


def test_ubuntu_debian_host_with_apt_candidate_recommends_apt_install_plan() -> None:
    report = _module().run_decision(
        env={},
        os_release_text='ID=ubuntu\nVERSION_ID="24.04"\nPRETTY_NAME="Ubuntu 24.04 LTS"\n',
        command_available=_available,
        subprocess_runner=_runner_with_apt_candidate,
        machine_provider=lambda: "x86_64",
        platform_provider=lambda: "Linux-test",
    )

    assert report["contract_status"] == "package_decision_ready"
    assert report["recommended_next_slice"] == "tdjson_apt_install_plan"
    assert report["host"]["os_id"] == "ubuntu"
    assert report["inspection"]["apt_policy_candidates"][0]["candidate_visible"] is True


def test_ubuntu_debian_host_with_no_apt_candidate_recommends_source_build_plan() -> None:
    report = _module().run_decision(
        env={},
        os_release_text='ID=debian\nVERSION_ID="12"\nPRETTY_NAME="Debian 12"\n',
        command_available=_available,
        subprocess_runner=_runner_without_apt_candidate,
        machine_provider=lambda: "x86_64",
        platform_provider=lambda: "Linux-test",
    )

    assert report["contract_status"] == "package_decision_ready"
    assert report["recommended_next_slice"] == "tdjson_source_build_plan"


def test_unsupported_host_defers_manual_review() -> None:
    report = _module().run_decision(
        env={},
        os_release_text='ID=fedora\nVERSION_ID="40"\nPRETTY_NAME="Fedora Linux"\n',
        command_available=_available,
        subprocess_runner=_runner_without_apt_candidate,
        machine_provider=lambda: "x86_64",
        platform_provider=lambda: "Linux-test",
    )

    assert report["contract_status"] == "unsupported_host"
    assert report["recommended_next_slice"] == "defer_manual_review"


def test_prior_preflight_json_is_only_parsed_when_explicitly_provided(tmp_path: Path) -> None:
    preflight = tmp_path / "preflight.json"
    preflight.write_text(
        json.dumps(
            {
                "contract_status": "tdjson_load_failed",
                "tdjson_available": False,
                "candidate_checks": [{"secret_like": "not surfaced"}],
            }
        ),
        encoding="utf-8",
    )

    def runner(args: list[str], **_kwargs: object) -> FakeCompletedProcess:
        if args == ["ldconfig", "-p"]:
            return FakeCompletedProcess(args, stdout="libtdjson.so (libc6,x86-64) => /usr/local/lib/libtdjson.so\n")
        return _runner_without_apt_candidate(args, **_kwargs)

    without_prior = _module().run_decision(
        env={},
        os_release_text='ID=ubuntu\nVERSION_ID="24.04"\nPRETTY_NAME="Ubuntu"\n',
        command_available=_available,
        subprocess_runner=runner,
        machine_provider=lambda: "x86_64",
        platform_provider=lambda: "Linux-test",
    )
    with_prior = _module().run_decision(
        env={},
        os_release_text='ID=ubuntu\nVERSION_ID="24.04"\nPRETTY_NAME="Ubuntu"\n',
        command_available=_available,
        subprocess_runner=runner,
        preflight_json=str(preflight),
        machine_provider=lambda: "x86_64",
        platform_provider=lambda: "Linux-test",
    )

    assert without_prior["prior_preflight"] == {
        "provided": False,
        "contract_status": None,
        "tdjson_available": None,
    }
    assert with_prior["prior_preflight"] == {
        "provided": True,
        "contract_status": "tdjson_load_failed",
        "tdjson_available": False,
    }
    assert with_prior["recommended_next_slice"] == "tdjson_prebuilt_library_path_plan"
    assert "not surfaced" not in json.dumps(with_prior)


def test_output_always_contains_required_safety_booleans_and_they_remain_false() -> None:
    module = _module()
    report = module.run_decision(
        env={},
        os_release_text='ID=ubuntu\nVERSION_ID="24.04"\nPRETTY_NAME="Ubuntu"\n',
        command_available=_available,
        subprocess_runner=_runner_with_apt_candidate,
        machine_provider=lambda: "x86_64",
        platform_provider=lambda: "Linux-test",
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
        ("apt-cache", "search", "tdjson"),
        ("ldconfig", "-p"),
        ("uname", "-m"),
    ]
    forbidden = [
        ("sudo", "apt", "install", "libtdjson"),
        ("apt", "update"),
        ("apt-get", "install", "libtdjson"),
        ("dpkg", "-i", "libtdjson.deb"),
        ("curl", "https://example.invalid"),
        ("git", "clone", "https://example.invalid/repo.git"),
        ("cmake", "--build", "build"),
        ("make", "install"),
        ("ninja", "install"),
        ("systemctl", "restart", "service"),
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
    assert report["schema_version"] == "dedicated_vps_tdjson_runtime_dependency_package_decision_v1"
    assert report["contract_name"] == "dedicated_vps_tdjson_runtime_dependency_package_decision"
    assert report["boundary_check"] == "pass"
