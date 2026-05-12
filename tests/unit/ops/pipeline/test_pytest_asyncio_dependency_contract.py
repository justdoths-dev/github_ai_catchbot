from __future__ import annotations

from pathlib import Path
import tomllib


ROOT = Path(__file__).resolve().parents[4]


def _pyproject() -> dict[str, object]:
    return tomllib.loads((ROOT / "pyproject.toml").read_text(encoding="utf-8"))


def _dependency_name(requirement: str) -> str:
    return (
        requirement.split("[", 1)[0]
        .split("<", 1)[0]
        .split(">", 1)[0]
        .split("=", 1)[0]
        .strip()
        .lower()
        .replace("_", "-")
    )


def test_pytest_asyncio_is_declared_in_test_dependency_contract() -> None:
    data = _pyproject()
    project = data["project"]
    assert isinstance(project, dict)

    optional_dependencies = project["optional-dependencies"]
    assert isinstance(optional_dependencies, dict)

    test_dependencies = optional_dependencies["test"]
    assert isinstance(test_dependencies, list)

    declared_names = {
        _dependency_name(requirement)
        for requirement in test_dependencies
        if isinstance(requirement, str)
    }
    assert "pytest-asyncio" in declared_names


def test_pytest_asyncio_is_not_a_runtime_dependency() -> None:
    data = _pyproject()
    project = data["project"]
    assert isinstance(project, dict)

    dependencies = project["dependencies"]
    assert isinstance(dependencies, list)

    runtime_names = {
        _dependency_name(requirement)
        for requirement in dependencies
        if isinstance(requirement, str)
    }
    assert "pytest-asyncio" not in runtime_names
