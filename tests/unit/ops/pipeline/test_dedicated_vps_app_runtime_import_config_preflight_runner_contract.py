from __future__ import annotations

import ast
import json
import subprocess
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[4]
SCRIPT = ROOT / "scripts" / "ops" / "dedicated_vps_app_runtime_import_config_preflight_runner.py"

FAKE_DB_PASSWORD = "fake-db-password-import-config-preflight"
FAKE_REDIS_PASSWORD = "fake-redis-password-import-config-preflight"
FAKE_DATABASE_URL = (
    "postgresql+psycopg://github_ai_catchbot_app:"
    f"{FAKE_DB_PASSWORD}@203.0.113.77:5432/github_ai_catchbot"
)
FAKE_REDIS_URL = f"redis://:{FAKE_REDIS_PASSWORD}@203.0.113.88:6379/0"
FAKE_OPENAI_KEY = "fake-openai-key-import-config-preflight"
FAKE_TELEGRAM_HASH = "fake-telegram-hash-import-config-preflight"
FAKE_TELEGRAM_TOKEN = "fake-telegram-token-import-config-preflight"
FAKE_GITHUB_PRIVATE_KEY = "fake-github-private-key-import-config-preflight"
FAKE_X_TOKEN = "fake-x-token-import-config-preflight"
FAKE_TDLIB_SECRET = "fake-tdlib-secret-import-config-preflight"


def _module():
    from scripts.ops import dedicated_vps_app_runtime_import_config_preflight_runner as module

    return module


def _runtime_env(tmp_path: Path, **overrides: str | None) -> Path:
    values: dict[str, str] = {
        "APP_ENV": "prod",
        "DATABASE_URL": FAKE_DATABASE_URL,
        "REDIS_URL": FAKE_REDIS_URL,
        "LOG_LEVEL": "198.51.100.44",
        "ENABLE_NOTIFICATION_SEND": "false",
        "ENABLE_REPLAY_TO_PROD_DB": "false",
        "MAINTENANCE_ENABLE_NOTIFICATION_RETRY_PROMOTION": "false",
        "OPENAI_API_KEY": FAKE_OPENAI_KEY,
        "TELEGRAM_API_HASH": FAKE_TELEGRAM_HASH,
        "TELEGRAM_BOT_TOKEN": FAKE_TELEGRAM_TOKEN,
        "GITHUB_PRIVATE_KEY": FAKE_GITHUB_PRIVATE_KEY,
        "X_BEARER_TOKEN": FAKE_X_TOKEN,
        "TDLIB_DB_ENCRYPTION_KEY": FAKE_TDLIB_SECRET,
    }
    for key, value in overrides.items():
        if value is None:
            values.pop(key, None)
        else:
            values[key] = value

    path = tmp_path / "runtime.env"
    path.write_text(
        "\n".join(["# fixture only", *(f"{key}={value}" for key, value in values.items())]),
        encoding="utf-8",
    )
    return path


def _approved_report(tmp_path: Path, **overrides: str | None) -> dict[str, object]:
    module = _module()
    runtime_env = _runtime_env(tmp_path, **overrides)
    result = module.generate_report(
        repo_root=ROOT,
        runtime_env_path=runtime_env,
        approved_app_runtime_import_config_preflight=True,
    )
    return result.report


def test_runner_imports() -> None:
    module = _module()

    assert module.SCHEMA_VERSION == "dedicated_vps_app_runtime_import_config_preflight_v1"
    assert callable(module.main)


def test_no_approval_json_is_parseable_and_does_not_read_runtime_env(tmp_path: Path) -> None:
    runtime_env = _runtime_env(tmp_path)

    result = subprocess.run(
        [sys.executable, str(SCRIPT), "--format", "json", "--runtime-env-path", str(runtime_env)],
        check=False,
        capture_output=True,
        text=True,
        timeout=20,
    )

    assert result.returncode != 0
    report = json.loads(result.stdout)
    assert report["contract_status"] == "approval_required"
    assert report["runtime_env_read"] is False
    assert report["app_imports_attempted"] is False
    assert report["import_surface_attempted"] is False
    assert report["config_surface_attempted"] is False
    assert "approval.required" in report["checks_failed"]


def test_no_approval_output_does_not_expose_unread_fixture_values(tmp_path: Path) -> None:
    runtime_env = _runtime_env(tmp_path)

    result = subprocess.run(
        [sys.executable, str(SCRIPT), "--format", "json", "--runtime-env-path", str(runtime_env)],
        check=False,
        capture_output=True,
        text=True,
        timeout=20,
    )

    rendered = result.stdout
    assert FAKE_DATABASE_URL not in rendered
    assert FAKE_DB_PASSWORD not in rendered
    assert FAKE_REDIS_URL not in rendered
    assert FAKE_REDIS_PASSWORD not in rendered
    assert FAKE_OPENAI_KEY not in rendered
    assert FAKE_TELEGRAM_HASH not in rendered
    assert FAKE_TELEGRAM_TOKEN not in rendered
    assert FAKE_GITHUB_PRIVATE_KEY not in rendered
    assert FAKE_X_TOKEN not in rendered
    assert FAKE_TDLIB_SECRET not in rendered
    assert "198.51.100.44" not in rendered


def test_no_approval_side_effect_flags_are_false(tmp_path: Path) -> None:
    report = _module().generate_report(runtime_env_path=tmp_path / "missing-runtime.env").report

    for key in _module().SIDE_EFFECT_FALSE_FLAGS:
        assert report[key] is False
    assert report["runtime_env_read"] is False
    assert report["runtime_env_values_printed"] is False
    assert report["secret_values_printed"] is False
    assert report["process_env_inspected"] is False


def test_approved_temp_fixture_passes_and_redacts_raw_values(tmp_path: Path) -> None:
    module = _module()
    runtime_env = _runtime_env(tmp_path)

    result = module.generate_report(
        repo_root=ROOT,
        runtime_env_path=runtime_env,
        approved_app_runtime_import_config_preflight=True,
    )

    report = result.report
    rendered = module.render_json(report)
    assert result.exit_code == 0
    assert report["contract_status"] == "passed"
    assert report["runtime_env_read"] is True
    assert report["runtime_env_key_count"] >= 8
    assert report["app_env_seen"] == "prod"
    assert report["runtime_env_values_printed"] is False
    assert report["secret_values_printed"] is False
    assert report["process_env_inspected"] is False
    assert report["import_surface_attempted"] is True
    assert report["config_surface_attempted"] is True
    assert report["import_surface_passed"] is True
    assert report["safe_config_surface_passed"] is True
    assert report["secret_bound_config_loaders_deferred"] is True
    assert FAKE_DATABASE_URL not in rendered
    assert "postgresql+psycopg://" not in rendered
    assert FAKE_DB_PASSWORD not in rendered
    assert FAKE_REDIS_URL not in rendered
    assert "redis://" not in rendered
    assert FAKE_REDIS_PASSWORD not in rendered
    assert FAKE_OPENAI_KEY not in rendered
    assert FAKE_TELEGRAM_HASH not in rendered
    assert FAKE_TELEGRAM_TOKEN not in rendered
    assert FAKE_GITHUB_PRIVATE_KEY not in rendered
    assert FAKE_X_TOKEN not in rendered
    assert FAKE_TDLIB_SECRET not in rendered
    assert "203.0.113.77" not in rendered
    assert "203.0.113.88" not in rendered
    assert "198.51.100.44" not in rendered


def test_import_surface_imports_allowlisted_config_modules_only_and_skips_forbidden_runtime_surfaces(tmp_path: Path) -> None:
    report = _approved_report(tmp_path)
    import_results = report["import_results"]
    imported_modules = [item["module"] for item in import_results if item["status"] == "import_ok"]
    skipped_modules = [item["module"] for item in import_results if item["status"] == "skipped_forbidden_runtime_surface"]

    assert imported_modules
    assert all(module.endswith(".config") for module in imported_modules)
    assert "src.services.collector_telegram.config" in imported_modules
    assert "src.services.outbox_relay.config" in imported_modules
    assert any(module.endswith(".main") for module in skipped_modules)
    assert "src.services.collector_telegram.tdlib_client" in skipped_modules
    assert not any(module.endswith(".main") for module in imported_modules)
    assert not any("tdlib_client" in module for module in imported_modules)
    assert not any("telegram_client" in module for module in imported_modules)
    assert not any("openai_client" in module for module in imported_modules)
    assert not any("github_client" in module for module in imported_modules)
    assert not any("redis_streams" in module for module in imported_modules)


def test_config_surface_exercises_safe_loaders_and_defers_secret_bound_loaders(tmp_path: Path) -> None:
    report = _approved_report(tmp_path)
    config_results = report["config_results"]
    statuses = {(item["module"], item["status"]) for item in config_results}

    assert ("src.services.outbox_relay.config", "config_loader_ok") in statuses
    assert ("src.services.router_normalizer.config", "config_loader_ok") in statuses
    assert ("src.services.collector_telegram.config", "config_loader_deferred_secret_bound") in statuses
    assert ("src.services.judge_openai.config", "config_loader_deferred_secret_bound") in statuses
    assert ("src.services.notifier_telegram.config", "config_loader_deferred_secret_bound") in statuses
    assert ("src.services.gh_enricher.config", "config_loader_deferred_secret_bound") in statuses
    assert ("src.services.x_enricher.config", "config_loader_deferred_secret_bound") in statuses
    assert any(item["status"] == "config_loader_deferred_runtime_bound" for item in config_results)


def test_boundary_flags_remain_false_in_approved_mode(tmp_path: Path) -> None:
    report = _approved_report(tmp_path)

    for key in _module().SIDE_EFFECT_FALSE_FLAGS:
        assert report[key] is False


def test_runner_source_never_calls_runtime_method_names() -> None:
    module = _module()
    tree = ast.parse(SCRIPT.read_text(encoding="utf-8"))
    forbidden = set(module.FORBIDDEN_RUNTIME_METHOD_NAMES)
    calls: list[str] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Call):
            if isinstance(node.func, ast.Attribute) and node.func.attr in forbidden:
                calls.append(node.func.attr)
            if isinstance(node.func, ast.Name) and node.func.id in forbidden:
                calls.append(node.func.id)

    assert calls == []


def test_safe_runtime_shape_reports_categories_not_secret_values(tmp_path: Path) -> None:
    report = _approved_report(tmp_path)
    shape = report["safe_runtime_shape"]

    assert shape["app_env_present"] is True
    assert shape["database_url_present"] is True
    assert shape["redis_url_present"] is True
    assert set(shape["secret_bound_key_categories_present"]) >= {
        "telegram_tdlib",
        "openai",
        "github",
        "x",
        "telegram_notifier",
    }
