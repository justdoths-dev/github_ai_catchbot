from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[4]
SCRIPT = ROOT / "scripts" / "ops" / "dedicated_vps_tdlib_auth_package_check.py"
RUNBOOK = (
    ROOT / "ops" / "pipeline" / "runbooks" / "dedicated_vps_tdlib_auth_package.md"
)


def _module():
    from scripts.ops import dedicated_vps_tdlib_auth_package_check as module

    return module


def _valid_runbook_text() -> str:
    return RUNBOOK.read_text(encoding="utf-8")


def _write_runbook(tmp_path: Path, text: str) -> None:
    module = _module()
    runbook = tmp_path / module.CHECKED_FILE
    runbook.parent.mkdir(parents=True, exist_ok=True)
    runbook.write_text(text, encoding="utf-8")


def test_checker_returns_pass_for_committed_package() -> None:
    result = subprocess.run(
        [sys.executable, str(SCRIPT), "--format", "json"],
        check=False,
        capture_output=True,
        text=True,
        timeout=20,
    )

    assert result.returncode == 0
    report = json.loads(result.stdout)
    assert report["report_type"] == "dedicated_vps_tdlib_auth_package_check_v1"
    assert report["contract_status"] == "passed"
    assert report["checks_failed"] == []
    assert report["failures"] == []


def test_required_runbook_sections_are_checked(tmp_path: Path) -> None:
    text = _valid_runbook_text().replace(
        "## Future approved TDLib auth execution shape",
        "## Future execution shape",
        1,
    )
    _write_runbook(tmp_path, text)

    result = _module().generate_report(tmp_path)

    assert result.exit_code == 1
    assert "runbook.required_sections" in result.report["checks_failed"]


def test_required_tdlib_key_names_are_checked(tmp_path: Path) -> None:
    text = _valid_runbook_text().replace("TELEGRAM_API_ID", "TELEGRAM_READER_ID")
    _write_runbook(tmp_path, text)

    result = _module().generate_report(tmp_path)

    assert result.exit_code == 1
    assert "runbook.required_key_names" in result.report["checks_failed"]
    assert any(
        "TELEGRAM_API_ID" in failure.get("missing_required_keys", [])
        for failure in result.report["failures"]
    )


def test_telegram_bot_token_is_not_tdlib_auth_credential(tmp_path: Path) -> None:
    text = _valid_runbook_text().replace(
        "- `TELEGRAM_BOT_TOKEN`",
        "- `TELEGRAM_BOT_TOKEN`\n- `TELEGRAM_API_HASH`",
        1,
    )
    text = text.replace(
        "Required for TDLib auth/preflight:",
        "Required for TDLib auth/preflight:\n\n- `TELEGRAM_BOT_TOKEN`",
        1,
    )
    _write_runbook(tmp_path, text)

    result = _module().generate_report(tmp_path)

    assert result.exit_code == 1
    assert "runbook.telegram_bot_token_not_tdlib_credential" in result.report[
        "checks_failed"
    ]


def test_collector_notifier_separation_is_checked(tmp_path: Path) -> None:
    text = _valid_runbook_text().replace(
        "Notifier bot token does not authorize channel collection.",
        "Notifier bot token is separate.",
    )
    _write_runbook(tmp_path, text)

    result = _module().generate_report(tmp_path)

    assert result.exit_code == 1
    assert "runbook.credential_surface_separation" in result.report["checks_failed"]


def test_non_authorizations_are_checked(tmp_path: Path) -> None:
    text = _valid_runbook_text().replace(
        "This package does not execute TDLib auth.",
        "TDLib auth is deferred.",
    )
    _write_runbook(tmp_path, text)

    result = _module().generate_report(tmp_path)

    assert result.exit_code == 1
    assert "runbook.non_authorizations" in result.report["checks_failed"]


def test_next_bounded_action_is_checked(tmp_path: Path) -> None:
    text = _valid_runbook_text().replace(
        "dedicated_vps_tdlib_auth_operator_execution",
        "generic_tdlib_execution",
    )
    _write_runbook(tmp_path, text)

    result = _module().generate_report(tmp_path)

    assert result.exit_code == 1
    assert "runbook.next_bounded_action" in result.report["checks_failed"]


def test_unsafe_command_patterns_are_rejected(tmp_path: Path) -> None:
    unsafe_snippets = {
        "cat_runtime_env": "cat /etc/github-ai-catchbot/runtime.env",
        "source_runtime_env": "source /etc/github-ai-catchbot/runtime.env",
        "dot_source_runtime_env": ". /etc/github-ai-catchbot/runtime.env",
        "export_cat": "export $(cat /tmp/example)",
        "echo_secret_append": (
            "echo TELEGRAM_API_HASH=secret >> /etc/github-ai-catchbot/runtime.env"
        ),
        "tee_append_runtime_env": "tee -a /etc/github-ai-catchbot/runtime.env",
    }

    for expected, snippet in unsafe_snippets.items():
        _write_runbook(tmp_path, f"{_valid_runbook_text()}\n{snippet}\n")

        result = _module().generate_report(tmp_path)

        assert result.exit_code == 1
        assert any(expected in failure["check"] for failure in result.report["failures"])


def test_real_looking_secret_url_invite_and_ip_patterns_are_rejected(
    tmp_path: Path,
) -> None:
    real_looking_snippets = {
        "telegram_bot_token": "123456789:ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghi",
        "telegram_api_hash_assignment": "TELEGRAM_API_HASH=0123456789abcdef0123456789abcdef",
        "telegram_phone_assignment": "TELEGRAM_PHONE_NUMBER=+82105551234",
        "telegram_login_code_assignment": "TELEGRAM_LOGIN_CODE=12345",
        "auth_code_assignment": "AUTH_CODE=12345",
        "private_invite_plus": "https://t.me/+privateInviteCode",
        "private_invite_joinchat": "https://t.me/joinchat/privateInviteCode",
        "legacy_private_invite_joinchat": "telegram.me/joinchat/privateInviteCode",
        "postgresql_url": "postgresql://user:password@dbhost/dbname",
        "postgresql_psycopg_url": "postgresql+psycopg://user:password@dbhost/dbname",
        "redis_url": "redis://localhost:6379/0",
        "ipv4_literal": "203.0.113.10",
    }

    for expected, snippet in real_looking_snippets.items():
        _write_runbook(tmp_path, f"{_valid_runbook_text()}\n{snippet}\n")

        result = _module().generate_report(tmp_path)

        assert result.exit_code == 1
        assert any(expected in failure["check"] for failure in result.report["failures"])


def test_checker_output_includes_side_effect_booleans_all_false() -> None:
    report = _module().generate_report(ROOT).report

    assert report["checker_side_effects"] == {
        "runtime_env_read": False,
        "runtime_env_modified": False,
        "runtime_env_values_printed": False,
        "database_connected": False,
        "redis_connected": False,
        "alembic_run": False,
        "app_runtime_started": False,
        "tdlib_auth_performed": False,
        "telegram_connected": False,
        "live_collector_started": False,
        "notifier_transport_enabled": False,
        "production_rollout_performed": False,
        "files_mutated": False,
        "network_called": False,
    }


def test_tests_do_not_read_runtime_env() -> None:
    text = Path(__file__).read_text(encoding="utf-8")
    direct_path_single = "Path('/etc/github-ai-catchbot/" + "runtime.env')"
    direct_path_double = 'Path("/etc/github-ai-catchbot/' + 'runtime.env")'
    open_single = "open('/etc/github-ai-catchbot/" + "runtime.env'"
    open_double = 'open("/etc/github-ai-catchbot/' + 'runtime.env"'

    assert direct_path_single not in text
    assert direct_path_double not in text
    assert open_single not in text
    assert open_double not in text
