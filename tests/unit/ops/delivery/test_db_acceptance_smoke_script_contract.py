from __future__ import annotations

import importlib
from pathlib import Path


ROOT = Path(__file__).resolve().parents[4]
SCRIPT = ROOT / "scripts" / "ops" / "delivery_db_acceptance_smoke.py"
RUNBOOK = ROOT / "ops" / "delivery" / "runbooks" / "db_backed_acceptance_smoke.md"


def _fake_database_url() -> str:
    password = "super" + "secret"
    return f"postgresql+psycopg://user:{password}@localhost:5432/catchbot"


def test_script_exists() -> None:
    assert SCRIPT.exists()


def test_script_contains_no_forbidden_sql_mutations() -> None:
    text = SCRIPT.read_text(encoding="utf-8")
    forbidden = (
        "UPDATE " + "notification_plans",
        "DELETE FROM " + "notification_plans",
        "INSERT INTO " + "notification_plans",
        "ALTER " + "TABLE",
        "CREATE " + "TABLE",
        "DROP " + "TABLE",
    )
    for phrase in forbidden:
        assert phrase not in text


def test_script_documents_select_only_mode() -> None:
    text = SCRIPT.read_text(encoding="utf-8").lower()

    assert "select-only" in text
    assert "select_only" in text
    assert "dev/test" in text


def test_script_redacts_database_url() -> None:
    text = SCRIPT.read_text(encoding="utf-8")

    assert "database_url_redacted" in text
    assert "database_url_redacted=True" in text
    assert "database_url_redacted = false" not in text.lower()


def test_redaction_helper_removes_exact_database_url() -> None:
    module = importlib.import_module("scripts.ops.delivery_db_acceptance_smoke")
    database_url = _fake_database_url()

    redacted = module._redact_database_url(f"connection failed for {database_url}", database_url)

    assert database_url not in redacted
    assert "<redacted-database-url>" in redacted


def test_redaction_helper_masks_password_bearing_url_fragments() -> None:
    module = importlib.import_module("scripts.ops.delivery_db_acceptance_smoke")
    database_url = _fake_database_url()
    password = "super" + "secret"
    message = f"driver failed for postgresql://other:{password}@localhost/db password={password}"

    redacted = module._redact_database_url(message, database_url)

    assert password not in redacted
    assert "<redacted-credential>" in redacted


def test_script_exposes_required_cli_flags() -> None:
    text = SCRIPT.read_text(encoding="utf-8")

    assert "--database-url" in text
    assert "--check" in text
    assert "schema" in text
    assert "delivery-gate" in text
    assert "maintenance-cli" in text
    assert "--format" in text
    assert "json" in text


def test_runbook_contains_commands_and_prod_warning() -> None:
    text = RUNBOOK.read_text(encoding="utf-8")

    assert "python scripts/ops/delivery_db_acceptance_smoke.py --database-url \"$DATABASE_URL\"" in text
    assert "--check schema" in text
    assert "--check delivery-gate" in text
    assert "--check maintenance-cli" in text
    assert "SELECT-only" in text
    assert "Do not run this casually against production" in text
    assert "does not send Telegram, OpenAI, GitHub, X, or Web requests" in text
    assert "does not authorize a live rollout by itself" in text
