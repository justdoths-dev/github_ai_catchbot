from __future__ import annotations

import re
from pathlib import Path


ROOT = Path(__file__).resolve().parents[4]
RUNBOOK = (
    ROOT
    / "ops"
    / "pipeline"
    / "runbooks"
    / "dedicated_vps_telegram_credentials_acquisition_plan.md"
)

REQUIRED_SECTIONS = (
    "# Dedicated VPS Telegram credentials acquisition plan",
    "## Purpose",
    "## Source-of-truth / architecture boundary",
    "## Scope",
    "## Non-authorizations",
    "## Credential surface A - collector reader account / TDLib / MTProto",
    "## Credential surface B - notifier bot / Telegram Bot API",
    "## Operator acquisition checklist",
    "## Channel source inventory checklist/template",
    "## Secure storage / password manager expectation",
    "## No-secret / redaction rules",
    "## Later secret placement boundary",
    "## Later TDLib auth package boundary",
    "## Later notifier target verification boundary",
    "## Rotation / recovery notes",
    "## Acceptance criteria",
    "## Next bounded slice",
)

COLLECTOR_KEYS = (
    "TELEGRAM_API_ID",
    "TELEGRAM_API_HASH",
    "TELEGRAM_PHONE_NUMBER",
    "TELEGRAM_2FA_PASSWORD",
    "TDLIB_DB_ENCRYPTION_KEY",
    "TDLIB_STATE_DIR",
    "TDLIB_FILES_DIR",
)

CHANNEL_FIELDS = (
    "public username",
    "invite link",
    "source_kind",
    "source_value",
    "desired_state",
    "notes",
    "priority_weight",
)

NON_AUTHORIZATIONS = (
    "This slice does not mutate `/etc/github-ai-catchbot/runtime.env`.",
    "This slice does not read or print runtime env values.",
    "This slice does not connect to DB or Redis.",
    "This slice does not run Alembic.",
    "This slice does not modify Docker or systemd.",
    "This slice does not start any app runtime.",
    "This slice does not run TDLib auth.",
    "This slice does not connect Telegram.",
    "This slice does not enable live collector.",
    "This slice does not enable notifier transport.",
    "This slice does not perform production rollout.",
)

FORBIDDEN_PATTERNS = {
    "telegram_bot_token": r"\b\d{7,12}:[A-Za-z0-9_-]{30,}\b",
    "telegram_api_hash_assignment": r"\bTELEGRAM_API_HASH\s*[:=]\s*[0-9a-fA-F]{32}\b",
    "telegram_phone_assignment": r"\bTELEGRAM_PHONE_NUMBER\s*[:=]\s*\+?\d[\d\s().-]{6,}\b",
    "private_invite_plus": r"https?://t\.me/\+",
    "private_invite_joinchat": r"https?://t\.me/joinchat/",
    "legacy_private_invite_joinchat": r"telegram\.me/joinchat/",
    "cat_runtime_env": r"\bcat /etc/github-ai-catchbot/runtime\.env\b",
    "source_runtime_env": r"\bsource /etc/github-ai-catchbot/runtime\.env\b",
    "dot_source_runtime_env": r"(?m)^\s*\.\s+/etc/github-ai-catchbot/runtime\.env\b",
    "export_cat": r"\bexport \$\(cat\b",
    "postgresql_url": r"postgresql://",
    "postgresql_psycopg_url": r"postgresql\+psycopg://",
    "redis_url": r"redis://",
    "ipv4_literal": r"\b(?:\d{1,3}\.){3}\d{1,3}\b",
}


def _text() -> str:
    return RUNBOOK.read_text(encoding="utf-8")


def _normalized_text() -> str:
    return " ".join(_text().split())


def _section_body(heading: str) -> str:
    text = _text()
    start = text.index(heading)
    next_heading = text.find("\n## ", start + len(heading))
    if next_heading == -1:
        return text[start:]
    return text[start:next_heading]


def test_runbook_exists_and_has_required_sections_once() -> None:
    assert RUNBOOK.exists()
    text = _text()

    for section in REQUIRED_SECTIONS:
        assert text.count(section) == 1, section


def test_collector_reader_tdlib_mtproto_surface_is_present() -> None:
    text = _text()
    normalized = _normalized_text()

    for phrase in (
        "collector reader account / TDLib / MTProto",
        "Telegram reader account through TDLib/MTProto",
        "separate from the operator personal or main Telegram account",
        "TDLib auth remains later and separately reviewed.",
        "Telegram connection remains unauthorized.",
        "Live collector startup remains unauthorized.",
    ):
        assert phrase in normalized, phrase

    for key in COLLECTOR_KEYS:
        assert key in text, key


def test_notifier_bot_telegram_bot_api_surface_is_present() -> None:
    text = _text()
    normalized = _normalized_text()

    for phrase in (
        "notifier bot / Telegram Bot API",
        "Telegram notification bot created by the operator through BotFather",
        "TELEGRAM_BOT_TOKEN",
        "operator target chat ID / delivery target ID acquisition plan",
        "Notifier target verification remains a later separately reviewed slice.",
        "Notifier transport remains unauthorized.",
    ):
        assert phrase in normalized, phrase


def test_collector_and_notifier_credential_surfaces_are_separated() -> None:
    normalized = _normalized_text()

    for phrase in (
        "collector reader account and notifier bot are separate credentials",
        "separate blast-radius domains",
        "collector uses TDLib/MTProto reader account",
        "notifier uses Telegram Bot API bot token",
        "Bot token does not authorize channel collection.",
        "Reader account credentials do not authorize notifier transport.",
    ):
        assert phrase in normalized, phrase


def test_channel_inventory_fields_are_present() -> None:
    text = _text()

    for field in CHANNEL_FIELDS:
        assert field in text, field


def test_non_authorizations_are_present() -> None:
    text = _text()

    for phrase in NON_AUTHORIZATIONS:
        assert phrase in text, phrase


def test_next_bounded_slice_is_secret_placement_update_package_only() -> None:
    next_section = _section_body("## Next bounded slice")

    assert "dedicated_vps_telegram_runtime_secret_placement_update_package" in next_section
    assert "dedicated_vps_telegram_credentials_acquisition_plan" not in next_section

    forbidden_immediate_next_patterns = (
        r"(?i)next bounded slice\s*[:=-]\s*TDLib auth",
        r"(?i)next bounded slice\s*[:=-]\s*Telegram connection",
        r"(?i)next bounded slice\s*[:=-]\s*live collector",
        r"(?i)next bounded slice\s*[:=-]\s*notifier transport",
        r"(?i)next bounded slice\s*[:=-]\s*production rollout",
    )
    for pattern in forbidden_immediate_next_patterns:
        assert re.search(pattern, next_section) is None, pattern


def test_runbook_rejects_real_looking_secrets_runtime_env_urls_and_ips() -> None:
    text = _text()

    for name, pattern in FORBIDDEN_PATTERNS.items():
        assert re.search(pattern, text) is None, name


def test_runbook_does_not_instruct_secret_pasting_to_external_or_repo_surfaces() -> None:
    text = _text()
    normalized = _normalized_text()

    forbidden_instruction_patterns = {
        "chatgpt": r"(?i)\bpaste\b[^.\n]*(secret|token|password|hash|credential)[^.\n]*ChatGPT",
        "codex": r"(?i)\bpaste\b[^.\n]*(secret|token|password|hash|credential)[^.\n]*Codex",
        "github": r"(?i)\bpaste\b[^.\n]*(secret|token|password|hash|credential)[^.\n]*GitHub",
        "repository_files": r"(?i)\bpaste\b[^.\n]*(secret|token|password|hash|credential)[^.\n]*repository files",
        "markdown_runbooks": r"(?i)\bpaste\b[^.\n]*(secret|token|password|hash|credential)[^.\n]*markdown runbooks",
        "terminal_history": r"(?i)\bpaste\b[^.\n]*(secret|token|password|hash|credential)[^.\n]*terminal history",
        "write_secrets_repo": r"(?i)\bwrite\b[^.\n]*(secret|token|password|hash|credential)[^.\n]*repository files",
        "commit_secret_values": r"(?i)\bcommit\b[^.\n]*(secret|token|password|hash|credential) values",
    }

    for name, pattern in forbidden_instruction_patterns.items():
        assert re.search(pattern, text) is None, name

    assert "store in the operator password manager" in text.lower()
    assert "Record only whether each item has been acquired and where it is stored, not the value." in normalized
    assert "operator commands for updating the dedicated VPS runtime secret boundary without printing values" in normalized
