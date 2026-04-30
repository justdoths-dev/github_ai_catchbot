from __future__ import annotations

from services.notifier_telegram.keyboard_builder import InlineKeyboardBuilder


def test_keyboard_builder_tolerates_missing_urls() -> None:
    assert InlineKeyboardBuilder().build() is None


def test_keyboard_builder_adds_source_and_primary_buttons() -> None:
    keyboard = InlineKeyboardBuilder().build(
        source_message_link="https://t.me/c/1/2",
        primary_url="https://github.com/example/repo",
    )

    assert keyboard == {
        "inline_keyboard": [
            [
                {"text": "Source Telegram", "url": "https://t.me/c/1/2"},
                {"text": "Primary Link", "url": "https://github.com/example/repo"},
            ]
        ]
    }
