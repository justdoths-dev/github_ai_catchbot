from __future__ import annotations

from services.notifier_telegram.keyboard_builder import InlineKeyboardBuilder


def test_keyboard_builder_tolerates_missing_urls() -> None:
    assert InlineKeyboardBuilder().build() is None


def test_keyboard_builder_adds_source_and_primary_buttons() -> None:
    keyboard = InlineKeyboardBuilder().build(
        source_message_link="https://t.me/c/1/2",
        primary_url="https://github.com/example/repo",
        primary_artifact_type="github_repo",
    )

    assert keyboard == {
        "inline_keyboard": [
            [
                {"text": "원문 Telegram", "url": "https://t.me/c/1/2"},
                {"text": "GitHub 열기", "url": "https://github.com/example/repo"},
            ]
        ]
    }


def test_keyboard_builder_uses_primary_label_for_unknown_hosts() -> None:
    keyboard = InlineKeyboardBuilder().build(primary_url="https://example.com/item")

    assert keyboard == {"inline_keyboard": [[{"text": "Primary 링크", "url": "https://example.com/item"}]]}
