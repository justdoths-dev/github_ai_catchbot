from __future__ import annotations


class InlineKeyboardBuilder:
    def build(
        self,
        *,
        source_message_link: str | None = None,
        primary_url: str | None = None,
    ) -> dict | None:
        rows: list[list[dict[str, str]]] = []
        first_row: list[dict[str, str]] = []
        if source_message_link:
            first_row.append({"text": "Source Telegram", "url": source_message_link})
        if primary_url:
            first_row.append({"text": "Primary Link", "url": primary_url})
        if first_row:
            rows.append(first_row)
        return {"inline_keyboard": rows} if rows else None
