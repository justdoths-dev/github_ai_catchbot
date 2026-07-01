from __future__ import annotations

from urllib.parse import urlparse


class InlineKeyboardBuilder:
    def build(
        self,
        *,
        source_message_link: str | None = None,
        primary_url: str | None = None,
        primary_artifact_type: str | None = None,
    ) -> dict | None:
        rows: list[list[dict[str, str]]] = []
        first_row: list[dict[str, str]] = []
        if source_message_link:
            first_row.append({"text": "원문 Telegram", "url": source_message_link})
        if primary_url:
            first_row.append(
                {"text": _primary_button_label(primary_url, primary_artifact_type), "url": primary_url}
            )
        if first_row:
            rows.append(first_row)
        return {"inline_keyboard": rows} if rows else None


def _primary_button_label(primary_url: str, primary_artifact_type: str | None) -> str:
    if primary_artifact_type == "github_repo":
        return "GitHub 열기"
    host = urlparse(primary_url).netloc.lower()
    if host == "github.com" or host.endswith(".github.com"):
        return "GitHub 열기"
    return "Primary 링크"
