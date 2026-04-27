from __future__ import annotations

import pytest

from services.web_enricher.web_fetch_client import UnsupportedContentTypeError, ensure_supported_content_type


def test_content_type_allowlist_accepts_supported_text_types() -> None:
    allowlist = ("text/html", "application/xhtml+xml", "text/plain", "text/markdown")

    assert ensure_supported_content_type("text/html; charset=utf-8", allowlist) == "text/html"
    assert ensure_supported_content_type("application/xhtml+xml", allowlist) == "application/xhtml+xml"
    assert ensure_supported_content_type("text/plain", allowlist) == "text/plain"
    assert ensure_supported_content_type("text/markdown", allowlist) == "text/markdown"


@pytest.mark.parametrize("content_type", ["application/pdf", "image/png", "application/json"])
def test_content_type_allowlist_rejects_unsupported_types(content_type: str) -> None:
    with pytest.raises(UnsupportedContentTypeError):
        ensure_supported_content_type(content_type, ("text/html", "text/plain", "text/markdown"))
