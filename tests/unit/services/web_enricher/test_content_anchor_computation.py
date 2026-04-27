from __future__ import annotations

from services.web_enricher.service import compute_content_anchor


def test_content_anchor_is_stable_for_same_final_url_and_content_hash() -> None:
    first = compute_content_anchor(final_url="https://example.com/a", content_hash="abc")
    second = compute_content_anchor(final_url="https://example.com/a", content_hash="abc")

    assert first == second
    assert first.startswith("web:")


def test_content_anchor_changes_when_final_url_or_content_hash_changes() -> None:
    base = compute_content_anchor(final_url="https://example.com/a", content_hash="abc")

    assert compute_content_anchor(final_url="https://example.com/b", content_hash="abc") != base
    assert compute_content_anchor(final_url="https://example.com/a", content_hash="def") != base
