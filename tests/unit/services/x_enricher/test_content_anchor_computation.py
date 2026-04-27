from __future__ import annotations

from services.x_enricher.response_mapper import compute_content_anchor
from services.x_enricher.service import extract_post_id_from_canonical_id


def test_content_anchor_uses_latest_edit_history_id() -> None:
    assert compute_content_anchor("1881234567890123456", ["1881234567890123456", "1881234567890999999"]) == (
        "xpost:1881234567890123456:1881234567890999999"
    )


def test_content_anchor_falls_back_to_post_id_when_edit_history_missing() -> None:
    assert compute_content_anchor("1881234567890123456", None) == (
        "xpost:1881234567890123456:1881234567890123456"
    )


def test_canonical_id_parser_accepts_x_post_contract() -> None:
    assert extract_post_id_from_canonical_id("x:post:1881234567890123456") == "1881234567890123456"


def test_canonical_id_parser_accepts_legacy_x_post_for_dev_rows() -> None:
    assert extract_post_id_from_canonical_id("x_post:1881234567890123456") == "1881234567890123456"
    assert extract_post_id_from_canonical_id("github_repo:openai/openai-python") is None
