from __future__ import annotations

from uuid import uuid4

from services.router_normalizer.models import SourceMessageSnapshot
from services.router_normalizer.text_surfaces import build_text_surfaces


def test_build_text_surfaces_normalizes_for_scan_and_hash() -> None:
    snapshot = SourceMessageSnapshot(
        source_message_id=uuid4(),
        source_version_no=1,
        text_body="  New\r\nAI\tRepo  ",
        caption_text=None,
        text_surface=None,
        entities_json=None,
        url_surface_json=None,
        raw_message_json={},
    )

    surfaces = build_text_surfaces(snapshot)

    assert surfaces.raw_text_surface == "New\nAI\tRepo"
    assert surfaces.display_surface == "New\nAI Repo"
    assert surfaces.keyword_scan_surface == "new ai repo"
    assert surfaces.hash_surface == "new ai repo"


def test_build_text_surfaces_removes_zero_width_from_normalized_surfaces() -> None:
    snapshot = SourceMessageSnapshot(
        source_message_id=uuid4(),
        source_version_no=1,
        text_body="AI\u200b Repo",
        caption_text=None,
        text_surface=None,
        entities_json=None,
        url_surface_json=None,
        raw_message_json={},
    )

    surfaces = build_text_surfaces(snapshot)

    assert surfaces.raw_text_surface == "AI\u200b Repo"
    assert surfaces.display_surface == "AI Repo"
    assert surfaces.keyword_scan_surface == "ai repo"
    assert surfaces.hash_surface == "ai repo"
