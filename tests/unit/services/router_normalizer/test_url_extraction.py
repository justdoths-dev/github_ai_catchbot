from __future__ import annotations

from uuid import uuid4

from services.router_normalizer.models import SourceMessageSnapshot
from services.router_normalizer.text_surfaces import build_text_surfaces
from services.router_normalizer.url_extraction import extract_urls


def test_extract_urls_prefers_entity_surface_before_regex() -> None:
    snapshot = SourceMessageSnapshot(
        source_message_id=uuid4(),
        source_version_no=1,
        text_body="Check https://example.com/regex and https://github.com/openai/openai-python",
        caption_text=None,
        text_surface="Check https://example.com/regex and https://github.com/openai/openai-python",
        entities_json=None,
        url_surface_json=[
            {
                "observed_url": "https://github.com/openai/openai-python",
                "source_kind": "entity",
                "context": "text_body",
            }
        ],
        raw_message_json={},
    )

    urls = extract_urls(snapshot, build_text_surfaces(snapshot))

    assert urls[0].observed_url == "https://github.com/openai/openai-python"
    assert urls[0].source_kind == "entity"
    assert [url.observed_url for url in urls] == [
        "https://github.com/openai/openai-python",
        "https://example.com/regex",
    ]


def test_extract_urls_reads_entities_when_url_surface_is_missing() -> None:
    snapshot = SourceMessageSnapshot(
        source_message_id=uuid4(),
        source_version_no=1,
        text_body="Repo",
        caption_text=None,
        text_surface="Repo",
        entities_json=[
            {
                "type": {
                    "@type": "textEntityTypeTextUrl",
                    "url": "https://github.com/openai/openai-python",
                },
            }
        ],
        url_surface_json=None,
        raw_message_json={},
    )

    urls = extract_urls(snapshot, build_text_surfaces(snapshot))

    assert len(urls) == 1
    assert urls[0].observed_url == "https://github.com/openai/openai-python"
    assert urls[0].source_kind == "entity"
    assert urls[0].context_path == "entities_json[0]"
