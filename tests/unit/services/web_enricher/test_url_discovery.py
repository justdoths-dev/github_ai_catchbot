from __future__ import annotations

from uuid import uuid4

from services.web_enricher.url_discovery import WebUrlDiscovery


def test_url_discovery_reuses_router_normalizer_canonicalizer() -> None:
    candidate_group_id = uuid4()
    artifact_id = uuid4()

    links_json, drafts = WebUrlDiscovery().discover(
        candidate_group_id=candidate_group_id,
        parent_artifact_id=artifact_id,
        outbound_links=[
            "https://github.com/OpenAI/openai-python?utm_source=newsletter",
            "https://x.com/dev/status/1881234567890123456",
            "https://example.com/article?utm_campaign=feed",
        ],
        depth_remaining=0,
    )

    assert [item["classification"] for item in links_json] == ["github_repo", "x_post", "web_article"]
    assert links_json[0]["canonical_url"] == "https://github.com/openai/openai-python"
    assert links_json[1]["canonical_id"] == "x:post:1881234567890123456"
    assert links_json[2]["canonical_url"] == "https://example.com/article"
    assert drafts[0].parent_candidate_group_id == candidate_group_id
    assert drafts[0].parent_artifact_id == artifact_id
    assert drafts[0].discovery_reason == "web_article_embedded_link"
