from __future__ import annotations

from services.x_enricher.response_mapper import XResponseMapper
from services.x_enricher.url_discovery import XUrlDiscovery


def test_mapper_preserves_only_direct_referenced_posts_for_depth_one() -> None:
    payload = {
        "data": [
            {
                "id": "1",
                "text": "root",
                "referenced_tweets": [{"type": "quoted", "id": "2"}],
                "edit_history_tweet_ids": ["1"],
            },
            {
                "id": "2",
                "text": "direct ref",
                "referenced_tweets": [{"type": "quoted", "id": "3"}],
                "entities": {"urls": [{"expanded_url": "https://example.com/direct"}]},
            },
            {
                "id": "3",
                "text": "second hop should not become a referenced projection",
                "entities": {"urls": [{"expanded_url": "https://example.com/second-hop"}]},
            },
        ]
    }

    draft = XResponseMapper().map_post_lookup_response(requested_post_id="1", payload=payload)
    links_json, _ = XUrlDiscovery().discover(
        candidate_group_id=__import__("uuid").uuid4(),
        parent_artifact_id=__import__("uuid").uuid4(),
        projection=draft.normalized_projection,
        depth_remaining=0,
    )

    assert draft.referenced_post_ids_json == ["2"]
    assert [item["observed_url"] for item in links_json] == ["https://example.com/direct"]
