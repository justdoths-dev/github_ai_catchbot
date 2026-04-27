from __future__ import annotations

from uuid import uuid4

from services.x_enricher.url_discovery import XUrlDiscovery


def test_discovers_root_and_direct_referenced_post_urls_only() -> None:
    candidate_group_id = uuid4()
    artifact_id = uuid4()
    projection = {
        "root_post": {
            "entities": {
                "urls": [
                    {
                        "url": "https://t.co/root",
                        "expanded_url": "https://github.com/openai/openai-python?utm_source=x",
                    }
                ]
            }
        },
        "referenced_posts": [
            {
                "raw_post": {
                    "entities": {
                        "urls": [
                            {
                                "url": "https://t.co/ref",
                                "expanded_url": "https://example.com/article?utm_campaign=ignored",
                            }
                        ]
                    },
                    "referenced_tweets": [{"id": "second-hop"}],
                }
            }
        ],
    }

    links_json, observations = XUrlDiscovery().discover(
        candidate_group_id=candidate_group_id,
        parent_artifact_id=artifact_id,
        projection=projection,
        depth_remaining=0,
    )

    assert [item.observed_url for item in observations] == [
        "https://github.com/openai/openai-python?utm_source=x",
        "https://example.com/article?utm_campaign=ignored",
    ]
    assert links_json[0]["classification"] == "github_repo"
    assert links_json[1]["classification"] == "web_article"
    assert observations[0].parent_candidate_group_id == candidate_group_id
    assert observations[0].parent_artifact_id == artifact_id
    assert observations[0].depth_remaining == 0
    assert observations[1].context_path == "referenced_posts[0].entities.urls[0]"
