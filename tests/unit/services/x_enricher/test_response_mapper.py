from __future__ import annotations

from services.x_enricher.response_mapper import XResponseMapper


def test_maps_root_post_with_includes_media_and_partial_errors() -> None:
    payload = {
        "data": [
            {
                "id": "1881234567890123456",
                "text": "Check this SDK https://t.co/a",
                "author_id": "42",
                "conversation_id": "1881234567890123456",
                "edit_history_tweet_ids": ["1881234567890123456", "1881234567890999999"],
                "referenced_tweets": [{"type": "quoted", "id": "1880000000000000000"}],
                "attachments": {"media_keys": ["m1"]},
                "public_metrics": {"like_count": 12},
            },
            {
                "id": "1880000000000000000",
                "text": "Referenced context",
                "author_id": "43",
            },
        ],
        "includes": {
            "users": [{"id": "42", "username": "dev", "name": "Dev", "verified": True}],
            "media": [{"media_key": "m1", "type": "photo", "url": "https://media.example/p.jpg"}],
        },
        "errors": [{"title": "Partial include unavailable"}],
    }

    draft = XResponseMapper().map_post_lookup_response(
        requested_post_id="1881234567890123456",
        payload=payload,
    )

    assert draft.status == "partial_ready"
    assert draft.content_anchor == "xpost:1881234567890123456:1881234567890999999"
    assert draft.author_summary_json["username"] == "dev"
    assert draft.referenced_post_ids_json == ["1880000000000000000"]
    assert draft.media_summary_json[0]["media_key"] == "m1"
    assert draft.fetch_anomalies == ["partial_errors_present"]


def test_missing_root_post_maps_failed_permanent_without_reference_crawl() -> None:
    draft = XResponseMapper().map_post_lookup_response(
        requested_post_id="1881234567890123456",
        payload={"data": [], "errors": [{"title": "Not Found"}]},
    )

    assert draft.status == "failed_permanent"
    assert draft.content_anchor == "xpost:1881234567890123456:1881234567890123456"
    assert draft.evidence_limitations == ["x_root_post_unavailable"]
