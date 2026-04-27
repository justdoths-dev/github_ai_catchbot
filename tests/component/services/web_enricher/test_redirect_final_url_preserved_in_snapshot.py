from __future__ import annotations

from uuid import uuid4

import pytest

from services.web_enricher.article_parser import ArticleParser
from services.web_enricher.service import WebEnricherService
from services.web_enricher.url_discovery import WebUrlDiscovery
from tests.component.services.web_enricher.test_web_snapshot_write_and_outbox_emit import (
    Config,
    FakeFetchClient,
    FakeRepository,
    _artifact,
    _document,
    _job,
)


@pytest.mark.asyncio
async def test_redirect_final_url_preserved_in_snapshot_without_rewriting_registry_canonical_url() -> None:
    artifact_id = uuid4()
    artifact = _artifact(artifact_id)
    repository = FakeRepository(artifact)
    service = WebEnricherService(
        Config(),
        repository=repository,
        fetch_client=FakeFetchClient(
            _document(
                final_url="https://example.com/final",
                body="""
                <html><head><title>Redirected</title>
                <meta name="description" content="Redirected description">
                <link rel="canonical" href="https://example.com/canonical"></head>
                <body><article><p>Redirected page excerpt.</p>
                <a href="https://x.com/dev/status/1881234567890123456">x</a></article></body></html>
                """,
            )
        ),
        article_parser=ArticleParser(excerpt_chars=200, max_outbound_links=10),
        url_discovery=WebUrlDiscovery(),
    )

    await service.handle_job(_job(uuid4(), artifact_id))

    child_draft = repository.web_children[0]["draft"]
    assert child_draft.final_url == "https://example.com/final"
    assert child_draft.canonical_url_candidate == "https://example.com/canonical"
    assert artifact.canonical_url == "https://example.com/start"
