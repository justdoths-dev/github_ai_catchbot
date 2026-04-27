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
async def test_partial_ready_on_metadata_sparse_page() -> None:
    artifact_id = uuid4()
    repository = FakeRepository(_artifact(artifact_id))
    service = WebEnricherService(
        Config(),
        repository=repository,
        fetch_client=FakeFetchClient(_document(body="<html><body><main><p>Only an excerpt is available.</p></main></body></html>")),
        article_parser=ArticleParser(excerpt_chars=200, max_outbound_links=10),
        url_discovery=WebUrlDiscovery(),
    )

    result = await service.handle_job(_job(uuid4(), artifact_id))

    assert result.status == "partial_ready"
    draft = repository.snapshots[0]["draft"]
    assert draft.status == "partial_ready"
    assert "web_metadata_sparse" in draft.evidence_limitations
