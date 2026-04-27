from __future__ import annotations

import hashlib
from datetime import datetime, timezone
from uuid import uuid4

import pytest

from services.web_enricher.article_parser import ArticleParser
from services.web_enricher.models import ArtifactEnrichmentJob, ArtifactRecord, FetchedDocument
from services.web_enricher.service import WebEnricherService
from services.web_enricher.url_discovery import WebUrlDiscovery


class _Tx:
    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False


class FakeRepository:
    def __init__(self, artifact: ArtifactRecord) -> None:
        self.artifact = artifact
        self.runs = []
        self.finished_runs = []
        self.snapshots = []
        self.web_children = []
        self.discovered_urls = []
        self.current_updates = []
        self.outbox = []

    def transaction(self):
        return _Tx()

    async def load_artifact(self, artifact_id):
        return self.artifact

    async def load_current_snapshot(self, snapshot_id):
        return None

    async def insert_enrichment_run_if_absent(self, **kwargs):
        self.runs.append(kwargs)
        return uuid4()

    async def mark_enrichment_run_started(self, run_id):
        pass

    async def mark_enrichment_run_finished(self, **kwargs):
        self.finished_runs.append(kwargs)

    async def insert_snapshot(self, **kwargs):
        self.snapshots.append(kwargs)
        return uuid4()

    async def upsert_web_article_child(self, **kwargs):
        self.web_children.append(kwargs)

    async def insert_discovered_url(self, **kwargs):
        self.discovered_urls.append(kwargs)

    async def update_artifact_current_snapshot(self, **kwargs):
        self.current_updates.append(kwargs)

    async def insert_snapshot_updated_outbox(self, **kwargs):
        self.outbox.append(kwargs)


class FakeFetchClient:
    def __init__(self, document: FetchedDocument) -> None:
        self.document = document
        self.fetched_urls: list[str] = []

    async def fetch(self, url: str) -> FetchedDocument:
        self.fetched_urls.append(url)
        return self.document


class Config:
    excerpt_chars = 200


def _document(*, body: str, final_url: str = "https://example.com/final") -> FetchedDocument:
    body_bytes = body.encode("utf-8")
    return FetchedDocument(
        requested_url="https://example.com/start",
        final_url=final_url,
        status_code=200,
        content_type="text/html",
        body_bytes=body_bytes,
        body_text=body,
        response_headers_subset={"content-type": "text/html; charset=utf-8"},
        content_hash=hashlib.sha256(body_bytes).hexdigest(),
        fetch_anomalies=[],
    )


def _artifact(artifact_id):
    return ArtifactRecord(
        artifact_id=artifact_id,
        artifact_type="web_article",
        canonical_id="web_article:abc",
        canonical_url="https://example.com/start",
        normalized_host="example.com",
        artifact_key_json={"url": "https://example.com/start"},
        current_snapshot_id=None,
        current_status=None,
    )


def _job(candidate_group_id, artifact_id):
    return ArtifactEnrichmentJob(
        trigger_event_id=uuid4(),
        event_type="artifact.enrich.requested.v1",
        candidate_group_id=candidate_group_id,
        artifact_id=artifact_id,
        artifact_type="web_article",
        provider_route="web",
        refresh_mode="standard",
        depth_budget=1,
        requested_at=datetime.now(timezone.utc),
    )


@pytest.mark.asyncio
async def test_web_snapshot_write_updates_current_pointer_and_emits_outbox() -> None:
    artifact_id = uuid4()
    candidate_group_id = uuid4()
    repository = FakeRepository(_artifact(artifact_id))
    service = WebEnricherService(
        Config(),
        repository=repository,
        fetch_client=FakeFetchClient(
            _document(
                body="""
                <html><head><title>AI Article</title>
                <meta name="description" content="Useful web evidence"></head>
                <body><main><p>Evidence excerpt for the article.</p>
                <a href="https://github.com/openai/openai-python">repo</a></main></body></html>
                """
            )
        ),
        article_parser=ArticleParser(excerpt_chars=200, max_outbound_links=10),
        url_discovery=WebUrlDiscovery(),
    )

    result = await service.handle_job(_job(candidate_group_id, artifact_id))

    assert result.emitted_snapshot_updated is True
    assert result.status == "ready"
    assert result.content_anchor is not None
    assert repository.snapshots[0]["draft"].snapshot_type == "web_article"
    assert repository.snapshots[0]["draft"].auth_mode == "anonymous_public"
    assert repository.web_children[0]["draft"].title == "AI Article"
    assert repository.current_updates[0]["artifact_id"] == artifact_id
    assert repository.current_updates[0]["status"] == "ready"
    assert repository.outbox[0]["artifact_id"] == artifact_id
    assert repository.outbox[0]["candidate_group_id"] == candidate_group_id
    assert repository.discovered_urls[0]["draft"].observed_url == "https://github.com/openai/openai-python"
