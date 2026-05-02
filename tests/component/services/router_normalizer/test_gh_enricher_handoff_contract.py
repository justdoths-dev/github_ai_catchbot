from __future__ import annotations

import json
from uuid import uuid4

import pytest

from services.gh_enricher.repositories import GhEnricherRepository
from services.router_normalizer.models import CanonicalArtifact
from services.router_normalizer.repositories import RouterNormalizerRepository


class CapturingSession:
    def __init__(self) -> None:
        self.params = None

    async def execute(self, statement, params=None):
        self.params = params


class SingleRowResult:
    def __init__(self, row):
        self._row = row

    def mappings(self):
        return self

    def first(self):
        return self._row


class SingleRowSession:
    def __init__(self, row) -> None:
        self.row = row

    def in_transaction(self) -> bool:
        return False

    async def execute(self, statement, params=None):
        return SingleRowResult(self.row)


@pytest.mark.asyncio
async def test_router_normalizer_enrich_payload_rehydrates_as_gh_enricher_job() -> None:
    trigger_event_id = uuid4()
    candidate_group_id = uuid4()
    artifact_id = uuid4()
    source_message_id = uuid4()
    artifact = CanonicalArtifact(
        artifact_type="github_repo",
        canonical_id="github:repo:openai/openai-python",
        canonical_url="https://github.com/openai/openai-python",
        normalized_host="github.com",
        artifact_key_json={"owner": "openai", "repo": "openai-python"},
        provider_route="github",
    )
    router_session = CapturingSession()
    router_repository = RouterNormalizerRepository(router_session)

    await router_repository.insert_enrichment_requested_outbox(
        candidate_group_id=candidate_group_id,
        artifact_id=artifact_id,
        artifact=artifact,
        source_message_id=source_message_id,
        source_version_no=7,
    )

    payload = json.loads(router_session.params["payload_json"])
    assert payload == {
        "candidate_group_id": str(candidate_group_id),
        "artifact_id": str(artifact_id),
        "artifact_type": "github_repo",
        "canonical_id": "github:repo:openai/openai-python",
        "provider_route": "github",
        "refresh_mode": "standard",
        "depth_budget": 1,
        "source_message_id": str(source_message_id),
        "source_version_no": 7,
    }
    assert router_session.params["dedupe_key"] == (
        f"artifact:enrich:{candidate_group_id}:github:repo:openai/openai-python:{source_message_id}:7"
    )

    gh_repository = GhEnricherRepository(
        SingleRowSession(
            {
                "event_id": trigger_event_id,
                "event_type": "artifact.enrich.requested.v1",
                "payload_json": payload,
            }
        )
    )

    job = await gh_repository.load_job_by_trigger_event_id(trigger_event_id)

    assert job is not None
    assert job.trigger_event_id == trigger_event_id
    assert job.candidate_group_id == candidate_group_id
    assert job.artifact_id == artifact_id
    assert job.artifact_type == "github_repo"
    assert job.provider_route == "github"
    assert job.refresh_mode == "standard"
    assert job.depth_budget == 1
